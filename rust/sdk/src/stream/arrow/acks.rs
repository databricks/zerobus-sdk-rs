//! ACK processing and server-requested stream rotation.
//!
//! ACK watermarks are monotonic and bounded by records submitted on the active
//! connection. Rotation preserves permanent peer/protocol errors while draining.

use std::mem::replace;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::{watch, Mutex};
use tokio::time::{sleep_until, timeout, Duration, Instant};
use tracing::{debug, error, info, warn};

use super::batch::PendingBatch;
use super::connection::{FlightResponseStream, RequestBodyControl};
#[cfg(feature = "test-hooks")]
use super::AckAppliedGate;
use super::{BatchSender, ZerobusArrowStream};
use crate::arrow_configuration::ArrowStreamConfigurationOptions;
use crate::arrow_metadata::FlightAckMetadata;
use crate::errors::ZerobusError;
use crate::offset_generator::OffsetId;
use crate::ZerobusResult;

const ROTATION_DRAIN_TIMEOUT_MS: u64 = 500;

/// Owns the shared stream state and configuration used while processing ACKs.
pub(super) struct AckProcessor {
    is_closed: Arc<AtomicBool>,
    last_ack_tx: watch::Sender<Option<OffsetId>>,
    pending_batches: Arc<Mutex<Vec<PendingBatch>>>,
    server_error_tx: watch::Sender<Option<ZerobusError>>,
    submitted_records: Arc<AtomicU64>,
    last_acked_records: Arc<AtomicU64>,
    is_paused: Arc<AtomicBool>,
    ingest_mutex: Arc<Mutex<()>>,
    batch_tx: BatchSender,
    options: ArrowStreamConfigurationOptions,
    #[cfg(feature = "test-hooks")]
    ack_applied_gate: AckAppliedGate,
}

/// State captured when rotation stops waiting for acknowledgments and begins transport
/// cleanup. The response may already have ended, but the request must still reach EOF.
struct DrainState {
    /// Hard cutoff shared by request EOF observation and response draining.
    deadline: Instant,
    /// Whether the response ended before the request entered the drain helper.
    response_finished: bool,
    /// Peer or protocol error to preserve while the remaining transport settles.
    terminal_error: Option<ZerobusError>,
}

/// Server-initiated rotation has only three phases: normal traffic, waiting for the
/// pre-signal acknowledgment snapshot, and transport drain.
enum RotationState {
    Open,
    WaitingForAcks {
        target_records: u64,
        deadlines: RotationDeadlines,
    },
    Draining(DrainState),
}

/// Deadlines for the ACK-wait and transport-drain phases of rotation.
#[derive(Clone, Copy)]
struct RotationDeadlines {
    ack: Instant,
    drain: Instant,
}

/// Borrowed view of the active connection's acknowledgment bookkeeping.
///
/// Keeping these values together makes every ACK path use the same connection-local
/// submitted watermark, monotonic durable watermark, and pending-batch collection.
struct AckProgress<'a> {
    submitted_records: &'a AtomicU64,
    last_acked_records: &'a AtomicU64,
    pending_batches: &'a Mutex<Vec<PendingBatch>>,
    last_ack_tx: &'a watch::Sender<Option<OffsetId>>,
    #[cfg(feature = "test-hooks")]
    ack_applied_gate: &'a AckAppliedGate,
}

impl AckProgress<'_> {
    /// Validates an ACK against the active connection, advances the monotonic durable
    /// watermark, removes fully acknowledged batches, and wakes completed offset waiters.
    async fn apply(&self, ack: &FlightAckMetadata) -> ZerobusResult<()> {
        let acked_records = ack.ack_up_to_records;
        // `ack_up_to_records` is the durability boundary. Derive completed SDK offsets
        // from local pending ranges so an inconsistent `ack_up_to_offset` cannot advance
        // a waiter; keep the server-provided offset only for diagnostics.
        let (effective_acked_records, max_acked_offset) = {
            // Ingest publishes submitted_records and commits to the active sender while
            // holding this same lock. Validation therefore cannot observe a submitted
            // watermark before its handoff, or a handoff before its watermark.
            let mut pending = self.pending_batches.lock().await;
            let submitted_records = self.submitted_records.load(Ordering::Acquire);
            if acked_records > submitted_records {
                return Err(ZerobusError::InvalidStateError(format!(
                    "Acknowledgement claims {acked_records} records, but only {submitted_records} records were submitted"
                )));
            }

            let previous_acked_records = self
                .last_acked_records
                .fetch_max(acked_records, Ordering::AcqRel);
            let effective_acked_records = previous_acked_records.max(acked_records);
            let mut max_acked_offset: Option<OffsetId> = None;
            pending.retain(|pending_batch| {
                if pending_batch.is_fully_acknowledged(effective_acked_records) {
                    let offset_id = pending_batch.offset_id();
                    max_acked_offset =
                        Some(max_acked_offset.map_or(offset_id, |offset| offset.max(offset_id)));
                    false
                } else {
                    true
                }
            });
            (effective_acked_records, max_acked_offset)
        };

        debug!(target: super::LOG_TARGET,
            ack_up_to_offset = ack.ack_up_to_offset,
            ack_up_to_records = acked_records,
            effective_acked_records,
            "Received acknowledgment"
        );

        #[cfg(feature = "test-hooks")]
        if acked_records > 0 {
            if let Some(notify) = self.ack_applied_gate.lock().await.as_ref() {
                notify.notify_one();
            }
        }

        if let Some(offset) = max_acked_offset {
            let _ = self.last_ack_tx.send(Some(offset));
        }

        Ok(())
    }
}

/// Borrowed handles needed to stop sending and half-close the active Flight request.
///
/// The pause gate and sender are changed under `ingest_mutex`, ensuring a concurrent ingest
/// either reaches the old connection first or remains buffered for replay.
struct RequestControl<'a> {
    request_body: &'a RequestBodyControl,
    ingest_mutex: &'a Mutex<()>,
    is_paused: &'a AtomicBool,
    batch_tx: &'a BatchSender,
}

/// Atomically pauses ingest and detaches the sender, under `ingest_mutex`.
///
/// Holding `ingest_mutex` across both stores makes the pause + sender-detach a
/// single step relative to `ingest_batch`'s critical section: a concurrent ingest
/// either finishes first, or observes `is_paused == true` and buffers — it never
/// reads a detached (`None`) sender while `is_paused` is still false.
pub(super) async fn pause_and_detach_sender(
    ingest_mutex: &Mutex<()>,
    is_paused: &AtomicBool,
    batch_tx: &BatchSender,
) {
    let _guard = ingest_mutex.lock().await;
    is_paused.store(true, Ordering::Relaxed);
    let mut tx = batch_tx.lock().await;
    *tx = None;
}

/// Pauses new sends and snapshots exactly the records submitted to this connection.
/// Ingests admitted after this critical section remain pending for replay and cannot
/// extend the active connection's acknowledgment target.
async fn pause_and_snapshot_submitted(
    ingest_mutex: &Arc<Mutex<()>>,
    is_paused: &Arc<AtomicBool>,
    submitted_records: &AtomicU64,
) -> u64 {
    let _guard = ingest_mutex.lock().await;
    is_paused.store(true, Ordering::Relaxed);
    submitted_records.load(Ordering::Acquire)
}

impl RequestControl<'_> {
    /// Atomically stops new sends, detaches queued work, then asks tonic to poll the request
    /// body to EOF. [`RequestBodyControl::wait_for_eof`] observes completion separately.
    async fn half_close(&self) {
        pause_and_detach_sender(self.ingest_mutex, self.is_paused, self.batch_tx).await;
        self.request_body.shutdown();
    }
}

impl AckProcessor {
    pub(super) fn new(stream: &ZerobusArrowStream) -> Self {
        Self {
            is_closed: Arc::clone(&stream.is_closed),
            last_ack_tx: stream.last_ack_tx.clone(),
            pending_batches: Arc::clone(&stream.pending_batches),
            server_error_tx: stream.server_error_tx.clone(),
            submitted_records: Arc::clone(&stream.submitted_records),
            last_acked_records: Arc::clone(&stream.last_acked_records),
            is_paused: Arc::clone(&stream.is_paused),
            ingest_mutex: Arc::clone(&stream.ingest_mutex),
            batch_tx: Arc::clone(&stream.batch_tx),
            options: stream.options.clone(),
            #[cfg(feature = "test-hooks")]
            ack_applied_gate: Arc::clone(&stream.ack_applied_gate),
        }
    }

    #[cfg(test)]
    pub(super) fn for_test(
        pending_batches: Arc<Mutex<Vec<PendingBatch>>>,
        submitted_records: Arc<AtomicU64>,
        last_acked_records: Arc<AtomicU64>,
        is_paused: bool,
    ) -> (Self, RequestBodyControl, watch::Receiver<Option<OffsetId>>) {
        let (last_ack_tx, last_ack_rx) = watch::channel(None);
        let (server_error_tx, _server_error_rx) = watch::channel(None);
        let processor = Self {
            is_closed: Arc::new(AtomicBool::new(false)),
            last_ack_tx,
            pending_batches,
            server_error_tx,
            submitted_records,
            last_acked_records,
            is_paused: Arc::new(AtomicBool::new(is_paused)),
            ingest_mutex: Arc::new(Mutex::new(())),
            batch_tx: Arc::new(Mutex::new(None)),
            options: ArrowStreamConfigurationOptions {
                server_lack_of_ack_timeout_ms: Duration::from_secs(60).as_millis() as u64,
                ..ArrowStreamConfigurationOptions::default()
            },
            #[cfg(feature = "test-hooks")]
            ack_applied_gate: Arc::new(Mutex::new(None)),
        };
        (
            processor,
            RequestBodyControl::completed_for_test(),
            last_ack_rx,
        )
    }

    fn rotation_error() -> ZerobusError {
        ZerobusError::StreamClosedError(tonic::Status::unavailable(
            "Server requested graceful stream rotation",
        ))
    }

    /// Records the latest rotation error without allowing a retryable transport status
    /// to hide an earlier permanent peer or protocol failure.
    fn update_rotation_error(current: &mut Option<ZerobusError>, candidate: ZerobusError) {
        let preserves_permanent = current
            .as_ref()
            .map(|error| !error.is_retryable() && candidate.is_retryable())
            .unwrap_or(false);
        if !preserves_permanent {
            *current = Some(candidate);
        }
    }

    /// Splits the advertised grace into an ACK-wait portion and a bounded transport
    /// drain. Very short grace periods skip ACK waiting and receive a best-effort local
    /// drain window; the peer may already have closed in that case.
    fn rotation_deadlines(
        server_duration_ms: u64,
        configured_ack_wait_ms: Option<u64>,
    ) -> RotationDeadlines {
        let now = Instant::now();
        let drain_budget = Duration::from_millis(ROTATION_DRAIN_TIMEOUT_MS);
        let server_grace = Duration::from_millis(server_duration_ms);
        let bounded_fallback = now + Duration::from_secs(365 * 24 * 60 * 60);
        let server_deadline = now.checked_add(server_grace).unwrap_or(bounded_fallback);
        let available_ack_wait = server_grace.saturating_sub(drain_budget);
        let configured_ack_wait = configured_ack_wait_ms
            .map(Duration::from_millis)
            .unwrap_or(available_ack_wait);
        let ack_wait = available_ack_wait.min(configured_ack_wait);
        let ack_deadline = now.checked_add(ack_wait).unwrap_or(server_deadline);
        let drain_deadline = if server_grace < drain_budget {
            now + drain_budget
        } else {
            server_deadline
        };
        RotationDeadlines {
            ack: ack_deadline,
            drain: drain_deadline,
        }
    }

    fn bounded_rotation_drain_deadline(close_deadline: Instant) -> Instant {
        close_deadline.min(Instant::now() + Duration::from_millis(ROTATION_DRAIN_TIMEOUT_MS))
    }

    /// Half-closes the active request and drains the response under the rotation
    /// deadline. Late acknowledgments are applied while tonic settles request EOF.
    /// A real peer status or invalid acknowledgment outranks the synthetic retryable
    /// rotation result.
    async fn close_request_and_drain_response(
        response_stream: &mut FlightResponseStream,
        request: RequestControl<'_>,
        acknowledgments: AckProgress<'_>,
        state: DrainState,
    ) -> ZerobusResult<()> {
        let DrainState {
            deadline,
            mut response_finished,
            mut terminal_error,
        } = state;
        request.half_close().await;

        let mut request_eof = Box::pin(request.request_body.wait_for_eof());
        let mut request_finished = false;

        loop {
            if request_finished && response_finished {
                return Err(terminal_error.unwrap_or_else(Self::rotation_error));
            }
            if Instant::now() >= deadline {
                return Err(terminal_error.unwrap_or_else(Self::rotation_error));
            }

            tokio::select! {
                biased;
                _ = sleep_until(deadline) => {
                    return Err(terminal_error.unwrap_or_else(Self::rotation_error));
                }
                _ = &mut request_eof, if !request_finished => {
                    request_finished = true;
                }
                response = response_stream.next(), if !response_finished => {
                    match response {
                        Some(Ok(put_result)) => {
                            match FlightAckMetadata::from_bytes(&put_result.app_metadata) {
                                Ok(ack) if ack.ack_up_to_records > 0 => {
                                    if let Err(error) = acknowledgments.apply(&ack).await {
                                        Self::update_rotation_error(&mut terminal_error, error);
                                    }
                                }
                                Ok(_) => {}
                                Err(error) => {
                                    warn!(target: super::LOG_TARGET, "Failed to parse ack metadata while draining: {error}");
                                }
                            }
                        }
                        Some(Err(error)) => {
                            let status: tonic::Status = error.into();
                            Self::update_rotation_error(
                                &mut terminal_error,
                                ZerobusError::StreamClosedError(status),
                            );
                            response_finished = true;
                        }
                        None => response_finished = true,
                    }
                }
            }
        }
    }

    /// Processes acknowledgments and the single server-initiated rotation path.
    ///
    /// Rotation pauses sends and snapshots submitted records, waits only for that
    /// connection-local target, then half-closes the request and drains late responses
    /// before returning a retryable result to the supervisor.
    pub(super) async fn process(
        &self,
        mut response_stream: FlightResponseStream,
        request_body: RequestBodyControl,
    ) -> ZerobusResult<()> {
        let ack_timeout = Duration::from_millis(self.options.server_lack_of_ack_timeout_ms);
        let mut rotation = RotationState::Open;
        let request = RequestControl {
            request_body: &request_body,
            ingest_mutex: self.ingest_mutex.as_ref(),
            is_paused: self.is_paused.as_ref(),
            batch_tx: &self.batch_tx,
        };
        let acknowledgments = AckProgress {
            submitted_records: self.submitted_records.as_ref(),
            last_acked_records: self.last_acked_records.as_ref(),
            pending_batches: self.pending_batches.as_ref(),
            last_ack_tx: &self.last_ack_tx,
            #[cfg(feature = "test-hooks")]
            ack_applied_gate: &self.ack_applied_gate,
        };

        loop {
            if self.is_closed.load(Ordering::Relaxed) {
                debug!(target: super::LOG_TARGET, "Stream closed, stopping ack processor");
                return Ok(());
            }

            if let RotationState::WaitingForAcks {
                target_records,
                deadlines,
            } = &rotation
            {
                if self.last_acked_records.load(Ordering::Acquire) >= *target_records
                    || Instant::now() >= deadlines.ack
                {
                    rotation = RotationState::Draining(DrainState {
                        deadline: Self::bounded_rotation_drain_deadline(deadlines.drain),
                        response_finished: false,
                        terminal_error: None,
                    });
                    continue;
                }
            }

            if matches!(rotation, RotationState::Draining(_)) {
                let RotationState::Draining(state) = replace(&mut rotation, RotationState::Open)
                else {
                    unreachable!()
                };
                return Self::close_request_and_drain_response(
                    &mut response_stream,
                    request,
                    acknowledgments,
                    state,
                )
                .await;
            }

            let response = match &rotation {
                RotationState::Open => match timeout(ack_timeout, response_stream.next()).await {
                    Ok(response) => response,
                    Err(_) => {
                        let pending = self.pending_batches.lock().await;
                        if !pending.is_empty() {
                            error!(target: super::LOG_TARGET,
                                pending_count = pending.len(),
                                "Server ack timeout with pending batches"
                            );
                            return Err(ZerobusError::StreamClosedError(
                                tonic::Status::deadline_exceeded("Server ack timeout"),
                            ));
                        }
                        continue;
                    }
                },
                RotationState::WaitingForAcks { deadlines, .. } => {
                    tokio::select! {
                        _ = sleep_until(deadlines.ack) => continue,
                        response = response_stream.next() => response,
                    }
                }
                RotationState::Draining(_) => unreachable!(),
            };

            match response {
                Some(Ok(put_result)) => {
                    let ack = match FlightAckMetadata::from_bytes(&put_result.app_metadata) {
                        Ok(ack) => ack,
                        Err(error) => {
                            warn!(target: super::LOG_TARGET, "Failed to parse ack metadata: {error}");
                            continue;
                        }
                    };

                    if ack.is_close_signal() && matches!(&rotation, RotationState::Open) {
                        let server_duration_ms = ack.close_stream_duration_ms.unwrap_or(0);
                        let deadlines = Self::rotation_deadlines(
                            server_duration_ms,
                            self.options.stream_paused_max_wait_time_ms,
                        );
                        let target_records = pause_and_snapshot_submitted(
                            &self.ingest_mutex,
                            &self.is_paused,
                            &self.submitted_records,
                        )
                        .await;
                        rotation = RotationState::WaitingForAcks {
                            target_records,
                            deadlines,
                        };
                        info!(target: super::LOG_TARGET,
                            server_duration_ms,
                            target_records, "Server requested graceful stream rotation"
                        );
                    }

                    if ack.ack_up_to_records > 0 {
                        let ack_result = acknowledgments.apply(&ack).await;
                        if let Err(error) = ack_result {
                            if let RotationState::WaitingForAcks { deadlines, .. } = rotation {
                                rotation = RotationState::Draining(DrainState {
                                    deadline: Self::bounded_rotation_drain_deadline(
                                        deadlines.drain,
                                    ),
                                    response_finished: false,
                                    terminal_error: Some(error),
                                });
                                continue;
                            }
                            return Err(error);
                        }
                    }
                }
                Some(Err(error)) => {
                    let status: tonic::Status = error.into();
                    let error = ZerobusError::StreamClosedError(status);
                    if let RotationState::WaitingForAcks { deadlines, .. } = rotation {
                        rotation = RotationState::Draining(DrainState {
                            deadline: Self::bounded_rotation_drain_deadline(deadlines.drain),
                            response_finished: true,
                            terminal_error: Some(error),
                        });
                        continue;
                    }
                    let _ = self.server_error_tx.send(Some(error.clone()));
                    return Err(error);
                }
                None => {
                    if let RotationState::WaitingForAcks { deadlines, .. } = rotation {
                        rotation = RotationState::Draining(DrainState {
                            deadline: Self::bounded_rotation_drain_deadline(deadlines.drain),
                            response_finished: true,
                            terminal_error: None,
                        });
                        continue;
                    }
                    return Err(ZerobusError::StreamClosedError(tonic::Status::unknown(
                        "Server closed the stream",
                    )));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    use arrow_array::Int32Array;
    use arrow_flight::PutResult;
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use futures::stream::iter;
    use tokio::sync::{watch, Mutex, Semaphore};

    use super::super::RecordBatch;
    use super::{
        AckProcessor, FlightAckMetadata, OffsetId, PendingBatch, RequestBodyControl, ZerobusError,
    };

    fn one_col_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]))
    }

    fn batch_with_rows(schema: &Arc<ArrowSchema>, n: i32) -> RecordBatch {
        let ids: Vec<i32> = (0..n).collect();
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(ids))]).unwrap()
    }

    fn pending_batch(
        sem: &Arc<Semaphore>,
        batch: RecordBatch,
        offset_id: OffsetId,
        start_record: u64,
        end_record: u64,
    ) -> PendingBatch {
        PendingBatch::new(
            batch,
            offset_id,
            start_record,
            end_record,
            Arc::clone(sem).try_acquire_owned().unwrap(),
        )
    }

    fn ack_processor(
        pending_batches: Arc<Mutex<Vec<PendingBatch>>>,
        submitted_records: Arc<AtomicU64>,
        last_acked_records: Arc<AtomicU64>,
        is_paused: bool,
    ) -> (
        AckProcessor,
        RequestBodyControl,
        watch::Receiver<Option<OffsetId>>,
    ) {
        AckProcessor::for_test(
            pending_batches,
            submitted_records,
            last_acked_records,
            is_paused,
        )
    }

    /// An acknowledgement beyond the connection-local submitted-record count is a protocol
    /// violation and must not make unsent records appear durable.
    #[tokio::test]
    async fn forward_ack_is_rejected_without_mutating_state() {
        let schema = one_col_schema();
        let sem = Arc::new(Semaphore::new(1));
        let pending_batches = Arc::new(Mutex::new(vec![pending_batch(
            &sem,
            batch_with_rows(&schema, 10),
            0,
            0,
            10,
        )]));
        let response_stream = iter([Ok(PutResult {
            app_metadata: serde_json::to_vec(&FlightAckMetadata {
                ack_up_to_offset: 0,
                ack_up_to_records: 11,
                close_stream_duration_ms: None,
            })
            .unwrap()
            .into(),
        })]);
        let submitted_records = Arc::new(AtomicU64::new(10));
        let last_acked_records = Arc::new(AtomicU64::new(0));
        let (processor, request_body, last_ack_rx) = ack_processor(
            Arc::clone(&pending_batches),
            Arc::clone(&submitted_records),
            Arc::clone(&last_acked_records),
            false,
        );

        let error = processor
            .process(Box::pin(response_stream), request_body)
            .await
            .expect_err("a forward acknowledgement must be rejected");

        assert!(
            !error.is_retryable(),
            "a protocol violation must be terminal"
        );
        match error {
            ZerobusError::InvalidStateError(message) => {
                assert!(message.contains("11 records"));
                assert!(message.contains("10 records were submitted"));
            }
            other => panic!("expected an invalid-state error, got {other:?}"),
        }
        assert_eq!(submitted_records.load(Ordering::Acquire), 10);
        assert_eq!(last_acked_records.load(Ordering::Acquire), 0);
        assert_eq!(*last_ack_rx.borrow(), None);
        let pending = pending_batches.lock().await;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].record_range(), (0, 10));
    }

    /// Assigned ranges buffered while paused are not valid acknowledgement targets until replay
    /// submits them to the active connection.
    #[tokio::test]
    async fn forward_ack_through_paused_batch_is_rejected() {
        let schema = one_col_schema();
        let sem = Arc::new(Semaphore::new(2));
        let pending_batches = Arc::new(Mutex::new(vec![
            pending_batch(&sem, batch_with_rows(&schema, 10), 0, 0, 10),
            pending_batch(&sem, batch_with_rows(&schema, 10), 1, 10, 20),
        ]));
        let response_stream = iter([Ok(PutResult {
            app_metadata: serde_json::to_vec(&FlightAckMetadata {
                ack_up_to_offset: 1,
                ack_up_to_records: 20,
                close_stream_duration_ms: None,
            })
            .unwrap()
            .into(),
        })]);
        let submitted_records = Arc::new(AtomicU64::new(10));
        let last_acked_records = Arc::new(AtomicU64::new(0));
        let (processor, request_body, last_ack_rx) = ack_processor(
            Arc::clone(&pending_batches),
            Arc::clone(&submitted_records),
            Arc::clone(&last_acked_records),
            true,
        );

        let error = processor
            .process(Box::pin(response_stream), request_body)
            .await
            .expect_err("an acknowledgement through a paused, unsent range must be rejected");

        assert!(
            !error.is_retryable(),
            "a protocol violation must be terminal"
        );
        match error {
            ZerobusError::InvalidStateError(message) => {
                assert!(message.contains("20 records"));
                assert!(message.contains("10 records were submitted"));
            }
            other => panic!("expected an invalid-state error, got {other:?}"),
        }
        assert_eq!(last_acked_records.load(Ordering::Acquire), 0);
        assert_eq!(*last_ack_rx.borrow(), None);
        let pending = pending_batches.lock().await;
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].record_range(), (0, 10));
        assert_eq!(pending[1].record_range(), (10, 20));
    }

    /// A delayed or duplicate acknowledgement must never move the cumulative watermark backward,
    /// otherwise recovery can resend a prefix that the server already made durable.
    #[tokio::test]
    async fn regressive_ack_does_not_move_watermark_backward() {
        let schema = one_col_schema();
        let sem = Arc::new(Semaphore::new(1));
        let pending_batches = Arc::new(Mutex::new(vec![pending_batch(
            &sem,
            batch_with_rows(&schema, 10),
            0,
            0,
            10,
        )]));
        let response_stream = iter([5, 0].map(|acked_records| {
            Ok(PutResult {
                app_metadata: serde_json::to_vec(&FlightAckMetadata {
                    ack_up_to_offset: 0,
                    ack_up_to_records: acked_records,
                    close_stream_duration_ms: None,
                })
                .unwrap()
                .into(),
            })
        }));
        let last_acked_records = Arc::new(AtomicU64::new(0));
        let (processor, request_body, _last_ack_rx) = ack_processor(
            Arc::clone(&pending_batches),
            Arc::new(AtomicU64::new(10)),
            Arc::clone(&last_acked_records),
            false,
        );

        let _stream_closed = processor
            .process(Box::pin(response_stream), request_body)
            .await;

        assert_eq!(last_acked_records.load(Ordering::Acquire), 5);
    }
}
