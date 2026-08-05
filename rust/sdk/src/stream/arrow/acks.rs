//! ACK processing and server-requested stream rotation.
//!
//! ACK watermarks are monotonic and bounded by records submitted on the active
//! connection. Rotation preserves permanent peer/protocol errors while draining.

use std::mem::replace;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use arrow_flight::error::FlightError;
use arrow_flight::PutResult;
use futures::StreamExt;
use tokio::sync::{watch, Mutex, Notify};
use tokio::time::{sleep_until, Duration, Instant};
use tracing::{debug, error, info, warn};

use super::batch::{
    oldest_pending_ack_deadline, PendingAckDeadline, PendingBatch, PendingBatchIdentity,
};
use super::connection::{FlightResponseStream, RequestBodyControl};
use super::metadata::FlightAckMetadata;
#[cfg(feature = "test-hooks")]
use super::AckAppliedGate;
use super::{ArrowStreamConfigurationOptions, BatchSender, ZerobusArrowStream};
use crate::errors::ZerobusError;
use crate::offset_generator::OffsetId;
use crate::ZerobusResult;

const ROTATION_DRAIN_TIMEOUT_MS: u64 = 500;
const MAX_SERVER_ROTATION_GRACE: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// Owns the shared stream state and configuration used while processing ACKs.
pub(super) struct AckProcessor {
    is_closed: Arc<AtomicBool>,
    last_ack_tx: watch::Sender<Option<OffsetId>>,
    pending_batches: Arc<Mutex<Vec<PendingBatch>>>,
    pending_notify: Arc<Notify>,
    request_send_failure: Arc<RequestSendFailure>,
    server_error_tx: watch::Sender<Option<ZerobusError>>,
    submitted_records: Arc<AtomicU64>,
    last_acked_records: Arc<AtomicU64>,
    is_paused: Arc<AtomicBool>,
    ingest_mutex: Arc<Mutex<()>>,
    batch_tx: BatchSender,
    options: ArrowStreamConfigurationOptions,
    #[cfg(feature = "test-hooks")]
    ack_applied_gate: AckAppliedGate,
    #[cfg(feature = "test-hooks")]
    ack_idle_gate: super::AckIdleGate,
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

/// Event that wakes the acknowledgment processor before transport drain begins.
enum AckEvent {
    Response(Option<Result<PutResult, FlightError>>),
    PendingBatchAvailable,
    RequestSendFailed,
    AckDeadline(PendingAckDeadline),
}

/// Coalesces request-sender failures until the supervisor has paused the failed
/// connection. The notification wakes an ACK processor that has no deadline armed.
#[derive(Default)]
pub(super) struct RequestSendFailure {
    pending: AtomicBool,
    notify: Notify,
}

impl RequestSendFailure {
    pub(super) fn report(&self) {
        if !self.pending.swap(true, Ordering::AcqRel) {
            self.notify.notify_one();
        }
    }

    fn take(&self) -> bool {
        self.pending.swap(false, Ordering::AcqRel)
    }

    fn clear(&self) {
        self.pending.store(false, Ordering::Release);
    }
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
            pending_notify: Arc::clone(&stream.pending_notify),
            request_send_failure: Arc::clone(&stream.request_send_failure),
            server_error_tx: stream.server_error_tx.clone(),
            submitted_records: Arc::clone(&stream.submitted_records),
            last_acked_records: Arc::clone(&stream.last_acked_records),
            is_paused: Arc::clone(&stream.is_paused),
            ingest_mutex: Arc::clone(&stream.ingest_mutex),
            batch_tx: Arc::clone(&stream.batch_tx),
            options: stream.options.clone(),
            #[cfg(feature = "test-hooks")]
            ack_applied_gate: Arc::clone(&stream.ack_applied_gate),
            #[cfg(feature = "test-hooks")]
            ack_idle_gate: Arc::clone(&stream.ack_idle_gate),
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
            pending_notify: Arc::new(Notify::new()),
            request_send_failure: Arc::new(RequestSendFailure::default()),
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
            #[cfg(feature = "test-hooks")]
            ack_idle_gate: Arc::new(Mutex::new(None)),
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
        Self::rotation_deadlines_at(Instant::now(), server_duration_ms, configured_ack_wait_ms)
    }

    fn rotation_deadlines_at(
        rotation_started_at: Instant,
        server_duration_ms: u64,
        configured_ack_wait_ms: Option<u64>,
    ) -> RotationDeadlines {
        let drain_budget = Duration::from_millis(ROTATION_DRAIN_TIMEOUT_MS);
        let advertised_server_grace = Duration::from_millis(server_duration_ms);
        let server_grace = advertised_server_grace.min(MAX_SERVER_ROTATION_GRACE);
        if server_grace != advertised_server_grace {
            warn!(target: super::LOG_TARGET,
                server_duration_ms,
                max_server_rotation_grace_ms = MAX_SERVER_ROTATION_GRACE.as_millis(),
                "Server rotation grace exceeds the client limit; clamping it"
            );
        }
        let server_deadline = match rotation_started_at.checked_add(server_grace) {
            Some(deadline) => deadline,
            // No later deadline can be represented at this clock boundary.
            None => rotation_started_at,
        };
        let available_ack_wait = server_grace.saturating_sub(drain_budget);
        let configured_ack_wait = configured_ack_wait_ms
            .map(Duration::from_millis)
            .unwrap_or(available_ack_wait);
        let ack_wait = available_ack_wait.min(configured_ack_wait);
        let ack_deadline = match rotation_started_at.checked_add(ack_wait) {
            Some(deadline) => deadline,
            None => server_deadline,
        };
        let drain_deadline = if server_grace < drain_budget {
            match rotation_started_at.checked_add(drain_budget) {
                Some(deadline) => deadline,
                None => server_deadline,
            }
        } else {
            server_deadline
        };
        RotationDeadlines {
            ack: ack_deadline,
            drain: drain_deadline,
        }
    }

    fn bounded_rotation_drain_deadline(close_deadline: Instant) -> Instant {
        let now = Instant::now();
        let bounded_deadline =
            match now.checked_add(Duration::from_millis(ROTATION_DRAIN_TIMEOUT_MS)) {
                Some(deadline) => deadline,
                None => close_deadline,
            };
        close_deadline.min(bounded_deadline)
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

    /// Returns the oldest submitted batch and its absolute ACK deadline while holding
    /// the pending lock that also synchronizes submitted-watermark publication.
    async fn oldest_ack_deadline(
        &self,
        ack_timeout: Duration,
    ) -> ZerobusResult<Option<PendingAckDeadline>> {
        let pending = self.pending_batches.lock().await;
        let submitted_records = self.submitted_records.load(Ordering::Acquire);
        oldest_pending_ack_deadline(&pending, submitted_records, ack_timeout)
    }

    /// Returns the pending count from the locked snapshot when a fired deadline still
    /// belongs to the same expired head. This closes the race with an acknowledgment
    /// that removes the head as the timer fires.
    async fn expired_pending_count(
        &self,
        expected: PendingAckDeadline,
        ack_timeout: Duration,
    ) -> ZerobusResult<Option<usize>> {
        let pending = self.pending_batches.lock().await;
        let submitted_records = self.submitted_records.load(Ordering::Acquire);
        Ok(
            oldest_pending_ack_deadline(&pending, submitted_records, ack_timeout)?
                .filter(|current| {
                    current.identity == expected.identity && Instant::now() >= current.deadline
                })
                .map(|_| pending.len()),
        )
    }

    /// Waits without arming an ACK timeout while no submitted batch is pending.
    async fn wait_while_idle(&self, response_stream: &mut FlightResponseStream) -> AckEvent {
        #[cfg(feature = "test-hooks")]
        if let Some(notify) = self.ack_idle_gate.lock().await.take() {
            notify.notify_one();
        }

        tokio::select! {
            biased;
            response = response_stream.next() => AckEvent::Response(response),
            _ = self.request_send_failure.notify.notified() => AckEvent::RequestSendFailed,
            _ = self.pending_notify.notified() => AckEvent::PendingBatchAvailable,
        }
    }

    /// Waits for a response while enforcing the oldest pending ACK deadline. A ready
    /// response may win one already-expired tie for a given head; it cannot defer that
    /// same head twice.
    async fn wait_with_pending_deadline(
        &self,
        response_stream: &mut FlightResponseStream,
        pending_deadline: PendingAckDeadline,
        expiry_tie_winner: &mut Option<PendingBatchIdentity>,
    ) -> AckEvent {
        if *expiry_tie_winner == Some(pending_deadline.identity)
            && Instant::now() >= pending_deadline.deadline
        {
            return AckEvent::AckDeadline(pending_deadline);
        }

        let event = tokio::select! {
            biased;
            response = response_stream.next() => AckEvent::Response(response),
            _ = self.request_send_failure.notify.notified() => AckEvent::RequestSendFailed,
            _ = sleep_until(pending_deadline.deadline) => {
                AckEvent::AckDeadline(pending_deadline)
            }
        };

        *expiry_tie_winner = if matches!(event, AckEvent::Response(_))
            && Instant::now() >= pending_deadline.deadline
        {
            Some(pending_deadline.identity)
        } else {
            None
        };
        event
    }

    fn ack_timeout_error() -> ZerobusError {
        ZerobusError::StreamClosedError(tonic::Status::deadline_exceeded("Server ack timeout"))
    }

    fn request_send_error() -> ZerobusError {
        ZerobusError::StreamClosedError(tonic::Status::unavailable(
            "Flight request stream closed while sending",
        ))
    }

    /// Clears all coalesced failures from the old request sender. The supervisor calls
    /// this only after pausing under `ingest_mutex`, so no later ingest can report a
    /// failure for that connection.
    pub(super) fn clear_request_send_failure(&self) {
        self.request_send_failure.clear();
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
        let mut expiry_tie_winner: Option<PendingBatchIdentity> = None;
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

            let event = match &rotation {
                RotationState::Open => match self.oldest_ack_deadline(ack_timeout).await? {
                    Some(pending_deadline) => {
                        self.wait_with_pending_deadline(
                            &mut response_stream,
                            pending_deadline,
                            &mut expiry_tie_winner,
                        )
                        .await
                    }
                    None => self.wait_while_idle(&mut response_stream).await,
                },
                RotationState::WaitingForAcks { deadlines, .. } => {
                    tokio::select! {
                        biased;
                        response = response_stream.next() => AckEvent::Response(response),
                        _ = self.request_send_failure.notify.notified() => {
                            AckEvent::RequestSendFailed
                        }
                        _ = sleep_until(deadlines.ack) => continue,
                    }
                }
                RotationState::Draining(_) => unreachable!(),
            };

            match event {
                AckEvent::PendingBatchAvailable => continue,
                AckEvent::RequestSendFailed => {
                    if self.request_send_failure.take() {
                        return Err(Self::request_send_error());
                    }
                    continue;
                }
                AckEvent::AckDeadline(expected) => {
                    if let Some(pending_count) =
                        self.expired_pending_count(expected, ack_timeout).await?
                    {
                        error!(target: super::LOG_TARGET,
                            pending_count,
                            "Server ack timeout with pending batches"
                        );
                        return Err(Self::ack_timeout_error());
                    }
                    continue;
                }
                AckEvent::Response(Some(Ok(put_result))) => {
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
                AckEvent::Response(Some(Err(error))) => {
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
                AckEvent::Response(None) => {
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
    use arrow_flight::error::FlightError;
    use arrow_flight::PutResult;
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use futures::stream::{iter, pending};
    use futures::StreamExt as _;
    use tokio::sync::{watch, Mutex, Semaphore};
    use tokio::time::{Duration, Instant};

    use super::super::RecordBatch;
    use super::{
        AckProcessor, FlightAckMetadata, OffsetId, PendingBatch, RequestBodyControl, ZerobusError,
        MAX_SERVER_ROTATION_GRACE, ROTATION_DRAIN_TIMEOUT_MS,
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

    #[test]
    fn request_send_error_is_retryable_unavailable() {
        let error = AckProcessor::request_send_error();
        assert!(error.is_retryable());
        match error {
            ZerobusError::StreamClosedError(status) => {
                assert_eq!(status.code(), tonic::Code::Unavailable);
            }
            other => panic!("expected a stream-closed error, got {other:?}"),
        }
    }

    /// A buffered ACK is authoritative even when the request sender has already
    /// reported failure. Apply its durable watermark before asking recovery to replay.
    #[tokio::test]
    async fn ready_ack_is_applied_before_reported_request_send_failure() {
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
                ack_up_to_records: 5,
                close_stream_duration_ms: None,
            })
            .unwrap()
            .into(),
        })])
        .chain(pending::<Result<PutResult, FlightError>>());
        let last_acked_records = Arc::new(AtomicU64::new(0));
        let (processor, request_body, _last_ack_rx) = ack_processor(
            Arc::clone(&pending_batches),
            Arc::new(AtomicU64::new(10)),
            Arc::clone(&last_acked_records),
            false,
        );
        processor.request_send_failure.report();

        let error = processor
            .process(Box::pin(response_stream), request_body)
            .await
            .expect_err("the reported request-send failure must trigger recovery");

        match error {
            ZerobusError::StreamClosedError(status) => {
                assert_eq!(status.code(), tonic::Code::Unavailable);
            }
            other => panic!("expected a stream-closed error, got {other:?}"),
        }
        assert_eq!(last_acked_records.load(Ordering::Acquire), 5);
        assert_eq!(pending_batches.lock().await.len(), 1);
    }

    /// A peer status already buffered on the response stream must keep its real
    /// error and retry classification instead of being masked by local send failure.
    #[tokio::test]
    async fn ready_server_error_wins_reported_request_send_failure() {
        let response_stream = iter([Err::<PutResult, FlightError>(
            tonic::Status::permission_denied("permanent server rejection").into(),
        )]);
        let (processor, request_body, _last_ack_rx) = ack_processor(
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            false,
        );
        processor.request_send_failure.report();

        let error = processor
            .process(Box::pin(response_stream), request_body)
            .await
            .expect_err("the ready server rejection must be returned");

        assert!(!error.is_retryable());
        match error {
            ZerobusError::StreamClosedError(status) => {
                assert_eq!(status.code(), tonic::Code::PermissionDenied);
                assert_eq!(status.message(), "permanent server rejection");
            }
            other => panic!("expected a stream-closed error, got {other:?}"),
        }
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

    /// A response already ready at expiry is applied before deadline recovery.
    #[tokio::test]
    async fn ready_ack_wins_expired_deadline_tie() {
        const ACK_TIMEOUT: Duration = Duration::from_millis(10);

        let schema = one_col_schema();
        let sem = Arc::new(Semaphore::new(1));
        let mut pending = pending_batch(&sem, batch_with_rows(&schema, 1), 0, 0, 1);
        pending.refresh_enqueued_at(Instant::now() - ACK_TIMEOUT);
        let pending_batches = Arc::new(Mutex::new(vec![pending]));
        let response_stream = iter([Ok(PutResult {
            app_metadata: serde_json::to_vec(&FlightAckMetadata {
                ack_up_to_offset: 0,
                ack_up_to_records: 1,
                close_stream_duration_ms: None,
            })
            .unwrap()
            .into(),
        })]);
        let last_acked_records = Arc::new(AtomicU64::new(0));
        let (mut processor, request_body, last_ack_rx) = ack_processor(
            Arc::clone(&pending_batches),
            Arc::new(AtomicU64::new(1)),
            Arc::clone(&last_acked_records),
            false,
        );
        processor.options.server_lack_of_ack_timeout_ms = ACK_TIMEOUT.as_millis() as u64;

        let _stream_closed = processor
            .process(Box::pin(response_stream), request_body)
            .await;

        assert_eq!(last_acked_records.load(Ordering::Acquire), 1);
        assert_eq!(*last_ack_rx.borrow(), Some(0));
        assert!(pending_batches.lock().await.is_empty());
    }

    /// Only one ready response may defer an already-expired pending head. Partial
    /// progress does not grant the same head a second response-first tie.
    #[tokio::test]
    async fn expired_head_gets_only_one_ready_response() {
        const ACK_TIMEOUT: Duration = Duration::from_millis(10);

        let schema = one_col_schema();
        let sem = Arc::new(Semaphore::new(1));
        let mut pending = pending_batch(&sem, batch_with_rows(&schema, 10), 0, 0, 10);
        pending.refresh_enqueued_at(Instant::now() - ACK_TIMEOUT);
        let pending_batches = Arc::new(Mutex::new(vec![pending]));
        let response_stream = iter([5, 10].map(|acked_records| {
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
        let (mut processor, request_body, _last_ack_rx) = ack_processor(
            Arc::clone(&pending_batches),
            Arc::new(AtomicU64::new(10)),
            Arc::clone(&last_acked_records),
            false,
        );
        processor.options.server_lack_of_ack_timeout_ms = ACK_TIMEOUT.as_millis() as u64;

        let error = processor
            .process(Box::pin(response_stream), request_body)
            .await
            .expect_err("second ready response must not defer the expired head");

        match error {
            ZerobusError::StreamClosedError(status) => {
                assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
            }
            other => panic!("expected ACK deadline error, got {other:?}"),
        }
        assert_eq!(last_acked_records.load(Ordering::Acquire), 5);
        assert_eq!(pending_batches.lock().await.len(), 1);
    }

    /// A timer fired for a removed head cannot fail the next head, even when that next
    /// batch is also already expired; it must receive its own response-first tie.
    #[tokio::test]
    async fn deadline_recheck_requires_same_pending_head() {
        const ACK_TIMEOUT: Duration = Duration::from_millis(10);

        let schema = one_col_schema();
        let sem = Arc::new(Semaphore::new(2));
        let expired_at = Instant::now() - ACK_TIMEOUT;
        let mut first = pending_batch(&sem, batch_with_rows(&schema, 1), 0, 0, 1);
        first.refresh_enqueued_at(expired_at);
        let mut second = pending_batch(&sem, batch_with_rows(&schema, 1), 1, 1, 2);
        second.refresh_enqueued_at(expired_at);
        let pending_batches = Arc::new(Mutex::new(vec![first, second]));
        let (processor, _request_body, _last_ack_rx) = ack_processor(
            Arc::clone(&pending_batches),
            Arc::new(AtomicU64::new(2)),
            Arc::new(AtomicU64::new(0)),
            false,
        );
        let fired = processor
            .oldest_ack_deadline(ACK_TIMEOUT)
            .await
            .expect("deadline calculation")
            .expect("expired head");

        assert_eq!(
            processor
                .expired_pending_count(fired, ACK_TIMEOUT)
                .await
                .expect("deadline recheck"),
            Some(2)
        );

        pending_batches.lock().await.remove(0);

        assert!(processor
            .expired_pending_count(fired, ACK_TIMEOUT)
            .await
            .expect("deadline recheck")
            .is_none());
    }

    #[test]
    fn server_rotation_grace_is_capped() {
        let rotation_started_at = Instant::now();
        let deadlines = AckProcessor::rotation_deadlines_at(rotation_started_at, u64::MAX, None);

        assert_eq!(
            deadlines.drain.duration_since(rotation_started_at),
            MAX_SERVER_ROTATION_GRACE
        );
        assert_eq!(
            deadlines.ack.duration_since(rotation_started_at),
            MAX_SERVER_ROTATION_GRACE - Duration::from_millis(ROTATION_DRAIN_TIMEOUT_MS)
        );
    }
}
