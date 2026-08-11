//! Background recovery supervisor and terminal finalization.
//!
//! `Supervisor` owns cloned connection configuration and shared stream state.
//! It is the sole task that reconnects, replays pending work, and finalizes failures.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use arrow_flight::error::FlightError;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::{spawn, JoinHandle};
use tokio::time::{sleep, timeout_at, Duration, Instant};
use tracing::{debug, error, info, warn};

use super::acks::{pause_and_detach_sender, AckProcessor};
use super::batch::{rebuild_pending_for_replay, refresh_pending_ack_deadlines, PendingBatch};
use super::connection::{FlightConnection, FlightResponseStream, RequestBodyControl};
use super::{
    configured_deadline, ArrowStreamConfigurationOptions, ArrowTableProperties, BatchSender,
    RecordBatch, ZerobusArrowStream,
};
use crate::errors::ZerobusError;
use crate::headers_provider::HeadersProvider;
use crate::proxy::ConnectorFactory;
use crate::tls_config::TlsConfig;
use crate::ZerobusResult;

pub(super) struct Supervisor {
    endpoint: String,
    tls_config: Arc<dyn TlsConfig>,
    connector_factory: Option<ConnectorFactory>,
    table_properties: ArrowTableProperties,
    options: ArrowStreamConfigurationOptions,
    headers_provider: Arc<dyn HeadersProvider>,
    ack_processor: AckProcessor,
    batch_tx: BatchSender,
    is_closed: Arc<AtomicBool>,
    pending_batches: Arc<Mutex<Vec<PendingBatch>>>,
    failed_batches: Arc<Mutex<Vec<RecordBatch>>>,
    recovery_attempts: Arc<AtomicU32>,
    server_error_tx: watch::Sender<Option<ZerobusError>>,
    cumulative_records_assigned: Arc<AtomicU64>,
    submitted_records: Arc<AtomicU64>,
    last_acked_records: Arc<AtomicU64>,
    is_paused: Arc<AtomicBool>,
    ingest_mutex: Arc<Mutex<()>>,
    sdk_identifier: Arc<str>,
    #[cfg(feature = "test-hooks")]
    reconnect_rebuild_gate: super::ReconnectRebuildGate,
    #[cfg(feature = "test-hooks")]
    replay_send_gate: super::ReplaySendGate,
}

impl Supervisor {
    pub(super) fn new(stream: &ZerobusArrowStream) -> Self {
        Self {
            endpoint: stream.endpoint.clone(),
            tls_config: Arc::clone(&stream.tls_config),
            connector_factory: stream.connector_factory.clone(),
            table_properties: stream.table_properties.clone(),
            options: stream.options.clone(),
            headers_provider: Arc::clone(&stream.headers_provider),
            ack_processor: AckProcessor::new(stream),
            batch_tx: Arc::clone(&stream.batch_tx),
            is_closed: Arc::clone(&stream.is_closed),
            pending_batches: Arc::clone(&stream.pending_batches),
            failed_batches: Arc::clone(&stream.failed_batches),
            recovery_attempts: Arc::clone(&stream.recovery_attempts),
            server_error_tx: stream.server_error_tx.clone(),
            cumulative_records_assigned: Arc::clone(&stream.cumulative_records_assigned),
            submitted_records: Arc::clone(&stream.submitted_records),
            last_acked_records: Arc::clone(&stream.last_acked_records),
            is_paused: Arc::clone(&stream.is_paused),
            ingest_mutex: Arc::clone(&stream.ingest_mutex),
            sdk_identifier: Arc::clone(&stream.sdk_identifier),
            #[cfg(feature = "test-hooks")]
            reconnect_rebuild_gate: Arc::clone(&stream.reconnect_rebuild_gate),
            #[cfg(feature = "test-hooks")]
            replay_send_gate: Arc::clone(&stream.replay_send_gate),
        }
    }

    pub(super) fn spawn(
        self,
        initial_connection: FlightConnection,
    ) -> JoinHandle<ZerobusResult<()>> {
        let (response_stream, request_body) = initial_connection.into_supervisor_io();
        spawn(self.run(response_stream, request_body))
    }

    async fn run(
        self,
        initial_response_stream: FlightResponseStream,
        initial_request_body: RequestBodyControl,
    ) -> ZerobusResult<()> {
        let mut response_stream = Some(initial_response_stream);
        let mut request_body = Some(initial_request_body);
        // Carries a failed reconnect's real error into the next iteration's handling
        // instead of round-tripping a synthetic error through a dummy stream.
        let mut pending_error: Option<ZerobusError> = None;
        // True when `pending_error` is a reconnect auth rejection: the cached token was
        // invalidated and we want to retry (mint a fresh one) even though auth errors
        // classify as non-retryable — while still surfacing the original error if
        // retries are ultimately exhausted.
        let mut reconnect_auth_retry = false;

        loop {
            if self.is_closed.load(Ordering::Relaxed) {
                debug!(target: super::LOG_TARGET, "Supervisor: Stream closed, exiting");
                return Ok(());
            }

            // Run ACK processing until it returns — unless a prior reconnect attempt
            // failed, in which case carry that real error into the handling below
            // (preserving its message and retry classification).
            let result = if let Some(e) = pending_error.take() {
                Err(e)
            } else {
                self.ack_processor
                    .process(
                        response_stream
                            .take()
                            .expect("response_stream present when no pending reconnect error"),
                        request_body
                            .take()
                            .expect("request_body present when no pending reconnect error"),
                    )
                    .await
            };

            // Check if stream was closed during processing.
            if self.is_closed.load(Ordering::Relaxed) {
                debug!(target: super::LOG_TARGET, "Supervisor: Stream closed after process_acks, exiting");
                return result;
            }

            // Handle the result.
            match result {
                Ok(()) => {
                    // Stream ended gracefully.
                    debug!(target: super::LOG_TARGET, "Supervisor: process_acks completed successfully");
                    return Ok(());
                }
                Err(ref error)
                    if (error.is_retryable() || reconnect_auth_retry) && self.options.recovery =>
                {
                    // Retriable error (or a reconnect auth rejection we've chosen to
                    // retry with re-minted credentials) - attempt recovery.
                    reconnect_auth_retry = false;
                    let attempts = self.recovery_attempts.fetch_add(1, Ordering::Relaxed);
                    if attempts >= self.options.recovery_retries {
                        error!(target: super::LOG_TARGET,
                            attempts = attempts,
                            max_retries = self.options.recovery_retries,
                            "Supervisor: Max recovery retries exceeded"
                        );
                        // Publish the terminal error before finalization (so a waiter
                        // checking is_closed right after it already sees the real error;
                        // reconnect-failure errors carried via pending_error are never
                        // pre-published by ACK processing) and again after (to wake
                        // already-parked waiters). finalize_closed also drains pending
                        // under ingest_mutex so a concurrent ingest can't be omitted.
                        let _ = self.server_error_tx.send(Some(error.clone()));
                        Self::finalize_closed(
                            &self.ingest_mutex,
                            &self.is_closed,
                            &self.pending_batches,
                            &self.failed_batches,
                            &self.last_acked_records,
                        )
                        .await;
                        let _ = self.server_error_tx.send(Some(error.clone()));
                        return result;
                    }

                    info!(target: super::LOG_TARGET,
                        attempt = attempts + 1,
                        max_retries = self.options.recovery_retries,
                        error = %error,
                        "Supervisor: Attempting recovery after retriable error"
                    );

                    // Atomically pause ingest and detach the sender under
                    // ingest_mutex, so an in-flight ingest_batch either completes
                    // before the pause or observes is_paused and buffers — it never
                    // sees is_paused=false with a detached sender. Successful replay
                    // lifts the gate; failed attempts remain paused for retry/finalization.
                    pause_and_detach_sender(&self.ingest_mutex, &self.is_paused, &self.batch_tx)
                        .await;
                    self.ack_processor.clear_request_send_failure();

                    sleep(Duration::from_millis(self.options.recovery_backoff_ms)).await;

                    let _ = self.server_error_tx.send(None);

                    // Share one absolute timeout budget across reconnect and
                    // auth-rejection invalidation.
                    let recovery_timeout = Duration::from_millis(self.options.recovery_timeout_ms);
                    let recovery_started = Instant::now();
                    let recovery_deadline = match configured_deadline(
                        recovery_started,
                        recovery_timeout,
                        "recovery_timeout_ms",
                    ) {
                        Ok(deadline) => deadline,
                        Err(error) => {
                            pending_error = Some(error);
                            continue;
                        }
                    };
                    let reconnect_result = timeout_at(recovery_deadline, self.reconnect()).await;

                    match reconnect_result {
                        Ok(Ok((new_response_stream, new_request_body))) => {
                            info!(target: super::LOG_TARGET, "Supervisor: Recovery successful, resuming");
                            self.recovery_attempts.store(0, Ordering::Relaxed);
                            // is_paused was already cleared inside reconnect().
                            response_stream = Some(new_response_stream);
                            request_body = Some(new_request_body);
                        }
                        Ok(Err(e)) => {
                            warn!(target: super::LOG_TARGET, "Supervisor: Reconnection failed: {}", e);
                            // Ask the provider to invalidate cached authentication
                            // state after an auth rejection, then retry even though
                            // such errors are otherwise non-retryable. Preserve this
                            // reconnect error if refresh or later recovery cannot proceed.
                            if e.is_auth_rejection() {
                                match timeout_at(
                                    recovery_deadline,
                                    self.headers_provider.invalidate(),
                                )
                                .await
                                {
                                    Ok(()) => reconnect_auth_retry = true,
                                    Err(_) => {
                                        warn!(target: super::LOG_TARGET,
                                            timeout_ms = self.options.recovery_timeout_ms,
                                            "Recovery deadline reached while invalidating \
                                             the headers provider; terminating recovery"
                                        );
                                        // A custom provider must not stall recovery
                                        // indefinitely. Close with the original auth
                                        // rejection; publish before and after
                                        // finalization for waiter race-freedom.
                                        let _ = self.server_error_tx.send(Some(e.clone()));
                                        Self::finalize_closed(
                                            &self.ingest_mutex,
                                            &self.is_closed,
                                            &self.pending_batches,
                                            &self.failed_batches,
                                            &self.last_acked_records,
                                        )
                                        .await;
                                        let _ = self.server_error_tx.send(Some(e.clone()));
                                        return Err(e);
                                    }
                                }
                            }
                            pending_error = Some(e);
                        }
                        Err(_timeout) => {
                            warn!(target: super::LOG_TARGET, "Supervisor: Reconnection timed out");
                            pending_error = Some(ZerobusError::ConnectionTimeout(format!(
                                "Reconnection timed out after {}ms",
                                self.options.recovery_timeout_ms
                            )));
                        }
                    }
                }
                Err(error) => {
                    error!(target: super::LOG_TARGET, "Supervisor: Non-retriable error, closing stream: {}", error);
                    // Publish the terminal error before finalization (so a waiter
                    // checking is_closed right after it already sees the real error;
                    // reconnect-failure errors carried via pending_error are never
                    // pre-published by ACK processing) and again after (to wake
                    // already-parked waiters). finalize_closed drains pending under
                    // ingest_mutex so a concurrent ingest can't be omitted.
                    let _ = self.server_error_tx.send(Some(error.clone()));
                    Self::finalize_closed(
                        &self.ingest_mutex,
                        &self.is_closed,
                        &self.pending_batches,
                        &self.failed_batches,
                        &self.last_acked_records,
                    )
                    .await;
                    let _ = self.server_error_tx.send(Some(error.clone()));
                    // Ask the provider to invalidate cached authentication state after
                    // a terminal rejection. The stream is already finalized and waiters
                    // have the real error; bound the callback so the supervisor cannot
                    // remain alive indefinitely.
                    if error.is_auth_rejection() {
                        match configured_deadline(
                            Instant::now(),
                            Duration::from_millis(self.options.recovery_timeout_ms),
                            "recovery_timeout_ms",
                        ) {
                            Ok(deadline) => {
                                if timeout_at(deadline, self.headers_provider.invalidate())
                                    .await
                                    .is_err()
                                {
                                    warn!(target: super::LOG_TARGET,
                                        timeout_ms = self.options.recovery_timeout_ms,
                                        "Terminal headers provider invalidation timed out"
                                    );
                                }
                            }
                            Err(deadline_error) => {
                                warn!(target: super::LOG_TARGET,
                                    error = %deadline_error,
                                    "Skipping terminal headers provider invalidation because its deadline is unrepresentable"
                                );
                            }
                        }
                    }
                    return Err(error);
                }
            }
        }
    }

    /// Reconnects to the server and replays pending batches.
    ///
    /// On successful replay, holds `ingest_mutex` until `is_paused` is cleared so
    /// subsequently admitted ingests send normally. Error paths remain paused for
    /// supervisor retry or finalization.
    async fn reconnect(&self) -> ZerobusResult<(FlightResponseStream, RequestBodyControl)> {
        let connection = ZerobusArrowStream::reconnect_transport(
            &self.endpoint,
            &self.tls_config,
            self.connector_factory.as_ref(),
            &self.table_properties,
            &self.options,
            &self.headers_provider,
            &self.sdk_identifier,
        )
        .await?;
        let (response_stream, tx, request_body) = connection.into_parts();

        // Counters are reset atomically with the range rebuild inside
        // replay_pending_batches, so a concurrent ingest can't fetch_add a reset counter,
        // fabricate a low range, and have replay drop it as fully-acked.
        let acked_before_disconnect = self.last_acked_records.load(Ordering::Acquire);

        // Test seam: pause after the connection is established but before ingest_mutex is
        // held and ranges/watermark are rebuilt, so a test can schedule a paused ingest
        // that wins ingest_mutex first (reset/rebase race) or drive a concurrent close().
        #[cfg(feature = "test-hooks")]
        {
            let barrier = self.reconnect_rebuild_gate.lock().await.take();
            if let Some(barrier) = barrier {
                barrier.reached.notify_one();
                barrier.proceed.notified().await;
            }
        }

        // Hold ingest_mutex across the replay so no concurrent ingest interleaves.
        let _ingest_guard = self.ingest_mutex.lock().await;
        let replay_result = Self::replay_pending_batches(
            &tx,
            &self.pending_batches,
            &self.cumulative_records_assigned,
            &self.submitted_records,
            &self.last_acked_records,
            acked_before_disconnect,
            #[cfg(feature = "test-hooks")]
            Some(&self.replay_send_gate),
        )
        .await;

        // Commit the replacement sender only after replay succeeds. While ingest_mutex
        // remains held, publish the sender before clearing the pause gate so normal ingest
        // cannot observe an unpaused stream without its active sender.
        Self::commit_reconnect_after_replay(replay_result, tx, &self.batch_tx, &self.is_paused)
            .await?;

        // ACK processing cannot resume until reconnect returns. Refresh only after replay
        // is fully committed so connection setup, backlog sends, and sender publication do
        // not consume any batch's new ACK budget.
        let mut pending = self.pending_batches.lock().await;
        refresh_pending_ack_deadlines(&mut pending, Instant::now());
        drop(pending);

        Ok((response_stream, request_body))
    }

    /// Commits the replacement sender after replay. The caller holds `ingest_mutex`.
    async fn commit_reconnect_after_replay(
        replay_result: ZerobusResult<()>,
        tx: mpsc::Sender<Result<RecordBatch, FlightError>>,
        batch_tx: &BatchSender,
        is_paused: &AtomicBool,
    ) -> ZerobusResult<()> {
        replay_result?;
        {
            let mut tx_guard = batch_tx.lock().await;
            *tx_guard = Some(tx.clone());
        }
        is_paused.store(false, Ordering::Relaxed);
        Ok(())
    }

    /// Rebuilds `pending_batches` for replay after a reconnect and replays them over
    /// `tx`: partially-acked batches (vs `acked_before_disconnect`) are sliced to their
    /// un-acked suffix, fully-acked ones dropped.
    ///
    /// The rebuilt pending set and the counter reset are installed together under the
    /// `pending_batches` lock, before any send: a replay-send failure keeps pending (and
    /// permits) intact, and no concurrent ingest can observe reset counters against stale
    /// ranges. The caller refreshes pending ACK timestamps together only after replay is
    /// committed, immediately before ACK processing can resume. Caller holds `ingest_mutex`.
    async fn replay_pending_batches(
        tx: &mpsc::Sender<Result<RecordBatch, FlightError>>,
        pending_batches: &Arc<Mutex<Vec<PendingBatch>>>,
        cumulative_records_assigned: &Arc<AtomicU64>,
        submitted_records: &Arc<AtomicU64>,
        last_acked_records: &Arc<AtomicU64>,
        acked_before_disconnect: u64,
        #[cfg(feature = "test-hooks")] replay_send_gate: Option<&super::ReplaySendGate>,
    ) -> ZerobusResult<()> {
        let replay_batches: Vec<RecordBatch> = {
            let mut pending = pending_batches.lock().await;

            if !pending.is_empty() {
                info!(target: super::LOG_TARGET,
                    batch_count = pending.len(),
                    acked_records = acked_before_disconnect,
                    "Replaying pending batches after recovery"
                );
            }
            let (replay, new_cumulative) =
                rebuild_pending_for_replay(&mut pending, acked_before_disconnect);

            // Reset counters together with the range install, before any send.
            cumulative_records_assigned.store(new_cumulative, Ordering::Relaxed);
            submitted_records.store(0, Ordering::Release);
            last_acked_records.store(0, Ordering::Release);

            replay
        };

        // Send only after the pending_batches lock is released (ingest_mutex is still
        // held by the caller); pending stays intact on failure. The replacement response
        // stream is not polled until replay returns, so publishing after each successful
        // handoff cannot race a valid acknowledgement on this connection.
        for batch in replay_batches {
            let record_count = batch.num_rows() as u64;
            if tx.send(Ok(batch)).await.is_err() {
                return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                    "Failed to replay batch during recovery",
                )));
            }
            submitted_records.fetch_add(record_count, Ordering::Release);

            #[cfg(feature = "test-hooks")]
            {
                let barrier = match replay_send_gate {
                    Some(gate) => gate.lock().await.take(),
                    None => None,
                };
                if let Some(barrier) = barrier {
                    barrier.reached.notify_one();
                    barrier.proceed.notified().await;
                }
            }
        }

        Ok(())
    }

    /// Moves each pending batch's unacknowledged suffix to the failed list, dropping
    /// fully acknowledged batches.
    pub(super) async fn move_pending_to_failed(
        pending_batches: &Arc<Mutex<Vec<PendingBatch>>>,
        failed_batches: &Arc<Mutex<Vec<RecordBatch>>>,
        last_acked_records: &Arc<AtomicU64>,
    ) {
        // Lock failed first and hold it across the pending drain so this serializes with
        // get_unacked_batches (which uses the same order): whichever runs first drains
        // pending; the other then sees an empty pending and the same failed snapshot.
        // Lock order is always failed -> pending; no path takes them in the reverse.
        let mut failed = failed_batches.lock().await;
        let mut pending = pending_batches.lock().await;
        let acked = last_acked_records.load(Ordering::Acquire);
        for pb in pending.drain(..) {
            // Slice off any durably-acked prefix so a manual retry via
            // get_unacked_batches doesn't re-send already-persisted records.
            if let Some(batch) = pb.unacknowledged_suffix(acked) {
                failed.push(batch);
            }
        }
    }

    /// Publishes stream closure and drains pending -> failed atomically with respect to
    /// `ingest_batch`. Holding `ingest_mutex` across the `is_closed` store and the drain
    /// means an ingest either finishes its append before this runs (and is drained here)
    /// or observes `is_closed` after the mutex is released (and refuses to append), so a
    /// retrieval snapshot can never omit an accepted batch that a later call reveals.
    pub(super) async fn finalize_closed(
        ingest_mutex: &Arc<Mutex<()>>,
        is_closed: &Arc<AtomicBool>,
        pending_batches: &Arc<Mutex<Vec<PendingBatch>>>,
        failed_batches: &Arc<Mutex<Vec<RecordBatch>>>,
        last_acked_records: &Arc<AtomicU64>,
    ) {
        let _guard = ingest_mutex.lock().await;
        is_closed.store(true, Ordering::Relaxed);
        Self::move_pending_to_failed(pending_batches, failed_batches, last_acked_records).await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;

    use arrow_array::Int32Array;
    use arrow_flight::error::FlightError;
    use arrow_flight::PutResult;
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use futures::stream::iter;
    use tokio::sync::{mpsc, Mutex, Semaphore};
    use tokio::time::{timeout, Duration, Instant};

    use super::super::metadata::FlightAckMetadata;
    use super::{
        pause_and_detach_sender, AckProcessor, BatchSender, PendingBatch, RecordBatch, Supervisor,
        ZerobusError,
    };
    use crate::offset_generator::OffsetId;

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

    #[tokio::test]
    async fn failed_replay_leaves_sender_detached_and_closes_request_channel() {
        let (tx, mut request_rx) = mpsc::channel::<Result<RecordBatch, FlightError>>(1);
        let batch_tx: BatchSender = Arc::new(Mutex::new(None));
        let is_paused = AtomicBool::new(true);
        let replay_result = Err(ZerobusError::StreamClosedError(tonic::Status::internal(
            "failed replay",
        )));

        let result =
            Supervisor::commit_reconnect_after_replay(replay_result, tx, &batch_tx, &is_paused)
                .await;

        assert!(result.is_err());
        assert!(batch_tx.lock().await.is_none());
        assert!(is_paused.load(Ordering::Relaxed));
        assert!(
            request_rx.recv().await.is_none(),
            "failed replay must drop the only replacement sender"
        );
    }

    #[tokio::test]
    async fn regressive_ack_replays_only_unacknowledged_suffix() {
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
        let cumulative_records_assigned = Arc::new(AtomicU64::new(10));
        let submitted_records = Arc::new(AtomicU64::new(10));
        let last_acked_records = Arc::new(AtomicU64::new(0));
        let (processor, request_body, _last_ack_rx) = AckProcessor::for_test(
            Arc::clone(&pending_batches),
            Arc::clone(&submitted_records),
            Arc::clone(&last_acked_records),
            false,
        );

        let _stream_closed = processor
            .process(Box::pin(response_stream), request_body)
            .await;

        let acked_before_disconnect = last_acked_records.load(Ordering::Acquire);
        assert_eq!(acked_before_disconnect, 5);
        let (tx, mut rx) = mpsc::channel::<Result<RecordBatch, FlightError>>(1);
        Supervisor::replay_pending_batches(
            &tx,
            &pending_batches,
            &cumulative_records_assigned,
            &submitted_records,
            &last_acked_records,
            acked_before_disconnect,
            #[cfg(feature = "test-hooks")]
            None,
        )
        .await
        .expect("replay should succeed");

        let pending = pending_batches.lock().await;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].record_count(), 5);
        assert_eq!(pending[0].record_range(), (0, 5));
        drop(pending);
        assert_eq!(cumulative_records_assigned.load(Ordering::Relaxed), 5);
        assert_eq!(submitted_records.load(Ordering::Acquire), 5);
        assert_eq!(last_acked_records.load(Ordering::Acquire), 0);
        assert_eq!(rx.try_recv().unwrap().unwrap().num_rows(), 5);
        assert!(rx.try_recv().is_err());
    }

    /// A replay-send failure must not drop pending batches, their permits, or desync
    /// the counter. A dropped receiver gives a deterministic send failure.
    #[tokio::test]
    async fn replay_send_failure_retains_pending_permits_and_cumulative() {
        let schema = one_col_schema();
        let sem = Arc::new(Semaphore::new(4));
        let original_enqueued_at = Instant::now() - Duration::from_secs(1);
        let mut pending_batches = vec![
            pending_batch(&sem, batch_with_rows(&schema, 3), 0, 0, 3),
            pending_batch(&sem, batch_with_rows(&schema, 2), 1, 3, 5),
        ];
        for batch in &mut pending_batches {
            batch.refresh_enqueued_at(original_enqueued_at);
        }
        let pending = Arc::new(Mutex::new(pending_batches));
        assert_eq!(
            sem.available_permits(),
            2,
            "two permits held by pending batches"
        );

        // Stale values that must be overwritten by the atomic install.
        let cumulative = Arc::new(AtomicU64::new(999));
        let submitted = Arc::new(AtomicU64::new(999));
        let last_acked = Arc::new(AtomicU64::new(7));

        // Receiver dropped -> every send fails.
        let (tx, rx) = mpsc::channel::<Result<RecordBatch, FlightError>>(4);
        drop(rx);

        let res = Supervisor::replay_pending_batches(
            &tx,
            &pending,
            &cumulative,
            &submitted,
            &last_acked,
            0,
            #[cfg(feature = "test-hooks")]
            None,
        )
        .await;
        assert!(res.is_err(), "replay must surface the send failure");

        let guard = pending.lock().await;
        assert_eq!(
            guard.len(),
            2,
            "pending must retain all batches on replay failure"
        );
        assert_eq!(guard[0].record_range(), (0, 3));
        assert_eq!(guard[1].record_range(), (3, 5));
        assert!(
            guard
                .iter()
                .all(|batch| batch.enqueued_at() == original_enqueued_at),
            "failed replay must not refresh pending ACK timestamps"
        );
        drop(guard);

        assert_eq!(
            cumulative.load(Ordering::Relaxed),
            5,
            "cumulative_records_assigned must match the reinstalled ranges, not the stale value"
        );
        assert_eq!(
            submitted.load(Ordering::Acquire),
            0,
            "a failed replay must not publish unsent records"
        );
        assert_eq!(
            last_acked.load(Ordering::Relaxed),
            0,
            "watermark must be rebased to 0 atomically with the ranges"
        );
        assert_eq!(
            sem.available_permits(),
            2,
            "permits must not be released on replay failure"
        );
    }

    /// With an open receiver, both batches remain pending, replay in order, and reset the
    /// connection-relative counters.
    #[tokio::test]
    async fn replay_success_reinstalls_and_sends_all() {
        let schema = one_col_schema();
        let sem = Arc::new(Semaphore::new(4));
        let original_enqueued_at = Instant::now() - Duration::from_secs(1);
        let mut pending_batches = vec![
            pending_batch(&sem, batch_with_rows(&schema, 3), 0, 0, 3),
            pending_batch(&sem, batch_with_rows(&schema, 2), 1, 3, 5),
        ];
        for batch in &mut pending_batches {
            batch.refresh_enqueued_at(original_enqueued_at);
        }
        let pending = Arc::new(Mutex::new(pending_batches));
        let cumulative = Arc::new(AtomicU64::new(0));
        let submitted = Arc::new(AtomicU64::new(0));
        let last_acked = Arc::new(AtomicU64::new(9));

        let (tx, mut rx) = mpsc::channel::<Result<RecordBatch, FlightError>>(4);

        let res = Supervisor::replay_pending_batches(
            &tx,
            &pending,
            &cumulative,
            &submitted,
            &last_acked,
            0,
            #[cfg(feature = "test-hooks")]
            None,
        )
        .await;
        assert!(res.is_ok());

        let pending_guard = pending.lock().await;
        assert_eq!(pending_guard.len(), 2);
        assert!(
            pending_guard
                .iter()
                .all(|batch| batch.enqueued_at() == original_enqueued_at),
            "the replay send phase must not start pending ACK deadlines"
        );
        drop(pending_guard);
        assert_eq!(cumulative.load(Ordering::Relaxed), 5);
        assert_eq!(submitted.load(Ordering::Acquire), 5);
        assert_eq!(last_acked.load(Ordering::Relaxed), 0);

        let first = rx.try_recv().expect("first replay batch");
        assert_eq!(first.unwrap().num_rows(), 3);
        let second = rx.try_recv().expect("second replay batch");
        assert_eq!(second.unwrap().num_rows(), 2);
    }

    /// A fully-acked batch is dropped during replay (permit released), and a partially
    /// acked batch is sliced to its un-acked suffix.
    #[tokio::test]
    async fn replay_slices_partial_and_drops_fully_acked() {
        let schema = one_col_schema();
        let sem = Arc::new(Semaphore::new(4));
        // Batch 0: rows [0,3) fully acked (acked_before_disconnect = 4 covers it).
        // Batch 1: rows [3,6), 1 record acked -> 2-row suffix replayed.
        let pending = Arc::new(Mutex::new(vec![
            pending_batch(&sem, batch_with_rows(&schema, 3), 0, 0, 3),
            pending_batch(&sem, batch_with_rows(&schema, 3), 1, 3, 6),
        ]));
        assert_eq!(sem.available_permits(), 2);
        let cumulative = Arc::new(AtomicU64::new(0));
        let submitted = Arc::new(AtomicU64::new(0));
        let last_acked = Arc::new(AtomicU64::new(4));
        let (tx, mut rx) = mpsc::channel::<Result<RecordBatch, FlightError>>(4);

        let res = Supervisor::replay_pending_batches(
            &tx,
            &pending,
            &cumulative,
            &submitted,
            &last_acked,
            4,
            #[cfg(feature = "test-hooks")]
            None,
        )
        .await;
        assert!(res.is_ok());

        // Only the partially-acked batch remains, rebuilt from cumulative 0.
        let guard = pending.lock().await;
        assert_eq!(guard.len(), 1);
        assert_eq!(guard[0].record_range(), (0, 2));
        drop(guard);
        assert_eq!(cumulative.load(Ordering::Relaxed), 2);
        assert_eq!(submitted.load(Ordering::Acquire), 2);
        assert_eq!(last_acked.load(Ordering::Relaxed), 0);
        // Fully-acked batch's permit was released; one remains.
        assert_eq!(sem.available_permits(), 3);

        let replayed = rx.try_recv().expect("suffix replay batch");
        assert_eq!(replayed.unwrap().num_rows(), 2);
        assert!(rx.try_recv().is_err(), "only one batch should be replayed");
    }

    /// `pause_and_detach_sender` must block while an ingest holds `ingest_mutex`, so an
    /// ingest can never observe `is_paused == false` together with a detached sender.
    #[tokio::test]
    async fn pause_and_detach_waits_for_in_flight_ingest() {
        let ingest_mutex = Arc::new(Mutex::new(()));
        let is_paused = Arc::new(AtomicBool::new(false));
        let (tx, _rx) = mpsc::channel::<Result<RecordBatch, FlightError>>(1);
        let batch_tx: BatchSender = Arc::new(Mutex::new(Some(tx)));

        // Deterministic sync point: hold ingest_mutex to represent an ingest in its
        // critical section, past the is_paused observation and about to read the sender.
        let guard = ingest_mutex.lock().await;

        let fut = pause_and_detach_sender(&ingest_mutex, &is_paused, &batch_tx);
        tokio::pin!(fut);

        // Polling the future while the ingest holds ingest_mutex must return Pending
        // (it is actively driven, not merely unscheduled) and must not have flipped
        // is_paused or detached the sender.
        assert!(
            futures::poll!(fut.as_mut()).is_pending(),
            "pause_and_detach_sender must block while an ingest holds ingest_mutex"
        );
        assert!(
            !is_paused.load(Ordering::Relaxed),
            "is_paused flipped mid-ingest"
        );
        assert!(
            batch_tx.lock().await.is_some(),
            "sender detached mid-ingest"
        );

        // Once the ingest leaves its critical section, the transition completes.
        drop(guard);
        timeout(Duration::from_secs(1), fut)
            .await
            .expect("pause_and_detach_sender should proceed after ingest_mutex is released");
        assert!(is_paused.load(Ordering::Relaxed));
        assert!(batch_tx.lock().await.is_none());
    }

    /// `finalize_closed` must serialize with an in-flight ingest: while an ingest holds
    /// `ingest_mutex` (past its closed check, about to append), finalization blocks and
    /// does not publish `is_closed`; a batch appended just before the mutex is released is
    /// still drained into the failed set, so a retrieval snapshot never omits it.
    #[tokio::test]
    async fn finalize_closed_waits_for_in_flight_ingest() {
        let ingest_mutex = Arc::new(Mutex::new(()));
        let is_closed = Arc::new(AtomicBool::new(false));
        let sem = Arc::new(Semaphore::new(4));
        let pending = Arc::new(Mutex::new(Vec::new()));
        let failed = Arc::new(Mutex::new(Vec::new()));
        let last_acked = Arc::new(AtomicU64::new(0));

        // Represent an ingest in its critical section (past its second is_closed check,
        // about to append): hold ingest_mutex.
        let guard = ingest_mutex.lock().await;

        let fut =
            Supervisor::finalize_closed(&ingest_mutex, &is_closed, &pending, &failed, &last_acked);
        tokio::pin!(fut);

        // Finalization must block while the ingest holds ingest_mutex, and must not
        // publish is_closed while blocked.
        assert!(
            futures::poll!(fut.as_mut()).is_pending(),
            "finalize_closed must wait for the in-flight ingest"
        );
        assert!(
            !is_closed.load(Ordering::Relaxed),
            "is_closed must not be published mid-ingest"
        );

        // The ingest appends its batch, then releases the mutex.
        let schema = one_col_schema();
        pending
            .lock()
            .await
            .push(pending_batch(&sem, batch_with_rows(&schema, 2), 0, 0, 2));
        drop(guard);

        timeout(Duration::from_secs(1), fut)
            .await
            .expect("finalize_closed should proceed after ingest_mutex is released");

        // The batch appended just before the mutex release is in the drained snapshot.
        assert!(is_closed.load(Ordering::Relaxed));
        assert_eq!(
            failed.lock().await.len(),
            1,
            "batch appended before mutex release must be drained into failed"
        );
        assert!(pending.lock().await.is_empty());
    }
}
