//! Background recovery supervisor and terminal finalization.
//!
//! `Supervisor` owns cloned connection configuration and shared stream state.
//! It is the sole task that reconnects, replays pending work, and finalizes failures.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use arrow_flight::error::FlightError;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::{spawn, AbortHandle, JoinError, JoinHandle};
use tokio::time::{sleep, sleep_until, timeout_at, Duration, Instant};
use tracing::{debug, error, info, warn};

use super::acks::{pause_and_detach_sender, AckProcessOutcome, AckProcessor};
use super::batch::{rebuild_pending_for_replay, refresh_pending_ack_deadlines, PendingBatch};
use super::close::{CloseCoordinator, CloseFinalizer, CloseState};
use super::connection::{
    FlightConnection, FlightResponseStream, RequestBodyControl, RequestBodyRegistry,
};
use super::{
    configured_deadline, ArrowStreamConfigurationOptions, ArrowTableProperties, BatchSender,
    FlightConnectionParameters, RecordBatch, ZerobusArrowStream,
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
    close: CloseCoordinator,
    close_finalizer: CloseFinalizer,
    request_bodies: RequestBodyRegistry,
    pending_batches: Arc<Mutex<Vec<PendingBatch>>>,
    recovery_attempts: Arc<AtomicU32>,
    server_error_tx: watch::Sender<Option<ZerobusError>>,
    cumulative_records_assigned: Arc<AtomicU64>,
    submitted_records: Arc<AtomicU64>,
    last_acked_records: Arc<AtomicU64>,
    is_paused: Arc<AtomicBool>,
    ingest_mutex: Arc<Mutex<()>>,
    sdk_identifier: Arc<str>,
    #[cfg(feature = "test-hooks")]
    test_hooks: Arc<super::TestHooks>,
}

pub(super) struct SupervisorTaskHandle {
    worker: AbortHandle,
    // Dropping this handle intentionally detaches the reaper during ordinary Drop.
    #[cfg_attr(not(feature = "internal-arrow-c-data"), allow(dead_code))]
    reaper: JoinHandle<()>,
}

impl SupervisorTaskHandle {
    pub(super) fn abort(&self) {
        self.worker.abort();
    }

    #[cfg(feature = "internal-arrow-c-data")]
    pub(super) async fn abort_and_wait(self) {
        self.worker.abort();
        self.reaper
            .await
            .expect("Arrow supervisor reaper failed during shutdown");
    }
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
            close: stream.close.clone(),
            close_finalizer: CloseFinalizer::new(stream),
            request_bodies: stream.request_bodies.clone(),
            pending_batches: Arc::clone(&stream.pending_batches),
            recovery_attempts: Arc::clone(&stream.recovery_attempts),
            server_error_tx: stream.server_error_tx.clone(),
            cumulative_records_assigned: Arc::clone(&stream.cumulative_records_assigned),
            submitted_records: Arc::clone(&stream.submitted_records),
            last_acked_records: Arc::clone(&stream.last_acked_records),
            is_paused: Arc::clone(&stream.is_paused),
            ingest_mutex: Arc::clone(&stream.ingest_mutex),
            sdk_identifier: Arc::clone(&stream.sdk_identifier),
            #[cfg(feature = "test-hooks")]
            test_hooks: Arc::clone(&stream.test_hooks),
        }
    }

    pub(super) fn spawn(self, initial_connection: FlightConnection) -> SupervisorTaskHandle {
        let (response_stream, request_body) = initial_connection.into_supervisor_io();
        let close = self.close.clone();
        let finalizer = self.close_finalizer.clone();
        let worker = spawn(self.run(response_stream, request_body));
        let worker_abort = worker.abort_handle();
        // The detached reaper owns the JoinHandle so cancelling a close caller cannot
        // lose observation of an abnormal supervisor exit.
        let reaper = spawn(async move {
            let joined = worker.await;
            if matches!(close.state(), CloseState::Finalized(_)) {
                return;
            }
            let outcome = Self::unfinalized_exit_outcome(joined);
            let _ = finalizer.finish(outcome).await;
        });
        SupervisorTaskHandle {
            worker: worker_abort,
            reaper,
        }
    }

    fn unfinalized_exit_outcome(joined: Result<ZerobusResult<()>, JoinError>) -> ZerobusResult<()> {
        match joined {
            Ok(Err(error)) => Err(error),
            Ok(Ok(())) => Err(ZerobusError::InvalidStateError(
                "Supervisor exited successfully before close finalization".to_string(),
            )),
            Err(error) if error.is_cancelled() => Err(ZerobusError::InvalidStateError(
                "Supervisor task was cancelled before close finalization".to_string(),
            )),
            Err(_) => Err(ZerobusError::InvalidStateError(
                "Supervisor task panicked before close finalization".to_string(),
            )),
        }
    }

    fn spawn_headers_invalidation(&self, deadline: Instant) -> JoinHandle<bool> {
        let headers_provider = Arc::clone(&self.headers_provider);
        let timeout_ms = self.options.recovery_timeout_ms;
        spawn(async move {
            if timeout_at(deadline, headers_provider.invalidate())
                .await
                .is_ok()
            {
                true
            } else {
                warn!(target: super::LOG_TARGET,
                    timeout_ms,
                    "Headers provider invalidation timed out"
                );
                false
            }
        })
    }

    fn spawn_detached_headers_invalidation(&self) {
        match configured_deadline(
            Instant::now(),
            Duration::from_millis(self.options.recovery_timeout_ms),
            "recovery_timeout_ms",
        ) {
            Ok(deadline) => {
                drop(self.spawn_headers_invalidation(deadline));
            }
            Err(error) => {
                warn!(target: super::LOG_TARGET,
                    error = %error,
                    "Skipping headers provider invalidation because its deadline is unrepresentable"
                );
            }
        }
    }

    fn spawn_detached_auth_invalidation(&self, error: &ZerobusError) {
        if error.is_auth_rejection() {
            self.spawn_detached_headers_invalidation();
        }
    }

    async fn finish(&self, outcome: ZerobusResult<()>) -> ZerobusResult<()> {
        self.close_finalizer.finish(outcome).await
    }

    fn finalized_result(&self) -> ZerobusResult<()> {
        Self::result_from_close_state(self.close.state())
    }

    fn result_from_close_state(state: CloseState) -> ZerobusResult<()> {
        match state {
            CloseState::Finalized(result) => result,
            CloseState::Open | CloseState::Requested(_) => Err(ZerobusError::InvalidStateError(
                "Supervisor exited before close finalization".to_string(),
            )),
        }
    }

    async fn run(
        self,
        initial_response_stream: FlightResponseStream,
        initial_request_body: RequestBodyControl,
    ) -> ZerobusResult<()> {
        let mut response_stream = Some(initial_response_stream);
        let mut request_body = Some(initial_request_body);
        let mut pending_error: Option<ZerobusError> = None;
        let mut reconnect_auth_retry = false;
        let mut close_rx = self.close.subscribe();

        loop {
            if self.is_closed.load(Ordering::Relaxed) {
                debug!(target: super::LOG_TARGET, "Supervisor: Stream closed, exiting");
                return self.finalized_result();
            }

            let mut active_was_drained = false;
            let active_error = if let Some(error) = pending_error.take() {
                error
            } else {
                let active_response = response_stream
                    .as_mut()
                    .expect("response stream present outside recovery");
                let active_request = request_body
                    .as_ref()
                    .expect("request body present outside recovery");
                match self
                    .ack_processor
                    .process_active(active_response, active_request, &mut close_rx, true)
                    .await
                {
                    Ok(AckProcessOutcome::Stopped) => return self.finalized_result(),
                    Ok(AckProcessOutcome::Recovery { error, drained }) => {
                        active_was_drained = drained;
                        error
                    }
                    Ok(AckProcessOutcome::Close { request, outcome }) => {
                        debug_assert_eq!(self.close.request(), Some(request));
                        if let Err(error) = &outcome {
                            self.spawn_detached_auth_invalidation(error);
                        }
                        return self.finish(outcome).await;
                    }
                    Err(error) => error,
                }
            };

            if !reconnect_auth_retry {
                self.spawn_detached_auth_invalidation(&active_error);
            }

            if let Some(close_request) = self.close.request() {
                if !active_was_drained {
                    if let (Some(active_response), Some(active_request)) =
                        (response_stream.as_mut(), request_body.as_ref())
                    {
                        let selected = Err(active_error.clone());
                        match self
                            .ack_processor
                            .close_active_connection(
                                active_response,
                                active_request,
                                &mut close_rx,
                                close_request,
                                Some(selected),
                            )
                            .await
                        {
                            Ok(AckProcessOutcome::Close { request, outcome }) => {
                                debug_assert_eq!(self.close.request(), Some(request));
                                return self.finish(outcome).await;
                            }
                            Ok(AckProcessOutcome::Stopped) => {
                                return self.finalized_result();
                            }
                            Ok(AckProcessOutcome::Recovery { error, .. }) | Err(error) => {
                                return self.finish(Err(error)).await;
                            }
                        }
                    }
                }
                return self.finish(Err(active_error)).await;
            }

            match active_error {
                error
                    if (error.is_retryable() || reconnect_auth_retry) && self.options.recovery =>
                {
                    reconnect_auth_retry = false;
                    let attempts = self.recovery_attempts.fetch_add(1, Ordering::Relaxed);
                    if attempts >= self.options.recovery_retries {
                        error!(target: super::LOG_TARGET,
                            attempts,
                            max_retries = self.options.recovery_retries,
                            "Supervisor: Max recovery retries exceeded"
                        );
                        return self.finish(Err(error.clone())).await;
                    }

                    info!(target: super::LOG_TARGET,
                        attempt = attempts + 1,
                        max_retries = self.options.recovery_retries,
                        error = %error,
                        "Supervisor: Attempting recovery after retriable error"
                    );

                    pause_and_detach_sender(&self.ingest_mutex, &self.is_paused, &self.batch_tx)
                        .await;
                    response_stream = None;
                    request_body = None;
                    self.ack_processor.clear_request_send_failure();
                    // Close that cancels this attempt reports its trigger. Once an attempt
                    // failure is accepted below, that failure becomes the next trigger.
                    let recovery_error = error.clone();

                    let close_during_backoff = tokio::select! {
                        biased;
                        request = self.close.wait_for_request(&mut close_rx) => request.is_some(),
                        _ = sleep(Duration::from_millis(self.options.recovery_backoff_ms)) => false,
                    };
                    if close_during_backoff {
                        return self.finish(Err(recovery_error)).await;
                    }
                    if matches!(self.close.state(), CloseState::Finalized(_)) {
                        return self.finalized_result();
                    }

                    self.server_error_tx.send_replace(None);
                    let recovery_deadline = match configured_deadline(
                        Instant::now(),
                        Duration::from_millis(self.options.recovery_timeout_ms),
                        "recovery_timeout_ms",
                    ) {
                        Ok(deadline) => deadline,
                        Err(deadline_error) => {
                            if self.close.has_started() {
                                return self.finish(Err(recovery_error)).await;
                            }
                            pending_error = Some(deadline_error);
                            continue;
                        }
                    };

                    // A ready attempt wins a simultaneous close. If sender commit completed,
                    // the replacement is active and receives the normal graceful-close path.
                    let reconnect_result = tokio::select! {
                        biased;
                        result = self.reconnect() => Some(result),
                        request = self.close.wait_for_request(&mut close_rx) => {
                            if request.is_some() {
                                return self.finish(Err(recovery_error)).await;
                            }
                            return self.finalized_result();
                        }
                        _ = sleep_until(recovery_deadline) => None,
                    };

                    match reconnect_result {
                        Some(Ok(Some(connection))) => {
                            info!(target: super::LOG_TARGET, "Supervisor: Recovery successful, resuming");
                            self.recovery_attempts.store(0, Ordering::Relaxed);
                            let (new_response_stream, new_request_body) =
                                connection.into_supervisor_io();
                            response_stream = Some(new_response_stream);
                            request_body = Some(new_request_body);
                        }
                        Some(Ok(None)) => {
                            return self.finish(Err(recovery_error)).await;
                        }
                        None => {
                            warn!(target: super::LOG_TARGET, "Supervisor: Reconnection timed out");
                            pending_error = Some(ZerobusError::ConnectionTimeout(format!(
                                "Reconnection timed out after {}ms",
                                self.options.recovery_timeout_ms
                            )));
                        }
                        Some(Err(reconnect_error)) => {
                            if reconnect_error.is_auth_rejection() {
                                let mut invalidation =
                                    self.spawn_headers_invalidation(recovery_deadline);
                                let invalidated = tokio::select! {
                                    biased;
                                    request = self.close.wait_for_request(&mut close_rx) => {
                                        if request.is_some() {
                                            return self.finish(Err(recovery_error)).await;
                                        }
                                        return self.finalized_result();
                                    }
                                    result = &mut invalidation => result,
                                };
                                match invalidated {
                                    Ok(true) => reconnect_auth_retry = true,
                                    Ok(false) | Err(_) => {
                                        return self.finish(Err(reconnect_error)).await;
                                    }
                                }
                            } else if self.close.has_started() {
                                return self.finish(Err(recovery_error)).await;
                            }
                            pending_error = Some(reconnect_error);
                        }
                    }
                }
                error => {
                    error!(target: super::LOG_TARGET, "Supervisor: Non-retriable error, closing stream: {}", error);
                    return self.finish(Err(error)).await;
                }
            }
        }
    }

    /// Completes setup, READY, replay, and sender publication as one cancellable attempt.
    /// Cancellation before publication drops an established replacement best-effort.
    async fn reconnect(&self) -> ZerobusResult<Option<FlightConnection>> {
        let parameters = FlightConnectionParameters {
            endpoint: &self.endpoint,
            tls_config: &self.tls_config,
            connector_factory: self.connector_factory.as_ref(),
            table_properties: &self.table_properties,
            options: &self.options,
            headers_provider: &self.headers_provider,
            sdk_identifier: &self.sdk_identifier,
            request_bodies: &self.request_bodies,
            #[cfg(feature = "test-hooks")]
            test_hooks: &self.test_hooks,
        };
        let connection = ZerobusArrowStream::reconnect_transport(&parameters).await?;
        let tx = connection.sender();
        let acked_before_disconnect = self.last_acked_records.load(Ordering::Acquire);

        if self.replay_and_commit(&tx, acked_before_disconnect).await? {
            Ok(Some(connection))
        } else {
            Ok(None)
        }
    }

    async fn replay_and_commit(
        &self,
        tx: &mpsc::Sender<Result<RecordBatch, FlightError>>,
        acked_before_disconnect: u64,
    ) -> ZerobusResult<bool> {
        #[cfg(feature = "test-hooks")]
        {
            let barrier = self.test_hooks.reconnect_rebuild.lock().await.take();
            if let Some(barrier) = barrier {
                barrier.reached.notify_one();
                barrier.proceed.notified().await;
            }
        }

        let replay_batches = {
            let _ingest_guard = self.ingest_mutex.lock().await;
            if self.close.has_started() {
                return Ok(false);
            }
            Self::prepare_pending_replay(
                &self.pending_batches,
                &self.cumulative_records_assigned,
                &self.submitted_records,
                &self.last_acked_records,
                acked_before_disconnect,
            )
            .await
        };

        if !Self::send_replay_batches(
            tx,
            replay_batches,
            &self.submitted_records,
            &self.ingest_mutex,
            &self.close,
            #[cfg(feature = "test-hooks")]
            Some(&self.test_hooks.replay_send),
        )
        .await?
        {
            return Ok(false);
        }

        loop {
            let buffered = {
                let ingest_guard = self.ingest_mutex.lock().await;
                if self.close.has_started() {
                    return Ok(false);
                }
                let submitted = self.submitted_records.load(Ordering::Acquire);
                let buffered = self
                    .pending_batches
                    .lock()
                    .await
                    .iter()
                    .find_map(|batch| batch.unacknowledged_suffix(submitted));
                if buffered.is_none() {
                    return Ok(Self::commit_reconnect(
                        tx.clone(),
                        &self.pending_batches,
                        &self.batch_tx,
                        &self.is_paused,
                        &self.close,
                        &ingest_guard,
                    )
                    .await);
                }
                buffered
            };

            if !Self::send_replay_batch(
                tx,
                buffered.expect("buffered batch was selected"),
                &self.submitted_records,
                &self.ingest_mutex,
                &self.close,
                "Failed to replay buffered batch during recovery",
            )
            .await?
            {
                return Ok(false);
            }
        }
    }

    async fn commit_reconnect(
        tx: mpsc::Sender<Result<RecordBatch, FlightError>>,
        pending_batches: &Arc<Mutex<Vec<PendingBatch>>>,
        batch_tx: &BatchSender,
        is_paused: &AtomicBool,
        close: &CloseCoordinator,
        _ingest_guard: &tokio::sync::MutexGuard<'_, ()>,
    ) -> bool {
        if close.has_started() {
            return false;
        }
        let mut pending = pending_batches.lock().await;
        let mut sender = batch_tx.lock().await;
        refresh_pending_ack_deadlines(&mut pending, Instant::now());
        *sender = Some(tx);
        is_paused.store(false, Ordering::Relaxed);
        true
    }

    async fn prepare_pending_replay(
        pending_batches: &Arc<Mutex<Vec<PendingBatch>>>,
        cumulative_records_assigned: &Arc<AtomicU64>,
        submitted_records: &Arc<AtomicU64>,
        last_acked_records: &Arc<AtomicU64>,
        acked_before_disconnect: u64,
    ) -> Vec<RecordBatch> {
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
        cumulative_records_assigned.store(new_cumulative, Ordering::Relaxed);
        submitted_records.store(0, Ordering::Release);
        last_acked_records.store(0, Ordering::Release);
        replay
    }

    async fn send_replay_batches(
        tx: &mpsc::Sender<Result<RecordBatch, FlightError>>,
        replay_batches: Vec<RecordBatch>,
        submitted_records: &Arc<AtomicU64>,
        ingest_mutex: &Arc<Mutex<()>>,
        close: &CloseCoordinator,
        #[cfg(feature = "test-hooks")] replay_send_gate: Option<&super::TestBarrierGate>,
    ) -> ZerobusResult<bool> {
        for batch in replay_batches {
            if !Self::send_replay_batch(
                tx,
                batch,
                submitted_records,
                ingest_mutex,
                close,
                "Failed to replay batch during recovery",
            )
            .await?
            {
                return Ok(false);
            }

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
        Ok(true)
    }

    async fn send_replay_batch(
        tx: &mpsc::Sender<Result<RecordBatch, FlightError>>,
        batch: RecordBatch,
        submitted_records: &Arc<AtomicU64>,
        ingest_mutex: &Arc<Mutex<()>>,
        close: &CloseCoordinator,
        failure_message: &'static str,
    ) -> ZerobusResult<bool> {
        let permit = tx.reserve().await.map_err(|_| {
            ZerobusError::StreamClosedError(tonic::Status::internal(failure_message))
        })?;
        // Capacity waits stay outside the ingest lock. The close check and handoff share
        // that lock with publication, ordering each replay batch wholly before or after it.
        let _ingest_guard = ingest_mutex.lock().await;
        if close.has_started() {
            return Ok(false);
        }
        submitted_records.fetch_add(batch.num_rows() as u64, Ordering::Release);
        permit.send(Ok(batch));
        Ok(true)
    }
}
#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;

    use arrow_array::Int32Array;
    use arrow_flight::error::FlightError;
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use tokio::sync::{mpsc, Mutex, Semaphore};
    use tokio::task::JoinHandle;
    use tokio::time::{timeout, Duration, Instant};

    use super::super::close::{CloseCoordinator, CloseFinalizer, CloseRequest, CloseState};
    #[cfg(feature = "internal-arrow-c-data")]
    use super::SupervisorTaskHandle;
    use super::{
        pause_and_detach_sender, BatchSender, PendingBatch, RecordBatch, Supervisor, ZerobusError,
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

    #[test]
    fn non_finalized_supervisor_exit_is_an_invariant_error() {
        let request = CloseRequest {
            target_offset: Some(0),
            deadline: Instant::now() + Duration::from_secs(1),
        };

        for state in [CloseState::Open, CloseState::Requested(request)] {
            assert!(matches!(
                Supervisor::result_from_close_state(state),
                Err(ZerobusError::InvalidStateError(_))
            ));
        }
        assert!(Supervisor::result_from_close_state(CloseState::Finalized(Ok(()))).is_ok());
    }

    #[test]
    fn reaper_preserves_worker_error_and_rejects_unfinalized_success() {
        let returned = ZerobusError::ConnectionTimeout("worker error".to_string());
        assert!(matches!(
            Supervisor::unfinalized_exit_outcome(Ok(Err(returned))),
            Err(ZerobusError::ConnectionTimeout(message)) if message == "worker error"
        ));
        assert!(matches!(
            Supervisor::unfinalized_exit_outcome(Ok(Ok(()))),
            Err(ZerobusError::InvalidStateError(_))
        ));
    }

    #[cfg(feature = "internal-arrow-c-data")]
    #[tokio::test]
    #[should_panic(expected = "Arrow supervisor reaper failed during shutdown")]
    async fn abort_and_wait_propagates_reaper_failure() {
        let worker = tokio::spawn(std::future::pending::<()>());
        let worker_abort = worker.abort_handle();
        drop(worker);
        let reaper = tokio::spawn(async { panic!("reaper test panic") });

        SupervisorTaskHandle {
            worker: worker_abort,
            reaper,
        }
        .abort_and_wait()
        .await;
    }

    #[tokio::test]
    async fn panicked_supervisor_exit_is_an_invariant_error() {
        let worker: JoinHandle<crate::ZerobusResult<()>> =
            tokio::spawn(async { panic!("supervisor test panic") });
        let joined = worker.await;

        assert!(matches!(
            Supervisor::unfinalized_exit_outcome(joined),
            Err(ZerobusError::InvalidStateError(_))
        ));
    }

    #[tokio::test]
    async fn close_publication_precedes_queued_replay_handoff() {
        let schema = one_col_schema();
        let ingest_mutex = Arc::new(Mutex::new(()));
        let close = CloseCoordinator::new();
        let submitted = Arc::new(AtomicU64::new(0));
        let (tx, mut rx) = mpsc::channel::<Result<RecordBatch, FlightError>>(1);
        let request = CloseRequest {
            target_offset: Some(0),
            deadline: Instant::now() + Duration::from_secs(30),
        };

        let guard = ingest_mutex.lock().await;
        let publish = async {
            let _guard = ingest_mutex.lock().await;
            close.publish(request);
        };
        tokio::pin!(publish);
        assert!(futures::poll!(publish.as_mut()).is_pending());

        let send = Supervisor::send_replay_batch(
            &tx,
            batch_with_rows(&schema, 1),
            &submitted,
            &ingest_mutex,
            &close,
            "replay failed",
        );
        tokio::pin!(send);
        assert!(futures::poll!(send.as_mut()).is_pending());
        assert_eq!(tx.capacity(), 0, "the replay send must reserve capacity");

        drop(guard);
        timeout(Duration::from_secs(1), publish)
            .await
            .expect("close publication should acquire the mutex first");
        assert!(close.has_started());
        assert!(
            !timeout(Duration::from_secs(1), send)
                .await
                .expect("replay send should resume after close publication")
                .expect("replay send should not fail"),
            "a published close must reject the queued replay handoff"
        );
        assert_eq!(submitted.load(Ordering::Acquire), 0);
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    async fn replay_pending_batches(
        tx: &mpsc::Sender<Result<RecordBatch, FlightError>>,
        pending_batches: &Arc<Mutex<Vec<PendingBatch>>>,
        cumulative_records_assigned: &Arc<AtomicU64>,
        submitted_records: &Arc<AtomicU64>,
        last_acked_records: &Arc<AtomicU64>,
        acked_before_disconnect: u64,
    ) -> crate::ZerobusResult<()> {
        let ingest_mutex = Arc::new(Mutex::new(()));
        let close = CloseCoordinator::new();
        let replay_batches = Supervisor::prepare_pending_replay(
            pending_batches,
            cumulative_records_assigned,
            submitted_records,
            last_acked_records,
            acked_before_disconnect,
        )
        .await;
        let sent = Supervisor::send_replay_batches(
            tx,
            replay_batches,
            submitted_records,
            &ingest_mutex,
            &close,
            #[cfg(feature = "test-hooks")]
            None,
        )
        .await?;
        assert!(sent, "close was not published in the unit helper");
        Ok(())
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

        let res =
            replay_pending_batches(&tx, &pending, &cumulative, &submitted, &last_acked, 0).await;
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

        let res =
            replay_pending_batches(&tx, &pending, &cumulative, &submitted, &last_acked, 4).await;
        assert!(res.is_ok());

        // Only the partially-acked batch remains, rebuilt from cumulative 0.
        let guard = pending.lock().await;
        assert_eq!(guard.len(), 1);
        assert_eq!(guard[0].record_count(), 2);
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

        let fut = CloseFinalizer::finalize_closed(
            &ingest_mutex,
            &is_closed,
            &pending,
            &failed,
            &last_acked,
        );
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

    #[tokio::test]
    async fn published_close_prevents_sender_commit() {
        let close = CloseCoordinator::new();
        let request = CloseRequest {
            target_offset: Some(7),
            deadline: Instant::now() + Duration::from_secs(30),
        };
        close.publish(request);

        let (tx, _rx) = mpsc::channel(1);
        let pending = Arc::new(Mutex::new(Vec::new()));
        let batch_tx: BatchSender = Arc::new(Mutex::new(None));
        let is_paused = AtomicBool::new(true);
        let ingest_mutex = Mutex::new(());
        let ingest_guard = ingest_mutex.lock().await;
        assert!(
            !Supervisor::commit_reconnect(
                tx,
                &pending,
                &batch_tx,
                &is_paused,
                &close,
                &ingest_guard,
            )
            .await
        );
        assert!(batch_tx.lock().await.is_none());
        assert!(is_paused.load(Ordering::Relaxed));
    }
}
