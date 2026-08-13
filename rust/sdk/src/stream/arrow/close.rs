use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use tokio::sync::{watch, Mutex};
use tokio::time::Instant;

use crate::errors::ZerobusError;
use crate::offset_generator::OffsetId;
use crate::ZerobusResult;

use super::batch::PendingBatch;
use super::{BatchSender, RecordBatch, ZerobusArrowStream};

/// The immutable target and deadline selected by the first `close()` call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct CloseRequest {
    pub(super) target_offset: Option<OffsetId>,
    pub(super) deadline: Instant,
}

/// Shared close publication and completion state.
#[derive(Clone, Debug)]
pub(super) enum CloseState {
    Open,
    Requested(CloseRequest),
    Finalized(ZerobusResult<()>),
}

impl CloseState {
    pub(super) fn request(&self) -> Option<CloseRequest> {
        match self {
            Self::Requested(request) => Some(*request),
            Self::Open | Self::Finalized(_) => None,
        }
    }
}

#[derive(Clone)]
pub(super) struct CloseCoordinator {
    state_tx: watch::Sender<CloseState>,
    ack_state: Arc<StdMutex<CloseAckState>>,
}

#[derive(Default)]
struct CloseAckState {
    // Close snapshots the highest assigned offset, so the latest watermark time is sufficient.
    latest: Option<(OffsetId, Instant)>,
    target_reached_timely: bool,
}

impl CloseCoordinator {
    pub(super) fn new() -> Self {
        let (state_tx, _state_rx) = watch::channel(CloseState::Open);
        Self {
            state_tx,
            ack_state: Arc::new(StdMutex::new(CloseAckState::default())),
        }
    }

    pub(super) fn state(&self) -> CloseState {
        self.state_tx.borrow().clone()
    }

    pub(super) fn subscribe(&self) -> watch::Receiver<CloseState> {
        self.state_tx.subscribe()
    }

    pub(super) fn has_started(&self) -> bool {
        !matches!(*self.state_tx.borrow(), CloseState::Open)
    }

    pub(super) fn request(&self) -> Option<CloseRequest> {
        self.state_tx.borrow().request()
    }

    /// Publishes only the first request. The caller holds `ingest_mutex`.
    pub(super) fn publish(&self, request: CloseRequest) {
        // This lock orders publication with ACK application, covering either winner
        // without relying on when the supervisor later observes the request.
        let mut ack_state = self.ack_state.lock().expect("close ACK state poisoned");
        self.state_tx.send_if_modified(|state| {
            if matches!(state, CloseState::Open) {
                ack_state.target_reached_timely = request.target_offset.is_some_and(|target| {
                    ack_state.latest.is_some_and(|(offset, applied_at)| {
                        offset >= target && applied_at < request.deadline
                    })
                });
                *state = CloseState::Requested(request);
                true
            } else {
                false
            }
        });
    }

    /// Records when a durable ACK was fully applied and latches timely target completion.
    pub(super) fn observe_ack(&self, offset: OffsetId, applied_at: Instant) {
        let mut ack_state = self.ack_state.lock().expect("close ACK state poisoned");
        match ack_state.latest {
            Some((latest_offset, _)) if latest_offset > offset => {}
            Some((latest_offset, latest_at)) if latest_offset == offset => {
                ack_state.latest = Some((offset, latest_at.min(applied_at)));
            }
            _ => ack_state.latest = Some((offset, applied_at)),
        }

        if let CloseState::Requested(request) = *self.state_tx.borrow() {
            if request
                .target_offset
                .is_some_and(|target| offset >= target && applied_at < request.deadline)
            {
                ack_state.target_reached_timely = true;
            }
        }
    }

    pub(super) fn target_reached_timely(&self) -> bool {
        self.ack_state
            .lock()
            .expect("close ACK state poisoned")
            .target_reached_timely
    }

    /// Waits for request publication. `None` means finalization won the observation.
    pub(super) async fn wait_for_request(
        &self,
        close_rx: &mut watch::Receiver<CloseState>,
    ) -> Option<CloseRequest> {
        loop {
            match close_rx.borrow_and_update().clone() {
                CloseState::Requested(request) => return Some(request),
                CloseState::Finalized(_) => return None,
                CloseState::Open => {}
            }
            if close_rx.changed().await.is_err() {
                return None;
            }
        }
    }

    /// Publishes the result once; later calls cannot replace the finalized outcome.
    fn publish_finalized(&self, outcome: ZerobusResult<()>) {
        self.state_tx.send_if_modified(|state| {
            if matches!(state, CloseState::Finalized(_)) {
                false
            } else {
                *state = CloseState::Finalized(outcome);
                true
            }
        });
    }

    pub(super) fn flush_timeout_error() -> ZerobusError {
        ZerobusError::StreamClosedError(tonic::Status::deadline_exceeded("Flush timed out"))
    }
}

/// Performs the one terminal state transition shared by close and failure paths.
#[derive(Clone)]
pub(super) struct CloseFinalizer {
    close: CloseCoordinator,
    ingest_mutex: Arc<Mutex<()>>,
    batch_tx: BatchSender,
    is_paused: Arc<AtomicBool>,
    admission_closed: Arc<AtomicBool>,
    is_closed: Arc<AtomicBool>,
    pending_batches: Arc<Mutex<Vec<PendingBatch>>>,
    failed_batches: Arc<Mutex<Vec<RecordBatch>>>,
    last_acked_records: Arc<AtomicU64>,
    server_error_tx: watch::Sender<Option<ZerobusError>>,
    #[cfg(feature = "test-hooks")]
    test_hooks: Arc<super::TestHooks>,
}

impl CloseFinalizer {
    pub(super) fn new(stream: &ZerobusArrowStream) -> Self {
        Self {
            close: stream.close.clone(),
            ingest_mutex: Arc::clone(&stream.ingest_mutex),
            batch_tx: Arc::clone(&stream.batch_tx),
            is_paused: Arc::clone(&stream.is_paused),
            admission_closed: Arc::clone(&stream.admission_closed),
            is_closed: Arc::clone(&stream.is_closed),
            pending_batches: Arc::clone(&stream.pending_batches),
            failed_batches: Arc::clone(&stream.failed_batches),
            last_acked_records: Arc::clone(&stream.last_acked_records),
            server_error_tx: stream.server_error_tx.clone(),
            #[cfg(feature = "test-hooks")]
            test_hooks: Arc::clone(&stream.test_hooks),
        }
    }

    pub(super) async fn finish(&self, outcome: ZerobusResult<()>) -> ZerobusResult<()> {
        {
            let _guard = self.ingest_mutex.lock().await;
            if let CloseState::Finalized(existing) = self.close.state() {
                return existing;
            }
            // `is_closed` remains false until the retained-batch snapshot is complete.
            // Release: empty flush loads this flag without ingest_mutex.
            self.admission_closed.store(true, Ordering::Release);
            self.is_paused.store(true, Ordering::Relaxed);
            *self.batch_tx.lock().await = None;
        }

        self.server_error_tx
            .send_replace(outcome.as_ref().err().cloned());

        #[cfg(feature = "test-hooks")]
        {
            let barrier = self.test_hooks.close_finalize.lock().await.take();
            if let Some(barrier) = barrier {
                barrier.reached.notify_one();
                barrier.proceed.notified().await;
            }
        }

        Self::finalize_closed(
            &self.ingest_mutex,
            &self.is_closed,
            &self.pending_batches,
            &self.failed_batches,
            &self.last_acked_records,
        )
        .await;
        self.close.publish_finalized(outcome.clone());
        self.server_error_tx
            .send_replace(outcome.as_ref().err().cloned());
        outcome
    }

    pub(super) async fn move_pending_to_failed(
        pending_batches: &Arc<Mutex<Vec<PendingBatch>>>,
        failed_batches: &Arc<Mutex<Vec<RecordBatch>>>,
        last_acked_records: &Arc<AtomicU64>,
    ) {
        let mut failed = failed_batches.lock().await;
        let mut pending = pending_batches.lock().await;
        let acked = last_acked_records.load(Ordering::Acquire);
        for batch in pending.drain(..) {
            if let Some(batch) = batch.unacknowledged_suffix(acked) {
                failed.push(batch);
            }
        }
    }

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
    use super::{CloseCoordinator, CloseRequest, CloseState};
    use crate::errors::ZerobusError;
    use tokio::time::{Duration, Instant};

    fn request(target_offset: Option<i64>) -> CloseRequest {
        CloseRequest {
            target_offset,
            deadline: Instant::now() + Duration::from_secs(1),
        }
    }

    #[test]
    fn first_close_request_is_sticky() {
        let close = CloseCoordinator::new();
        let first = request(Some(4));
        close.publish(first);
        close.publish(request(Some(9)));

        assert_eq!(close.request(), Some(first));
    }

    #[test]
    fn ack_before_request_is_latched_timely() {
        let close = CloseCoordinator::new();
        let applied_at = Instant::now();
        close.observe_ack(4, applied_at);
        close.publish(CloseRequest {
            target_offset: Some(4),
            deadline: applied_at + Duration::from_secs(1),
        });

        assert!(close.target_reached_timely());
    }

    #[test]
    fn request_before_ack_is_latched_timely() {
        let close = CloseCoordinator::new();
        let requested_at = Instant::now();
        let deadline = requested_at + Duration::from_secs(1);
        close.publish(CloseRequest {
            target_offset: Some(4),
            deadline,
        });
        close.observe_ack(4, requested_at + Duration::from_millis(500));

        assert!(close.target_reached_timely());
    }

    #[test]
    fn ack_at_deadline_is_not_latched_timely() {
        let close = CloseCoordinator::new();
        let deadline = Instant::now();
        close.publish(CloseRequest {
            target_offset: Some(4),
            deadline,
        });
        close.observe_ack(4, deadline);

        assert!(!close.target_reached_timely());
    }

    #[tokio::test]
    async fn wait_observes_requested_and_finalized_states() {
        let close = CloseCoordinator::new();
        let requested = request(None);
        let mut requested_rx = close.subscribe();
        close.publish(requested);
        assert_eq!(
            close.wait_for_request(&mut requested_rx).await,
            Some(requested)
        );

        let finalized = CloseCoordinator::new();
        let mut finalized_rx = finalized.subscribe();
        finalized.publish_finalized(Err(ZerobusError::InvalidStateError("terminal".to_string())));
        assert!(finalized
            .wait_for_request(&mut finalized_rx)
            .await
            .is_none());
    }

    #[test]
    fn finalized_outcome_cannot_be_replaced() {
        let close = CloseCoordinator::new();
        close.publish_finalized(Ok(()));
        close.publish_finalized(Err(ZerobusError::InvalidStateError(
            "replacement".to_string(),
        )));

        assert!(matches!(close.state(), CloseState::Finalized(Ok(()))));
    }
}
