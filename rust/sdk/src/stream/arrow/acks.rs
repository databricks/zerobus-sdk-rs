//! ACK processing and server-requested stream rotation.
//!
//! ACK watermarks are monotonic and bounded by records submitted on the active
//! connection. Rotation preserves permanent peer/protocol errors while draining.

use std::mem::replace;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use arrow_flight::error::FlightError;
use arrow_flight::PutResult;
use futures::{FutureExt, StreamExt};
use tokio::sync::{watch, Mutex, Notify};
use tokio::time::{sleep_until, Duration, Instant};
use tracing::{debug, error, info, warn};

use super::batch::{
    oldest_pending_ack_deadline, PendingAckDeadline, PendingBatch, PendingBatchIdentity,
};
use super::close::{CloseCoordinator, CloseRequest, CloseState};
use super::connection::{FlightResponseStream, RequestBodyControl};
use super::metadata::FlightAckMetadata;
use super::{ArrowStreamConfigurationOptions, BatchSender, ZerobusArrowStream};
#[cfg(feature = "test-hooks")]
use super::{TestHooks, TestNotifyGate};
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
    close: CloseCoordinator,
    #[cfg(feature = "test-hooks")]
    test_hooks: Arc<TestHooks>,
}

/// State captured when rotation stops waiting for acknowledgments and begins transport
/// cleanup. The response may already have ended.
struct DrainState {
    /// Hard cutoff shared by request EOF observation and response draining.
    deadline: Instant,
    /// Whether the response ended before the request entered the drain helper.
    response_finished: bool,
    /// Peer or protocol error to preserve while the remaining transport settles.
    terminal_error: Option<ZerobusError>,
    /// Explicit close being finalized by this drain, if any.
    close: Option<CloseDrain>,
}

struct CloseDrain {
    request: CloseRequest,
    /// The first observed ACK, concrete error, or deadline result.
    /// This remains unset only for an empty close target.
    selected: Option<ZerobusResult<()>>,
}

impl CloseDrain {
    fn new(request: CloseRequest, selected: Option<ZerobusResult<()>>) -> Self {
        Self { request, selected }
    }

    fn selected(request: CloseRequest, outcome: ZerobusResult<()>) -> Self {
        Self::new(request, Some(outcome))
    }

    fn empty_target(request: CloseRequest) -> Self {
        Self::new(request, None)
    }

    fn flush_timeout(request: CloseRequest) -> Self {
        Self::selected(request, Err(CloseCoordinator::flush_timeout_error()))
    }

    fn rotation_interrupted(request: CloseRequest) -> Self {
        Self::selected(request, Err(AckProcessor::rotation_error()))
    }
}

impl DrainState {
    fn explicit_close(deadline: Instant, response_finished: bool, close: CloseDrain) -> Self {
        Self {
            deadline,
            response_finished,
            terminal_error: None,
            close: Some(close),
        }
    }

    fn rotation_drain(
        deadline: Instant,
        response_finished: bool,
        terminal_error: Option<ZerobusError>,
    ) -> Self {
        Self {
            deadline,
            response_finished,
            terminal_error,
            close: None,
        }
    }

    fn close_during_rotation(deadline: Instant, close: CloseDrain) -> Self {
        Self {
            deadline,
            response_finished: false,
            terminal_error: None,
            close: Some(close),
        }
    }
}

pub(super) enum AckProcessOutcome {
    Stopped,
    Recovery {
        error: ZerobusError,
        drained: bool,
    },
    Close {
        request: CloseRequest,
        outcome: ZerobusResult<()>,
    },
}

enum WaitState {
    Rotation {
        target_records: u64,
        deadlines: RotationDeadlines,
    },
    Close(CloseRequest),
}

/// One connection lifecycle covers active traffic, ACK waiting, and transport drain.
enum ConnectionState {
    Active,
    Waiting(WaitState),
    Draining(DrainState),
}

/// Connection context for mapping a terminal event into transport drain.
enum TerminalDrainTarget {
    Active,
    Rotation {
        deadline: Instant,
    },
    Close {
        request: CloseRequest,
        deadline: Instant,
    },
}

/// Terminal condition observed before transport drain begins.
enum TerminalEvent {
    RequestSendFailed,
    AckApplyFailed,
    ResponseError,
    ResponseEof,
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
    CloseRequested(CloseRequest),
    CloseFinalized,
    CloseDeadline,
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

    fn is_pending(&self) -> bool {
        self.pending.load(Ordering::Acquire)
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
    close: &'a CloseCoordinator,
    #[cfg(feature = "test-hooks")]
    ack_applied_gate: &'a TestNotifyGate,
}

impl AckProgress<'_> {
    /// Validates an ACK against the active connection, advances the monotonic durable
    /// watermark, removes fully acknowledged batches, and wakes completed offset waiters.
    /// Its close-deadline time is captured after durable state is applied, before notifications.
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
        let applied_at = Instant::now();

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
            self.close.observe_ack(offset, applied_at);
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
    /// Atomically stops new sends, detaches queued work, and shuts down the request body.
    /// The caller then drains the response and final status under its bounded deadline.
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
            close: stream.close.clone(),
            #[cfg(feature = "test-hooks")]
            test_hooks: Arc::clone(&stream.test_hooks),
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
            close: CloseCoordinator::new(),
            #[cfg(feature = "test-hooks")]
            test_hooks: Arc::new(TestHooks::default()),
        };
        (
            processor,
            RequestBodyControl::completed_for_test(),
            last_ack_rx,
        )
    }

    pub(super) fn rotation_error() -> ZerobusError {
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

    fn explicit_close_drain_deadline() -> Instant {
        let now = Instant::now();
        now.checked_add(Duration::from_millis(ROTATION_DRAIN_TIMEOUT_MS))
            .unwrap_or(now)
    }

    fn close_target_is_acknowledged(&self, request: CloseRequest) -> bool {
        request.target_offset.is_some_and(|target| {
            self.last_ack_tx
                .borrow()
                .is_some_and(|acknowledged| acknowledged >= target)
        })
    }

    fn acknowledged_close_outcome(&self) -> ZerobusResult<()> {
        if self.close.target_reached_timely() {
            Ok(())
        } else {
            Err(CloseCoordinator::flush_timeout_error())
        }
    }

    fn drain_for_acknowledged_close(
        request: CloseRequest,
        outcome: ZerobusResult<()>,
        deadline: Instant,
    ) -> ConnectionState {
        ConnectionState::Draining(DrainState::explicit_close(
            deadline,
            false,
            CloseDrain::selected(request, outcome),
        ))
    }

    fn begin_close(
        &self,
        connection: &ConnectionState,
        close_request: CloseRequest,
    ) -> ConnectionState {
        match connection {
            ConnectionState::Active if self.close_target_is_acknowledged(close_request) => {
                let outcome = self.acknowledged_close_outcome();
                let deadline = Self::explicit_close_drain_deadline();
                Self::drain_for_acknowledged_close(close_request, outcome, deadline)
            }
            ConnectionState::Active if close_request.target_offset.is_none() => {
                let deadline = Self::explicit_close_drain_deadline();
                ConnectionState::Draining(DrainState::explicit_close(
                    deadline,
                    false,
                    CloseDrain::empty_target(close_request),
                ))
            }
            ConnectionState::Active => ConnectionState::Waiting(WaitState::Close(close_request)),
            ConnectionState::Waiting(WaitState::Rotation { deadlines, .. }) => {
                let deadline = Self::bounded_rotation_drain_deadline(deadlines.drain);
                ConnectionState::Draining(DrainState::close_during_rotation(
                    deadline,
                    CloseDrain::rotation_interrupted(close_request),
                ))
            }
            ConnectionState::Waiting(WaitState::Close(_)) | ConnectionState::Draining(_) => {
                unreachable!()
            }
        }
    }

    fn drain_from_waiting(
        target: TerminalDrainTarget,
        event: TerminalEvent,
        error: ZerobusError,
    ) -> ZerobusResult<ConnectionState> {
        match target {
            TerminalDrainTarget::Active => Err(error),
            TerminalDrainTarget::Rotation { deadline } => {
                let (response_finished, terminal_error) = match event {
                    TerminalEvent::RequestSendFailed | TerminalEvent::AckApplyFailed => {
                        (false, Some(error))
                    }
                    TerminalEvent::ResponseError => (true, Some(error)),
                    // Rotation already has its trigger; EOF only marks the response complete.
                    TerminalEvent::ResponseEof => (true, None),
                };
                Ok(ConnectionState::Draining(DrainState::rotation_drain(
                    deadline,
                    response_finished,
                    terminal_error,
                )))
            }
            TerminalDrainTarget::Close { request, deadline } => {
                let response_finished = matches!(
                    event,
                    TerminalEvent::ResponseError | TerminalEvent::ResponseEof
                );
                Ok(ConnectionState::Draining(DrainState::explicit_close(
                    deadline,
                    response_finished,
                    CloseDrain::selected(request, Err(error)),
                )))
            }
        }
    }

    fn finish_drain(
        terminal_error: Option<ZerobusError>,
        close: Option<CloseDrain>,
    ) -> ZerobusResult<AckProcessOutcome> {
        match close {
            Some(CloseDrain {
                request,
                selected: Some(outcome),
            }) => Ok(AckProcessOutcome::Close { request, outcome }),
            Some(CloseDrain {
                request,
                selected: None,
            }) => Ok(AckProcessOutcome::Close {
                request,
                outcome: terminal_error.map_or(Ok(()), Err),
            }),
            None => Ok(AckProcessOutcome::Recovery {
                error: terminal_error.unwrap_or_else(Self::rotation_error),
                drained: true,
            }),
        }
    }

    /// Half-closes the active request and drains the response under a shared deadline.
    /// Late acknowledgments update retained suffixes but cannot replace a selected result.
    async fn half_close_and_drain_response(
        &self,
        response_stream: &mut FlightResponseStream,
        request: RequestControl<'_>,
        acknowledgments: AckProgress<'_>,
        close_rx: &mut watch::Receiver<CloseState>,
        mut observe_close: bool,
        state: DrainState,
    ) -> ZerobusResult<AckProcessOutcome> {
        let DrainState {
            deadline,
            mut response_finished,
            mut terminal_error,
            mut close,
        } = state;
        request.half_close().await;
        let mut request_eof = Box::pin(request.request_body.wait_for_eof());
        let mut request_finished = false;

        loop {
            // A continuously ready response must not starve close publication.
            if observe_close {
                if let Some(close_request) = self.close.request() {
                    close = Some(CloseDrain::rotation_interrupted(close_request));
                    observe_close = false;
                }
            }
            if request_finished && response_finished {
                return Self::finish_drain(terminal_error, close);
            }
            if Instant::now() >= deadline {
                return Self::finish_drain(terminal_error, close);
            }

            tokio::select! {
                biased;
                _ = sleep_until(deadline) => {
                    return Self::finish_drain(terminal_error, close);
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
                published = self.close.wait_for_request(close_rx), if observe_close => {
                    match published {
                        Some(request) => {
                            close = Some(CloseDrain::rotation_interrupted(request));
                            observe_close = false;
                        }
                        None => return Ok(AckProcessOutcome::Stopped),
                    }
                }
            }
        }
    }

    fn request_control<'a>(&'a self, request_body: &'a RequestBodyControl) -> RequestControl<'a> {
        RequestControl {
            request_body,
            ingest_mutex: self.ingest_mutex.as_ref(),
            is_paused: self.is_paused.as_ref(),
            batch_tx: &self.batch_tx,
        }
    }

    fn ack_progress(&self) -> AckProgress<'_> {
        AckProgress {
            submitted_records: self.submitted_records.as_ref(),
            last_acked_records: self.last_acked_records.as_ref(),
            pending_batches: self.pending_batches.as_ref(),
            last_ack_tx: &self.last_ack_tx,
            close: &self.close,
            #[cfg(feature = "test-hooks")]
            ack_applied_gate: &self.test_hooks.ack_applied,
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

    async fn wait_for_close_deadline(request: Option<CloseRequest>) {
        match request {
            Some(request) => sleep_until(request.deadline).await,
            None => std::future::pending().await,
        }
    }

    async fn wait_for_close_event(&self, close_rx: &mut watch::Receiver<CloseState>) -> AckEvent {
        match self.close.wait_for_request(close_rx).await {
            Some(request) => AckEvent::CloseRequested(request),
            None => AckEvent::CloseFinalized,
        }
    }

    /// Waits without arming an ACK timeout while no submitted batch is pending.
    async fn wait_while_idle(
        &self,
        response_stream: &mut FlightResponseStream,
        close_rx: &mut watch::Receiver<CloseState>,
        observe_close: bool,
        close_wait: Option<CloseRequest>,
    ) -> AckEvent {
        #[cfg(feature = "test-hooks")]
        if let Some(notify) = self.test_hooks.ack_idle.lock().await.take() {
            notify.notify_one();
        }

        tokio::select! {
            biased;
            response = response_stream.next() => AckEvent::Response(response),
            _ = self.request_send_failure.notify.notified() => AckEvent::RequestSendFailed,
            _ = self.pending_notify.notified() => AckEvent::PendingBatchAvailable,
            event = self.wait_for_close_event(close_rx), if observe_close => event,
            _ = Self::wait_for_close_deadline(close_wait) => AckEvent::CloseDeadline,
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
        close_rx: &mut watch::Receiver<CloseState>,
        observe_close: bool,
        close_wait: Option<CloseRequest>,
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
            event = self.wait_for_close_event(close_rx), if observe_close => event,
            _ = Self::wait_for_close_deadline(close_wait) => AckEvent::CloseDeadline,
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

    /// Shuts down the active request and drains response data and final status for a bounded
    /// interval. Late ACKs still advance the durable watermark.
    pub(super) async fn close_active_connection(
        &self,
        response_stream: &mut FlightResponseStream,
        request_body: &RequestBodyControl,
        close_rx: &mut watch::Receiver<CloseState>,
        request: CloseRequest,
        selected: Option<ZerobusResult<()>>,
    ) -> ZerobusResult<AckProcessOutcome> {
        self.half_close_and_drain_response(
            response_stream,
            self.request_control(request_body),
            self.ack_progress(),
            close_rx,
            false,
            DrainState::explicit_close(
                Self::explicit_close_drain_deadline(),
                false,
                CloseDrain::new(request, selected),
            ),
        )
        .await
    }

    /// Processes acknowledgments and the single server-initiated rotation path.
    ///
    /// Rotation pauses sends and snapshots submitted records, waits only for that
    /// connection-local target, then half-closes the request and drains late responses
    /// before returning a retryable result to the supervisor.
    #[cfg(test)]
    pub(super) async fn process(
        &self,
        mut response_stream: FlightResponseStream,
        request_body: RequestBodyControl,
    ) -> ZerobusResult<()> {
        let mut close_rx = self.close.subscribe();
        match self
            .process_active(&mut response_stream, &request_body, &mut close_rx, false)
            .await
        {
            Ok(AckProcessOutcome::Stopped) => Ok(()),
            Ok(AckProcessOutcome::Recovery { error, .. }) => Err(error),
            Ok(AckProcessOutcome::Close { .. }) => {
                unreachable!("test ACK processor has no close request")
            }
            Err(error) => Err(error),
        }
    }

    /// Borrowing the connection lets the supervisor interrupt normal processing for an
    /// explicit close without dropping either transport half.
    pub(super) async fn process_active(
        &self,
        response_stream: &mut FlightResponseStream,
        request_body: &RequestBodyControl,
        close_rx: &mut watch::Receiver<CloseState>,
        mut observe_close: bool,
    ) -> ZerobusResult<AckProcessOutcome> {
        let ack_timeout = Duration::from_millis(self.options.server_lack_of_ack_timeout_ms);
        let mut connection = ConnectionState::Active;
        let mut expiry_tie_winner: Option<PendingBatchIdentity> = None;
        let mut close_after_priority_response = None;
        let mut response_deferred_send_failure = false;
        let request_control = self.request_control(request_body);
        let acknowledgments = self.ack_progress();

        loop {
            if self.is_closed.load(Ordering::Relaxed) {
                debug!(target: super::LOG_TARGET, "Stream closed, stopping ack processor");
                return Ok(AckProcessOutcome::Stopped);
            }

            if let Some(close_request) = close_after_priority_response.take() {
                connection = self.begin_close(&connection, close_request);
                continue;
            }

            let mut priority_response = None;
            if observe_close {
                if let Some(close_request) = self.close.request() {
                    observe_close = false;
                    if matches!(connection, ConnectionState::Active)
                        && close_request.target_offset.is_none()
                        && !(response_deferred_send_failure
                            && self.request_send_failure.is_pending())
                    {
                        // Give a terminal response that predates an empty close one poll.
                        // A pending or nonterminal response cannot postpone close again.
                        // Skip if the send-failure one-tie already consumed a response.
                        priority_response = response_stream.next().now_or_never();
                        if priority_response.is_some() {
                            close_after_priority_response = Some(close_request);
                        } else {
                            connection = self.begin_close(&connection, close_request);
                            continue;
                        }
                    } else {
                        connection = self.begin_close(&connection, close_request);
                        continue;
                    }
                }
            }

            if let ConnectionState::Waiting(WaitState::Close(close_request)) = &connection {
                if Instant::now() >= close_request.deadline {
                    let deadline = Self::explicit_close_drain_deadline();
                    connection = ConnectionState::Draining(DrainState::explicit_close(
                        deadline,
                        false,
                        CloseDrain::flush_timeout(*close_request),
                    ));
                    continue;
                }
            }

            if let ConnectionState::Waiting(WaitState::Rotation {
                target_records,
                deadlines,
            }) = &connection
            {
                if self.last_acked_records.load(Ordering::Acquire) >= *target_records
                    || Instant::now() >= deadlines.ack
                {
                    let deadline = Self::bounded_rotation_drain_deadline(deadlines.drain);
                    connection = ConnectionState::Draining(DrainState::rotation_drain(
                        deadline, false, None,
                    ));
                    continue;
                }
            }

            if matches!(connection, ConnectionState::Draining(_)) {
                let ConnectionState::Draining(state) =
                    replace(&mut connection, ConnectionState::Active)
                else {
                    unreachable!()
                };
                return self
                    .half_close_and_drain_response(
                        response_stream,
                        request_control,
                        acknowledgments,
                        close_rx,
                        observe_close,
                        state,
                    )
                    .await;
            }

            let close_wait = match &connection {
                ConnectionState::Waiting(WaitState::Close(request)) => Some(*request),
                _ => None,
            };
            let event = if response_deferred_send_failure && self.request_send_failure.is_pending()
            {
                response_deferred_send_failure = false;
                AckEvent::RequestSendFailed
            } else if let Some(response) = priority_response {
                AckEvent::Response(response)
            } else {
                response_deferred_send_failure = false;
                match &connection {
                    ConnectionState::Active => match self.oldest_ack_deadline(ack_timeout).await? {
                        Some(pending_deadline) => {
                            self.wait_with_pending_deadline(
                                response_stream,
                                pending_deadline,
                                &mut expiry_tie_winner,
                                close_rx,
                                observe_close,
                                None,
                            )
                            .await
                        }
                        None => {
                            self.wait_while_idle(response_stream, close_rx, observe_close, None)
                                .await
                        }
                    },
                    ConnectionState::Waiting(WaitState::Rotation { deadlines, .. }) => {
                        tokio::select! {
                            biased;
                            response = response_stream.next() => AckEvent::Response(response),
                            _ = self.request_send_failure.notify.notified() => {
                                AckEvent::RequestSendFailed
                            }
                            _ = sleep_until(deadlines.ack) => continue,
                            event = self.wait_for_close_event(close_rx), if observe_close => event,
                        }
                    }
                    ConnectionState::Waiting(WaitState::Close(_)) => {
                        tokio::select! {
                            biased;
                            response = response_stream.next() => AckEvent::Response(response),
                            _ = self.request_send_failure.notify.notified() => {
                                AckEvent::RequestSendFailed
                            }
                            _ = Self::wait_for_close_deadline(close_wait) => AckEvent::CloseDeadline,
                        }
                    }
                    ConnectionState::Draining(_) => unreachable!(),
                }
            };

            // A ready response wins one tie so its ACK or terminal status is observed.
            // The next loop forces send failure without polling another response.
            if matches!(event, AckEvent::Response(_)) && self.request_send_failure.is_pending() {
                response_deferred_send_failure = true;
            }

            match event {
                AckEvent::PendingBatchAvailable => continue,
                AckEvent::CloseFinalized => return Ok(AckProcessOutcome::Stopped),
                AckEvent::CloseRequested(close_request) => {
                    observe_close = false;
                    connection = self.begin_close(&connection, close_request);
                }
                AckEvent::CloseDeadline => {
                    let close_request =
                        close_wait.expect("close deadline requires a close request");
                    let deadline = Self::explicit_close_drain_deadline();
                    connection = ConnectionState::Draining(DrainState::explicit_close(
                        deadline,
                        false,
                        CloseDrain::flush_timeout(close_request),
                    ));
                }
                AckEvent::RequestSendFailed => {
                    if !self.request_send_failure.take() {
                        continue;
                    }
                    let error = Self::request_send_error();
                    let target = match &connection {
                        ConnectionState::Waiting(WaitState::Rotation { deadlines, .. }) => {
                            TerminalDrainTarget::Rotation {
                                deadline: Self::bounded_rotation_drain_deadline(deadlines.drain),
                            }
                        }
                        ConnectionState::Waiting(WaitState::Close(request)) => {
                            TerminalDrainTarget::Close {
                                request: *request,
                                deadline: Self::explicit_close_drain_deadline(),
                            }
                        }
                        ConnectionState::Active => TerminalDrainTarget::Active,
                        ConnectionState::Draining(_) => unreachable!(),
                    };
                    connection =
                        Self::drain_from_waiting(target, TerminalEvent::RequestSendFailed, error)?;
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
                }
                AckEvent::Response(Some(Ok(put_result))) => {
                    let ack = match FlightAckMetadata::from_bytes(&put_result.app_metadata) {
                        Ok(ack) => ack,
                        Err(error) => {
                            warn!(target: super::LOG_TARGET, "Failed to parse ack metadata: {error}");
                            continue;
                        }
                    };

                    if ack.is_close_signal() && matches!(&connection, ConnectionState::Active) {
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
                        connection = ConnectionState::Waiting(WaitState::Rotation {
                            target_records,
                            deadlines,
                        });
                        info!(target: super::LOG_TARGET,
                            server_duration_ms,
                            target_records, "Server requested graceful stream rotation"
                        );
                    }

                    if ack.ack_up_to_records > 0 {
                        if let Err(error) = acknowledgments.apply(&ack).await {
                            let target = match &connection {
                                ConnectionState::Waiting(WaitState::Rotation {
                                    deadlines, ..
                                }) => TerminalDrainTarget::Rotation {
                                    deadline: Self::bounded_rotation_drain_deadline(
                                        deadlines.drain,
                                    ),
                                },
                                ConnectionState::Waiting(WaitState::Close(request)) => {
                                    TerminalDrainTarget::Close {
                                        request: *request,
                                        deadline: Self::explicit_close_drain_deadline(),
                                    }
                                }
                                ConnectionState::Active => TerminalDrainTarget::Active,
                                ConnectionState::Draining(_) => unreachable!(),
                            };
                            connection = Self::drain_from_waiting(
                                target,
                                TerminalEvent::AckApplyFailed,
                                error,
                            )?;
                            continue;
                        }
                    }

                    if let ConnectionState::Waiting(WaitState::Close(close_request)) = &connection {
                        if self.close_target_is_acknowledged(*close_request) {
                            let outcome = self.acknowledged_close_outcome();
                            let deadline = Self::explicit_close_drain_deadline();
                            connection = Self::drain_for_acknowledged_close(
                                *close_request,
                                outcome,
                                deadline,
                            );
                        }
                    }
                }
                AckEvent::Response(Some(Err(error))) => {
                    let status: tonic::Status = error.into();
                    let error = ZerobusError::StreamClosedError(status);
                    if matches!(&connection, ConnectionState::Active) {
                        let _ = self.server_error_tx.send(Some(error.clone()));
                    }
                    let target = match &connection {
                        ConnectionState::Waiting(WaitState::Rotation { deadlines, .. }) => {
                            TerminalDrainTarget::Rotation {
                                deadline: Self::bounded_rotation_drain_deadline(deadlines.drain),
                            }
                        }
                        ConnectionState::Waiting(WaitState::Close(request)) => {
                            TerminalDrainTarget::Close {
                                request: *request,
                                deadline: Self::explicit_close_drain_deadline(),
                            }
                        }
                        ConnectionState::Active => TerminalDrainTarget::Active,
                        ConnectionState::Draining(_) => unreachable!(),
                    };
                    connection =
                        Self::drain_from_waiting(target, TerminalEvent::ResponseError, error)?;
                }
                AckEvent::Response(None) => {
                    let error = ZerobusError::StreamClosedError(tonic::Status::unknown(
                        "Server closed the stream",
                    ));
                    let target = match &connection {
                        ConnectionState::Waiting(WaitState::Rotation { deadlines, .. }) => {
                            TerminalDrainTarget::Rotation {
                                deadline: Self::bounded_rotation_drain_deadline(deadlines.drain),
                            }
                        }
                        ConnectionState::Waiting(WaitState::Close(request)) => {
                            TerminalDrainTarget::Close {
                                request: *request,
                                deadline: Self::explicit_close_drain_deadline(),
                            }
                        }
                        ConnectionState::Active => TerminalDrainTarget::Active,
                        ConnectionState::Draining(_) => unreachable!(),
                    };
                    connection =
                        Self::drain_from_waiting(target, TerminalEvent::ResponseEof, error)?;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::task::Poll;

    use arrow_array::Int32Array;
    use arrow_flight::error::FlightError;
    use arrow_flight::PutResult;
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use futures::stream::{iter, pending, repeat_with};
    use futures::StreamExt as _;
    use tokio::sync::{watch, Mutex, Semaphore};
    use tokio::time::{Duration, Instant};

    use super::super::{CloseRequest, RecordBatch};
    use super::{
        AckProcessOutcome, AckProcessor, CloseDrain, ConnectionState, FlightAckMetadata,
        FlightResponseStream, OffsetId, PendingBatch, RequestBodyControl, ZerobusError,
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

    #[tokio::test]
    async fn preacked_close_preserves_latched_timeout() {
        let schema = one_col_schema();
        let semaphore = Arc::new(Semaphore::new(1));
        let (processor, _request_body, _last_ack_rx) = ack_processor(
            Arc::new(Mutex::new(vec![pending_batch(
                &semaphore,
                batch_with_rows(&schema, 1),
                0,
                0,
                1,
            )])),
            Arc::new(AtomicU64::new(1)),
            Arc::new(AtomicU64::new(0)),
            false,
        );
        let request = CloseRequest {
            target_offset: Some(0),
            deadline: Instant::now(),
        };
        processor.close.publish(request);
        processor
            .ack_progress()
            .apply(&FlightAckMetadata {
                ack_up_to_offset: 0,
                ack_up_to_records: 1,
                close_stream_duration_ms: None,
            })
            .await
            .expect("ACK application should succeed");

        let ConnectionState::Draining(state) =
            processor.begin_close(&ConnectionState::Active, request)
        else {
            panic!("a pre-acked close must begin draining")
        };
        let Some(CloseDrain {
            selected: Some(outcome),
            ..
        }) = state.close
        else {
            panic!("a pre-acked close must select an outcome")
        };
        let error = outcome.expect_err("an ACK applied at the deadline must time out");
        match error {
            ZerobusError::StreamClosedError(status) => {
                assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
            }
            other => panic!("expected a flush deadline error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn published_close_is_not_starved_by_ready_malformed_responses() {
        let (processor, request_body, _last_ack_rx) = ack_processor(
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            false,
        );
        let mut response_stream: FlightResponseStream = Box::pin(
            repeat_with(|| {
                Ok(PutResult {
                    app_metadata: b"not ack metadata".to_vec().into(),
                })
            })
            .take(8),
        );
        let mut close_rx = processor.close.subscribe();
        let request = CloseRequest {
            target_offset: None,
            deadline: Instant::now() + Duration::from_secs(1),
        };
        processor.close.publish(request);

        let outcome = tokio::time::timeout(
            Duration::from_millis(100),
            processor.process_active(&mut response_stream, &request_body, &mut close_rx, true),
        )
        .await
        .expect("a ready response stream must not starve close")
        .expect("close observation should not fail");

        assert!(matches!(
            outcome,
            AckProcessOutcome::Close {
                request: CloseRequest {
                    target_offset: None,
                    ..
                },
                outcome: Ok(()),
            }
        ));
    }

    #[tokio::test]
    async fn ready_terminal_eof_precedes_published_empty_close() {
        let (processor, request_body, _last_ack_rx) = ack_processor(
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            false,
        );
        let mut response_stream: FlightResponseStream = Box::pin(iter([]));
        let mut close_rx = processor.close.subscribe();
        processor.close.publish(CloseRequest {
            target_offset: None,
            deadline: Instant::now() + Duration::from_secs(1),
        });

        let result = processor
            .process_active(&mut response_stream, &request_body, &mut close_rx, true)
            .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("terminal EOF that predates close must not become a clean close"),
        };
        match error {
            ZerobusError::StreamClosedError(status) => {
                assert_eq!(status.code(), tonic::Code::Unknown);
                assert_eq!(status.message(), "Server closed the stream");
            }
            other => panic!("expected terminal stream error, got {other:?}"),
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

    /// After the one response-first tie, a later terminal status is not polled.
    #[tokio::test]
    async fn later_terminal_status_does_not_replace_reported_send_failure() {
        let no_progress = PutResult {
            app_metadata: serde_json::to_vec(&FlightAckMetadata {
                ack_up_to_offset: -1,
                ack_up_to_records: 0,
                close_stream_duration_ms: None,
            })
            .unwrap()
            .into(),
        };
        let response_stream = iter([
            Ok(no_progress),
            Err(tonic::Status::permission_denied("permanent server rejection").into()),
        ]);
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
            .expect_err("the reported request-send failure must trigger recovery");

        assert!(error.is_retryable());
        match error {
            ZerobusError::StreamClosedError(status) => {
                assert_eq!(status.code(), tonic::Code::Unavailable);
            }
            other => panic!("expected a stream-closed error, got {other:?}"),
        }
    }

    /// Empty-close's one-shot peek must not consume a later item after send-failure
    /// has already used its response-first tie.
    #[tokio::test]
    async fn deferred_send_failure_skips_empty_close_peek() {
        let no_progress = PutResult {
            app_metadata: serde_json::to_vec(&FlightAckMetadata {
                ack_up_to_offset: -1,
                ack_up_to_records: 0,
                close_stream_duration_ms: None,
            })
            .unwrap()
            .into(),
        };
        let (processor, request_body, _last_ack_rx) = ack_processor(
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            false,
        );
        processor.request_send_failure.report();

        let taken = Arc::new(AtomicU64::new(0));
        let taken_for_stream = Arc::clone(&taken);
        let close = processor.close.clone();
        let request = CloseRequest {
            target_offset: None,
            deadline: Instant::now() + Duration::from_secs(1),
        };
        let mut items = vec![
            Ok(no_progress),
            Err(tonic::Status::permission_denied("permanent server rejection").into()),
        ]
        .into_iter();
        let mut response_stream: FlightResponseStream =
            Box::pin(futures::stream::poll_fn(move |_cx| {
                let n = taken_for_stream.fetch_add(1, Ordering::SeqCst) + 1;
                if n == 1 {
                    close.publish(request);
                }
                Poll::Ready(items.next())
            }));
        let mut close_rx = processor.close.subscribe();

        let result = processor
            .process_active(&mut response_stream, &request_body, &mut close_rx, true)
            .await;
        match result {
            Ok(AckProcessOutcome::Close {
                request:
                    CloseRequest {
                        target_offset: None,
                        ..
                    },
                outcome: Err(error),
            }) => {
                assert!(!error.is_retryable());
                match error {
                    ZerobusError::StreamClosedError(status) => {
                        assert_eq!(status.code(), tonic::Code::PermissionDenied);
                    }
                    other => panic!("expected drained peer status, got {other:?}"),
                }
            }
            Err(ZerobusError::StreamClosedError(status))
                if status.code() == tonic::Code::Unavailable =>
            {
                panic!("empty-close peek discarded the buffered peer status");
            }
            _ => panic!("expected empty close to surface the buffered peer status"),
        }
        assert!(
            taken.load(Ordering::SeqCst) >= 2,
            "drain must observe the buffered peer status instead of dropping it"
        );
    }

    /// At most one ready nonterminal response may defer a reported request-send failure.
    #[tokio::test]
    async fn continuously_ready_nonprogress_responses_do_not_starve_send_failure() {
        let valid_no_progress = PutResult {
            app_metadata: serde_json::to_vec(&FlightAckMetadata {
                ack_up_to_offset: -1,
                ack_up_to_records: 0,
                close_stream_duration_ms: None,
            })
            .unwrap()
            .into(),
        };
        let malformed = PutResult {
            app_metadata: b"not ack metadata".to_vec().into(),
        };

        for response in [valid_no_progress, malformed] {
            let response_stream = repeat_with(move || Ok(response.clone()));
            let (processor, request_body, _last_ack_rx) = ack_processor(
                Arc::new(Mutex::new(Vec::new())),
                Arc::new(AtomicU64::new(0)),
                Arc::new(AtomicU64::new(0)),
                false,
            );
            processor.request_send_failure.report();

            let error = tokio::time::timeout(
                Duration::from_millis(100),
                processor.process(Box::pin(response_stream), request_body),
            )
            .await
            .expect("ready responses must not starve request-send failure")
            .expect_err("the reported request-send failure must trigger recovery");

            match error {
                ZerobusError::StreamClosedError(status) => {
                    assert_eq!(status.code(), tonic::Code::Unavailable);
                }
                other => panic!("expected a stream-closed error, got {other:?}"),
            }
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

    #[tokio::test]
    async fn close_during_rotation_ack_wait_preserves_rotation_state() {
        async fn run(acked_before: bool, expired: bool, ack_during_drain: bool) {
            let schema = one_col_schema();
            let semaphore = Arc::new(Semaphore::new(1));
            let pending_batches = Arc::new(Mutex::new(vec![pending_batch(
                &semaphore,
                batch_with_rows(&schema, 1),
                0,
                0,
                1,
            )]));
            let last_acked_records = Arc::new(AtomicU64::new(0));
            let (processor, request_body, _last_ack_rx) = ack_processor(
                pending_batches,
                Arc::new(AtomicU64::new(1)),
                Arc::clone(&last_acked_records),
                false,
            );
            let response = |offset, records, close_stream_duration_ms| PutResult {
                app_metadata: serde_json::to_vec(&FlightAckMetadata {
                    ack_up_to_offset: offset,
                    ack_up_to_records: records,
                    close_stream_duration_ms,
                })
                .unwrap()
                .into(),
            };
            let (response_tx, response_rx) = futures::channel::mpsc::unbounded();
            response_tx
                .unbounded_send(Ok(response(-1, 0, Some(1_000))))
                .unwrap();
            let mut response_stream: FlightResponseStream = Box::pin(response_rx);
            let mut close_rx = processor.close.subscribe();
            let process =
                processor.process_active(&mut response_stream, &request_body, &mut close_rx, true);
            tokio::pin!(process);

            assert!(futures::poll!(process.as_mut()).is_pending());
            assert!(processor.is_paused.load(Ordering::Relaxed));

            if acked_before {
                processor.last_ack_tx.send_replace(Some(0));
                last_acked_records.store(1, Ordering::Release);
            }
            let request = CloseRequest {
                target_offset: Some(0),
                deadline: if expired {
                    Instant::now()
                } else {
                    Instant::now() + Duration::from_secs(5)
                },
            };
            processor.close.publish(request);
            assert!(futures::poll!(process.as_mut()).is_pending());

            if ack_during_drain {
                response_tx
                    .unbounded_send(Ok(response(0, 1, None)))
                    .unwrap();
                assert!(futures::poll!(process.as_mut()).is_pending());
            }

            drop(response_tx);
            let AckProcessOutcome::Close {
                request: retained,
                outcome,
            } = process.await.expect("rotation drain outcome")
            else {
                panic!("close did not preserve the active rotation")
            };
            assert_eq!(retained.target_offset, request.target_offset);
            assert_eq!(retained.deadline, request.deadline);
            let error = outcome.expect_err("rotation remains the recovery trigger");
            match error {
                ZerobusError::StreamClosedError(status) => {
                    assert_eq!(status.code(), tonic::Code::Unavailable);
                }
                other => panic!("expected rotation error, got {other:?}"),
            }
        }

        for case in [
            (true, false, false),
            (false, true, false),
            (false, true, true),
            (false, false, true),
        ] {
            run(case.0, case.1, case.2).await;
        }
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
