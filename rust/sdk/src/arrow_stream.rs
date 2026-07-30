//! Arrow Flight stream implementation for high-performance Arrow data ingestion.
//!
//! **Beta**: This module is in Beta. The API is stabilising but may still change
//! before reaching GA.
//!
//! This module provides `ZerobusArrowStream`, a client for ingesting Arrow `RecordBatch`
//! data into Databricks Delta tables using the Arrow Flight protocol.
//! Native Rust callers use `ingest_batch` with `RecordBatch` values; FFI callers
//! (Go, Python, Java, TypeScript) can use `ingest_ipc_batch` with pre-serialised
//! Arrow IPC bytes.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::Poll;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::{FlightClient, FlightData, PutResult};
use arrow_ipc::writer::IpcWriteOptions;
use bytes::Bytes;
use futures::{Stream, StreamExt};
#[cfg(feature = "test-hooks")]
use tokio::sync::Notify;
use tokio::sync::{mpsc, watch, Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::time::{sleep, Duration, Instant};
use tokio_retry::strategy::FixedInterval;
use tokio_retry::RetryIf;
use tokio_util::sync::CancellationToken;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tracing::{debug, error, info, instrument, warn};

/// Maximum combined time for request EOF and response draining during rotation.
///
/// When the advertised server grace is shorter, this also provides a local
/// best-effort window for tonic to observe request EOF. The server may already have
/// hard-closed in that case, so peer status always takes precedence over clean drain.
const CLOSE_SIGNAL_DRAIN_TIMEOUT_MS: u64 = 500;
/// Scheduling headroom after the transport deadline for the supervisor to publish its result.
const EXPLICIT_CLOSE_JOIN_GRACE_MS: u64 = 50;
/// Shared prefix for close-owned timeout errors admitted through the published-error filter.
const EXPLICIT_CLOSE_TIMEOUT_PREFIX: &str = "Explicit close timed out";
/// Timeout when a replacement DoPut never yields a drainable response before close's deadline.
const EXPLICIT_CLOSE_RECONNECT_TIMEOUT: &str =
    "Explicit close timed out while waiting for the replacement Flight response stream";
/// Timeout when close aborts the supervisor after the cleanup deadline.
const EXPLICIT_CLOSE_ABORTED_TIMEOUT: &str =
    "Explicit close timed out while shutting down the Arrow Flight stream";
/// Timeout when tonic does not poll the controlled request body to EOF.
const EXPLICIT_CLOSE_REQUEST_EOF_TIMEOUT: &str =
    "Explicit close timed out before the Flight request reached EOF";
/// Timeout when the peer does not finish the response after request EOF.
const EXPLICIT_CLOSE_RESPONSE_DRAIN_TIMEOUT: &str =
    "Explicit close timed out while draining the Flight response";

// Re-export arrow types for public API
pub use arrow_array::RecordBatch;
pub use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};

use crate::arrow_configuration::ArrowStreamConfigurationOptions;
use crate::arrow_metadata::{FlightAckMetadata, FlightBatchMetadata};
use crate::errors::{should_retry_initial_connection, ZerobusError};
use crate::headers_provider::HeadersProvider;
use crate::offset_generator::{OffsetId, OffsetIdGenerator};
use crate::proxy::{self, ConnectorFactory};
use crate::tls_config::TlsConfig;
use crate::ZerobusResult;

/// Type alias for the batch sender channel, wrapped for thread-safe sharing.
type BatchSender = Arc<Mutex<Option<mpsc::Sender<Result<RecordBatch, FlightError>>>>>;

/// Stream of Arrow Flight responses carrying acknowledgments from the server.
type FlightResponseStream = Pin<Box<dyn Stream<Item = Result<PutResult, FlightError>> + Send>>;

/// Encoded request body supplied to Arrow Flight `DoPut`.
type FlightRequestStream = Pin<Box<dyn Stream<Item = Result<FlightData, FlightError>> + Send>>;

/// Per-connection handle used to stop the outbound body and observe when tonic has
/// polled it to EOF. Sender detachment alone is insufficient because mpsc and the
/// Flight encoder may still contain queued batches or chunks.
#[derive(Clone)]
struct RequestBodyControl {
    /// Cancellation observed by the controlled Flight request stream before it polls
    /// any additional encoded data.
    shutdown: CancellationToken,
    /// Becomes `true` only after tonic polls the controlled request stream to `None`.
    eof_rx: watch::Receiver<bool>,
}

impl RequestBodyControl {
    /// Stops the request stream from yielding queued or partially encoded batches.
    fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// Returns whether tonic has already polled the controlled request stream to EOF.
    fn reached_eof(&self) -> bool {
        *self.eof_rx.borrow()
    }

    /// Waits until tonic observes request-body EOF. A dropped observer that never
    /// published EOF is not a clean half-close, so keep waiting for the caller's
    /// surrounding deadline instead of treating channel closure as success.
    async fn wait_for_eof(&self) {
        let mut eof_rx = self.eof_rx.clone();
        loop {
            if *eof_rx.borrow_and_update() {
                return;
            }
            if eof_rx.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }

    /// Waits until tonic observes request-body EOF, bounded by the rotation deadline.
    async fn wait_for_eof_until(&self, deadline: Instant) -> bool {
        if self.reached_eof() {
            return true;
        }
        tokio::time::timeout_at(deadline, self.wait_for_eof())
            .await
            .is_ok()
    }
}

/// Connection-local handles used by ACK processing to stop outbound data and reach
/// request EOF. Grouping them keeps state-machine transitions focused on why shutdown
/// occurs instead of repeating the transport mechanics at every terminal branch.
struct AckRequestControl<'a> {
    request_body: &'a RequestBodyControl,
    ingest_mutex: &'a Arc<Mutex<()>>,
    is_paused: &'a Arc<AtomicBool>,
    batch_tx: &'a BatchSender,
}

impl AckRequestControl<'_> {
    /// Stops the request body by an explicit deadline.
    async fn shutdown_until(&self, deadline: Instant) -> bool {
        ZerobusArrowStream::shutdown_request_body(
            self.request_body,
            deadline,
            self.ingest_mutex,
            self.is_paused,
            self.batch_tx,
        )
        .await
    }

    /// Stops the request body within the cleanup budget of the current close phase.
    async fn shutdown_for_phase(&self, phase: &GracefulClosePhase) -> bool {
        self.shutdown_until(ZerobusArrowStream::request_cleanup_deadline(phase))
            .await
    }

    /// Transitions a server-initiated close from ACK waiting to response draining.
    async fn begin_response_drain(
        &self,
        phase: &mut GracefulClosePhase,
        recovery_reason: ZerobusError,
    ) {
        ZerobusArrowStream::begin_response_drain(
            phase,
            recovery_reason,
            self.request_body,
            self.ingest_mutex,
            self.is_paused,
            self.batch_tx,
        )
        .await;
    }
}

/// All state created for one Arrow Flight `DoPut` connection.
///
/// The supervisor replaces this bundle atomically across recovery so response polling,
/// batch submission, and request shutdown always refer to the same HTTP/2 stream.
struct FlightConnection {
    /// Server-to-client acknowledgments and terminal statuses.
    response_stream: FlightResponseStream,
    /// Client-facing sender feeding batches into this connection's encoder.
    batch_tx: mpsc::Sender<Result<RecordBatch, FlightError>>,
    /// Control plane for cancelling and observing the outbound request body.
    request_body: RequestBodyControl,
    /// Whether pending ranges and counters describe this connection, making positive ACKs safe.
    replay_state_installed: bool,
}

/// Result of rebuilding an Arrow Flight connection during recovery.
enum ReconnectOutcome {
    /// Recovery completed and the replacement connection is ready for normal ACK processing.
    Connected(FlightConnection),
    /// Explicit close arrived before reconnect created a Flight request.
    ClosedBeforeRequest,
    /// Explicit close arrived after the replacement `DoPut` started. The supervisor must hand
    /// this connection to ACK processing so it is half-closed and drained rather than dropped.
    Closing(FlightConnection),
    /// The replacement request started, but did not produce a drainable response stream before
    /// the explicit-close deadline.
    CloseTimedOutAfterRequest,
}

/// Error carried from a failed reconnect into the supervisor's next loop iteration.
struct PendingSupervisorError {
    /// The original reconnect failure exposed to waiters and explicit close.
    error: ZerobusError,
    /// Whether this exact authentication rejection has already invalidated cached credentials.
    auth_invalidated: bool,
}

/// Result of waiting for the next ACK-side state-machine input.
enum AckEvent {
    /// A response item, peer error, or clean response EOF.
    Response(Option<Result<PutResult, FlightError>>),
    /// No acknowledgment arrived while ordinary pending work existed.
    AckTimeout,
    /// The ACK-wait portion of server-initiated graceful close expired.
    GracefulCloseDeadline,
    /// The bounded concurrent request/response drain period expired.
    ResponseDrainDeadline,
    /// Tonic polled the controlled request stream to clean EOF.
    RequestEof,
    /// Explicit `close()` requested a clean request half-close by this deadline.
    ExplicitClose { deadline: Instant },
}

/// Explicit phases of Arrow Flight stream shutdown and server-initiated rotation.
///
/// A single enum prevents invalid combinations that were possible with separate optional
/// targets, ACK deadlines, drain deadlines, and recovery errors.
enum GracefulClosePhase {
    /// Normal bidirectional ingestion; no server close signal is active.
    Open,
    /// New sends are paused while acknowledgments for the pre-signal snapshot may arrive.
    AwaitingAcks {
        /// Connection-local cumulative record watermark captured at the first signal.
        target_records: u64,
        /// End of the user-configurable ACK-wait portion of the grace period.
        ack_deadline: Instant,
        /// End of the bounded local close attempt. Normally this is the server deadline;
        /// very short server grace periods receive a best-effort local EOF settle window.
        close_deadline: Instant,
    },
    /// Request shutdown has started and both HTTP/2 directions are being drained.
    DrainingResponse {
        /// Earliest of the total server deadline and the private drain cap.
        deadline: Instant,
        /// Synthetic rotation reason used only for clean response EOF or local timeout.
        recovery_reason: ZerobusError,
        /// Whether tonic has observed request-body EOF.
        request_reached_eof: bool,
        /// Whether the server response has reached EOF.
        response_reached_eof: bool,
    },
    /// Explicit `close()` is driving request EOF and response draining concurrently.
    DrainingExplicitClose {
        /// End of the bounded explicit-close transport cleanup.
        deadline: Instant,
        /// Whether tonic observed request-body EOF.
        request_reached_eof: bool,
        /// Whether the server response has reached EOF.
        response_reached_eof: bool,
    },
}

impl GracefulClosePhase {
    /// Returns whether the connection is in ordinary ACK processing.
    fn is_open(&self) -> bool {
        match self {
            Self::Open => true,
            Self::AwaitingAcks { .. }
            | Self::DrainingResponse { .. }
            | Self::DrainingExplicitClose { .. } => false,
        }
    }

    /// Returns whether request shutdown has completed and response draining is active.
    fn is_draining(&self) -> bool {
        match self {
            Self::DrainingResponse { .. } | Self::DrainingExplicitClose { .. } => true,
            Self::Open | Self::AwaitingAcks { .. } => false,
        }
    }

    /// Returns the outer transport-cleanup deadline for the active close phase.
    fn close_deadline(&self) -> Option<Instant> {
        match self {
            Self::Open => None,
            Self::AwaitingAcks { close_deadline, .. } => Some(*close_deadline),
            Self::DrainingResponse { deadline, .. }
            | Self::DrainingExplicitClose { deadline, .. } => Some(*deadline),
        }
    }

    /// Returns transport progress while either close path owns the connection.
    fn drain_progress(&self) -> Option<(bool, bool)> {
        match self {
            Self::DrainingResponse {
                request_reached_eof,
                response_reached_eof,
                ..
            }
            | Self::DrainingExplicitClose {
                request_reached_eof,
                response_reached_eof,
                ..
            } => Some((*request_reached_eof, *response_reached_eof)),
            Self::Open | Self::AwaitingAcks { .. } => None,
        }
    }

    fn mark_request_eof(&mut self) {
        match self {
            Self::DrainingResponse {
                request_reached_eof,
                ..
            }
            | Self::DrainingExplicitClose {
                request_reached_eof,
                ..
            } => *request_reached_eof = true,
            Self::Open | Self::AwaitingAcks { .. } => {}
        }
    }

    fn mark_response_eof(&mut self) {
        match self {
            Self::DrainingResponse {
                response_reached_eof,
                ..
            }
            | Self::DrainingExplicitClose {
                response_reached_eof,
                ..
            } => *response_reached_eof = true,
            Self::Open | Self::AwaitingAcks { .. } => {}
        }
    }
}

/// Publishes whether the ACK processor is handling a server close signal for test observers.
/// Production `close()` waits by joining the supervisor, not by reading this watch channel.
struct GracefulCloseActivity {
    /// Watch publisher used by test hooks to observe the active close deadline.
    tx: watch::Sender<Option<Instant>>,
    /// Ensures duplicate close signals publish activity only once.
    active: bool,
}

impl GracefulCloseActivity {
    /// Creates an inactive activity guard for one invocation of `process_acks`.
    fn new(tx: watch::Sender<Option<Instant>>) -> Self {
        Self { tx, active: false }
    }

    /// Publishes the active close deadline. Duplicate signals can shorten it.
    fn activate(&mut self, deadline: Instant) {
        self.tx.send_replace(Some(deadline));
        self.active = true;
    }
}

impl Drop for GracefulCloseActivity {
    fn drop(&mut self) {
        if self.active {
            self.tx.send_replace(None);
        }
    }
}

/// Test-only barrier used to pause `reconnect` at a precise point — the new connection
/// is established but pending ranges are not yet rebuilt — so a test can schedule a
/// concurrent ingest or `close()`.
#[cfg(feature = "test-hooks")]
type ReconnectRebuildGate = Arc<Mutex<Option<ReconnectRebuildBarrier>>>;

/// Notifications that deterministically park and release reconnect range rebuilding.
/// `reached` fires when reconnect hits the barrier; `proceed` releases it, while an
/// explicit-close signal transfers the live connection to bounded shutdown.
#[cfg(feature = "test-hooks")]
#[derive(Clone)]
struct ReconnectRebuildBarrier {
    /// Fired after the new connection exists but before replay state is rebuilt.
    reached: Arc<Notify>,
    /// Released by the test to let reconnect continue.
    proceed: Arc<Notify>,
}

/// Test-only hook fired after `process_acks` fully applies a non-empty ack. Tests can
/// either observe `reached`, or also provide `proceed` to park the ack processor there.
#[cfg(feature = "test-hooks")]
type AckAppliedGate = Arc<Mutex<Option<AckAppliedHook>>>;

/// Notification, and optional barrier, after a non-empty ACK is fully applied.
#[cfg(feature = "test-hooks")]
#[derive(Clone)]
struct AckAppliedHook {
    /// Fired after the durable watermark and waiter-visible offset are published.
    reached: Arc<Notify>,
    /// When present, keeps `process_acks` parked until the test releases it.
    proceed: Option<Arc<Notify>>,
}

/// Test-only observations proving that the client consumed a response after request
/// EOF and then finished the response drain.
#[cfg(feature = "test-hooks")]
type ResponseDrainGate = Arc<Mutex<Option<ResponseDrainHook>>>;

/// Client-side observations for the two important response-drain milestones.
#[cfg(feature = "test-hooks")]
#[derive(Clone)]
struct ResponseDrainHook {
    /// Fired when the SDK polls a response after request EOF.
    response_consumed: Arc<Notify>,
    /// Fired on response EOF, peer error, or drain timeout.
    drain_completed: Arc<Notify>,
}

/// Test-only barrier that parks `close()` after the supervisor and sender are gone but
/// before pending batches are finalized, allowing cancellation-safe teardown tests.
#[cfg(feature = "test-hooks")]
type CloseFinalizeGate = Arc<Mutex<Option<CloseFinalizeBarrier>>>;

/// Notifications that park explicit close between supervisor teardown and finalization.
#[cfg(feature = "test-hooks")]
#[derive(Clone)]
struct CloseFinalizeBarrier {
    /// Fired once close teardown is irreversible but pending batches remain unfinalized.
    reached: Arc<Notify>,
    /// Released by the test to let finalization continue.
    proceed: Arc<Notify>,
}

/// Shared record-watermark state needed to validate and apply an acknowledgment.
struct AckProgress<'a> {
    /// Number of records handed to the active connection.
    submitted_records: &'a AtomicU64,
    /// Highest cumulative record watermark applied on the active connection.
    last_acked_records: &'a AtomicU64,
    /// Pending batch ranges used to translate record progress into SDK offsets.
    pending_batches: &'a Mutex<Vec<PendingBatch>>,
    /// Publishes the highest fully acknowledged SDK offset to waiters.
    last_ack_tx: &'a watch::Sender<Option<OffsetId>>,
    /// Deterministic test hook run after an acknowledgment is fully applied.
    #[cfg(feature = "test-hooks")]
    ack_applied_gate: &'a AckAppliedGate,
}

/// Properties for an Arrow Flight ingestion table.
///
/// **Do not construct this directly.** Configure Arrow streams via the builder API:
/// `sdk.stream_builder().table("catalog.schema.table").arrow(schema)`.
#[derive(Debug, Clone)]
pub(crate) struct ArrowTableProperties {
    /// The fully qualified table name (e.g., "catalog.schema.table").
    pub(crate) table_name: String,
    /// The Arrow schema for the data being ingested.
    /// This is used to validate RecordBatches before sending and is sent
    /// as the first message in the Flight stream.
    pub(crate) schema: Arc<ArrowSchema>,
}

/// A pending batch waiting for acknowledgment.
struct PendingBatch {
    batch: RecordBatch,
    /// Offset ID assigned by the client for this batch.
    offset_id: OffsetId,
    /// Cumulative record count before this batch.
    start_record: u64,
    /// Cumulative record count after this batch.
    /// Batch is fully acked when `acked_records >= end_record`.
    end_record: u64,
    /// Backpressure permit; dropping it frees one `max_inflight_batches` slot.
    _permit: OwnedSemaphorePermit,
}

/// Returns the batch portion not durably acknowledged, avoiding duplicate retry of an
/// acknowledged prefix.
///
/// Returns `None` when fully acknowledged, the original batch when fully unacknowledged,
/// or a sliced suffix when partially acknowledged.
fn slice_batch_for_recovery(
    pb: &PendingBatch,
    acked_before_disconnect: u64,
) -> Option<RecordBatch> {
    if pb.start_record >= acked_before_disconnect {
        return Some(pb.batch.clone());
    }

    let records_already_acked =
        (acked_before_disconnect - pb.start_record).min(pb.batch.num_rows() as u64);
    let remaining_rows = pb
        .batch
        .num_rows()
        .saturating_sub(records_already_acked as usize);

    if remaining_rows == 0 {
        None
    } else {
        debug!(
            offset_id = pb.offset_id,
            total_rows = pb.batch.num_rows(),
            records_already_acked = records_already_acked,
            remaining_rows = remaining_rows,
            "Slicing partially-acked batch for recovery"
        );
        Some(
            pb.batch
                .slice(records_already_acked as usize, remaining_rows),
        )
    }
}

/// Deserialises Arrow IPC stream bytes into a [`RecordBatch`].
#[allow(clippy::result_large_err)]
fn materialize_ipc(bytes: &Bytes) -> ZerobusResult<RecordBatch> {
    use std::io::Cursor;
    let mut reader = arrow_ipc::reader::StreamReader::try_new(Cursor::new(bytes.as_ref()), None)
        .map_err(|e| {
            ZerobusError::InvalidArgument(format!("IPC: invalid Arrow IPC stream: {e}"))
        })?;
    let batch = match reader.next() {
        None => {
            return Err(ZerobusError::InvalidArgument(
                "IPC stream contains no RecordBatch".into(),
            ));
        }
        Some(Err(e)) => {
            return Err(ZerobusError::InvalidArgument(format!(
                "IPC: record batch read failed: {e}"
            )));
        }
        Some(Ok(b)) => b,
    };
    match reader.next() {
        None => Ok(batch),
        Some(Ok(_)) => Err(ZerobusError::InvalidArgument(
            "IPC stream must contain exactly one RecordBatch (found extra batch)".into(),
        )),
        Some(Err(e)) => Err(ZerobusError::InvalidArgument(format!(
            "IPC: trailing message read failed: {e}"
        ))),
    }
}

/// Builds [`IpcWriteOptions`] for the given optional compression codec.
#[allow(clippy::result_large_err)]
fn make_ipc_write_options(
    compression: Option<arrow_ipc::CompressionType>,
) -> ZerobusResult<IpcWriteOptions> {
    match compression {
        None => Ok(IpcWriteOptions::default()),
        Some(c) => IpcWriteOptions::default()
            .try_with_compression(Some(c))
            .map_err(|e| {
                ZerobusError::InvalidArgument(format!(
                    "Failed to enable Arrow IPC compression: {e}"
                ))
            }),
    }
}

/// An Arrow Flight stream for ingesting Arrow RecordBatches into a Delta table.
///
/// This stream provides a high-performance interface for streaming Arrow data
/// to Databricks Delta tables using the Arrow Flight protocol.
///
/// # Lifecycle
///
/// 1. Build a stream with `sdk.stream_builder().table(...).arrow(...).build_arrow()`
/// 2. Queue batches with `ingest_batch()` without waiting after each call
/// 3. Call `flush()` at durability boundaries
/// 4. Call `close()` to flush remaining work and stop background I/O
///
/// # Recovery
///
/// When recovery is enabled (default), the stream will automatically attempt to
/// reconnect and replay unacknowledged batches on transient failures. If recovery
/// fails after the configured number of retries, use `get_unacked_batches()` to
/// retrieve the failed batches for manual handling.
///
/// # Examples
///
/// ```no_run
/// # use databricks_zerobus_ingest_sdk::*;
/// # use arrow_array::RecordBatch;
/// # async fn example(mut stream: ZerobusArrowStream, batches: Vec<RecordBatch>) -> Result<(), ZerobusError> {
/// // Idiomatic flow: ingest each batch (which only queues it), then flush() once.
/// for batch in batches {
///     stream.ingest_batch(batch).await?;
/// }
/// stream.flush().await?; // Returns once every queued batch is acknowledged.
///
/// // Close the stream gracefully
/// stream.close().await?;
/// # Ok(())
/// # }
/// ```
///
/// For low-volume cases where you must confirm one specific batch is durable
/// before continuing, wait on its offset:
///
/// ```no_run
/// # use databricks_zerobus_ingest_sdk::*;
/// # use arrow_array::RecordBatch;
/// # async fn example(mut stream: ZerobusArrowStream, batch: RecordBatch) -> Result<(), ZerobusError> {
/// let offset = stream.ingest_batch(batch).await?;
/// stream.wait_for_offset(offset).await?;
/// println!("Batch acknowledged at offset: {}", offset);
/// # Ok(())
/// # }
/// ```
#[non_exhaustive]
pub struct ZerobusArrowStream {
    /// Table properties including name and schema.
    pub(crate) table_properties: ArrowTableProperties,
    /// Configuration options for this stream.
    pub(crate) options: ArrowStreamConfigurationOptions,
    /// Sender to the Flight encoder; replaced or detached during recovery and close.
    batch_tx: BatchSender,
    /// Generates logical batch offsets returned to callers, distinct from wire offsets.
    offset_generator: OffsetIdGenerator,
    /// Watch channel for tracking the last acknowledged offset.
    last_ack_tx: tokio::sync::watch::Sender<Option<OffsetId>>,
    /// Receiver for the watch channel (kept alive to prevent sender errors).
    _last_ack_rx: tokio::sync::watch::Receiver<Option<OffsetId>>,
    /// True once the stream is terminally closed and unacknowledged batches may be retrieved.
    is_closed: Arc<AtomicBool>,
    /// Separates resumable teardown from final closure so retries skip flushing while
    /// new ingests remain rejected.
    close_teardown_started: AtomicBool,
    /// Retains the highest-precedence error resolved so far during explicit close. It begins
    /// with the flush result and is finalized after supervisor shutdown, making repeated and
    /// resumed close calls return one stable outcome.
    close_error: Mutex<Option<ZerobusError>>,
    /// Handle to the supervisor task that processes acknowledgments and recovery.
    receiver_task: Arc<Mutex<Option<tokio::task::JoinHandle<ZerobusResult<()>>>>>,
    /// Accepted batches not yet fully acknowledged; retained for replay or retrieval.
    pending_batches: Arc<Mutex<Vec<PendingBatch>>>,
    /// Unacknowledged batch suffixes finalized after terminal failure or failed close.
    failed_batches: Arc<Mutex<Vec<RecordBatch>>>,
    /// Count of recovery attempts.
    recovery_attempts: Arc<AtomicU32>,
    /// Endpoint retained for reconnect attempts.
    endpoint: String,
    /// TLS configuration for the connection.
    tls_config: Arc<dyn TlsConfig>,
    /// Proxy connector policy, reused for every replacement channel.
    connector_factory: Option<ConnectorFactory>,
    headers_provider: Arc<dyn HeadersProvider>,
    /// Serializes ingestion with pause, replay, and finalization transitions.
    ingest_mutex: Arc<Mutex<()>>,
    /// Bounds batches awaiting ack (`max_inflight_batches`). Capacity mirrors the
    /// `batch_tx` channel so the inline send never blocks while holding `ingest_mutex`.
    inflight: Arc<Semaphore>,
    /// Watch channel carrying the latest cross-task stream error.
    server_error_tx: watch::Sender<Option<ZerobusError>>,
    server_error_rx: watch::Receiver<Option<ZerobusError>>,
    /// Permanent recovery cause retained only for explicit close; ordinary waiters must not
    /// observe an authentication rejection while its credential-refresh retry is active.
    recovery_close_error_rx: watch::Receiver<Option<ZerobusError>>,
    /// Cumulative record count assigned to pending ranges for the current connection.
    /// This includes batches buffered while paused.
    cumulative_records_assigned: Arc<AtomicU64>,
    /// Connection-local cumulative record count committed to the active Flight sender.
    /// Unlike `cumulative_records_assigned`, this excludes batches buffered while paused.
    submitted_records: Arc<AtomicU64>,
    /// Last acknowledged cumulative record count (for recovery slicing).
    last_acked_records: Arc<AtomicU64>,
    /// Pause gate used while draining a close signal or rebuilding after failure; accepted
    /// ingests remain pending until recovery replays or finalizes them.
    is_paused: Arc<AtomicBool>,
    /// Observes server-initiated request/response teardown for test hooks.
    #[cfg(feature = "test-hooks")]
    graceful_close_rx: watch::Receiver<Option<Instant>>,
    /// Requests that the ACK processor cleanly half-close the current request body.
    explicit_close_tx: watch::Sender<Option<Instant>>,
    /// Final value sent as the HTTP `user-agent` header on every request.
    /// Either `"zerobus-sdk-rs/<version>"` or `"zerobus-sdk-rs/<version> <application_name>"`.
    /// Re-applied to each fresh Channel built during recovery.
    sdk_identifier: Arc<str>,
    /// Test seam (see [`ReconnectRebuildGate`]); compiled only under `test-hooks`.
    #[cfg(feature = "test-hooks")]
    reconnect_rebuild_gate: ReconnectRebuildGate,
    /// Test seam (see [`AckAppliedGate`]); compiled only under `test-hooks`.
    #[cfg(feature = "test-hooks")]
    ack_applied_gate: AckAppliedGate,
    /// Test seam (see [`ResponseDrainGate`]); compiled only under `test-hooks`.
    #[cfg(feature = "test-hooks")]
    response_drain_gate: ResponseDrainGate,
    /// Test seam (see [`CloseFinalizeGate`]); compiled only under `test-hooks`.
    #[cfg(feature = "test-hooks")]
    close_finalize_gate: CloseFinalizeGate,
}

impl ZerobusArrowStream {
    /// Creates a new Arrow Flight stream.
    ///
    /// If `recovery` is enabled in options, initial connection will be retried
    /// up to `recovery_retries` times with `recovery_backoff_ms` delay between attempts.
    /// One available retry may refresh credentials after an authentication rejection;
    /// a second authentication rejection remains terminal.
    #[instrument(level = "debug", skip_all, fields(table_name = %table_properties.table_name))]
    pub(crate) async fn new(
        endpoint: &str,
        tls_config: Arc<dyn TlsConfig>,
        connector_factory: Option<ConnectorFactory>,
        table_properties: ArrowTableProperties,
        headers_provider: Arc<dyn HeadersProvider>,
        options: ArrowStreamConfigurationOptions,
        sdk_identifier: Arc<str>,
    ) -> ZerobusResult<Self> {
        // A zero bound would deadlock every ingest and panic the zero-capacity channel.
        if options.max_inflight_batches == 0 {
            return Err(ZerobusError::InvalidArgument(
                "max_inflight_batches must be greater than 0".to_string(),
            ));
        }

        let (last_ack_tx, _last_ack_rx) = tokio::sync::watch::channel(None);
        let is_closed = Arc::new(AtomicBool::new(false));
        let pending_batches = Arc::new(Mutex::new(Vec::new()));
        let failed_batches = Arc::new(Mutex::new(Vec::new()));
        let recovery_attempts = Arc::new(AtomicU32::new(0));
        let batch_tx = Arc::new(Mutex::new(None));
        let receiver_task = Arc::new(Mutex::new(None));
        let cumulative_records_assigned = Arc::new(AtomicU64::new(0));
        let submitted_records = Arc::new(AtomicU64::new(0));
        let last_acked_records = Arc::new(AtomicU64::new(0));
        let is_paused = Arc::new(AtomicBool::new(false));
        let (graceful_close_tx, graceful_close_rx) = watch::channel(None);
        #[cfg(not(feature = "test-hooks"))]
        drop(graceful_close_rx);
        let (explicit_close_tx, explicit_close_rx) = watch::channel(None);
        // Capacity mirrors the batch_tx channel so a permit holder always has a slot.
        let inflight = Arc::new(Semaphore::new(options.max_inflight_batches));

        let (server_error_tx, server_error_rx) = watch::channel(None);
        let (recovery_close_error_tx, recovery_close_error_rx) = watch::channel(None);

        let stream = Self {
            table_properties,
            options,
            batch_tx,
            offset_generator: OffsetIdGenerator::default(),
            last_ack_tx,
            _last_ack_rx,
            is_closed,
            close_teardown_started: AtomicBool::new(false),
            close_error: Mutex::new(None),
            receiver_task,
            pending_batches,
            failed_batches,
            recovery_attempts,
            endpoint: endpoint.to_string(),
            tls_config,
            connector_factory,
            headers_provider,
            ingest_mutex: Arc::new(Mutex::new(())),
            inflight,
            server_error_tx,
            server_error_rx,
            recovery_close_error_rx,
            cumulative_records_assigned,
            submitted_records,
            last_acked_records,
            is_paused,
            #[cfg(feature = "test-hooks")]
            graceful_close_rx,
            explicit_close_tx,
            sdk_identifier,
            #[cfg(feature = "test-hooks")]
            reconnect_rebuild_gate: Arc::new(Mutex::new(None)),
            #[cfg(feature = "test-hooks")]
            ack_applied_gate: Arc::new(Mutex::new(None)),
            #[cfg(feature = "test-hooks")]
            response_drain_gate: Arc::new(Mutex::new(None)),
            #[cfg(feature = "test-hooks")]
            close_finalize_gate: Arc::new(Mutex::new(None)),
        };

        // Initialize the connection with retry logic.
        let endpoint = stream.endpoint.clone();
        let tls_config = Arc::clone(&stream.tls_config);
        let connector_factory = stream.connector_factory.clone();
        let table_properties = stream.table_properties.clone();
        let options = stream.options.clone();
        let headers_provider = Arc::clone(&stream.headers_provider);
        let strategy = FixedInterval::from_millis(options.recovery_backoff_ms)
            .take(options.recovery_retries as usize);

        let create_attempt = || {
            let endpoint = endpoint.clone();
            let tls_config = Arc::clone(&tls_config);
            let connector_factory = connector_factory.clone();
            let table_properties = table_properties.clone();
            let options = options.clone();
            let headers_provider = Arc::clone(&headers_provider);
            let sdk_identifier = Arc::clone(&stream.sdk_identifier);

            async move {
                Self::try_connect(
                    &endpoint,
                    &tls_config,
                    connector_factory.as_ref(),
                    &table_properties,
                    &options,
                    &headers_provider,
                    &sdk_identifier,
                )
                .await
            }
        };
        // Keep auth errors globally non-retryable, but let initial setup refresh one
        // stale credential when recovery has at least one retry in its budget.
        let mut auth_retry_available = options.recovery_retries > 0;
        let should_retry = |e: &ZerobusError| {
            should_retry_initial_connection(e, options.recovery, &mut auth_retry_available)
        };
        let creation = RetryIf::spawn(strategy, create_attempt, should_retry).await;

        let FlightConnection {
            response_stream,
            batch_tx: tx,
            request_body,
            replay_state_installed,
        } = match creation {
            Ok(result) => result,
            Err(e) => {
                error!("Arrow Flight stream creation failed after retries: {}", e);
                return Err(e);
            }
        };

        // Store the sender.
        {
            let mut batch_tx = stream.batch_tx.lock().await;
            *batch_tx = Some(tx);
        }

        // Spawn the supervisor task.
        let task = Self::spawn_supervisor_task(
            stream.endpoint.clone(),
            Arc::clone(&stream.tls_config),
            stream.connector_factory.clone(),
            stream.table_properties.clone(),
            stream.options.clone(),
            Arc::clone(&stream.headers_provider),
            Arc::clone(&stream.batch_tx),
            Arc::clone(&stream.is_closed),
            stream.last_ack_tx.clone(),
            Arc::clone(&stream.pending_batches),
            Arc::clone(&stream.failed_batches),
            Arc::clone(&stream.recovery_attempts),
            stream.server_error_tx.clone(),
            recovery_close_error_tx,
            Arc::clone(&stream.cumulative_records_assigned),
            Arc::clone(&stream.submitted_records),
            Arc::clone(&stream.last_acked_records),
            Arc::clone(&stream.is_paused),
            Arc::clone(&stream.ingest_mutex),
            response_stream,
            request_body,
            replay_state_installed,
            graceful_close_tx,
            explicit_close_rx,
            Arc::clone(&stream.sdk_identifier),
            #[cfg(feature = "test-hooks")]
            Arc::clone(&stream.reconnect_rebuild_gate),
            #[cfg(feature = "test-hooks")]
            Arc::clone(&stream.ack_applied_gate),
            #[cfg(feature = "test-hooks")]
            Arc::clone(&stream.response_drain_gate),
        );

        {
            let mut receiver_task = stream.receiver_task.lock().await;
            *receiver_task = Some(task);
        }

        info!(
            table_name = %stream.table_properties.table_name,
            "Arrow Flight stream created successfully"
        );

        Ok(stream)
    }

    /// Attempts to establish a Flight connection.
    /// Returns the response stream, batch sender, and request-body control on success.
    async fn try_connect(
        endpoint: &str,
        tls_config: &Arc<dyn TlsConfig>,
        connector_factory: Option<&ConnectorFactory>,
        table_properties: &ArrowTableProperties,
        options: &ArrowStreamConfigurationOptions,
        headers_provider: &Arc<dyn HeadersProvider>,
        sdk_identifier: &str,
    ) -> ZerobusResult<FlightConnection> {
        // Share one deadline across connection setup and auth-rejection invalidation.
        // This preserves the original auth error if a custom provider stalls instead of
        // reclassifying the attempt as a retryable setup timeout.
        let attempt_timeout = Duration::from_millis(options.recovery_timeout_ms);
        let attempt_started = tokio::time::Instant::now();
        let result = tokio::time::timeout(attempt_timeout, async {
            let client = Self::create_flight_client(
                endpoint,
                tls_config,
                connector_factory,
                table_properties,
                options,
                headers_provider,
                sdk_identifier,
            )
            .await?;

            Self::start_stream_connection(client, table_properties, options).await
        })
        .await
        .map_err(|_| {
            ZerobusError::CreateStreamError(tonic::Status::deadline_exceeded(
                "Stream creation timed out",
            ))
        })?;

        match result {
            Ok(connection) => Ok(connection),
            Err(error) => {
                // Drop the rejected token so the next attempt re-mints. A provider must
                // not be able to turn a known auth rejection into repeated generic
                // timeout retries by stalling here.
                let invalidate_timeout = attempt_timeout.saturating_sub(attempt_started.elapsed());
                if error.is_auth_rejection()
                    && tokio::time::timeout(invalidate_timeout, headers_provider.invalidate())
                        .await
                        .is_err()
                {
                    warn!(
                        timeout_ms = options.recovery_timeout_ms,
                        "Initial headers provider invalidation timed out; preserving auth rejection"
                    );
                }
                Err(error)
            }
        }
    }

    /// Creates a Flight client connected to the endpoint.
    async fn create_flight_client(
        endpoint: &str,
        tls_config: &Arc<dyn TlsConfig>,
        connector_factory: Option<&ConnectorFactory>,
        table_properties: &ArrowTableProperties,
        options: &ArrowStreamConfigurationOptions,
        headers_provider: &Arc<dyn HeadersProvider>,
        sdk_identifier: &str,
    ) -> ZerobusResult<FlightClient> {
        let connection_timeout = Duration::from_millis(options.connection_timeout_ms);

        let base_endpoint = Channel::from_shared(endpoint.to_string())
            .map_err(|e| ZerobusError::ChannelCreationError(e.to_string()))?
            .user_agent(sdk_identifier)
            .map_err(|e| ZerobusError::ChannelCreationError(e.to_string()))?
            .connect_timeout(connection_timeout)
            .timeout(connection_timeout);

        let configured_endpoint = tls_config.configure_endpoint(base_endpoint)?;
        let host = configured_endpoint.uri().host().unwrap_or_default();
        let channel = match proxy::resolve_connector(host, connector_factory)? {
            Some(proxy_connector) => {
                configured_endpoint.connect_with_connector_lazy(proxy_connector)
            }
            None => configured_endpoint.connect_lazy(),
        };

        let mut client = FlightClient::new(channel);

        // Add headers from the provider first, filtering out reserved headers.
        // The table name header is authoritative and must not be overridden.
        const TABLE_NAME_HEADER: &str = "x-databricks-zerobus-table-name";
        const AUTHORIZATION_HEADER: &str = "authorization";
        let headers = headers_provider.get_headers().await?;
        for (key, value) in headers {
            if key.eq_ignore_ascii_case(TABLE_NAME_HEADER) {
                warn!(
                    "HeadersProvider attempted to set reserved header '{}', ignoring",
                    TABLE_NAME_HEADER
                );
                continue;
            }
            if key.eq_ignore_ascii_case(AUTHORIZATION_HEADER) {
                let mut auth_value = MetadataValue::try_from(value.as_str()).map_err(|_| {
                    error!(table_name = %table_properties.table_name, "authorization token is not a valid HTTP header value");
                    ZerobusError::InvalidUCTokenError(
                        "authorization token is not a valid HTTP header value".to_string(),
                    )
                })?;
                auth_value.set_sensitive(true);
                client
                    .metadata_mut()
                    .insert(AUTHORIZATION_HEADER, auth_value);
                continue;
            }
            client.add_header(key, &value).map_err(|e| {
                ZerobusError::InvalidArgument(format!("Failed to add header '{}': {}", key, e))
            })?;
        }

        // Add the required table name header (authoritative, added last to ensure it's set).
        client
            .add_header(TABLE_NAME_HEADER, &table_properties.table_name)
            .map_err(|e| {
                ZerobusError::InvalidArgument(format!("Failed to add table name header: {}", e))
            })?;

        Ok(client)
    }

    /// Builds an encoded Flight request stream together with a control handle that can
    /// discard queued encoder output and report when tonic has polled the body to EOF.
    fn make_request_stream(
        batch_rx: mpsc::Receiver<Result<RecordBatch, FlightError>>,
        table_properties: &ArrowTableProperties,
        options: &ArrowStreamConfigurationOptions,
    ) -> ZerobusResult<(FlightRequestStream, RequestBodyControl)> {
        let ipc_write_options = make_ipc_write_options(options.ipc_compression)?;
        let schema = Arc::clone(&table_properties.schema);
        let batch_stream = tokio_stream::wrappers::ReceiverStream::new(batch_rx);

        // FlightDataEncoderBuilder handles schema framing, dictionary encoding, and
        // automatic batch chunking at 2 MiB. Each non-schema FlightData message gets a
        // sequential wire offset in app_metadata (the schema is message zero).
        let offset_counter = Arc::new(std::sync::atomic::AtomicI64::new(0));
        let offset_counter_clone = Arc::clone(&offset_counter);
        let encoded: FlightRequestStream = Box::pin(
            FlightDataEncoderBuilder::new()
                .with_schema(schema)
                .with_options(ipc_write_options)
                .build(batch_stream)
                .enumerate()
                .map(move |(idx, result)| {
                    result.map(|mut flight_data| {
                        if idx > 0 {
                            let offset = offset_counter_clone
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let metadata = FlightBatchMetadata::new(offset);
                            if let Ok(bytes) = metadata.to_bytes() {
                                flight_data.app_metadata = bytes.into();
                            }
                        }
                        flight_data
                    })
                }),
        );

        let shutdown = CancellationToken::new();
        let mut cancelled = Box::pin(shutdown.clone().cancelled_owned());
        let (eof_tx, eof_rx) = watch::channel(false);
        let mut encoded = encoded;
        // Cancellation is observed between complete FlightData items. A large logical
        // RecordBatch may therefore stop between independently decodable Arrow chunks,
        // but an item already yielded to tonic is never truncated mid-message.
        let controlled = futures::stream::poll_fn(move |cx| {
            if cancelled.as_mut().poll(cx).is_ready() {
                eof_tx.send_replace(true);
                return Poll::Ready(None);
            }

            match encoded.as_mut().poll_next(cx) {
                Poll::Ready(None) => {
                    eof_tx.send_replace(true);
                    Poll::Ready(None)
                }
                result => result,
            }
        });

        Ok((
            Box::pin(controlled),
            RequestBodyControl { shutdown, eof_rx },
        ))
    }

    /// Starts the Flight stream with the given client.
    /// Returns all per-connection state needed by the supervisor.
    ///
    /// This method waits for the server's "ready" signal (ack_up_to_offset = -1)
    /// to confirm that stream setup succeeded (auth, schema validation, table access).
    /// This allows setup errors to be detected during stream creation rather than
    /// later during batch ingestion.
    async fn start_stream_connection(
        mut client: FlightClient,
        table_properties: &ArrowTableProperties,
        options: &ArrowStreamConfigurationOptions,
    ) -> ZerobusResult<FlightConnection> {
        // Create channel for sending RecordBatches.
        let (batch_tx, batch_rx) =
            mpsc::channel::<Result<RecordBatch, FlightError>>(options.max_inflight_batches);

        let (flight_data_stream, request_body) =
            Self::make_request_stream(batch_rx, table_properties, options)?;

        // Start the DoPut stream.
        let mut response_stream = client
            .do_put(flight_data_stream)
            .await
            // `.into()` preserves the inner gRPC code; `Status::from_error` would
            // flatten it to `Unknown` and break auth/retry classification.
            .map_err(|e| ZerobusError::CreateStreamError(e.into()))?;

        // Wait for server's "ready" signal to confirm setup succeeded.
        // The server sends ack_up_to_offset = -1 after successful auth, schema validation,
        // and stream setup. This allows us to detect setup errors early.
        let setup_timeout = Duration::from_millis(options.connection_timeout_ms);
        match tokio::time::timeout(setup_timeout, response_stream.next()).await {
            Ok(Some(Ok(put_result))) => {
                // Parse the ack metadata to verify it's the ready signal.
                match FlightAckMetadata::from_bytes(&put_result.app_metadata) {
                    Ok(metadata) if metadata.is_stream_ready() => {
                        info!("Stream setup confirmed by server (ready signal received)");
                    }
                    Ok(metadata) => {
                        // Unexpected: got a real ack before sending any batches - protocol error.
                        error!(
                            "Unexpected ack during setup (offset {}), expected ready signal",
                            metadata.ack_up_to_offset
                        );
                        return Err(ZerobusError::UnexpectedStreamResponseError(format!(
                            "Expected ready signal, got ack for offset {}",
                            metadata.ack_up_to_offset
                        )));
                    }
                    Err(e) => {
                        // Malformed metadata - protocol error.
                        error!("Failed to parse setup response metadata: {}", e);
                        return Err(ZerobusError::UnexpectedStreamResponseError(format!(
                            "Malformed setup response metadata: {}",
                            e
                        )));
                    }
                }
            }
            Ok(Some(Err(flight_error))) => {
                // Server sent an error during setup (auth failed, schema mismatch, blocked table, etc.)
                error!("Stream setup failed: {:?}", flight_error);
                // Classify so a schema mismatch surfaces as ZerobusError::InvalidSchema
                // rather than a generic CreateStreamError.
                return Err(ZerobusError::from_setup_status(flight_error.into()));
            }
            Ok(None) => {
                // Server closed the stream without sending anything.
                error!("Server closed stream during setup without response");
                return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                    "Server closed stream during setup",
                )));
            }
            Err(_timeout) => {
                // Timeout waiting for server response.
                error!(
                    "Timed out waiting for server setup confirmation ({}ms)",
                    options.connection_timeout_ms
                );
                return Err(ZerobusError::ConnectionTimeout(format!(
                    "Timed out waiting for server setup confirmation ({}ms)",
                    options.connection_timeout_ms
                )));
            }
        }

        Ok(FlightConnection {
            response_stream: Box::pin(response_stream),
            batch_tx,
            request_body,
            replay_state_installed: true,
        })
    }

    /// Publishes a terminal supervisor error around finalization so both new and already
    /// parked waiters observe the same cause.
    async fn publish_and_finalize_terminal_error(
        error: &ZerobusError,
        server_error_tx: &watch::Sender<Option<ZerobusError>>,
        ingest_mutex: &Arc<Mutex<()>>,
        is_closed: &Arc<AtomicBool>,
        pending_batches: &Arc<Mutex<Vec<PendingBatch>>>,
        failed_batches: &Arc<Mutex<Vec<RecordBatch>>>,
        last_acked_records: &Arc<AtomicU64>,
    ) {
        let _ = server_error_tx.send(Some(error.clone()));
        Self::finalize_closed(
            ingest_mutex,
            is_closed,
            pending_batches,
            failed_batches,
            last_acked_records,
        )
        .await;
        let _ = server_error_tx.send(Some(error.clone()));
    }

    /// Returns the permanent recovery cause retained for a later explicit close.
    fn retained_permanent_recovery_error(
        recovery_close_error_tx: &watch::Sender<Option<ZerobusError>>,
    ) -> Option<ZerobusError> {
        recovery_close_error_tx
            .borrow()
            .clone()
            .filter(|error| !error.is_retryable())
    }

    /// Pauses new sends and waits the configured recovery backoff unless explicit close
    /// takes ownership first.
    async fn prepare_recovery_attempt(
        ingest_mutex: &Arc<Mutex<()>>,
        is_paused: &Arc<AtomicBool>,
        batch_tx: &BatchSender,
        backoff: Duration,
        explicit_close_rx: &mut watch::Receiver<Option<Instant>>,
    ) -> bool {
        // Pause + detach is atomic relative to ingest_batch: an in-flight ingest either
        // completes first or observes the pause and buffers for replay.
        tokio::select! {
            biased;
            _ = Self::pause_and_detach_sender(ingest_mutex, is_paused, batch_tx) => {}
            _ = Self::wait_for_explicit_close(explicit_close_rx) => {
                debug!("Supervisor: explicit close interrupted recovery pause");
                return false;
            }
        }

        tokio::select! {
            _ = sleep(backoff) => true,
            _ = Self::wait_for_explicit_close(explicit_close_rx) => {
                debug!("Supervisor: explicit close interrupted recovery backoff");
                false
            }
        }
    }

    /// Drives reconnect until it completes or explicit close takes ownership. Reconnect
    /// enforces its own recovery deadline so a live replacement request can be half-closed
    /// before the future returns.
    async fn wait_for_reconnect_result<F>(
        reconnect: F,
        explicit_close_rx: &mut watch::Receiver<Option<Instant>>,
    ) -> ZerobusResult<ReconnectOutcome>
    where
        F: Future<Output = ZerobusResult<ReconnectOutcome>>,
    {
        tokio::pin!(reconnect);
        tokio::select! {
            biased;
            result = &mut reconnect => result,
            _ = Self::wait_for_explicit_close(explicit_close_rx) => {
                reconnect.await
            }
        }
    }

    /// Spawns the supervisor task that manages the stream lifecycle and recovery.
    ///
    /// The supervisor runs a loop that:
    /// 1. Processes acknowledgments from the server
    /// 2. When the ack processor returns with a retriable error, attempts recovery
    /// 3. Continues until the stream closes or a terminal error occurs
    #[allow(clippy::too_many_arguments)]
    fn spawn_supervisor_task(
        endpoint: String,
        tls_config: Arc<dyn TlsConfig>,
        connector_factory: Option<ConnectorFactory>,
        table_properties: ArrowTableProperties,
        options: ArrowStreamConfigurationOptions,
        headers_provider: Arc<dyn HeadersProvider>,
        batch_tx: BatchSender,
        is_closed: Arc<AtomicBool>,
        last_ack_tx: tokio::sync::watch::Sender<Option<OffsetId>>,
        pending_batches: Arc<Mutex<Vec<PendingBatch>>>,
        failed_batches: Arc<Mutex<Vec<RecordBatch>>>,
        recovery_attempts: Arc<AtomicU32>,
        server_error_tx: watch::Sender<Option<ZerobusError>>,
        recovery_close_error_tx: watch::Sender<Option<ZerobusError>>,
        cumulative_records_assigned: Arc<AtomicU64>,
        submitted_records: Arc<AtomicU64>,
        last_acked_records: Arc<AtomicU64>,
        is_paused: Arc<AtomicBool>,
        ingest_mutex: Arc<Mutex<()>>,
        initial_response_stream: FlightResponseStream,
        initial_request_body: RequestBodyControl,
        initial_replay_state_installed: bool,
        graceful_close_tx: watch::Sender<Option<Instant>>,
        mut explicit_close_rx: watch::Receiver<Option<Instant>>,
        sdk_identifier: Arc<str>,
        #[cfg(feature = "test-hooks")] reconnect_rebuild_gate: ReconnectRebuildGate,
        #[cfg(feature = "test-hooks")] ack_applied_gate: AckAppliedGate,
        #[cfg(feature = "test-hooks")] response_drain_gate: ResponseDrainGate,
    ) -> tokio::task::JoinHandle<ZerobusResult<()>> {
        tokio::spawn(async move {
            let ack_timeout = Duration::from_millis(options.server_lack_of_ack_timeout_ms);
            let mut response_stream = Some(initial_response_stream);
            let mut request_body = Some(initial_request_body);
            let mut replay_state_installed = initial_replay_state_installed;
            // Carries a failed reconnect's real error into the next iteration's handling
            // instead of round-tripping a synthetic error through a dummy stream.
            let mut pending_error: Option<PendingSupervisorError> = None;
            // Retains a permanent recovery-arm error while an explicit close drains a
            // replacement connection. Clean transport teardown must not erase its cause.
            let mut pending_close_error: Option<PendingSupervisorError> = None;

            loop {
                if is_closed.load(Ordering::Relaxed) {
                    debug!("Supervisor: Stream closed, exiting");
                    return Ok(());
                }

                // Run process_acks until it returns — unless a prior reconnect attempt
                // failed, in which case carry that real error into the handling below
                // (preserving its message and retry classification).
                let (result, auth_already_invalidated) = if let Some(pending) = pending_error.take()
                {
                    (Err(pending.error), pending.auth_invalidated)
                } else {
                    (
                        Self::process_acks(
                            response_stream
                                .take()
                                .expect("response_stream present when no pending reconnect error"),
                            Arc::clone(&is_closed),
                            last_ack_tx.clone(),
                            Arc::clone(&pending_batches),
                            ack_timeout,
                            server_error_tx.clone(),
                            Arc::clone(&submitted_records),
                            Arc::clone(&last_acked_records),
                            Arc::clone(&is_paused),
                            Arc::clone(&ingest_mutex),
                            Arc::clone(&batch_tx),
                            request_body
                                .take()
                                .expect("request_body present when processing a response stream"),
                            graceful_close_tx.clone(),
                            explicit_close_rx.clone(),
                            &options,
                            replay_state_installed,
                            #[cfg(feature = "test-hooks")]
                            Arc::clone(&ack_applied_gate),
                            #[cfg(feature = "test-hooks")]
                            Arc::clone(&response_drain_gate),
                        )
                        .await,
                        false,
                    )
                };
                let (result, auth_already_invalidated) = match pending_close_error.take() {
                    // A retained auth rejection is a fallback for clean or retryable
                    // shutdown outcomes. A newer permanent peer error is more specific.
                    Some(_) if result.as_ref().is_err_and(|error| !error.is_retryable()) => {
                        (result, auth_already_invalidated)
                    }
                    Some(pending) => (Err(pending.error), pending.auth_invalidated),
                    None => (result, auth_already_invalidated),
                };

                // Explicit close owns teardown from this point. `process_acks` either
                // completed its clean half-close/drain or returned an error while doing so;
                // in both cases, do not reconnect a stream the caller is closing.
                if explicit_close_rx.borrow().is_some() {
                    debug!("Supervisor: explicit close completed, skipping recovery");
                    let close_deadline =
                        (*explicit_close_rx.borrow()).expect("explicit close was checked above");
                    return Self::finish_explicit_close_result(
                        result,
                        auth_already_invalidated,
                        close_deadline,
                        &headers_provider,
                        &server_error_tx,
                    )
                    .await;
                }

                // Check if stream was closed during processing.
                if is_closed.load(Ordering::Relaxed) {
                    debug!("Supervisor: Stream closed after process_acks, exiting");
                    return result;
                }

                // Handle the result.
                match result {
                    Ok(()) => {
                        // Stream ended gracefully.
                        debug!("Supervisor: process_acks completed successfully");
                        return Ok(());
                    }
                    Err(ref error)
                        if (error.is_retryable() || auth_already_invalidated)
                            && options.recovery =>
                    {
                        // Retriable error (or a reconnect auth rejection we've chosen to
                        // retry with re-minted credentials) - attempt recovery.
                        if auth_already_invalidated {
                            recovery_close_error_tx.send_replace(Some(error.clone()));
                        } else if !error.is_retryable()
                            || Self::retained_permanent_recovery_error(&recovery_close_error_tx)
                                .is_none()
                        {
                            // Keep a permanent retained cause across a later retryable
                            // reconnect failure; otherwise clear stale close-only state.
                            recovery_close_error_tx.send_replace(None);
                        }
                        let attempts = recovery_attempts.fetch_add(1, Ordering::Relaxed);
                        if attempts >= options.recovery_retries {
                            error!(
                                attempts = attempts,
                                max_retries = options.recovery_retries,
                                "Supervisor: Max recovery retries exceeded"
                            );
                            // Prefer a permanent retained auth/peer cause over the latest
                            // retryable reconnect failure that exhausted the budget.
                            let terminal =
                                Self::retained_permanent_recovery_error(&recovery_close_error_tx)
                                    .unwrap_or_else(|| error.clone());
                            Self::publish_and_finalize_terminal_error(
                                &terminal,
                                &server_error_tx,
                                &ingest_mutex,
                                &is_closed,
                                &pending_batches,
                                &failed_batches,
                                &last_acked_records,
                            )
                            .await;
                            return Err(terminal);
                        }

                        info!(
                            attempt = attempts + 1,
                            max_retries = options.recovery_retries,
                            error = %error,
                            "Supervisor: Attempting recovery after retriable error"
                        );

                        if !Self::prepare_recovery_attempt(
                            &ingest_mutex,
                            &is_paused,
                            &batch_tx,
                            Duration::from_millis(options.recovery_backoff_ms),
                            &mut explicit_close_rx,
                        )
                        .await
                        {
                            return Self::explicit_close_result(
                                Err(error.clone()),
                                &server_error_tx,
                            );
                        }

                        // The current error is being retried, so do not expose it to ordinary
                        // waiters as terminal. Explicit-close outcomes publish a retained
                        // permanent cause when close actually takes ownership.
                        let _ = server_error_tx.send(None);

                        if explicit_close_rx.borrow().is_some() {
                            debug!("Supervisor: explicit close skipped reconnect");
                            return Self::explicit_close_result(
                                Err(error.clone()),
                                &server_error_tx,
                            );
                        }

                        // Share one deadline across reconnect and auth-rejection
                        // invalidation so refresh receives only the remaining recovery
                        // timeout instead of starting a second full timeout.
                        let recovery_deadline = tokio::time::Instant::now()
                            + Duration::from_millis(options.recovery_timeout_ms);
                        let mut reconnect_close_rx = explicit_close_rx.clone();
                        let reconnect = Self::reconnect(
                            &endpoint,
                            &tls_config,
                            connector_factory.as_ref(),
                            &table_properties,
                            &options,
                            &headers_provider,
                            &batch_tx,
                            &pending_batches,
                            &last_ack_tx,
                            &cumulative_records_assigned,
                            &submitted_records,
                            &last_acked_records,
                            &sdk_identifier,
                            &ingest_mutex,
                            &is_paused,
                            &mut reconnect_close_rx,
                            recovery_deadline,
                            #[cfg(feature = "test-hooks")]
                            &reconnect_rebuild_gate,
                            #[cfg(feature = "test-hooks")]
                            &ack_applied_gate,
                        );
                        let reconnect_result =
                            Self::wait_for_reconnect_result(reconnect, &mut explicit_close_rx)
                                .await;

                        match reconnect_result {
                            Ok(ReconnectOutcome::Connected(connection)) => {
                                info!("Supervisor: Recovery successful, resuming");
                                recovery_attempts.store(0, Ordering::Relaxed);
                                recovery_close_error_tx.send_replace(None);
                                // Clear any transient error published by the prior connection.
                                let _ = server_error_tx.send(None);
                                // is_paused was already cleared inside reconnect().
                                response_stream = Some(connection.response_stream);
                                request_body = Some(connection.request_body);
                                replay_state_installed = connection.replay_state_installed;
                            }
                            Ok(ReconnectOutcome::ClosedBeforeRequest) => {
                                debug!(
                                    "Supervisor: explicit close interrupted reconnect before a \
                                     replacement Flight request was created"
                                );
                                // Preserve the recovery-arm error (including auth rejections
                                // cleared from server_error_tx before this reconnect attempt).
                                return Self::explicit_close_result(
                                    Err(error.clone()),
                                    &server_error_tx,
                                );
                            }
                            Ok(ReconnectOutcome::Closing(connection)) => {
                                debug!(
                                    "Supervisor: handing replacement connection to explicit-close cleanup"
                                );
                                if auth_already_invalidated {
                                    let _ = server_error_tx.send(Some(error.clone()));
                                    pending_close_error = Some(PendingSupervisorError {
                                        error: error.clone(),
                                        auth_invalidated: true,
                                    });
                                }
                                response_stream = Some(connection.response_stream);
                                request_body = Some(connection.request_body);
                                replay_state_installed = connection.replay_state_installed;
                                continue;
                            }
                            Ok(ReconnectOutcome::CloseTimedOutAfterRequest) => {
                                if auth_already_invalidated {
                                    return Self::explicit_close_result(
                                        Err(error.clone()),
                                        &server_error_tx,
                                    );
                                }
                                let error = Self::explicit_close_reconnect_timeout();
                                let _ = server_error_tx.send(Some(error.clone()));
                                return Err(error);
                            }
                            Err(e) => {
                                warn!("Supervisor: Reconnection failed: {}", e);
                                // Ask the provider to invalidate cached authentication
                                // state after an auth rejection, then retry even though
                                // such errors are otherwise non-retryable. Preserve this
                                // reconnect error if refresh or later recovery cannot proceed.
                                let mut auth_invalidated = false;
                                if e.is_auth_rejection() {
                                    let invalidation = headers_provider.invalidate();
                                    tokio::pin!(invalidation);
                                    let invalidation_result = tokio::select! {
                                        biased;
                                        result = tokio::time::timeout_at(
                                            recovery_deadline,
                                            &mut invalidation,
                                        ) => result,
                                        close_deadline = Self::wait_for_explicit_close(
                                            &mut explicit_close_rx,
                                        ) => {
                                            // Publish before cleanup so close() cannot lose the
                                            // permanent rejection if invalidation stalls.
                                            let _ = server_error_tx.send(Some(e.clone()));
                                            let invalidation_deadline =
                                                close_deadline.min(recovery_deadline);
                                            if tokio::time::timeout_at(
                                                invalidation_deadline,
                                                &mut invalidation,
                                            )
                                            .await
                                            .is_err()
                                            {
                                                warn!(
                                                    "Explicit close ended while invalidating the \
                                                     rejected reconnect credential"
                                                );
                                            }
                                            return Err(e);
                                        }
                                    };
                                    match invalidation_result {
                                        Ok(()) => auth_invalidated = true,
                                        Err(_) => {
                                            warn!(
                                                timeout_ms = options.recovery_timeout_ms,
                                                "Recovery deadline reached while invalidating \
                                                 the headers provider; terminating recovery"
                                            );
                                            // A custom provider must not stall recovery
                                            // indefinitely. Close with the original auth
                                            // rejection.
                                            Self::publish_and_finalize_terminal_error(
                                                &e,
                                                &server_error_tx,
                                                &ingest_mutex,
                                                &is_closed,
                                                &pending_batches,
                                                &failed_batches,
                                                &last_acked_records,
                                            )
                                            .await;
                                            return Err(e);
                                        }
                                    }
                                }
                                if explicit_close_rx.borrow().is_some() {
                                    // A retryable reconnect failure is only cleanup context;
                                    // retain the earlier permanent auth rejection. Any newer
                                    // permanent peer error supersedes it.
                                    let close_error = Self::preferred_close_error(
                                        auth_already_invalidated.then(|| error.clone()),
                                        Some(e),
                                    )
                                    .expect("reconnect supplied a close error");
                                    return Self::explicit_close_result(
                                        Err(close_error),
                                        &server_error_tx,
                                    );
                                }
                                // Supersede the retained close-only cause only when this
                                // reconnect outcome replaces it. Retryable failures must keep
                                // any permanent retained auth cause for a later close() or
                                // retry-budget exhaustion.
                                if e.is_auth_rejection()
                                    || !e.is_retryable()
                                    || Self::retained_permanent_recovery_error(
                                        &recovery_close_error_tx,
                                    )
                                    .is_none()
                                {
                                    recovery_close_error_tx.send_replace(None);
                                }
                                pending_error = Some(PendingSupervisorError {
                                    error: e,
                                    auth_invalidated,
                                });
                            }
                        }
                    }
                    Err(error) => {
                        error!("Supervisor: Non-retriable error, closing stream: {}", error);
                        Self::publish_and_finalize_terminal_error(
                            &error,
                            &server_error_tx,
                            &ingest_mutex,
                            &is_closed,
                            &pending_batches,
                            &failed_batches,
                            &last_acked_records,
                        )
                        .await;
                        // Ask the provider to invalidate cached authentication state after
                        // a terminal rejection. The stream is already finalized and waiters
                        // have the real error; bound the callback so the supervisor cannot
                        // remain alive indefinitely.
                        if error.is_auth_rejection()
                            && tokio::time::timeout(
                                Duration::from_millis(options.recovery_timeout_ms),
                                headers_provider.invalidate(),
                            )
                            .await
                            .is_err()
                        {
                            warn!(
                                timeout_ms = options.recovery_timeout_ms,
                                "Terminal headers provider invalidation timed out"
                            );
                        }
                        return Err(error);
                    }
                }
            }
        })
    }

    fn reconnect_timeout_error(recovery_timeout_ms: u64) -> ZerobusError {
        ZerobusError::ConnectionTimeout(format!(
            "Reconnection timed out after {recovery_timeout_ms}ms"
        ))
    }

    /// Half-closes and drains a replacement connection after a setup or recovery timeout.
    /// The triggering timeout remains the primary result unless cleanup observes a newer
    /// permanent peer error.
    async fn finish_timed_out_reconnect(
        connection: FlightConnection,
        cleanup_deadline: Instant,
        timeout_error: ZerobusError,
        ack_progress: &AckProgress<'_>,
    ) -> ZerobusResult<ReconnectOutcome> {
        let FlightConnection {
            mut response_stream,
            request_body,
            replay_state_installed,
            ..
        } = connection;
        request_body.shutdown();

        let drain_response = async {
            loop {
                // `timeout_at` polls its inner future first. Without this precheck, an
                // always-ready response stream could win every poll past the deadline.
                if Instant::now() >= cleanup_deadline {
                    return None;
                }
                match tokio::time::timeout_at(cleanup_deadline, response_stream.next()).await {
                    Ok(Some(Ok(put_result))) => {
                        match FlightAckMetadata::from_bytes(&put_result.app_metadata) {
                            Ok(ack) if ack.ack_up_to_records > 0 && replay_state_installed => {
                                if let Err(error) =
                                    Self::apply_acknowledgment(&ack, ack_progress).await
                                {
                                    return Some(error);
                                }
                            }
                            Ok(ack) if ack.ack_up_to_records > 0 => {
                                return Some(Self::pre_replay_ack_error(&ack));
                            }
                            Ok(_) => {}
                            Err(error) => {
                                warn!(%error, "Failed to parse ack metadata during reconnect cleanup");
                            }
                        }
                    }
                    Ok(Some(Err(flight_error))) => {
                        let status: tonic::Status = flight_error.into();
                        return Some(ZerobusError::StreamClosedError(status));
                    }
                    Ok(None) | Err(_) => return None,
                }
            }
        };
        let (request_reached_eof, peer_error) = tokio::join!(
            request_body.wait_for_eof_until(cleanup_deadline),
            drain_response,
        );
        if !request_reached_eof {
            warn!("Replacement request did not reach EOF before recovery cleanup ended");
        }

        if let Some(error) = peer_error.filter(|error| !error.is_retryable()) {
            return Err(error);
        }
        Err(timeout_error)
    }

    /// Reconnects to the server and replays pending batches.
    ///
    /// On successful replay, holds `ingest_mutex` until `is_paused` is cleared so
    /// subsequently admitted ingests send normally. Error paths remain paused for
    /// supervisor retry or finalization.
    #[allow(clippy::too_many_arguments)]
    async fn reconnect(
        endpoint: &str,
        tls_config: &Arc<dyn TlsConfig>,
        connector_factory: Option<&ConnectorFactory>,
        table_properties: &ArrowTableProperties,
        options: &ArrowStreamConfigurationOptions,
        headers_provider: &Arc<dyn HeadersProvider>,
        batch_tx: &BatchSender,
        pending_batches: &Arc<Mutex<Vec<PendingBatch>>>,
        last_ack_tx: &watch::Sender<Option<OffsetId>>,
        cumulative_records_assigned: &Arc<AtomicU64>,
        submitted_records: &Arc<AtomicU64>,
        last_acked_records: &Arc<AtomicU64>,
        sdk_identifier: &str,
        ingest_mutex: &Arc<Mutex<()>>,
        is_paused: &Arc<AtomicBool>,
        explicit_close_rx: &mut watch::Receiver<Option<Instant>>,
        recovery_deadline: Instant,
        #[cfg(feature = "test-hooks")] reconnect_rebuild_gate: &ReconnectRebuildGate,
        #[cfg(feature = "test-hooks")] ack_applied_gate: &AckAppliedGate,
    ) -> ZerobusResult<ReconnectOutcome> {
        // Positive ACKs from the replacement are meaningful only after replay has
        // installed connection-local ranges and reset the record counters.
        let replay_state_installed = AtomicBool::new(false);
        let ack_progress = AckProgress {
            submitted_records,
            last_acked_records,
            pending_batches,
            last_ack_tx,
            #[cfg(feature = "test-hooks")]
            ack_applied_gate,
        };

        // No Flight request exists yet, so reconnect can stop immediately if explicit close
        // arrives while credentials or client setup are still in progress.
        let client = tokio::select! {
            biased;
            result = Self::create_flight_client(
                endpoint,
                tls_config,
                connector_factory,
                table_properties,
                options,
                headers_provider,
                sdk_identifier,
            ) => result?,
            _ = Self::wait_for_explicit_close(explicit_close_rx) => {
                return Ok(ReconnectOutcome::ClosedBeforeRequest);
            }
            _ = tokio::time::sleep_until(recovery_deadline) => {
                return Err(Self::reconnect_timeout_error(options.recovery_timeout_ms));
            }
        };

        // Create new channel.
        let (tx, batch_rx) =
            mpsc::channel::<Result<RecordBatch, FlightError>>(options.max_inflight_batches);

        let (flight_data_stream, request_body) =
            Self::make_request_stream(batch_rx, table_properties, options)?;

        // Start the DoPut stream. Once this future has created a response stream, explicit
        // close must return the whole connection to the supervisor instead of dropping it.
        let mut flight_client = client;
        let mut do_put = Box::pin(flight_client.do_put(flight_data_stream));
        let mut response_stream = tokio::select! {
            biased;
            result = &mut do_put => result
                // `.into()` preserves the inner gRPC code; `Status::from_error` would
                // flatten it to `Unknown` and break auth/retry classification.
                .map_err(|e| ZerobusError::CreateStreamError(e.into()))?,
            close_deadline = Self::wait_for_explicit_close(explicit_close_rx) => {
                // The request may already be live even though tonic has not returned its
                // response stream. Stop its body, then keep driving tonic so the supervisor
                // can recover a drainable response until the caller's original close deadline.
                request_body.shutdown();
                let response_and_eof = tokio::time::timeout_at(close_deadline, async {
                    tokio::join!(
                        &mut do_put,
                        request_body.wait_for_eof_until(close_deadline),
                    )
                })
                .await;
                match response_and_eof {
                    Ok((Ok(response_stream), _)) => {
                        return Ok(ReconnectOutcome::Closing(FlightConnection {
                            response_stream: Box::pin(response_stream),
                            batch_tx: tx,
                            request_body,
                            replay_state_installed: replay_state_installed.load(Ordering::Acquire),
                        }));
                    }
                    Ok((Err(error), _)) => {
                        debug!(%error, "Replacement DoPut ended while explicit close was starting");
                        return Err(ZerobusError::CreateStreamError(error.into()));
                    }
                    Err(_) => {
                        warn!(
                            "Explicit close reached its deadline before replacement DoPut \
                             returned a response stream"
                        );
                        return Ok(ReconnectOutcome::CloseTimedOutAfterRequest);
                    }
                }
            }
            _ = tokio::time::sleep_until(recovery_deadline) => {
                // Unlike the old outer timeout, keep ownership of the live DoPut long
                // enough to send request EOF and drain any response the peer provides.
                let cleanup_deadline =
                    Instant::now() + Duration::from_millis(CLOSE_SIGNAL_DRAIN_TIMEOUT_MS);
                request_body.shutdown();
                let response_and_eof = tokio::time::timeout_at(cleanup_deadline, async {
                    tokio::join!(
                        &mut do_put,
                        request_body.wait_for_eof_until(cleanup_deadline),
                    )
                })
                .await;
                return match response_and_eof {
                    Ok((Ok(response_stream), _)) => Self::finish_timed_out_reconnect(
                        FlightConnection {
                            response_stream: Box::pin(response_stream),
                            batch_tx: tx,
                            request_body,
                            replay_state_installed: replay_state_installed.load(Ordering::Acquire),
                        },
                        cleanup_deadline,
                        Self::reconnect_timeout_error(options.recovery_timeout_ms),
                        &ack_progress,
                    )
                    .await,
                    Ok((Err(error), _)) => {
                        Err(ZerobusError::CreateStreamError(error.into()))
                    }
                    Err(_) => Err(Self::reconnect_timeout_error(
                        options.recovery_timeout_ms,
                    )),
                };
            }
        };

        // Wait for server's "ready" signal to confirm reconnection succeeded.
        let setup_timeout = Duration::from_millis(options.connection_timeout_ms);
        let setup_result = tokio::select! {
            biased;
            result = tokio::time::timeout(setup_timeout, response_stream.next()) => Some(result),
            _ = Self::wait_for_explicit_close(explicit_close_rx) => None,
            _ = tokio::time::sleep_until(recovery_deadline) => {
                let cleanup_deadline =
                    Instant::now() + Duration::from_millis(CLOSE_SIGNAL_DRAIN_TIMEOUT_MS);
                return Self::finish_timed_out_reconnect(
                    FlightConnection {
                        response_stream: Box::pin(response_stream),
                        batch_tx: tx,
                        request_body,
                        replay_state_installed: replay_state_installed.load(Ordering::Acquire),
                    },
                    cleanup_deadline,
                    Self::reconnect_timeout_error(options.recovery_timeout_ms),
                    &ack_progress,
                )
                .await;
            }
        };
        let Some(setup_result) = setup_result else {
            return Ok(ReconnectOutcome::Closing(FlightConnection {
                response_stream: Box::pin(response_stream),
                batch_tx: tx,
                request_body,
                replay_state_installed: replay_state_installed.load(Ordering::Acquire),
            }));
        };
        match setup_result {
            Ok(Some(Ok(put_result))) => {
                // Verify it's the ready signal.
                match FlightAckMetadata::from_bytes(&put_result.app_metadata) {
                    Ok(metadata) if metadata.is_stream_ready() => {
                        info!("Reconnection confirmed by server (ready signal received)");
                    }
                    Ok(metadata) => {
                        error!(
                            "Unexpected ack during reconnect (offset {}), expected ready signal",
                            metadata.ack_up_to_offset
                        );
                        return Err(ZerobusError::UnexpectedStreamResponseError(format!(
                            "Expected ready signal, got ack for offset {}",
                            metadata.ack_up_to_offset
                        )));
                    }
                    Err(e) => {
                        error!("Failed to parse reconnect response metadata: {}", e);
                        return Err(ZerobusError::UnexpectedStreamResponseError(format!(
                            "Malformed reconnect response metadata: {}",
                            e
                        )));
                    }
                }
            }
            Ok(Some(Err(flight_error))) => {
                error!("Reconnection setup failed: {:?}", flight_error);
                // Classify so a schema mismatch surfaces as ZerobusError::InvalidSchema
                // rather than a generic CreateStreamError.
                return Err(ZerobusError::from_setup_status(flight_error.into()));
            }
            Ok(None) => {
                error!("Server closed stream during reconnect without response");
                return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                    "Server closed stream during reconnect",
                )));
            }
            Err(_timeout) => {
                error!(
                    "Timed out waiting for server reconnect confirmation ({}ms)",
                    options.connection_timeout_ms
                );
                let timeout_error = ZerobusError::ConnectionTimeout(format!(
                    "Timed out waiting for server reconnect confirmation ({}ms)",
                    options.connection_timeout_ms
                ));
                let cleanup_deadline =
                    Instant::now() + Duration::from_millis(CLOSE_SIGNAL_DRAIN_TIMEOUT_MS);
                return Self::finish_timed_out_reconnect(
                    FlightConnection {
                        response_stream: Box::pin(response_stream),
                        batch_tx: tx,
                        request_body,
                        replay_state_installed: replay_state_installed.load(Ordering::Acquire),
                    },
                    cleanup_deadline,
                    timeout_error,
                    &ack_progress,
                )
                .await;
            }
        }

        // Store the new sender before taking ingest_mutex for the rebuild. Safe only
        // because is_paused stays true until after replay: a concurrent ingest that
        // observes this new sender still buffers rather than sending out of order.
        let mut tx_guard = tokio::select! {
            biased;
            guard = batch_tx.lock() => guard,
            _ = Self::wait_for_explicit_close(explicit_close_rx) => {
                return Ok(ReconnectOutcome::Closing(FlightConnection {
                    response_stream: Box::pin(response_stream),
                    batch_tx: tx,
                    request_body,
                    replay_state_installed: replay_state_installed.load(Ordering::Acquire),
                }));
            }
            _ = tokio::time::sleep_until(recovery_deadline) => {
                let cleanup_deadline =
                    Instant::now() + Duration::from_millis(CLOSE_SIGNAL_DRAIN_TIMEOUT_MS);
                return Self::finish_timed_out_reconnect(
                    FlightConnection {
                        response_stream: Box::pin(response_stream),
                        batch_tx: tx,
                        request_body,
                        replay_state_installed: replay_state_installed.load(Ordering::Acquire),
                    },
                    cleanup_deadline,
                    Self::reconnect_timeout_error(options.recovery_timeout_ms),
                    &ack_progress,
                )
                .await;
            }
        };
        *tx_guard = Some(tx.clone());
        drop(tx_guard);

        if explicit_close_rx.borrow().is_some() {
            return Ok(ReconnectOutcome::Closing(FlightConnection {
                response_stream: Box::pin(response_stream),
                batch_tx: tx,
                request_body,
                replay_state_installed: replay_state_installed.load(Ordering::Acquire),
            }));
        }

        // Counters are reset atomically with the range rebuild inside
        // replay_pending_batches, so a concurrent ingest can't fetch_add a reset counter,
        // fabricate a low range, and have replay drop it as fully-acked.
        let acked_before_disconnect = last_acked_records.load(Ordering::Acquire);

        // Test seam: pause after the connection is established but before ingest_mutex is
        // held and ranges/watermark are rebuilt, so a test can schedule a paused ingest
        // that wins ingest_mutex first (reset/rebase race) or drive a concurrent close().
        #[cfg(feature = "test-hooks")]
        {
            let barrier = reconnect_rebuild_gate.lock().await.take();
            if let Some(barrier) = barrier {
                barrier.reached.notify_one();
                let proceed = tokio::select! {
                    biased;
                    _ = barrier.proceed.notified() => true,
                    _ = Self::wait_for_explicit_close(explicit_close_rx) => false,
                    _ = tokio::time::sleep_until(recovery_deadline) => {
                        let cleanup_deadline = Instant::now()
                            + Duration::from_millis(CLOSE_SIGNAL_DRAIN_TIMEOUT_MS);
                        return Self::finish_timed_out_reconnect(
                            FlightConnection {
                                response_stream: Box::pin(response_stream),
                                batch_tx: tx,
                                request_body,
                                replay_state_installed: replay_state_installed.load(Ordering::Acquire),
                            },
                            cleanup_deadline,
                            Self::reconnect_timeout_error(options.recovery_timeout_ms),
                            &ack_progress,
                        )
                        .await;
                    }
                };
                if !proceed {
                    return Ok(ReconnectOutcome::Closing(FlightConnection {
                        response_stream: Box::pin(response_stream),
                        batch_tx: tx,
                        request_body,
                        replay_state_installed: replay_state_installed.load(Ordering::Acquire),
                    }));
                }
            }
        }

        // Hold ingest_mutex across the replay so no concurrent ingest interleaves.
        let _ingest_guard = tokio::select! {
            biased;
            guard = ingest_mutex.lock() => guard,
            _ = Self::wait_for_explicit_close(explicit_close_rx) => {
                return Ok(ReconnectOutcome::Closing(FlightConnection {
                    response_stream: Box::pin(response_stream),
                    batch_tx: tx,
                    request_body,
                    replay_state_installed: replay_state_installed.load(Ordering::Acquire),
                }));
            }
            _ = tokio::time::sleep_until(recovery_deadline) => {
                let cleanup_deadline =
                    Instant::now() + Duration::from_millis(CLOSE_SIGNAL_DRAIN_TIMEOUT_MS);
                return Self::finish_timed_out_reconnect(
                    FlightConnection {
                        response_stream: Box::pin(response_stream),
                        batch_tx: tx,
                        request_body,
                        replay_state_installed: replay_state_installed.load(Ordering::Acquire),
                    },
                    cleanup_deadline,
                    Self::reconnect_timeout_error(options.recovery_timeout_ms),
                    &ack_progress,
                )
                .await;
            }
        };
        let replay_completed = tokio::select! {
            biased;
            result = Self::replay_pending_batches(
                &tx,
                pending_batches,
                cumulative_records_assigned,
                submitted_records,
                last_acked_records,
                acked_before_disconnect,
                &replay_state_installed,
            ) => {
                result?;
                true
            }
            _ = Self::wait_for_explicit_close(explicit_close_rx) => false,
            _ = tokio::time::sleep_until(recovery_deadline) => {
                let cleanup_deadline =
                    Instant::now() + Duration::from_millis(CLOSE_SIGNAL_DRAIN_TIMEOUT_MS);
                return Self::finish_timed_out_reconnect(
                    FlightConnection {
                        response_stream: Box::pin(response_stream),
                        batch_tx: tx,
                        request_body,
                        replay_state_installed: replay_state_installed.load(Ordering::Acquire),
                    },
                    cleanup_deadline,
                    Self::reconnect_timeout_error(options.recovery_timeout_ms),
                    &ack_progress,
                )
                .await;
            }
        };
        if !replay_completed {
            // replay_pending_batches commits its rebuilt ranges before sending and publishes
            // submitted_records after each successful handoff. Cancelling between sends
            // therefore leaves a consistent prefix for process_acks to acknowledge.
            return Ok(ReconnectOutcome::Closing(FlightConnection {
                response_stream: Box::pin(response_stream),
                batch_tx: tx,
                request_body,
                replay_state_installed: replay_state_installed.load(Ordering::Acquire),
            }));
        }

        // Clear the pause gate while still holding ingest_mutex.
        is_paused.store(false, Ordering::Relaxed);

        Ok(ReconnectOutcome::Connected(FlightConnection {
            response_stream: Box::pin(response_stream),
            batch_tx: tx,
            request_body,
            replay_state_installed: replay_state_installed.load(Ordering::Acquire),
        }))
    }

    /// Rebuilds `pending_batches` for replay after a reconnect and replays them over
    /// `tx`: partially-acked batches (vs `acked_before_disconnect`) are sliced to their
    /// un-acked suffix, fully-acked ones dropped.
    ///
    /// The rebuilt pending set and the counter reset are installed together under the
    /// `pending_batches` lock, before any send: a replay-send failure keeps pending (and
    /// permits) intact, and no concurrent ingest can observe reset counters against stale
    /// ranges. Caller holds `ingest_mutex`.
    async fn replay_pending_batches(
        tx: &mpsc::Sender<Result<RecordBatch, FlightError>>,
        pending_batches: &Arc<Mutex<Vec<PendingBatch>>>,
        cumulative_records_assigned: &Arc<AtomicU64>,
        submitted_records: &Arc<AtomicU64>,
        last_acked_records: &Arc<AtomicU64>,
        acked_before_disconnect: u64,
        replay_state_installed: &AtomicBool,
    ) -> ZerobusResult<()> {
        let replay_batches: Vec<RecordBatch> = {
            let mut pending = pending_batches.lock().await;

            let mut new_pending = Vec::with_capacity(pending.len());
            let mut replay = Vec::with_capacity(pending.len());
            let mut new_cumulative: u64 = 0;

            if !pending.is_empty() {
                info!(
                    batch_count = pending.len(),
                    acked_records = acked_before_disconnect,
                    "Replaying pending batches after recovery"
                );

                for pb in pending.drain(..) {
                    let Some(batch) = slice_batch_for_recovery(&pb, acked_before_disconnect) else {
                        debug!(offset_id = pb.offset_id, "Skipping fully-acked batch");
                        continue;
                    };

                    let num_records = batch.num_rows() as u64;
                    let start_record = new_cumulative;
                    let end_record = new_cumulative + num_records;
                    new_cumulative = end_record;

                    replay.push(batch.clone());
                    new_pending.push(PendingBatch {
                        batch,
                        offset_id: pb.offset_id,
                        start_record,
                        end_record,
                        // Carry the permit across recovery; skipped batches drop theirs.
                        _permit: pb._permit,
                    });
                }

                *pending = new_pending;
            }

            // Reset counters together with the range install, before any send.
            cumulative_records_assigned.store(new_cumulative, Ordering::Relaxed);
            submitted_records.store(0, Ordering::Release);
            last_acked_records.store(0, Ordering::Release);
            replay_state_installed.store(true, Ordering::Release);

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
        }

        Ok(())
    }

    /// Atomically pauses ingest and detaches the sender, under `ingest_mutex`.
    ///
    /// Holding `ingest_mutex` across both stores makes the pause + sender-detach a
    /// single step relative to `ingest_batch`'s critical section: a concurrent ingest
    /// either finishes first, or observes `is_paused == true` and buffers — it never
    /// reads a detached (`None`) sender while `is_paused` is still false.
    async fn pause_and_detach_sender(
        ingest_mutex: &Arc<Mutex<()>>,
        is_paused: &Arc<AtomicBool>,
        batch_tx: &BatchSender,
    ) {
        let _guard = ingest_mutex.lock().await;
        is_paused.store(true, Ordering::Relaxed);
        let mut tx = batch_tx.lock().await;
        *tx = None;
    }

    /// Atomically pauses new sends and snapshots the cumulative record target for
    /// batches that were already sent. Batches accepted after this transition remain
    /// pending for replay, but do not extend the graceful-close ack wait.
    async fn pause_sender_and_snapshot_ack_target(
        ingest_mutex: &Arc<Mutex<()>>,
        is_paused: &Arc<AtomicBool>,
        submitted_records: &Arc<AtomicU64>,
    ) -> u64 {
        let _guard = ingest_mutex.lock().await;
        is_paused.store(true, Ordering::Relaxed);
        submitted_records.load(Ordering::Acquire)
    }

    /// Moves each pending batch's unacknowledged suffix to the failed list, dropping
    /// fully acknowledged batches.
    async fn move_pending_to_failed(
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
            if let Some(batch) = slice_batch_for_recovery(&pb, acked) {
                failed.push(batch);
            }
        }
    }

    /// Publishes stream closure and drains pending -> failed atomically with respect to
    /// `ingest_batch`. Holding `ingest_mutex` across the `is_closed` store and the drain
    /// means an ingest either finishes its append before this runs (and is drained here)
    /// or observes `is_closed` after the mutex is released (and refuses to append), so a
    /// retrieval snapshot can never omit an accepted batch that a later call reveals.
    async fn finalize_closed(
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

    fn pre_replay_ack_error(ack: &FlightAckMetadata) -> ZerobusError {
        ZerobusError::InvalidStateError(format!(
            "Replacement connection acknowledged {} records before replay state was installed",
            ack.ack_up_to_records
        ))
    }

    /// Advances the monotonic record watermark and removes fully acknowledged batches.
    async fn apply_acknowledgment(
        ack: &FlightAckMetadata,
        progress: &AckProgress<'_>,
    ) -> ZerobusResult<()> {
        let acked_records = ack.ack_up_to_records;
        // `ack_up_to_records` is the durability boundary. Derive completed SDK offsets
        // from local pending ranges so an inconsistent `ack_up_to_offset` cannot advance
        // a waiter; keep the server-provided offset only for diagnostics.
        let (effective_acked_records, max_acked_offset) = {
            // Ingest publishes submitted_records and commits to the active sender while
            // holding this same lock. Validation therefore cannot observe a submitted
            // watermark before its handoff, or a handoff before its watermark.
            let mut pending = progress.pending_batches.lock().await;
            let submitted_records = progress.submitted_records.load(Ordering::Acquire);
            if acked_records > submitted_records {
                return Err(ZerobusError::InvalidStateError(format!(
                    "Acknowledgement claims {acked_records} records, but only {submitted_records} records were submitted"
                )));
            }

            let previous_acked_records = progress
                .last_acked_records
                .fetch_max(acked_records, Ordering::AcqRel);
            let effective_acked_records = previous_acked_records.max(acked_records);
            let mut max_acked_offset: Option<OffsetId> = None;
            pending.retain(|pb| {
                if effective_acked_records >= pb.end_record {
                    max_acked_offset =
                        Some(max_acked_offset.map_or(pb.offset_id, |o| o.max(pb.offset_id)));
                    false
                } else {
                    true
                }
            });
            (effective_acked_records, max_acked_offset)
        };

        debug!(
            ack_up_to_offset = ack.ack_up_to_offset,
            ack_up_to_records = acked_records,
            effective_acked_records,
            "Received acknowledgment"
        );

        if let Some(offset) = max_acked_offset {
            let _ = progress.last_ack_tx.send(Some(offset));
        }

        // Test seam: observe a fully-applied ack, optionally parking here before the
        // graceful-close state machine can half-close the request.
        #[cfg(feature = "test-hooks")]
        if acked_records > 0 {
            let hook = {
                let mut gate = progress.ack_applied_gate.lock().await;
                if gate.as_ref().is_some_and(|hook| hook.proceed.is_some()) {
                    gate.take()
                } else {
                    gate.clone()
                }
            };
            if let Some(hook) = hook {
                hook.reached.notify_one();
                if let Some(proceed) = hook.proceed {
                    proceed.notified().await;
                }
            }
        }

        Ok(())
    }

    /// Waits until explicit `close()` publishes its bounded transport-cleanup deadline.
    async fn wait_for_explicit_close(
        explicit_close_rx: &mut watch::Receiver<Option<Instant>>,
    ) -> Instant {
        loop {
            if let Some(deadline) = *explicit_close_rx.borrow_and_update() {
                return deadline;
            }
            if explicit_close_rx.changed().await.is_err() {
                // A dropped sender without a close value is not an explicit-close request.
                return std::future::pending().await;
            }
        }
    }

    /// Finishes supervisor work after explicit close takes ownership of teardown.
    /// Permanent errors still reach the caller; transient transport failures are ignored
    /// because a stream being explicitly closed must not enter recovery.
    fn explicit_close_result(
        result: ZerobusResult<()>,
        server_error_tx: &watch::Sender<Option<ZerobusError>>,
    ) -> ZerobusResult<()> {
        match result {
            Err(error)
                if !error.is_retryable() || Self::is_explicit_close_owned_timeout(&error) =>
            {
                let _ = server_error_tx.send(Some(error.clone()));
                Err(error)
            }
            Ok(()) | Err(_) => Ok(()),
        }
    }

    /// Resolves ACK/recovery work after explicit close, invalidating a newly observed auth
    /// rejection exactly once while preserving one already invalidated by reconnect.
    async fn finish_explicit_close_result(
        result: ZerobusResult<()>,
        auth_already_invalidated: bool,
        close_deadline: Instant,
        headers_provider: &Arc<dyn HeadersProvider>,
        server_error_tx: &watch::Sender<Option<ZerobusError>>,
    ) -> ZerobusResult<()> {
        if let Err(error) = &result {
            if error.is_auth_rejection() {
                // Publish first so a stalled callback cannot hide the permanent error from
                // close(). A reconnect-carried rejection has already completed this step.
                let _ = server_error_tx.send(Some(error.clone()));
                if !auth_already_invalidated
                    && tokio::time::timeout_at(close_deadline, headers_provider.invalidate())
                        .await
                        .is_err()
                {
                    warn!("Explicit close ended while invalidating a rejected credential");
                }
                return result;
            }
        }
        Self::explicit_close_result(result, server_error_tx)
    }

    fn explicit_close_reconnect_timeout() -> ZerobusError {
        ZerobusError::ConnectionTimeout(EXPLICIT_CLOSE_RECONNECT_TIMEOUT.to_string())
    }

    fn explicit_close_aborted_timeout() -> ZerobusError {
        ZerobusError::ConnectionTimeout(EXPLICIT_CLOSE_ABORTED_TIMEOUT.to_string())
    }

    fn is_explicit_close_owned_timeout(error: &ZerobusError) -> bool {
        matches!(
            error,
            ZerobusError::ConnectionTimeout(message)
                if message.starts_with(EXPLICIT_CLOSE_TIMEOUT_PREFIX)
        )
    }

    /// Chooses between an older permanent recovery cause and the latest close outcome.
    /// A newer permanent peer error is authoritative; an older permanent cause only
    /// outranks retryable transport cleanup failures.
    fn preferred_close_error(
        retained: Option<ZerobusError>,
        latest: Option<ZerobusError>,
    ) -> Option<ZerobusError> {
        match latest {
            Some(error) if !error.is_retryable() => Some(error),
            latest => retained.or(latest),
        }
    }

    /// Flush first, then permanent retained/published errors, then supervisor (including
    /// abort), then any close-owned published timeout. A permanent peer/auth failure known
    /// before a forced abort must not be overwritten by an invented timeout.
    fn compose_explicit_close_result(
        flush_result: ZerobusResult<()>,
        supervisor_result: ZerobusResult<()>,
        published_close_result: ZerobusResult<()>,
    ) -> ZerobusResult<()> {
        flush_result?;
        if let Err(error) = &published_close_result {
            if !error.is_retryable() {
                return Err(error.clone());
            }
        }
        supervisor_result?;
        published_close_result
    }

    /// Waits for the next response or the deadline associated with the current close
    /// phase. Keeping the select logic here leaves `process_acks` focused on transitions.
    async fn wait_for_ack_event(
        response_stream: &mut FlightResponseStream,
        request_body: &RequestBodyControl,
        phase: &GracefulClosePhase,
        ack_timeout: Duration,
        explicit_close_rx: &mut watch::Receiver<Option<Instant>>,
    ) -> AckEvent {
        match phase {
            GracefulClosePhase::Open => {
                // Once close is already visible, do not let an always-ready response stream
                // starve request shutdown. If close arrives concurrently with a response,
                // the biased select may consume that one response; this precheck wins on the
                // following iteration.
                if let Some(deadline) = *explicit_close_rx.borrow_and_update() {
                    return AckEvent::ExplicitClose { deadline };
                }

                tokio::select! {
                    biased;
                    response = response_stream.next() => AckEvent::Response(response),
                    deadline = Self::wait_for_explicit_close(explicit_close_rx) => {
                        AckEvent::ExplicitClose { deadline }
                    }
                    _ = tokio::time::sleep(ack_timeout) => AckEvent::AckTimeout,
                }
            }
            GracefulClosePhase::AwaitingAcks { ack_deadline, .. } => {
                if let Some(deadline) = *explicit_close_rx.borrow_and_update() {
                    return AckEvent::ExplicitClose { deadline };
                }
                if Instant::now() >= *ack_deadline {
                    return AckEvent::GracefulCloseDeadline;
                }

                tokio::select! {
                    biased;
                    response = response_stream.next() => AckEvent::Response(response),
                    deadline = Self::wait_for_explicit_close(explicit_close_rx) => {
                        AckEvent::ExplicitClose { deadline }
                    }
                    _ = tokio::time::sleep_until(*ack_deadline) => {
                        AckEvent::GracefulCloseDeadline
                    }
                }
            }
            GracefulClosePhase::DrainingResponse {
                deadline,
                request_reached_eof,
                response_reached_eof,
                ..
            } => {
                if !*request_reached_eof && request_body.reached_eof() {
                    return AckEvent::RequestEof;
                }
                if let Some(deadline) = *explicit_close_rx.borrow_and_update() {
                    return AckEvent::ExplicitClose { deadline };
                }
                if Instant::now() >= *deadline {
                    return AckEvent::ResponseDrainDeadline;
                }

                tokio::select! {
                    biased;
                    _ = request_body.wait_for_eof(), if !*request_reached_eof => {
                        AckEvent::RequestEof
                    }
                    close_deadline = Self::wait_for_explicit_close(explicit_close_rx) => {
                        AckEvent::ExplicitClose { deadline: close_deadline }
                    }
                    response = response_stream.next(), if !*response_reached_eof => {
                        AckEvent::Response(response)
                    }
                    _ = tokio::time::sleep_until(*deadline) => {
                        AckEvent::ResponseDrainDeadline
                    }
                }
            }
            GracefulClosePhase::DrainingExplicitClose {
                deadline,
                request_reached_eof,
                response_reached_eof,
            } => {
                if !*request_reached_eof && request_body.reached_eof() {
                    return AckEvent::RequestEof;
                }
                if Instant::now() >= *deadline {
                    return AckEvent::ResponseDrainDeadline;
                }

                tokio::select! {
                    biased;
                    _ = request_body.wait_for_eof(), if !*request_reached_eof => {
                        AckEvent::RequestEof
                    }
                    response = response_stream.next(), if !*response_reached_eof => {
                        AckEvent::Response(response)
                    }
                    _ = tokio::time::sleep_until(*deadline) => {
                        AckEvent::ResponseDrainDeadline
                    }
                }
            }
        }
    }

    /// Adds a peer/configured duration without assuming every platform can represent the
    /// full `u64` millisecond range in `Instant`. On overflow, retain the largest duration
    /// reached by repeatedly halving; absurd values remain effectively unbounded while
    /// close-signal handling cannot panic.
    fn saturating_deadline(now: Instant, duration: Duration) -> Instant {
        let mut bounded = duration;
        loop {
            if let Some(deadline) = now.checked_add(bounded) {
                return deadline;
            }
            bounded /= 2;
        }
    }

    /// Calculates the ACK and close deadlines. The server grace normally bounds the
    /// whole operation, with its final portion reserved for request EOF and response
    /// draining. If the advertised grace is shorter than the cleanup cap, ACK waiting is
    /// skipped and the close deadline is extended locally so EOF is still attempted.
    fn graceful_close_deadlines(
        server_duration_ms: u64,
        configured_ack_wait_ms: Option<u64>,
    ) -> (Instant, Instant) {
        let now = Instant::now();
        let requested_server_duration = Duration::from_millis(server_duration_ms);
        let server_deadline = Self::saturating_deadline(now, requested_server_duration);
        let server_duration = server_deadline.saturating_duration_since(now);
        let cleanup_budget =
            Duration::from_millis(CLOSE_SIGNAL_DRAIN_TIMEOUT_MS).min(server_duration);
        let protocol_ack_wait = server_duration - cleanup_budget;
        // Cap the public configuration before adding it to Instant. Besides avoiding
        // overflow, values above the server allowance cannot affect behavior anyway.
        let ack_wait = configured_ack_wait_ms
            .map(Duration::from_millis)
            .unwrap_or(protocol_ack_wait)
            .min(protocol_ack_wait);
        let ack_deadline = Self::saturating_deadline(now, ack_wait);
        let local_cleanup_deadline =
            Self::saturating_deadline(now, Duration::from_millis(CLOSE_SIGNAL_DRAIN_TIMEOUT_MS));
        let close_deadline = server_deadline.max(local_cleanup_deadline);

        (ack_deadline, close_deadline)
    }

    /// Enters (or tightens) the ACK-wait phase. Duplicate signals retain the first
    /// snapshot target and can only shorten the original deadlines.
    async fn register_graceful_close(
        phase: &mut GracefulClosePhase,
        server_duration_ms: u64,
        configured_ack_wait_ms: Option<u64>,
        ingest_mutex: &Arc<Mutex<()>>,
        is_paused: &Arc<AtomicBool>,
        submitted_records: &Arc<AtomicU64>,
    ) -> u64 {
        let (new_ack_deadline, new_close_deadline) =
            Self::graceful_close_deadlines(server_duration_ms, configured_ack_wait_ms);

        match phase {
            GracefulClosePhase::Open => {
                let target_records = Self::pause_sender_and_snapshot_ack_target(
                    ingest_mutex,
                    is_paused,
                    submitted_records,
                )
                .await;
                *phase = GracefulClosePhase::AwaitingAcks {
                    target_records,
                    ack_deadline: new_ack_deadline,
                    close_deadline: new_close_deadline,
                };
                target_records
            }
            GracefulClosePhase::AwaitingAcks {
                target_records,
                ack_deadline,
                close_deadline,
            } => {
                *ack_deadline = (*ack_deadline).min(new_ack_deadline);
                *close_deadline = (*close_deadline).min(new_close_deadline);
                *target_records
            }
            GracefulClosePhase::DrainingResponse { deadline, .. } => {
                *deadline = (*deadline).min(new_close_deadline);
                submitted_records.load(Ordering::Acquire)
            }
            GracefulClosePhase::DrainingExplicitClose { deadline, .. } => {
                // The caller already owns shutdown. A late server close signal may only
                // tighten the shared transport-cleanup deadline, not trigger recovery.
                *deadline = (*deadline).min(new_close_deadline);
                submitted_records.load(Ordering::Acquire)
            }
        }
    }

    /// Stops new sends on the connection and requests outbound EOF.
    async fn start_request_shutdown(
        request_body: &RequestBodyControl,
        ingest_mutex: &Arc<Mutex<()>>,
        is_paused: &Arc<AtomicBool>,
        batch_tx: &BatchSender,
    ) {
        Self::pause_and_detach_sender(ingest_mutex, is_paused, batch_tx).await;
        request_body.shutdown();
    }

    async fn shutdown_request_body(
        request_body: &RequestBodyControl,
        deadline: Instant,
        ingest_mutex: &Arc<Mutex<()>>,
        is_paused: &Arc<AtomicBool>,
        batch_tx: &BatchSender,
    ) -> bool {
        Self::start_request_shutdown(request_body, ingest_mutex, is_paused, batch_tx).await;
        request_body.wait_for_eof_until(deadline).await
    }

    /// Gives every terminal path a short positive EOF-settle window without letting
    /// request cleanup outlive the active close attempt by more than the local cap.
    fn request_cleanup_deadline(phase: &GracefulClosePhase) -> Instant {
        if let GracefulClosePhase::DrainingExplicitClose { deadline, .. } = phase {
            // Explicit close carries one absolute deadline across every cleanup path;
            // never restart its budget after it expires.
            return *deadline;
        }
        let now = Instant::now();
        let local_cap = now + Duration::from_millis(CLOSE_SIGNAL_DRAIN_TIMEOUT_MS);
        match phase.close_deadline() {
            Some(deadline) if deadline > now => deadline.min(local_cap),
            Some(_) | None => local_cap,
        }
    }

    /// Stops accepting data on the current connection, cancels queued encoder output,
    /// and enters a phase that drives request EOF and response draining concurrently.
    async fn begin_response_drain(
        phase: &mut GracefulClosePhase,
        recovery_reason: ZerobusError,
        request_body: &RequestBodyControl,
        ingest_mutex: &Arc<Mutex<()>>,
        is_paused: &Arc<AtomicBool>,
        batch_tx: &BatchSender,
    ) {
        let deadline = Self::request_cleanup_deadline(phase);
        Self::start_request_shutdown(request_body, ingest_mutex, is_paused, batch_tx).await;

        *phase = GracefulClosePhase::DrainingResponse {
            deadline,
            recovery_reason,
            request_reached_eof: request_body.reached_eof(),
            response_reached_eof: false,
        };
    }

    #[cfg(feature = "test-hooks")]
    async fn notify_response_consumed(response_drain_gate: &ResponseDrainGate) {
        if let Some(hook) = response_drain_gate.lock().await.clone() {
            hook.response_consumed.notify_one();
        }
    }

    #[cfg(feature = "test-hooks")]
    async fn notify_response_drain_completed(response_drain_gate: &ResponseDrainGate) {
        if let Some(hook) = response_drain_gate.lock().await.take() {
            hook.drain_completed.notify_one();
        }
    }

    /// Half-closes the request, enters response draining, and publishes close activity.
    async fn start_response_drain(
        phase: &mut GracefulClosePhase,
        graceful_close_activity: &mut GracefulCloseActivity,
        recovery_reason: ZerobusError,
        request: &AckRequestControl<'_>,
    ) {
        request.begin_response_drain(phase, recovery_reason).await;
        graceful_close_activity.activate(
            phase
                .close_deadline()
                .expect("response drain installs a close deadline"),
        );
    }

    /// Applies the close-state transition carried by a successful ACK, when present.
    async fn register_close_signal_ack(
        ack: &FlightAckMetadata,
        phase: &mut GracefulClosePhase,
        graceful_close_activity: &mut GracefulCloseActivity,
        options: &ArrowStreamConfigurationOptions,
        request: &AckRequestControl<'_>,
        submitted_records: &Arc<AtomicU64>,
    ) {
        if !ack.is_close_signal() {
            return;
        }

        let server_duration_ms = ack.close_stream_duration_ms.unwrap_or(0);
        let target_records = Self::register_graceful_close(
            phase,
            server_duration_ms,
            options.stream_paused_max_wait_time_ms,
            request.ingest_mutex,
            request.is_paused,
            submitted_records,
        )
        .await;
        let close_deadline = phase
            .close_deadline()
            .expect("close signal registration installs a deadline");
        graceful_close_activity.activate(close_deadline);
        let ack_wait_ms = match phase {
            GracefulClosePhase::AwaitingAcks { ack_deadline, .. } => ack_deadline
                .saturating_duration_since(Instant::now())
                .as_millis(),
            _ => 0,
        };
        info!(
            server_duration_ms,
            ack_target_records = target_records,
            ack_wait_ms,
            "Server requested graceful stream close"
        );
    }

    /// Publishes a peer status before request cleanup and returns it to the supervisor.
    async fn finish_peer_error(
        flight_error: FlightError,
        phase: &GracefulClosePhase,
        server_error_tx: &watch::Sender<Option<ZerobusError>>,
        request: &AckRequestControl<'_>,
        #[cfg(feature = "test-hooks")] response_drain_gate: &ResponseDrainGate,
    ) -> ZerobusResult<()> {
        // Publishing permanent errors before bounded request cleanup prevents a short or
        // nearly-expired flush from replacing the real status with a retryable timeout.
        // Open-stream retryable errors remain observable to supervisor recovery as before.
        let status: tonic::Status = flight_error.into();
        let error = ZerobusError::StreamClosedError(status);
        if phase.is_open() || !error.is_retryable() {
            let _ = server_error_tx.send(Some(error.clone()));
        }

        let _ = request.shutdown_for_phase(phase).await;
        #[cfg(feature = "test-hooks")]
        if phase.is_draining() {
            Self::notify_response_drain_completed(response_drain_gate).await;
        }
        Err(error)
    }

    /// Maps response EOF according to the active close phase and stops any open request.
    async fn finish_response_eof(
        phase: &GracefulClosePhase,
        request: &AckRequestControl<'_>,
        #[cfg(feature = "test-hooks")] response_drain_gate: &ResponseDrainGate,
    ) -> ZerobusResult<()> {
        if let GracefulClosePhase::DrainingExplicitClose {
            request_reached_eof,
            ..
        } = phase
        {
            #[cfg(feature = "test-hooks")]
            Self::notify_response_drain_completed(response_drain_gate).await;
            if !*request_reached_eof {
                return Err(ZerobusError::ConnectionTimeout(
                    EXPLICIT_CLOSE_REQUEST_EOF_TIMEOUT.to_string(),
                ));
            }
            info!("Explicit close drained the server response");
            return Ok(());
        }

        if let GracefulClosePhase::DrainingResponse {
            recovery_reason, ..
        } = phase
        {
            #[cfg(feature = "test-hooks")]
            Self::notify_response_drain_completed(response_drain_gate).await;
            info!("Server sent END_STREAM after request half-close");
            return Err(recovery_reason.clone());
        }

        let _ = request.shutdown_for_phase(phase).await;
        if !phase.is_open() {
            return Err(ZerobusError::StreamClosedError(tonic::Status::unavailable(
                "Server closed stream during graceful close",
            )));
        }

        debug!("Server closed the stream");
        Err(ZerobusError::StreamClosedError(tonic::Status::unknown(
            "Server closed the stream",
        )))
    }

    /// Returns a terminal timeout only when unacknowledged batches still exist.
    async fn ack_timeout_error(
        phase: &GracefulClosePhase,
        pending_batches: &Arc<Mutex<Vec<PendingBatch>>>,
        request: &AckRequestControl<'_>,
    ) -> Option<ZerobusError> {
        let pending_count = pending_batches.lock().await.len();
        if pending_count == 0 {
            return None;
        }

        let _ = request.shutdown_for_phase(phase).await;
        error!(pending_count, "Server ack timeout with pending batches");
        Some(ZerobusError::StreamClosedError(
            tonic::Status::deadline_exceeded("Server ack timeout"),
        ))
    }

    /// Completes the result associated with an expired response-drain deadline.
    async fn finish_response_drain_deadline(
        phase: &GracefulClosePhase,
        #[cfg(feature = "test-hooks")] response_drain_gate: &ResponseDrainGate,
    ) -> ZerobusResult<()> {
        #[cfg(feature = "test-hooks")]
        Self::notify_response_drain_completed(response_drain_gate).await;
        if let GracefulClosePhase::DrainingExplicitClose {
            request_reached_eof,
            ..
        } = phase
        {
            let message = if *request_reached_eof {
                EXPLICIT_CLOSE_RESPONSE_DRAIN_TIMEOUT
            } else {
                EXPLICIT_CLOSE_REQUEST_EOF_TIMEOUT
            };
            warn!(
                message,
                "Explicit close transport cleanup reached its deadline"
            );
            return Err(ZerobusError::ConnectionTimeout(message.to_string()));
        }
        if let GracefulClosePhase::DrainingResponse {
            recovery_reason, ..
        } = phase
        {
            info!("Graceful close response drain reached its deadline");
            return Err(recovery_reason.clone());
        }
        Err(ZerobusError::InvalidStateError(
            "Response-drain deadline received outside the drain phase".to_string(),
        ))
    }

    /// Processes acknowledgments from the server response stream.
    ///
    /// Uses record-based tracking: the server sends `ack_up_to_records` indicating
    /// the cumulative number of records durably stored. We match this against pending
    /// batch ranges, including batches split into multiple Flight chunks.
    #[allow(clippy::too_many_arguments)]
    async fn process_acks(
        mut response_stream: FlightResponseStream,
        is_closed: Arc<AtomicBool>,
        last_ack_tx: tokio::sync::watch::Sender<Option<OffsetId>>,
        pending_batches: Arc<Mutex<Vec<PendingBatch>>>,
        ack_timeout: Duration,
        server_error_tx: watch::Sender<Option<ZerobusError>>,
        submitted_records: Arc<AtomicU64>,
        last_acked_records: Arc<AtomicU64>,
        is_paused: Arc<AtomicBool>,
        ingest_mutex: Arc<Mutex<()>>,
        batch_tx: BatchSender,
        request_body: RequestBodyControl,
        graceful_close_tx: watch::Sender<Option<Instant>>,
        mut explicit_close_rx: watch::Receiver<Option<Instant>>,
        options: &ArrowStreamConfigurationOptions,
        replay_state_installed: bool,
        #[cfg(feature = "test-hooks")] ack_applied_gate: AckAppliedGate,
        #[cfg(feature = "test-hooks")] response_drain_gate: ResponseDrainGate,
    ) -> ZerobusResult<()> {
        let mut phase = GracefulClosePhase::Open;
        let mut graceful_close_activity = GracefulCloseActivity::new(graceful_close_tx);
        let request = AckRequestControl {
            request_body: &request_body,
            ingest_mutex: &ingest_mutex,
            is_paused: &is_paused,
            batch_tx: &batch_tx,
        };

        loop {
            if is_closed.load(Ordering::Relaxed) {
                debug!("Stream closed, stopping ack processor");
                let _ = request.shutdown_for_phase(&phase).await;
                return Ok(());
            }

            let ack_target_reached = match &phase {
                GracefulClosePhase::AwaitingAcks { target_records, .. } => {
                    (last_acked_records.load(Ordering::Acquire) >= *target_records)
                        .then_some(*target_records)
                }
                _ => None,
            };
            if let Some(target_records) = ack_target_reached {
                info!(
                    target_records,
                    "All pre-close batches were acknowledged; half-closing request"
                );
                Self::start_response_drain(
                    &mut phase,
                    &mut graceful_close_activity,
                    ZerobusError::StreamClosedError(tonic::Status::unavailable(
                        "All submitted batches acknowledged during graceful close",
                    )),
                    &request,
                )
                .await;
                continue;
            }

            match Self::wait_for_ack_event(
                &mut response_stream,
                request.request_body,
                &phase,
                ack_timeout,
                &mut explicit_close_rx,
            )
            .await
            {
                AckEvent::Response(Some(Ok(put_result))) => {
                    if phase.is_draining() {
                        #[cfg(feature = "test-hooks")]
                        Self::notify_response_consumed(&response_drain_gate).await;
                    }

                    match FlightAckMetadata::from_bytes(&put_result.app_metadata) {
                        Ok(ack) => {
                            Self::register_close_signal_ack(
                                &ack,
                                &mut phase,
                                &mut graceful_close_activity,
                                options,
                                &request,
                                &submitted_records,
                            )
                            .await;

                            if ack.ack_up_to_records > 0 {
                                if !replay_state_installed {
                                    let error = Self::pre_replay_ack_error(&ack);
                                    let _ = server_error_tx.send(Some(error.clone()));
                                    let _ = request.shutdown_for_phase(&phase).await;
                                    return Err(error);
                                }
                                let progress = AckProgress {
                                    submitted_records: &submitted_records,
                                    last_acked_records: &last_acked_records,
                                    pending_batches: &pending_batches,
                                    last_ack_tx: &last_ack_tx,
                                    #[cfg(feature = "test-hooks")]
                                    ack_applied_gate: &ack_applied_gate,
                                };
                                if let Err(error) =
                                    Self::apply_acknowledgment(&ack, &progress).await
                                {
                                    let _ = server_error_tx.send(Some(error.clone()));
                                    let _ = request.shutdown_for_phase(&phase).await;
                                    return Err(error);
                                }
                            }
                        }
                        Err(e) => warn!("Failed to parse ack metadata: {}", e),
                    }
                }
                AckEvent::Response(Some(Err(flight_error))) => {
                    return Self::finish_peer_error(
                        flight_error,
                        &phase,
                        &server_error_tx,
                        &request,
                        #[cfg(feature = "test-hooks")]
                        &response_drain_gate,
                    )
                    .await;
                }
                AckEvent::Response(None) => {
                    if phase.is_draining() {
                        phase.mark_response_eof();
                        let (request_reached_eof, response_reached_eof) = phase
                            .drain_progress()
                            .expect("draining phase exposes transport progress");
                        if !request_reached_eof || !response_reached_eof {
                            debug!(
                                request_reached_eof,
                                "Server response reached EOF; waiting for request EOF"
                            );
                            continue;
                        }
                    }
                    return Self::finish_response_eof(
                        &phase,
                        &request,
                        #[cfg(feature = "test-hooks")]
                        &response_drain_gate,
                    )
                    .await;
                }
                AckEvent::AckTimeout => {
                    if let Some(error) =
                        Self::ack_timeout_error(&phase, &pending_batches, &request).await
                    {
                        return Err(error);
                    }
                }
                AckEvent::GracefulCloseDeadline => {
                    info!("Graceful close ACK wait ended; half-closing request");
                    Self::start_response_drain(
                        &mut phase,
                        &mut graceful_close_activity,
                        ZerobusError::StreamClosedError(tonic::Status::unavailable(
                            "Graceful close ACK wait reached its deadline",
                        )),
                        &request,
                    )
                    .await;
                }
                AckEvent::ExplicitClose { deadline } => {
                    if phase.is_draining() {
                        let active_deadline = phase
                            .close_deadline()
                            .expect("draining phase has a close deadline");
                        let (request_reached_eof, response_reached_eof) = phase
                            .drain_progress()
                            .expect("draining phase exposes transport progress");
                        phase = GracefulClosePhase::DrainingExplicitClose {
                            deadline: active_deadline.min(deadline),
                            request_reached_eof,
                            response_reached_eof,
                        };
                        debug!("Explicit close adopted the in-flight transport drain");
                    } else {
                        info!("Explicit close requested; half-closing request");
                        Self::start_request_shutdown(
                            request.request_body,
                            request.ingest_mutex,
                            request.is_paused,
                            request.batch_tx,
                        )
                        .await;
                        phase = GracefulClosePhase::DrainingExplicitClose {
                            deadline,
                            request_reached_eof: request.request_body.reached_eof(),
                            response_reached_eof: false,
                        };
                    }
                }
                AckEvent::RequestEof => {
                    phase.mark_request_eof();
                    debug!("Flight request body reached EOF");
                    let (_, response_reached_eof) = phase
                        .drain_progress()
                        .expect("request EOF is observed only while draining");
                    if response_reached_eof {
                        return Self::finish_response_eof(
                            &phase,
                            &request,
                            #[cfg(feature = "test-hooks")]
                            &response_drain_gate,
                        )
                        .await;
                    }
                }
                AckEvent::ResponseDrainDeadline => {
                    return Self::finish_response_drain_deadline(
                        &phase,
                        #[cfg(feature = "test-hooks")]
                        &response_drain_gate,
                    )
                    .await;
                }
            }
        }
    }

    /// Ingests a single Arrow RecordBatch into the stream.
    ///
    /// Queues the batch and returns its assigned offset. If
    /// `max_inflight_batches` accepted batches are still pending (including batches
    /// buffered during recovery), this waits until full acknowledgment or finalization
    /// releases a permit. Use `wait_for_offset()` only when this specific batch must be
    /// confirmed before continuing.
    ///
    /// # Arguments
    ///
    /// * `batch` - An Arrow RecordBatch to ingest
    ///
    /// # Returns
    ///
    /// The offset ID assigned to this batch.
    ///
    /// # Errors
    ///
    /// * `StreamClosedError` - If the stream is closing or closed
    /// * `InvalidArgument` - If the batch schema doesn't match the stream schema, or the
    ///   batch has zero rows (an empty batch carries no data to send or acknowledge)
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # use arrow_array::RecordBatch;
    /// # async fn example(stream: ZerobusArrowStream, batches: Vec<RecordBatch>) -> Result<(), ZerobusError> {
    /// // Ingest in a loop (each call only queues the batch), then flush() once.
    /// for batch in batches {
    ///     stream.ingest_batch(batch).await?;
    /// }
    /// stream.flush().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(level = "debug", skip_all, fields(table_name = %self.table_properties.table_name))]
    pub async fn ingest_batch(&self, batch: RecordBatch) -> ZerobusResult<OffsetId> {
        if self.is_closed.load(Ordering::Relaxed)
            || self.close_teardown_started.load(Ordering::Acquire)
        {
            return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                "Stream is closing or closed",
            )));
        }

        if batch.schema() != self.table_properties.schema {
            return Err(ZerobusError::InvalidArgument(format!(
                "RecordBatch schema does not match stream schema. Expected: {:?}, Got: {:?}",
                self.table_properties.schema,
                batch.schema()
            )));
        }

        // Reject empty batches: the Flight encoder emits no data message for a zero-row
        // RecordBatch, so it would enter pending_batches but never be sent or acknowledged,
        // hanging flush()/wait_for_offset() until they time out.
        if batch.num_rows() == 0 {
            return Err(ZerobusError::InvalidArgument(
                "Cannot ingest an empty RecordBatch (zero rows)".to_string(),
            ));
        }

        // Acquire the backpressure permit BEFORE ingest_mutex: reconnect() holds that
        // mutex, so blocking on a permit while holding it could stall recovery.
        // `inflight` is never closed, so the map_err is unreachable defensive code.
        // Permit waiters wake when acknowledgments or finalization release pending
        // permits, then re-check both lifecycle flags below.
        let permit = Arc::clone(&self.inflight)
            .acquire_owned()
            .await
            .map_err(|_| {
                ZerobusError::StreamClosedError(tonic::Status::internal("Stream is closed"))
            })?;

        let _guard = self.ingest_mutex.lock().await;

        // May have closed while we blocked on the permit; returning drops it.
        if self.is_closed.load(Ordering::Relaxed)
            || self.close_teardown_started.load(Ordering::Acquire)
        {
            return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                "Stream is closing or closed",
            )));
        }

        let offset_id = self.offset_generator.next();
        let record_count = batch.num_rows() as u64;
        let start_record = self
            .cumulative_records_assigned
            .fetch_add(record_count, Ordering::Relaxed);
        let end_record = start_record + record_count;

        {
            let mut pending = self.pending_batches.lock().await;
            pending.push(PendingBatch {
                batch: batch.clone(),
                offset_id,
                start_record,
                end_record,
                _permit: permit,
            });
        }

        // While paused for a close signal or recovery handoff, retain the batch as
        // pending; successful recovery replays it and terminal recovery finalizes it.
        if self.is_paused.load(Ordering::Relaxed) {
            return Ok(offset_id);
        }

        let sender = {
            let guard = self.batch_tx.lock().await;
            guard.clone()
        };

        let sender = match sender {
            Some(s) => s,
            None => {
                // Correct callers cannot reach a detached sender: lifecycle checks reject
                // close teardown, and recovery detaches under ingest_mutex. Retain this
                // fallback for unsupported concurrent close/ingest across foreign
                // boundaries, preferring a known terminal cause.
                if let Some(server_error) = self.server_error_rx.borrow().clone() {
                    return Err(server_error);
                }
                return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                    "Stream sender is closed",
                )));
            }
        };

        let send_permit = match sender.reserve().await {
            Ok(permit) => permit,
            Err(e) => {
                warn!("Send failed: {}", e);
                if self.options.recovery {
                    debug!(
                        offset_id = offset_id,
                        "Send failed but recovery enabled - supervisor will handle recovery"
                    );
                    return Ok(offset_id);
                }

                {
                    let mut pending = self.pending_batches.lock().await;
                    pending.retain(|pb| pb.offset_id != offset_id);
                }
                let _ = tokio::time::timeout(
                    Duration::from_millis(100),
                    self.server_error_rx.clone().changed(),
                )
                .await;
                if let Some(server_error) = self.server_error_rx.borrow().clone() {
                    return Err(server_error);
                }
                return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                    "Failed to send batch",
                )));
            }
        };

        // Commit the submitted watermark and channel handoff under the same lock used
        // by acknowledgement validation. The reserved channel slot makes the handoff infallible.
        {
            let _pending = self.pending_batches.lock().await;
            self.submitted_records.store(end_record, Ordering::Release);
            send_permit.send(Ok(batch));
        }

        debug!(offset_id = offset_id, "Batch queued for ingestion");
        Ok(offset_id)
    }

    /// Ingests a single Arrow RecordBatch supplied as raw Arrow IPC stream bytes.
    ///
    /// Convenience wrapper for callers that already hold IPC-serialised bytes.
    /// Deserialises the bytes to a [`RecordBatch`] and delegates to `ingest_batch`.
    /// Prefer `ingest_batch` directly when you already have a [`RecordBatch`].
    ///
    /// The `ipc_bytes` must be a valid Arrow IPC *stream* containing exactly one
    /// RecordBatch (i.e. the output of `pyarrow.RecordBatch.serialize()`,
    /// `tableToIPC(table, 'stream')`, etc.). Dictionary messages between the schema and
    /// the RecordBatch are supported. Trailing stream metadata (such as an end-of-stream
    /// marker after `finish()`) is allowed after that batch.
    #[instrument(level = "debug", skip_all, fields(table_name = %self.table_properties.table_name))]
    pub async fn ingest_ipc_batch(&self, ipc_bytes: Bytes) -> ZerobusResult<OffsetId> {
        if self.is_closed.load(Ordering::Relaxed)
            || self.close_teardown_started.load(Ordering::Acquire)
        {
            return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                "Stream is closing or closed",
            )));
        }

        // Deserialise IPC bytes into a RecordBatch.
        let batch = materialize_ipc(&ipc_bytes)
            .map_err(|e| ZerobusError::InvalidArgument(format!("Invalid Arrow IPC bytes: {e}")))?;

        if batch.schema() != self.table_properties.schema {
            return Err(ZerobusError::InvalidArgument(format!(
                "IPC batch schema does not match stream schema. Expected: {:?}, Got: {:?}",
                self.table_properties.schema,
                batch.schema()
            )));
        }

        self.ingest_batch(batch).await
    }

    /// Waits on the ACK and error watches while preserving ACK-first terminal precedence.
    async fn wait_for_offset_state(
        offset_to_wait: OffsetId,
        operation_name: &str,
        flush_timeout: Duration,
        is_closed: &AtomicBool,
        close_teardown_started: &AtomicBool,
        mut offset_rx: watch::Receiver<Option<OffsetId>>,
        mut error_rx: watch::Receiver<Option<ZerobusError>>,
    ) -> ZerobusResult<()> {
        let wait_future = async {
            loop {
                // Check the published watermark first so an acknowledged target wins over
                // concurrently visible closure/error, avoiding a duplicate retry.
                let current_ack = *offset_rx.borrow_and_update();
                if let Some(ack_offset) = current_ack {
                    if ack_offset >= offset_to_wait {
                        debug!(
                            ack_offset = ack_offset,
                            target_offset = offset_to_wait,
                            "{} completed",
                            operation_name
                        );
                        return Ok(());
                    }
                    debug!(
                        current_ack = ack_offset,
                        target_offset = offset_to_wait,
                        "Waiting for more acks"
                    );
                }

                // A permanent server or protocol error cannot recover. Surface it as soon
                // as it is published instead of letting bounded transport cleanup consume
                // the remaining flush budget. The target ACK check above intentionally wins
                // when both updates become visible together.
                if let Some(server_error) = error_rx.borrow_and_update().clone() {
                    if !server_error.is_retryable() {
                        return Err(server_error);
                    }
                }

                // Only after confirming the target isn't acked, honor terminal/teardown
                // state. Re-read first because the watermark can be published between the
                // read above and observing that state. Otherwise prefer the real terminal
                // error over a generic one.
                if is_closed.load(Ordering::Relaxed)
                    || close_teardown_started.load(Ordering::Acquire)
                {
                    if let Some(ack_offset) = *offset_rx.borrow_and_update() {
                        if ack_offset >= offset_to_wait {
                            return Ok(());
                        }
                    }
                    if let Some(server_error) = error_rx.borrow().clone() {
                        return Err(server_error);
                    }
                    return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                        format!(
                            "Stream closing or closed during {}",
                            operation_name.to_lowercase()
                        ),
                    )));
                }

                // Neither arm returns directly. After either watch changes, loop so the
                // watermark takes precedence when both updates are visible.
                tokio::select! {
                    result = offset_rx.changed() => {
                        if result.is_err() {
                            return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                                format!(
                                    "Ack channel closed during {}",
                                    operation_name.to_lowercase()
                                ),
                            )));
                        }
                    }
                    _ = error_rx.changed() => {}
                }
            }
        };

        tokio::time::timeout(flush_timeout, wait_future)
            .await
            .map_err(|_| {
                error!("{} timed out", operation_name);
                ZerobusError::StreamClosedError(tonic::Status::deadline_exceeded(format!(
                    "{} timed out",
                    operation_name
                )))
            })?
    }

    /// Internal method to wait for a specific offset to be acknowledged.
    /// Used by both `flush()` and `wait_for_offset()`.
    async fn wait_for_offset_internal(
        &self,
        offset_to_wait: OffsetId,
        operation_name: &str,
    ) -> ZerobusResult<()> {
        Self::wait_for_offset_state(
            offset_to_wait,
            operation_name,
            Duration::from_millis(self.options.flush_timeout_ms),
            &self.is_closed,
            &self.close_teardown_started,
            self.last_ack_tx.subscribe(),
            self.server_error_rx.clone(),
        )
        .await
    }

    /// Flushes all currently pending batches and waits for their acknowledgments.
    ///
    /// Snapshots the highest assigned offset when it begins and waits through that offset.
    /// Offsets assigned after the snapshot are not included.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the snapshotted offset has been acknowledged, including when that
    /// acknowledgment was published just before closure.
    ///
    /// # Errors
    ///
    /// Returns the terminal stream/recovery error when the target remains unacknowledged,
    /// or `StreamClosedError` when teardown starts or the wait times out.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # use arrow_array::RecordBatch;
    /// # async fn example(stream: ZerobusArrowStream, batches: Vec<RecordBatch>) -> Result<(), ZerobusError> {
    /// // Ingest many batches without waiting for each one
    /// for batch in batches {
    ///     let _offset = stream.ingest_batch(batch).await?;
    /// }
    ///
    /// // Wait for all batches to be acknowledged
    /// stream.flush().await?;
    /// println!("All batches have been acknowledged");
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(level = "debug", skip_all, fields(table_name = %self.table_properties.table_name))]
    pub async fn flush(&self) -> ZerobusResult<()> {
        let target_offset = match self.offset_generator.last() {
            Some(offset) => offset,
            None => {
                // Nothing was ingested: report closure if closed, otherwise nothing to do.
                // Prefer the real terminal error over a generic closed message.
                if self.is_closed.load(Ordering::Relaxed)
                    || self.close_teardown_started.load(Ordering::Acquire)
                {
                    if let Some(server_error) = self.server_error_rx.borrow().clone() {
                        return Err(server_error);
                    }
                    return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                        "Cannot flush: stream is closing or closed",
                    )));
                }
                debug!("No batches to flush");
                return Ok(());
            }
        };

        // Defer to the waiter (no early is_closed check): it applies ack-vs-closure
        // precedence, so a target acknowledged just before closure resolves as Ok(())
        // instead of a generic "stream is closed" error.
        self.wait_for_offset_internal(target_offset, "Flush").await
    }

    /// Waits asynchronously for the cumulative acknowledgment watermark to reach an offset.
    ///
    /// After queueing multiple batches, wait only for the final offset (which implies all
    /// earlier offsets) or prefer `flush()`.
    ///
    /// # Arguments
    ///
    /// * `offset` - The offset ID to wait for (returned from `ingest_batch()`)
    ///
    /// # Returns
    ///
    /// `Ok(())` when the batch at the specified offset has been acknowledged.
    ///
    /// # Errors
    ///
    /// Returns the terminal stream/recovery error while the target remains unacknowledged,
    /// or `StreamClosedError` when teardown starts or the wait times out.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # use arrow_array::RecordBatch;
    /// # async fn example(stream: ZerobusArrowStream, batches: Vec<RecordBatch>) -> Result<(), ZerobusError> {
    /// // Queue multiple batches, then wait once for the final cumulative offset.
    /// let mut last_offset = None;
    /// for batch in batches {
    ///     last_offset = Some(stream.ingest_batch(batch).await?);
    /// }
    /// if let Some(offset) = last_offset {
    ///     stream.wait_for_offset(offset).await?;
    /// }
    /// println!("All batches acknowledged");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn wait_for_offset(&self, offset: OffsetId) -> ZerobusResult<()> {
        self.wait_for_offset_internal(offset, "Waiting for acknowledgement")
            .await
    }

    /// Reaps the supervisor after giving deadline-bound cleanup a short scheduling grace.
    async fn join_supervisor_for_close(&self, close_deadline: Instant) -> ZerobusResult<()> {
        let join_deadline = close_deadline + Duration::from_millis(EXPLICIT_CLOSE_JOIN_GRACE_MS);
        let mut task = self.receiver_task.lock().await;
        let Some(handle) = task.as_mut() else {
            return Ok(());
        };

        let result = match tokio::time::timeout_at(join_deadline, &mut *handle).await {
            Ok(Ok(supervisor_result)) => supervisor_result,
            Ok(Err(join_error)) => {
                warn!(%join_error, "Explicit close supervisor task failed");
                Err(Self::explicit_close_aborted_timeout())
            }
            Err(_) => {
                // Give close work scheduled on the transport deadline one final poll before
                // aborting a task that is still live after the join grace.
                tokio::task::yield_now().await;
                if !handle.is_finished() {
                    warn!("Explicit close cleanup exceeded its deadline; aborting supervisor");
                    handle.abort();
                }

                match handle.await {
                    Ok(supervisor_result) => supervisor_result,
                    Err(join_error) if join_error.is_cancelled() => {
                        Err(Self::explicit_close_aborted_timeout())
                    }
                    Err(join_error) => {
                        warn!(%join_error, "Explicit close supervisor task failed");
                        Err(Self::explicit_close_aborted_timeout())
                    }
                }
            }
        };
        *task = None;
        result
    }

    /// Applies explicit-close error precedence after supervisor teardown.
    fn resolve_close_result(
        &self,
        flush_result: ZerobusResult<()>,
        supervisor_result: ZerobusResult<()>,
    ) -> ZerobusResult<()> {
        let published_close_error =
            self.server_error_rx.borrow().clone().filter(|error| {
                !error.is_retryable() || Self::is_explicit_close_owned_timeout(error)
            });
        let retained_recovery_error = self.recovery_close_error_rx.borrow().clone();
        let close_error =
            Self::preferred_close_error(retained_recovery_error, published_close_error);

        Self::compose_explicit_close_result(
            flush_result,
            supervisor_result,
            close_error.map_or(Ok(()), Err),
        )
    }

    /// Flushes pending work, stops background I/O, and retains unacknowledged batches for
    /// retrieval.
    ///
    /// While the stream is active, the first call attempts one flush before teardown. If
    /// teardown is interrupted, a later call resumes it without flushing again.
    ///
    /// # Returns
    ///
    /// `Ok(())` after clean teardown.
    ///
    /// # Errors
    ///
    /// Returns the initial flush error or a background terminal error. Teardown still
    /// completes; use `get_unacked_batches()` to retrieve unacknowledged batches.
    ///
    /// # Cancellation safety
    ///
    /// Cancelling before teardown begins does not itself close the stream, although an
    /// independent terminal failure may do so. Once teardown starts, further ingests are
    /// rejected; call `close()` again to resume incomplete teardown without repeating a
    /// completed flush.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # async fn example(mut stream: ZerobusArrowStream) -> Result<(), ZerobusError> {
    /// // After ingesting batches...
    /// stream.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(level = "debug", skip_all, fields(table_name = %self.table_properties.table_name))]
    pub async fn close(&mut self) -> ZerobusResult<()> {
        let close_teardown_started = self.close_teardown_started.load(Ordering::Acquire);
        if self.is_closed.load(Ordering::Relaxed) && !close_teardown_started {
            // Explicit close has one stable result. Otherwise return the background error
            // that closed the stream.
            if self.explicit_close_tx.borrow().is_some() {
                return match self.close_error.lock().await.clone() {
                    Some(error) => Err(error),
                    None => Ok(()),
                };
            }
            return match self.server_error_rx.borrow().clone() {
                Some(error) => Err(error),
                None => Ok(()),
            };
        }

        info!(
            table_name = %self.table_properties.table_name,
            "Closing Arrow Flight stream"
        );

        // Retain the first flush result so cancellation-safe retries do not flush again.
        let flush_result = if close_teardown_started {
            match self.close_error.lock().await.clone() {
                Some(error) => Err(error),
                None => Ok(()),
            }
        } else {
            let result = self.flush().await;
            *self.close_error.lock().await = result.as_ref().err().cloned();
            self.close_teardown_started.store(true, Ordering::Release);
            if let Err(error) = &result {
                warn!(
                    "Flush failed during close: {}. Draining pending batches to the failed set.",
                    error
                );
            }
            result
        };

        // Copy the watch value before publishing; holding borrow() across send_replace()
        // would deadlock the first close on the channel's read lock.
        let existing_close_deadline = *self.explicit_close_tx.borrow();
        let explicit_close_deadline = existing_close_deadline.unwrap_or_else(|| {
            let deadline = Instant::now() + Duration::from_millis(CLOSE_SIGNAL_DRAIN_TIMEOUT_MS);
            self.explicit_close_tx.send_replace(Some(deadline));
            deadline
        });
        let supervisor_result = self
            .join_supervisor_for_close(explicit_close_deadline)
            .await;
        let close_result = self.resolve_close_result(flush_result, supervisor_result);
        *self.close_error.lock().await = close_result.as_ref().err().cloned();

        // Detach the sender now that nothing can reinstall it.
        {
            let mut tx = self.batch_tx.lock().await;
            *tx = None;
        }

        // Test seam: cancel close after teardown became irreversible but before finalization.
        #[cfg(feature = "test-hooks")]
        {
            let barrier = self.close_finalize_gate.lock().await.take();
            if let Some(barrier) = barrier {
                barrier.reached.notify_one();
                barrier.proceed.notified().await;
            }
        }

        // Finalize under ingest_mutex so the pending drain is serialized with
        // ingest_batch. Keep close_teardown_started set while finalization is in flight,
        // then clear it immediately afterward; cancellation before completion remains
        // resumable even if closure was already published.
        Self::finalize_closed(
            &self.ingest_mutex,
            &self.is_closed,
            &self.pending_batches,
            &self.failed_batches,
            &self.last_acked_records,
        )
        .await;
        self.close_teardown_started.store(false, Ordering::Release);

        close_result
    }

    /// Returns the un-acknowledged batches after the stream has been closed, for manual
    /// retry or persistence.
    ///
    /// A partially-acknowledged batch (an auto-chunked batch whose prefix was durably
    /// stored) is sliced to its un-acknowledged suffix, so retrying it does not re-send
    /// already-persisted records. The call drains any still-pending batches into the
    /// failed set and returns the consolidated snapshot; repeated calls return the same
    /// snapshot (idempotent).
    ///
    /// # Errors
    ///
    /// * `InvalidStateError` - If closure has not been finalized, including after
    ///   interrupted teardown; call `close()` again first.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # async fn example(sdk: ZerobusSdk, mut stream: ZerobusArrowStream) -> Result<(), ZerobusError> {
    /// // close() returns Err on a failed flush or a background terminal failure; either
    /// // way, inspect the un-acked batches to retry them.
    /// if stream.close().await.is_err() {
    ///     let failed_batches = stream.get_unacked_batches().await?;
    ///     println!("Retrying {} un-acked batches", failed_batches.len());
    ///     // recreate_arrow_stream() re-ingests the un-acked batches on the new stream,
    ///     // so just flush it — don't re-ingest them yourself.
    ///     let new_stream = sdk.recreate_arrow_stream(&stream).await?;
    ///     new_stream.flush().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_unacked_batches(&self) -> ZerobusResult<Vec<RecordBatch>> {
        if !self.is_closed.load(Ordering::Relaxed) {
            error!(
                table_name = %self.table_properties.table_name,
                "Cannot get unacked batches from an active stream. Stream must be closed first."
            );
            return Err(ZerobusError::InvalidStateError(
                "Cannot get unacked batches from an active stream. Stream must be closed first."
                    .to_string(),
            ));
        }

        // Drain any still-pending batches (sliced to their un-acked suffix) into the
        // failed set, then return the consolidated snapshot. move_pending_to_failed locks
        // failed first, so this serializes with a concurrent terminal drain and repeated
        // calls are idempotent (pending is already empty on the second call).
        Self::move_pending_to_failed(
            &self.pending_batches,
            &self.failed_batches,
            &self.last_acked_records,
        )
        .await;
        Ok(self.failed_batches.lock().await.clone())
    }

    /// Returns true once terminal finalization publishes closure. Interrupted teardown
    /// remains false until finalization begins; cancellation during finalization may leave
    /// this true while `close_teardown_started` marks teardown as resumable.
    pub fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::Relaxed)
    }

    /// Test-only: arms the reconnect rebuild barrier. The next `reconnect` pauses after
    /// establishing the connection but before rebuilding pending ranges/watermark,
    /// firing the returned `reached` notify, then waits on `proceed`. A test either
    /// releases `proceed` to let recovery finish, or drives a concurrent `close()` that
    /// transfers the replacement connection to bounded shutdown without releasing it.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn arm_reconnect_rebuild_barrier(&self) -> (Arc<Notify>, Arc<Notify>) {
        let reached = Arc::new(Notify::new());
        let proceed = Arc::new(Notify::new());
        *self.reconnect_rebuild_gate.lock().await = Some(ReconnectRebuildBarrier {
            reached: Arc::clone(&reached),
            proceed: Arc::clone(&proceed),
        });
        (reached, proceed)
    }

    /// Test-only: arms a notify that fires each time `process_acks` applies a non-empty
    /// ack (after storing `last_acked_records`). Lets a test wait until a partial ack has
    /// been processed before proceeding.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn arm_ack_applied_notify(&self) -> Arc<Notify> {
        let notify = Arc::new(Notify::new());
        *self.ack_applied_gate.lock().await = Some(AckAppliedHook {
            reached: Arc::clone(&notify),
            proceed: None,
        });
        notify
    }

    /// Test-only: parks `process_acks` after fully applying the next non-empty ack.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn arm_ack_applied_barrier(&self) -> (Arc<Notify>, Arc<Notify>) {
        let reached = Arc::new(Notify::new());
        let proceed = Arc::new(Notify::new());
        *self.ack_applied_gate.lock().await = Some(AckAppliedHook {
            reached: Arc::clone(&reached),
            proceed: Some(Arc::clone(&proceed)),
        });
        (reached, proceed)
    }

    /// Test-only: observes one response consumed after request EOF and completion of
    /// the response-drain phase.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn arm_response_drain_hook(&self) -> (Arc<Notify>, Arc<Notify>) {
        let response_consumed = Arc::new(Notify::new());
        let drain_completed = Arc::new(Notify::new());
        *self.response_drain_gate.lock().await = Some(ResponseDrainHook {
            response_consumed: Arc::clone(&response_consumed),
            drain_completed: Arc::clone(&drain_completed),
        });
        (response_consumed, drain_completed)
    }

    /// Test-only: waits until the ACK processor has entered server-initiated graceful
    /// close handling.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn wait_for_graceful_close_start(&self) {
        let mut rx = self.graceful_close_rx.clone();
        while rx.borrow_and_update().is_none() {
            if rx.changed().await.is_err() {
                return;
            }
        }
    }

    /// Test-only: parks the next `close()` after supervisor/sender teardown but before
    /// finalization. Dropping the close future at that point simulates cancellation.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn arm_close_finalize_barrier(&self) -> (Arc<Notify>, Arc<Notify>) {
        let reached = Arc::new(Notify::new());
        let proceed = Arc::new(Notify::new());
        *self.close_finalize_gate.lock().await = Some(CloseFinalizeBarrier {
            reached: Arc::clone(&reached),
            proceed: Arc::clone(&proceed),
        });
        (reached, proceed)
    }

    /// Returns the table name for this stream.
    pub fn table_name(&self) -> &str {
        &self.table_properties.table_name
    }

    /// Returns the Arrow schema for this stream.
    pub fn schema(&self) -> &Arc<ArrowSchema> {
        &self.table_properties.schema
    }

    /// Returns the configuration options for this stream.
    pub fn options(&self) -> &ArrowStreamConfigurationOptions {
        &self.options
    }

    pub(crate) fn headers_provider(&self) -> Arc<dyn HeadersProvider> {
        Arc::clone(&self.headers_provider)
    }
}

impl Drop for ZerobusArrowStream {
    fn drop(&mut self) {
        self.is_closed.store(true, Ordering::Relaxed);
        // Best-effort abort the supervisor. Drop does not preserve pending batches for
        // retrieval; call close() or let recovery reach terminal finalization first.
        if let Ok(mut guard) = self.receiver_task.try_lock() {
            if let Some(handle) = guard.take() {
                handle.abort();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field};
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicUsize;

    struct PassthroughTlsConfig;

    impl TlsConfig for PassthroughTlsConfig {
        fn configure_endpoint(
            &self,
            endpoint: tonic::transport::Endpoint,
        ) -> ZerobusResult<tonic::transport::Endpoint> {
            Ok(endpoint)
        }
    }

    struct TestHeadersProvider;

    #[async_trait]
    impl HeadersProvider for TestHeadersProvider {
        async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
            Ok(HashMap::from([
                ("authorization", "Bearer secret-token".to_string()),
                ("x-custom-header", "custom-value".to_string()),
            ]))
        }
    }

    struct CountingInvalidationHeadersProvider {
        invalidations: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl HeadersProvider for CountingInvalidationHeadersProvider {
        async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
            Ok(HashMap::new())
        }

        async fn invalidate(&self) {
            self.invalidations.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_arrow_table_properties() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let props = ArrowTableProperties {
            table_name: "catalog.schema.table".to_string(),
            schema,
        };

        assert_eq!(props.table_name, "catalog.schema.table");
        assert_eq!(props.schema.fields().len(), 2);
    }

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
        PendingBatch {
            batch,
            offset_id,
            start_record,
            end_record,
            _permit: Arc::clone(sem).try_acquire_owned().unwrap(),
        }
    }

    fn completed_request_body() -> RequestBodyControl {
        let (_eof_tx, eof_rx) = watch::channel(true);
        RequestBodyControl {
            shutdown: CancellationToken::new(),
            eof_rx,
        }
    }

    fn stalled_request_body() -> (RequestBodyControl, watch::Sender<bool>) {
        let (eof_tx, eof_rx) = watch::channel(false);
        (
            RequestBodyControl {
                shutdown: CancellationToken::new(),
                eof_rx,
            },
            eof_tx,
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
        let response_stream = futures::stream::iter([Ok(PutResult {
            app_metadata: serde_json::to_vec(&FlightAckMetadata {
                ack_up_to_offset: 0,
                ack_up_to_records: 11,
                close_stream_duration_ms: None,
            })
            .unwrap()
            .into(),
        })]);
        let (last_ack_tx, last_ack_rx) = watch::channel(None);
        let (server_error_tx, mut server_error_rx) = watch::channel(None);
        let submitted_records = Arc::new(AtomicU64::new(10));
        let last_acked_records = Arc::new(AtomicU64::new(0));
        let (request_body, eof_tx) = stalled_request_body();
        let options = ArrowStreamConfigurationOptions::default();

        let process_acks = ZerobusArrowStream::process_acks(
            Box::pin(response_stream),
            Arc::new(AtomicBool::new(false)),
            last_ack_tx,
            Arc::clone(&pending_batches),
            Duration::from_secs(60),
            server_error_tx,
            Arc::clone(&submitted_records),
            Arc::clone(&last_acked_records),
            Arc::new(AtomicBool::new(false)),
            Arc::new(Mutex::new(())),
            Arc::new(Mutex::new(None)),
            request_body,
            watch::channel(None).0,
            watch::channel(None).1,
            &options,
            true,
            #[cfg(feature = "test-hooks")]
            Arc::new(Mutex::new(None)),
            #[cfg(feature = "test-hooks")]
            Arc::new(Mutex::new(None)),
        );
        tokio::pin!(process_acks);

        tokio::time::timeout(Duration::from_millis(100), async {
            tokio::select! {
                result = &mut process_acks => {
                    panic!("request cleanup completed before stalled EOF was released: {result:?}")
                }
                changed = server_error_rx.changed() => {
                    changed.expect("error receiver should remain open");
                }
            }
        })
        .await
        .expect("invalid ACK must be published before request cleanup finishes");
        assert!(matches!(
            server_error_rx.borrow().as_ref(),
            Some(ZerobusError::InvalidStateError(_))
        ));
        assert!(
            futures::poll!(process_acks.as_mut()).is_pending(),
            "ACK processing should still be waiting for request EOF"
        );

        eof_tx.send_replace(true);
        let error = tokio::time::timeout(Duration::from_millis(100), &mut process_acks)
            .await
            .expect("ACK processing should finish after request EOF")
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
        assert_eq!((pending[0].start_record, pending[0].end_record), (0, 10));
    }

    /// A permanent peer status must reach waiters before bounded request cleanup, which
    /// can consume more time than a caller's remaining flush budget.
    #[tokio::test]
    async fn terminal_peer_error_is_published_before_request_cleanup() {
        let response_stream = futures::stream::iter([Err(FlightError::from(
            tonic::Status::invalid_argument("permanent peer failure"),
        ))]);
        let (last_ack_tx, _last_ack_rx) = watch::channel(None);
        let (server_error_tx, mut server_error_rx) = watch::channel(None);
        let (request_body, eof_tx) = stalled_request_body();
        let options = ArrowStreamConfigurationOptions::default();

        let process_acks = ZerobusArrowStream::process_acks(
            Box::pin(response_stream),
            Arc::new(AtomicBool::new(false)),
            last_ack_tx,
            Arc::new(Mutex::new(Vec::new())),
            Duration::from_secs(60),
            server_error_tx,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(Mutex::new(())),
            Arc::new(Mutex::new(None)),
            request_body,
            watch::channel(None).0,
            watch::channel(None).1,
            &options,
            true,
            #[cfg(feature = "test-hooks")]
            Arc::new(Mutex::new(None)),
            #[cfg(feature = "test-hooks")]
            Arc::new(Mutex::new(None)),
        );
        tokio::pin!(process_acks);

        tokio::time::timeout(Duration::from_millis(100), async {
            tokio::select! {
                result = &mut process_acks => {
                    panic!("request cleanup completed before stalled EOF was released: {result:?}")
                }
                changed = server_error_rx.changed() => {
                    changed.expect("error receiver should remain open");
                }
            }
        })
        .await
        .expect("permanent peer error must be published before request cleanup finishes");
        let published_error = server_error_rx
            .borrow()
            .clone()
            .expect("permanent peer error should be published");
        assert!(!published_error.is_retryable());
        assert!(published_error
            .to_string()
            .contains("permanent peer failure"));
        assert!(
            futures::poll!(process_acks.as_mut()).is_pending(),
            "ACK processing should still be waiting for request EOF"
        );

        eof_tx.send_replace(true);
        let error = tokio::time::timeout(Duration::from_millis(100), &mut process_acks)
            .await
            .expect("ACK processing should finish after request EOF")
            .expect_err("permanent peer status must terminate ACK processing");
        assert!(!error.is_retryable());
        assert!(error.to_string().contains("permanent peer failure"));
    }

    #[tokio::test]
    async fn waiter_returns_published_terminal_error_before_stream_closure() {
        let terminal_error = ZerobusError::InvalidStateError("invalid ACK".to_string());
        let (_last_ack_tx, last_ack_rx) = watch::channel(None);
        let (_server_error_tx, server_error_rx) = watch::channel(Some(terminal_error));
        let is_closed = AtomicBool::new(false);
        let close_teardown_started = AtomicBool::new(false);

        let error = ZerobusArrowStream::wait_for_offset_state(
            0,
            "test waiter",
            Duration::from_millis(100),
            &is_closed,
            &close_teardown_started,
            last_ack_rx,
            server_error_rx,
        )
        .await
        .expect_err("published terminal error must beat the waiter timeout");

        assert!(matches!(error, ZerobusError::InvalidStateError(_)));
        assert!(!error.is_retryable());
        assert!(!is_closed.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn waiter_prefers_target_ack_over_published_terminal_error() {
        let (_last_ack_tx, last_ack_rx) = watch::channel(Some(0));
        let terminal_error = ZerobusError::InvalidStateError("invalid ACK".to_string());
        let (_server_error_tx, server_error_rx) = watch::channel(Some(terminal_error));

        ZerobusArrowStream::wait_for_offset_state(
            0,
            "test waiter",
            Duration::from_millis(100),
            &AtomicBool::new(false),
            &AtomicBool::new(false),
            last_ack_rx,
            server_error_rx,
        )
        .await
        .expect("the target ACK must win over a concurrently published terminal error");
    }

    #[tokio::test]
    async fn closing_reconnect_rejects_positive_ack_before_replay_install() {
        let schema = one_col_schema();
        let sem = Arc::new(Semaphore::new(1));
        let pending_batches = Arc::new(Mutex::new(vec![pending_batch(
            &sem,
            batch_with_rows(&schema, 10),
            7,
            0,
            10,
        )]));
        let response_stream = futures::stream::iter([Ok(PutResult {
            app_metadata: serde_json::to_vec(&FlightAckMetadata::new(7, 10))
                .unwrap()
                .into(),
        })]);
        let (last_ack_tx, last_ack_rx) = watch::channel(None);
        let (server_error_tx, _server_error_rx) = watch::channel(None);
        let last_acked_records = Arc::new(AtomicU64::new(0));

        let error = ZerobusArrowStream::process_acks(
            Box::pin(response_stream),
            Arc::new(AtomicBool::new(false)),
            last_ack_tx,
            Arc::clone(&pending_batches),
            Duration::from_secs(60),
            server_error_tx,
            Arc::new(AtomicU64::new(10)),
            Arc::clone(&last_acked_records),
            Arc::new(AtomicBool::new(true)),
            Arc::new(Mutex::new(())),
            Arc::new(Mutex::new(None)),
            completed_request_body(),
            watch::channel(None).0,
            watch::channel(None).1,
            &ArrowStreamConfigurationOptions::default(),
            false,
            #[cfg(feature = "test-hooks")]
            Arc::new(Mutex::new(None)),
            #[cfg(feature = "test-hooks")]
            Arc::new(Mutex::new(None)),
        )
        .await
        .expect_err("a replacement ACK before replay installation must be rejected");

        assert!(matches!(error, ZerobusError::InvalidStateError(_)));
        assert_eq!(last_acked_records.load(Ordering::Acquire), 0);
        assert_eq!(*last_ack_rx.borrow(), None);
        assert_eq!(pending_batches.lock().await.len(), 1);
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
        let response_stream = futures::stream::iter([Ok(PutResult {
            app_metadata: serde_json::to_vec(&FlightAckMetadata {
                ack_up_to_offset: 1,
                ack_up_to_records: 20,
                close_stream_duration_ms: None,
            })
            .unwrap()
            .into(),
        })]);
        let (last_ack_tx, last_ack_rx) = watch::channel(None);
        let (server_error_tx, _server_error_rx) = watch::channel(None);
        let submitted_records = Arc::new(AtomicU64::new(10));
        let last_acked_records = Arc::new(AtomicU64::new(0));

        let error = ZerobusArrowStream::process_acks(
            Box::pin(response_stream),
            Arc::new(AtomicBool::new(false)),
            last_ack_tx,
            Arc::clone(&pending_batches),
            Duration::from_secs(60),
            server_error_tx,
            Arc::clone(&submitted_records),
            Arc::clone(&last_acked_records),
            Arc::new(AtomicBool::new(true)),
            Arc::new(Mutex::new(())),
            Arc::new(Mutex::new(None)),
            completed_request_body(),
            watch::channel(None).0,
            watch::channel(None).1,
            &ArrowStreamConfigurationOptions::default(),
            true,
            #[cfg(feature = "test-hooks")]
            Arc::new(Mutex::new(None)),
            #[cfg(feature = "test-hooks")]
            Arc::new(Mutex::new(None)),
        )
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
        assert_eq!((pending[0].start_record, pending[0].end_record), (0, 10));
        assert_eq!((pending[1].start_record, pending[1].end_record), (10, 20));
    }

    /// A delayed or duplicate acknowledgement must never move the cumulative watermark backward,
    /// otherwise recovery can resend a prefix that the server already made durable.
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
        let response_stream = futures::stream::iter([5, 0].map(|acked_records| {
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
        let (last_ack_tx, _last_ack_rx) = watch::channel(None);
        let (server_error_tx, _server_error_rx) = watch::channel(None);
        let last_acked_records = Arc::new(AtomicU64::new(0));

        let _stream_closed = ZerobusArrowStream::process_acks(
            Box::pin(response_stream),
            Arc::new(AtomicBool::new(false)),
            last_ack_tx,
            Arc::clone(&pending_batches),
            Duration::from_secs(60),
            server_error_tx,
            Arc::new(AtomicU64::new(10)),
            Arc::clone(&last_acked_records),
            Arc::new(AtomicBool::new(false)),
            Arc::new(Mutex::new(())),
            Arc::new(Mutex::new(None)),
            completed_request_body(),
            watch::channel(None).0,
            watch::channel(None).1,
            &ArrowStreamConfigurationOptions::default(),
            true,
            #[cfg(feature = "test-hooks")]
            Arc::new(Mutex::new(None)),
            #[cfg(feature = "test-hooks")]
            Arc::new(Mutex::new(None)),
        )
        .await;

        let acked_before_disconnect = last_acked_records.load(Ordering::Acquire);
        assert_eq!(acked_before_disconnect, 5);
        let cumulative_records_assigned = Arc::new(AtomicU64::new(10));
        let submitted_records = Arc::new(AtomicU64::new(10));
        let (tx, mut rx) = mpsc::channel::<Result<RecordBatch, FlightError>>(1);
        let replay_state_installed = AtomicBool::new(false);
        ZerobusArrowStream::replay_pending_batches(
            &tx,
            &pending_batches,
            &cumulative_records_assigned,
            &submitted_records,
            &last_acked_records,
            acked_before_disconnect,
            &replay_state_installed,
        )
        .await
        .expect("replay should succeed");

        let pending = pending_batches.lock().await;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].batch.num_rows(), 5);
        assert_eq!((pending[0].start_record, pending[0].end_record), (0, 5));
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
        let pending = Arc::new(Mutex::new(vec![
            pending_batch(&sem, batch_with_rows(&schema, 3), 0, 0, 3),
            pending_batch(&sem, batch_with_rows(&schema, 2), 1, 3, 5),
        ]));
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

        let replay_state_installed = AtomicBool::new(false);
        let res = ZerobusArrowStream::replay_pending_batches(
            &tx,
            &pending,
            &cumulative,
            &submitted,
            &last_acked,
            0,
            &replay_state_installed,
        )
        .await;
        assert!(res.is_err(), "replay must surface the send failure");

        let guard = pending.lock().await;
        assert_eq!(
            guard.len(),
            2,
            "pending must retain all batches on replay failure"
        );
        assert_eq!((guard[0].start_record, guard[0].end_record), (0, 3));
        assert_eq!((guard[1].start_record, guard[1].end_record), (3, 5));
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
        let pending = Arc::new(Mutex::new(vec![
            pending_batch(&sem, batch_with_rows(&schema, 3), 0, 0, 3),
            pending_batch(&sem, batch_with_rows(&schema, 2), 1, 3, 5),
        ]));
        let cumulative = Arc::new(AtomicU64::new(0));
        let submitted = Arc::new(AtomicU64::new(0));
        let last_acked = Arc::new(AtomicU64::new(9));

        let (tx, mut rx) = mpsc::channel::<Result<RecordBatch, FlightError>>(4);

        let replay_state_installed = AtomicBool::new(false);
        let res = ZerobusArrowStream::replay_pending_batches(
            &tx,
            &pending,
            &cumulative,
            &submitted,
            &last_acked,
            0,
            &replay_state_installed,
        )
        .await;
        assert!(res.is_ok());

        assert_eq!(pending.lock().await.len(), 2);
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

        let replay_state_installed = AtomicBool::new(false);
        let res = ZerobusArrowStream::replay_pending_batches(
            &tx,
            &pending,
            &cumulative,
            &submitted,
            &last_acked,
            4,
            &replay_state_installed,
        )
        .await;
        assert!(res.is_ok());

        // Only the partially-acked batch remains, rebuilt from cumulative 0.
        let guard = pending.lock().await;
        assert_eq!(guard.len(), 1);
        assert_eq!((guard[0].start_record, guard[0].end_record), (0, 2));
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

        let fut = ZerobusArrowStream::pause_and_detach_sender(&ingest_mutex, &is_paused, &batch_tx);
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
        tokio::time::timeout(Duration::from_secs(1), fut)
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

        let fut = ZerobusArrowStream::finalize_closed(
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

        tokio::time::timeout(Duration::from_secs(1), fut)
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
    async fn duplicate_close_signals_only_shorten_deadlines() {
        let original_ack_deadline = Instant::now() + Duration::from_secs(1);
        let original_close_deadline = Instant::now() + Duration::from_secs(2);
        let mut phase = GracefulClosePhase::AwaitingAcks {
            target_records: 7,
            ack_deadline: original_ack_deadline,
            close_deadline: original_close_deadline,
        };
        let ingest_mutex = Arc::new(Mutex::new(()));
        let is_paused = Arc::new(AtomicBool::new(true));
        let submitted_records = Arc::new(AtomicU64::new(7));

        let target = ZerobusArrowStream::register_graceful_close(
            &mut phase,
            10_000,
            None,
            &ingest_mutex,
            &is_paused,
            &submitted_records,
        )
        .await;
        assert_eq!(target, 7);
        match &phase {
            GracefulClosePhase::AwaitingAcks {
                ack_deadline,
                close_deadline,
                ..
            } => {
                assert_eq!(*ack_deadline, original_ack_deadline);
                assert_eq!(*close_deadline, original_close_deadline);
            }
            _ => panic!("expected ACK-wait phase"),
        }

        ZerobusArrowStream::register_graceful_close(
            &mut phase,
            100,
            None,
            &ingest_mutex,
            &is_paused,
            &submitted_records,
        )
        .await;
        match phase {
            GracefulClosePhase::AwaitingAcks {
                ack_deadline,
                close_deadline,
                ..
            } => {
                assert!(ack_deadline < original_ack_deadline);
                assert!(close_deadline < original_close_deadline);
            }
            _ => panic!("expected ACK-wait phase"),
        }

        let original_drain_deadline = Instant::now() + Duration::from_secs(2);
        let mut phase = GracefulClosePhase::DrainingResponse {
            deadline: original_drain_deadline,
            recovery_reason: ZerobusError::StreamClosedError(tonic::Status::unavailable(
                "test rotation",
            )),
            request_reached_eof: false,
            response_reached_eof: false,
        };
        ZerobusArrowStream::register_graceful_close(
            &mut phase,
            100,
            None,
            &ingest_mutex,
            &is_paused,
            &submitted_records,
        )
        .await;
        match phase {
            GracefulClosePhase::DrainingResponse { deadline, .. } => {
                assert!(deadline < original_drain_deadline);
            }
            _ => panic!("expected response-drain phase"),
        }
    }

    #[test]
    fn graceful_close_deadlines_reserve_cleanup_inside_server_grace() {
        let (ack_deadline, close_deadline) =
            ZerobusArrowStream::graceful_close_deadlines(5_000, None);
        assert_eq!(
            close_deadline.duration_since(ack_deadline),
            Duration::from_millis(CLOSE_SIGNAL_DRAIN_TIMEOUT_MS)
        );

        let (ack_deadline, close_deadline) =
            ZerobusArrowStream::graceful_close_deadlines(5_000, Some(500));
        assert_eq!(
            close_deadline.duration_since(ack_deadline),
            Duration::from_millis(4_500)
        );

        let (ack_deadline, close_deadline) =
            ZerobusArrowStream::graceful_close_deadlines(5_000, Some(0));
        assert_eq!(
            close_deadline.duration_since(ack_deadline),
            Duration::from_millis(5_000)
        );

        let (ack_deadline, close_deadline) = ZerobusArrowStream::graceful_close_deadlines(0, None);
        assert_eq!(
            close_deadline.duration_since(ack_deadline),
            Duration::from_millis(CLOSE_SIGNAL_DRAIN_TIMEOUT_MS),
            "zero server grace must still get a local best-effort EOF settle window"
        );
    }

    #[test]
    fn graceful_close_deadlines_cap_extreme_durations_without_overflow() {
        let (ack_deadline, close_deadline) =
            ZerobusArrowStream::graceful_close_deadlines(5_000, Some(u64::MAX));
        assert!(ack_deadline <= close_deadline);
        assert_eq!(
            close_deadline.duration_since(ack_deadline),
            Duration::from_millis(CLOSE_SIGNAL_DRAIN_TIMEOUT_MS),
            "an oversized client override must be capped by the server allowance"
        );

        let (ack_deadline, close_deadline) =
            ZerobusArrowStream::graceful_close_deadlines(u64::MAX, None);
        assert!(ack_deadline <= close_deadline);
        assert_eq!(
            close_deadline.duration_since(ack_deadline),
            Duration::from_millis(CLOSE_SIGNAL_DRAIN_TIMEOUT_MS),
            "an oversized peer duration must retain the cleanup reservation"
        );
    }

    #[tokio::test]
    async fn timed_out_reconnect_cleanup_applies_queued_acknowledgments() {
        let schema = one_col_schema();
        let sem = Arc::new(Semaphore::new(1));
        let pending_batches = Arc::new(Mutex::new(vec![pending_batch(
            &sem,
            batch_with_rows(&schema, 10),
            7,
            0,
            10,
        )]));
        let submitted_records = Arc::new(AtomicU64::new(10));
        let last_acked_records = Arc::new(AtomicU64::new(0));
        let (last_ack_tx, last_ack_rx) = watch::channel(None);
        #[cfg(feature = "test-hooks")]
        let ack_applied_gate = Arc::new(Mutex::new(None));
        let progress = AckProgress {
            submitted_records: &submitted_records,
            last_acked_records: &last_acked_records,
            pending_batches: &pending_batches,
            last_ack_tx: &last_ack_tx,
            #[cfg(feature = "test-hooks")]
            ack_applied_gate: &ack_applied_gate,
        };
        let response_stream = futures::stream::iter([Ok(PutResult {
            app_metadata: serde_json::to_vec(&FlightAckMetadata::new(7, 10))
                .unwrap()
                .into(),
        })]);
        let (batch_tx, _batch_rx) = mpsc::channel(1);
        let connection = FlightConnection {
            response_stream: Box::pin(response_stream),
            batch_tx,
            request_body: completed_request_body(),
            replay_state_installed: true,
        };

        let result = ZerobusArrowStream::finish_timed_out_reconnect(
            connection,
            Instant::now() + Duration::from_secs(1),
            ZerobusArrowStream::reconnect_timeout_error(100),
            &progress,
        )
        .await;

        assert!(matches!(result, Err(ZerobusError::ConnectionTimeout(_))));
        assert!(pending_batches.lock().await.is_empty());
        assert_eq!(last_acked_records.load(Ordering::Acquire), 10);
        assert_eq!(*last_ack_rx.borrow(), Some(7));
    }

    #[tokio::test]
    async fn pre_replay_reconnect_cleanup_rejects_positive_acknowledgments() {
        let schema = one_col_schema();
        let sem = Arc::new(Semaphore::new(1));
        let pending_batches = Arc::new(Mutex::new(vec![pending_batch(
            &sem,
            batch_with_rows(&schema, 10),
            7,
            0,
            10,
        )]));
        // These values still belong to the failed connection. Applying the replacement
        // connection's ACK against them would falsely complete offset 7.
        let submitted_records = Arc::new(AtomicU64::new(10));
        let last_acked_records = Arc::new(AtomicU64::new(0));
        let (last_ack_tx, last_ack_rx) = watch::channel(None);
        #[cfg(feature = "test-hooks")]
        let ack_applied_gate = Arc::new(Mutex::new(None));
        let progress = AckProgress {
            submitted_records: &submitted_records,
            last_acked_records: &last_acked_records,
            pending_batches: &pending_batches,
            last_ack_tx: &last_ack_tx,
            #[cfg(feature = "test-hooks")]
            ack_applied_gate: &ack_applied_gate,
        };
        let response_stream = futures::stream::iter([Ok(PutResult {
            app_metadata: serde_json::to_vec(&FlightAckMetadata::new(7, 10))
                .unwrap()
                .into(),
        })]);
        let (batch_tx, _batch_rx) = mpsc::channel(1);
        let connection = FlightConnection {
            response_stream: Box::pin(response_stream),
            batch_tx,
            request_body: completed_request_body(),
            replay_state_installed: false,
        };

        let result = ZerobusArrowStream::finish_timed_out_reconnect(
            connection,
            Instant::now() + Duration::from_secs(1),
            ZerobusArrowStream::reconnect_timeout_error(100),
            &progress,
        )
        .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("a positive ACK before replay state installation must be rejected"),
        };

        assert!(matches!(error, ZerobusError::InvalidStateError(_)));
        let pending = pending_batches.lock().await;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].offset_id, 7);
        assert_eq!(pending[0].start_record, 0);
        assert_eq!(pending[0].end_record, 10);
        assert_eq!(last_acked_records.load(Ordering::Acquire), 0);
        assert_eq!(*last_ack_rx.borrow(), None);
    }

    #[tokio::test]
    async fn expired_reconnect_cleanup_deadline_preempts_ready_responses() {
        let response_polls = Arc::new(AtomicUsize::new(0));
        let observed_polls = Arc::clone(&response_polls);
        let ready_metadata: Bytes = serde_json::to_vec(&FlightAckMetadata::stream_ready())
            .unwrap()
            .into();
        let response_stream = futures::stream::poll_fn(move |_| {
            observed_polls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Some(Ok(PutResult {
                app_metadata: ready_metadata.clone(),
            })))
        });
        let (batch_tx, _batch_rx) = mpsc::channel(1);
        let connection = FlightConnection {
            response_stream: Box::pin(response_stream),
            batch_tx,
            request_body: completed_request_body(),
            replay_state_installed: false,
        };
        let pending_batches = Arc::new(Mutex::new(Vec::new()));
        let submitted_records = Arc::new(AtomicU64::new(0));
        let last_acked_records = Arc::new(AtomicU64::new(0));
        let (last_ack_tx, _last_ack_rx) = watch::channel(None);
        #[cfg(feature = "test-hooks")]
        let ack_applied_gate = Arc::new(Mutex::new(None));
        let progress = AckProgress {
            submitted_records: &submitted_records,
            last_acked_records: &last_acked_records,
            pending_batches: &pending_batches,
            last_ack_tx: &last_ack_tx,
            #[cfg(feature = "test-hooks")]
            ack_applied_gate: &ack_applied_gate,
        };

        let result = ZerobusArrowStream::finish_timed_out_reconnect(
            connection,
            Instant::now(),
            ZerobusArrowStream::reconnect_timeout_error(100),
            &progress,
        )
        .await;

        assert!(matches!(result, Err(ZerobusError::ConnectionTimeout(_))));
        assert_eq!(
            response_polls.load(Ordering::SeqCst),
            0,
            "an expired deadline must win before another ready response is polled"
        );
    }

    #[tokio::test]
    async fn explicit_close_deadline_reports_incomplete_transport_stage() {
        #[cfg(feature = "test-hooks")]
        let response_drain_gate = Arc::new(Mutex::new(None));

        let request_timeout = ZerobusArrowStream::finish_response_drain_deadline(
            &GracefulClosePhase::DrainingExplicitClose {
                deadline: Instant::now(),
                request_reached_eof: false,
                response_reached_eof: false,
            },
            #[cfg(feature = "test-hooks")]
            &response_drain_gate,
        )
        .await
        .expect_err("missing request EOF must be reported");
        assert!(request_timeout
            .to_string()
            .contains(EXPLICIT_CLOSE_REQUEST_EOF_TIMEOUT));

        let response_timeout = ZerobusArrowStream::finish_response_drain_deadline(
            &GracefulClosePhase::DrainingExplicitClose {
                deadline: Instant::now(),
                request_reached_eof: true,
                response_reached_eof: false,
            },
            #[cfg(feature = "test-hooks")]
            &response_drain_gate,
        )
        .await
        .expect_err("missing response EOF must be reported");
        assert!(response_timeout
            .to_string()
            .contains(EXPLICIT_CLOSE_RESPONSE_DRAIN_TIMEOUT));
    }

    #[tokio::test]
    async fn explicit_close_does_not_reinvalidate_a_carried_auth_rejection() {
        let invalidations = Arc::new(AtomicUsize::new(0));
        let headers_provider: Arc<dyn HeadersProvider> =
            Arc::new(CountingInvalidationHeadersProvider {
                invalidations: Arc::clone(&invalidations),
            });
        let error = ZerobusError::CreateStreamError(tonic::Status::unauthenticated(
            "credential already invalidated",
        ));
        let (server_error_tx, server_error_rx) = watch::channel(None);

        let result = ZerobusArrowStream::finish_explicit_close_result(
            Err(error.clone()),
            true,
            Instant::now() + Duration::from_secs(1),
            &headers_provider,
            &server_error_tx,
        )
        .await;

        assert_eq!(invalidations.load(Ordering::SeqCst), 0);
        assert_eq!(result.unwrap_err().to_string(), error.to_string());
        assert_eq!(
            server_error_rx.borrow().as_ref().map(ToString::to_string),
            Some(error.to_string())
        );
    }

    #[test]
    fn latest_permanent_close_error_supersedes_retained_auth_rejection() {
        let retained = ZerobusError::CreateStreamError(tonic::Status::unauthenticated(
            "old credential rejection",
        ));
        let latest_invalid_argument = ZerobusError::CreateStreamError(
            tonic::Status::invalid_argument("latest permanent rejection"),
        );
        let latest_auth = ZerobusError::CreateStreamError(tonic::Status::unauthenticated(
            "latest credential rejection",
        ));
        let retryable_cleanup = ZerobusError::StreamClosedError(tonic::Status::unavailable(
            "replacement transport failed",
        ));

        let selected = ZerobusArrowStream::preferred_close_error(
            Some(retained.clone()),
            Some(latest_invalid_argument.clone()),
        )
        .expect("a permanent close error should be selected");
        assert_eq!(selected.to_string(), latest_invalid_argument.to_string());

        let selected = ZerobusArrowStream::preferred_close_error(
            Some(retained.clone()),
            Some(latest_auth.clone()),
        )
        .expect("the latest auth rejection should be selected");
        assert_eq!(selected.to_string(), latest_auth.to_string());

        let selected = ZerobusArrowStream::preferred_close_error(
            Some(retained.clone()),
            Some(retryable_cleanup),
        )
        .expect("the retained permanent error should be selected");
        assert_eq!(selected.to_string(), retained.to_string());
    }

    #[tokio::test]
    async fn explicit_close_preempts_a_ready_response_backlog() {
        let response_stream = futures::stream::poll_fn(|_| {
            Poll::Ready(Some(Ok(PutResult {
                app_metadata: Bytes::new(),
            })))
        });
        let mut response_stream: FlightResponseStream = Box::pin(response_stream);
        let deadline = Instant::now() + Duration::from_secs(1);
        let (_explicit_close_tx, mut explicit_close_rx) = watch::channel(Some(deadline));
        let request_body = completed_request_body();

        let event = ZerobusArrowStream::wait_for_ack_event(
            &mut response_stream,
            &request_body,
            &GracefulClosePhase::Open,
            Duration::from_secs(60),
            &mut explicit_close_rx,
        )
        .await;

        assert!(matches!(
            event,
            AckEvent::ExplicitClose {
                deadline: observed
            } if observed == deadline
        ));
    }

    #[tokio::test]
    async fn expired_graceful_close_deadline_preempts_a_ready_response_backlog() {
        let response_stream = futures::stream::poll_fn(|_| {
            Poll::Ready(Some(Ok(PutResult {
                app_metadata: Bytes::new(),
            })))
        });
        let mut response_stream: FlightResponseStream = Box::pin(response_stream);
        let phase = GracefulClosePhase::AwaitingAcks {
            target_records: 1,
            ack_deadline: Instant::now(),
            close_deadline: Instant::now() + Duration::from_secs(1),
        };
        let (_explicit_close_tx, mut explicit_close_rx) = watch::channel(None);
        let request_body = completed_request_body();

        let event = ZerobusArrowStream::wait_for_ack_event(
            &mut response_stream,
            &request_body,
            &phase,
            Duration::from_secs(60),
            &mut explicit_close_rx,
        )
        .await;

        assert!(matches!(event, AckEvent::GracefulCloseDeadline));
    }

    #[tokio::test]
    async fn response_backlog_advances_acks_while_request_eof_settles() {
        let (eof_tx, eof_rx) = watch::channel(false);
        let request_body = RequestBodyControl {
            shutdown: CancellationToken::new(),
            eof_rx,
        };
        let polls = Arc::new(AtomicUsize::new(0));
        let observed_polls = Arc::clone(&polls);
        let response_stream = futures::stream::poll_fn(move |_| {
            let poll = observed_polls.fetch_add(1, Ordering::SeqCst);
            match poll {
                0 => Poll::Ready(Some(Ok(PutResult {
                    app_metadata: serde_json::to_vec(&FlightAckMetadata::new(0, 4))
                        .unwrap()
                        .into(),
                }))),
                1 => {
                    eof_tx.send_replace(true);
                    Poll::Ready(Some(Ok(PutResult {
                        app_metadata: serde_json::to_vec(&FlightAckMetadata::new(0, 10))
                            .unwrap()
                            .into(),
                    })))
                }
                _ => Poll::Ready(None),
            }
        });
        let mut response_stream: FlightResponseStream = Box::pin(response_stream);
        let mut phase = GracefulClosePhase::DrainingResponse {
            deadline: Instant::now() + Duration::from_secs(1),
            recovery_reason: ZerobusError::StreamClosedError(tonic::Status::unavailable(
                "test rotation",
            )),
            request_reached_eof: false,
            response_reached_eof: false,
        };
        let (_explicit_close_tx, mut explicit_close_rx) = watch::channel(None);

        let schema = one_col_schema();
        let sem = Arc::new(Semaphore::new(1));
        let pending_batches = Arc::new(Mutex::new(vec![pending_batch(
            &sem,
            batch_with_rows(&schema, 10),
            0,
            0,
            10,
        )]));
        let submitted_records = Arc::new(AtomicU64::new(10));
        let last_acked_records = Arc::new(AtomicU64::new(0));
        let (last_ack_tx, last_ack_rx) = watch::channel(None);
        #[cfg(feature = "test-hooks")]
        let ack_applied_gate = Arc::new(Mutex::new(None));
        let progress = AckProgress {
            submitted_records: &submitted_records,
            last_acked_records: &last_acked_records,
            pending_batches: &pending_batches,
            last_ack_tx: &last_ack_tx,
            #[cfg(feature = "test-hooks")]
            ack_applied_gate: &ack_applied_gate,
        };

        for expected_records in [4, 10] {
            let event = ZerobusArrowStream::wait_for_ack_event(
                &mut response_stream,
                &request_body,
                &phase,
                Duration::from_secs(60),
                &mut explicit_close_rx,
            )
            .await;
            let AckEvent::Response(Some(Ok(result))) = event else {
                panic!("expected a queued response while request EOF was pending");
            };
            let ack = FlightAckMetadata::from_bytes(&result.app_metadata).unwrap();
            assert_eq!(ack.ack_up_to_records, expected_records);
            ZerobusArrowStream::apply_acknowledgment(&ack, &progress)
                .await
                .unwrap();
        }

        let event = ZerobusArrowStream::wait_for_ack_event(
            &mut response_stream,
            &request_body,
            &phase,
            Duration::from_secs(60),
            &mut explicit_close_rx,
        )
        .await;
        assert!(matches!(event, AckEvent::RequestEof));
        phase.mark_request_eof();

        let event = ZerobusArrowStream::wait_for_ack_event(
            &mut response_stream,
            &request_body,
            &phase,
            Duration::from_secs(60),
            &mut explicit_close_rx,
        )
        .await;
        assert!(matches!(event, AckEvent::Response(None)));
        phase.mark_response_eof();

        assert_eq!(phase.drain_progress(), Some((true, true)));
        assert_eq!(last_acked_records.load(Ordering::Acquire), 10);
        assert_eq!(*last_ack_rx.borrow(), Some(0));
        assert!(pending_batches.lock().await.is_empty());
    }

    #[tokio::test]
    async fn request_shutdown_discards_queued_encoder_output_and_reports_eof() {
        let schema = one_col_schema();
        let table_properties = ArrowTableProperties {
            table_name: "catalog.schema.table".to_string(),
            schema: Arc::clone(&schema),
        };
        let options = ArrowStreamConfigurationOptions::default();
        let (tx, rx) = mpsc::channel(options.max_inflight_batches);
        let (mut request_stream, request_body) =
            ZerobusArrowStream::make_request_stream(rx, &table_properties, &options)
                .expect("request stream should build");

        tx.send(Ok(batch_with_rows(&schema, 10)))
            .await
            .expect("batch should queue");
        assert!(
            request_stream.next().await.is_some(),
            "encoder should first yield the schema"
        );

        request_body.shutdown();
        let next = tokio::time::timeout(Duration::from_secs(1), request_stream.next())
            .await
            .expect("cancellation should wake the request stream");
        assert!(
            next.is_none(),
            "queued batch data must be skipped after shutdown"
        );
        assert!(
            request_body
                .wait_for_eof_until(Instant::now() + Duration::from_secs(1))
                .await,
            "request-body control should observe EOF"
        );
    }

    #[tokio::test]
    async fn authorization_metadata_is_sensitive() {
        let table_properties = ArrowTableProperties {
            table_name: "catalog.schema.table".to_string(),
            schema: Arc::new(ArrowSchema::empty()),
        };
        let tls_config: Arc<dyn TlsConfig> = Arc::new(PassthroughTlsConfig);
        let headers_provider: Arc<dyn HeadersProvider> = Arc::new(TestHeadersProvider);
        let client = ZerobusArrowStream::create_flight_client(
            "http://127.0.0.1:1",
            &tls_config,
            None,
            &table_properties,
            &ArrowStreamConfigurationOptions::default(),
            &headers_provider,
            "test-sdk",
        )
        .await
        .unwrap();

        let authorization = client.metadata().get("authorization").unwrap();
        assert!(authorization.is_sensitive());
        assert_eq!(authorization.to_str().unwrap(), "Bearer secret-token");

        let custom_header = client.metadata().get("x-custom-header").unwrap();
        assert!(!custom_header.is_sensitive());
        assert_eq!(custom_header.to_str().unwrap(), "custom-value");
        assert_eq!(
            client
                .metadata()
                .get("x-databricks-zerobus-table-name")
                .unwrap()
                .to_str()
                .unwrap(),
            "catalog.schema.table"
        );
    }
}
