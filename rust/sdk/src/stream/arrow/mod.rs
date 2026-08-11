//! Arrow Flight stream API for high-performance Arrow data ingestion.
//!
//! **Beta**: This module is in Beta. The API is stabilising but may still change
//! before reaching GA.
//!
//! This directory module provides [`ZerobusArrowStream`] for ingesting Arrow
//! [`RecordBatch`] data into Databricks Delta tables using Arrow Flight. Native Rust
//! callers use [`ZerobusArrowStream::ingest_batch`] with `RecordBatch` values; FFI
//! wrappers can use [`ZerobusArrowStream::ingest_ipc_batch`] with pre-serialised
//! Arrow IPC bytes. The C FFI can also import canonical Arrow C Data through the
//! wrapper-only shared importer before calling [`ZerobusArrowStream::ingest_batch`].
//!
//! `ZerobusArrowStream` owns caller-facing state. Transport setup, acknowledgment
//! processing, pending-batch mechanics, and recovery supervision live in private
//! sibling modules so their invariants remain local.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use arrow_flight::error::FlightError;
use bytes::Bytes;
use tokio::sync::{mpsc, watch, Mutex, Notify, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::{timeout, Duration, Instant};
use tokio_retry::strategy::FixedInterval;
use tokio_retry::RetryIf;
use tracing::{debug, error, info, instrument, warn};

pub use arrow_array::RecordBatch;
pub use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};

use self::batch::{materialize_ipc, PendingBatch};
pub use self::options::ArrowStreamConfigurationOptions;
use self::supervisor::Supervisor;
use crate::errors::{should_retry_initial_connection, ZerobusError};
use crate::headers_provider::HeadersProvider;
use crate::offset_generator::{OffsetId, OffsetIdGenerator};
use crate::proxy::ConnectorFactory;
use crate::tls_config::TlsConfig;
use crate::ZerobusResult;

#[cfg(feature = "internal-arrow-c-data")]
pub(crate) mod c_data;

mod acks;
mod batch;
mod connection;
mod metadata;
mod options;
mod supervisor;

const LOG_TARGET: &str = module_path!();

type BatchSender = Arc<Mutex<Option<mpsc::Sender<Result<RecordBatch, FlightError>>>>>;

/// Converts a configured relative timeout into an absolute monotonic-clock deadline.
pub(super) fn configured_deadline(
    started_at: Instant,
    timeout: Duration,
    option_name: &str,
) -> ZerobusResult<Instant> {
    started_at.checked_add(timeout).ok_or_else(|| {
        ZerobusError::InvalidArgument(format!(
            "{option_name} ({}ms) exceeds the platform monotonic-clock range",
            timeout.as_millis()
        ))
    })
}

/// Test-only barrier used to pause `reconnect` at a precise point — the new connection
/// is established but pending ranges are not yet rebuilt — so a test can schedule a
/// concurrent ingest or `close()`.
#[cfg(feature = "test-hooks")]
type ReconnectRebuildGate = Arc<Mutex<Option<ReconnectRebuildBarrier>>>;

/// Paired notifications for [`ReconnectRebuildGate`]: `reached` fires when reconnect
/// hits the barrier; `proceed` releases it (or a test aborts via `close()` instead).
#[cfg(feature = "test-hooks")]
#[derive(Clone)]
struct ReconnectRebuildBarrier {
    reached: Arc<Notify>,
    proceed: Arc<Notify>,
}

/// Test-only barrier used to pause recovery after its first replay send, before the
/// remaining backlog is sent and pending ACK timestamps are refreshed.
#[cfg(feature = "test-hooks")]
type ReplaySendGate = Arc<Mutex<Option<ReplaySendBarrier>>>;

#[cfg(feature = "test-hooks")]
#[derive(Clone)]
struct ReplaySendBarrier {
    reached: Arc<Notify>,
    proceed: Arc<Notify>,
}

/// Test-only gate: when armed, the ACK processor fires the notify right after applying a
/// non-empty ack (i.e. after storing `last_acked_records`), letting a test confirm a
/// partial ack has landed before it proceeds.
#[cfg(feature = "test-hooks")]
type AckAppliedGate = Arc<Mutex<Option<Arc<Notify>>>>;

/// Test-only gate: when armed, the ACK processor fires the notify immediately before
/// waiting without any pending-work deadline.
#[cfg(feature = "test-hooks")]
type AckIdleGate = Arc<Mutex<Option<Arc<Notify>>>>;

/// Test-only barrier that parks `close()` after the supervisor and sender are gone but
/// before pending batches are finalized, allowing cancellation-safe teardown tests.
#[cfg(feature = "test-hooks")]
type CloseFinalizeGate = Arc<Mutex<Option<CloseFinalizeBarrier>>>;

#[cfg(feature = "test-hooks")]
#[derive(Clone)]
struct CloseFinalizeBarrier {
    reached: Arc<Notify>,
    proceed: Arc<Notify>,
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
    last_ack_tx: watch::Sender<Option<OffsetId>>,
    /// Receiver for the watch channel (kept alive to prevent sender errors).
    _last_ack_rx: watch::Receiver<Option<OffsetId>>,
    /// True once the stream is terminally closed and unacknowledged batches may be retrieved.
    is_closed: Arc<AtomicBool>,
    /// Separates resumable teardown from final closure so retries skip flushing while
    /// new ingests remain rejected.
    close_teardown_started: AtomicBool,
    /// Retains the first flush failure so resumed close calls return the same outcome.
    close_flush_error: Mutex<Option<ZerobusError>>,
    /// Handle to the supervisor task that processes acknowledgments and recovery.
    receiver_task: Arc<Mutex<Option<JoinHandle<ZerobusResult<()>>>>>,
    /// Accepted batches not yet fully acknowledged; retained for replay or retrieval.
    pending_batches: Arc<Mutex<Vec<PendingBatch>>>,
    /// Wakes the ACK processor when a batch is submitted after an idle period.
    pending_notify: Arc<Notify>,
    /// Carries a closed request-sender failure to the ACK processor so retained work
    /// starts recovery even when no submitted batch is eligible for an ACK deadline.
    request_send_failure: Arc<acks::RequestSendFailure>,
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
    /// Final value sent as the HTTP `user-agent` header on every request.
    /// Either `"zerobus-sdk-rs/<version>"` or `"zerobus-sdk-rs/<version> <application_name>"`.
    /// Re-applied to each fresh Channel built during recovery.
    sdk_identifier: Arc<str>,
    /// Test seam (see [`ReconnectRebuildGate`]); compiled only under `test-hooks`.
    #[cfg(feature = "test-hooks")]
    reconnect_rebuild_gate: ReconnectRebuildGate,
    /// Test seam (see [`ReplaySendGate`]); compiled only under `test-hooks`.
    #[cfg(feature = "test-hooks")]
    replay_send_gate: ReplaySendGate,
    /// Test seam (see [`AckAppliedGate`]); compiled only under `test-hooks`.
    #[cfg(feature = "test-hooks")]
    ack_applied_gate: AckAppliedGate,
    /// Test seam (see [`AckIdleGate`]); compiled only under `test-hooks`.
    #[cfg(feature = "test-hooks")]
    ack_idle_gate: AckIdleGate,
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

        let validation_started_at = Instant::now();
        configured_deadline(
            validation_started_at,
            Duration::from_millis(options.recovery_timeout_ms),
            "recovery_timeout_ms",
        )?;
        configured_deadline(
            validation_started_at,
            Duration::from_millis(options.server_lack_of_ack_timeout_ms),
            "server_lack_of_ack_timeout_ms",
        )?;

        let (last_ack_tx, _last_ack_rx) = watch::channel(None);
        let is_closed = Arc::new(AtomicBool::new(false));
        let pending_batches = Arc::new(Mutex::new(Vec::new()));
        let pending_notify = Arc::new(Notify::new());
        let request_send_failure = Arc::new(acks::RequestSendFailure::default());
        let failed_batches = Arc::new(Mutex::new(Vec::new()));
        let recovery_attempts = Arc::new(AtomicU32::new(0));
        let batch_tx = Arc::new(Mutex::new(None));
        let receiver_task = Arc::new(Mutex::new(None));
        let cumulative_records_assigned = Arc::new(AtomicU64::new(0));
        let submitted_records = Arc::new(AtomicU64::new(0));
        let last_acked_records = Arc::new(AtomicU64::new(0));
        let is_paused = Arc::new(AtomicBool::new(false));
        // Capacity mirrors the batch_tx channel so a permit holder always has a slot.
        let inflight = Arc::new(Semaphore::new(options.max_inflight_batches));

        let (server_error_tx, server_error_rx) = watch::channel(None);

        let stream = Self {
            table_properties,
            options,
            batch_tx,
            offset_generator: OffsetIdGenerator::default(),
            last_ack_tx,
            _last_ack_rx,
            is_closed,
            close_teardown_started: AtomicBool::new(false),
            close_flush_error: Mutex::new(None),
            receiver_task,
            pending_batches,
            pending_notify,
            request_send_failure,
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
            cumulative_records_assigned,
            submitted_records,
            last_acked_records,
            is_paused,
            sdk_identifier,
            #[cfg(feature = "test-hooks")]
            reconnect_rebuild_gate: Arc::new(Mutex::new(None)),
            #[cfg(feature = "test-hooks")]
            replay_send_gate: Arc::new(Mutex::new(None)),
            #[cfg(feature = "test-hooks")]
            ack_applied_gate: Arc::new(Mutex::new(None)),
            #[cfg(feature = "test-hooks")]
            ack_idle_gate: Arc::new(Mutex::new(None)),
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

        let connection = match creation {
            Ok(result) => result,
            Err(e) => {
                error!("Arrow Flight stream creation failed after retries: {}", e);
                return Err(e);
            }
        };

        // Store the sender.
        {
            let mut batch_tx = stream.batch_tx.lock().await;
            *batch_tx = Some(connection.sender());
        }

        // Spawn the supervisor task.
        let task = Supervisor::new(&stream).spawn(connection);

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
}

impl ZerobusArrowStream {
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
            pending.push(PendingBatch::new(
                batch.clone(),
                offset_id,
                start_record,
                end_record,
                permit,
            ));
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
                    self.request_send_failure.report();
                    return Ok(offset_id);
                }

                {
                    let mut pending = self.pending_batches.lock().await;
                    pending.retain(|pending_batch| pending_batch.offset_id() != offset_id);
                }
                let _ = timeout(
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
        // Notify after publishing `submitted_records`: waking earlier could let the ACK
        // processor observe the batch as buffered-but-unsent and go idle again.
        self.pending_notify.notify_one();

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

    /// Internal method to wait for a specific offset to be acknowledged.
    /// Used by both `flush()` and `wait_for_offset()`.
    async fn wait_for_offset_internal(
        &self,
        offset_to_wait: OffsetId,
        operation_name: &str,
    ) -> ZerobusResult<()> {
        let flush_timeout = Duration::from_millis(self.options.flush_timeout_ms);
        let mut offset_rx = self.last_ack_tx.subscribe();
        let mut error_rx = self.server_error_rx.clone();

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

                // Only after confirming the target isn't acked, honor terminal/teardown
                // state. Re-read first because the watermark can be published between the
                // read above and observing that state. Otherwise prefer the real terminal
                // error over a generic one.
                if self.is_closed.load(Ordering::Relaxed)
                    || self.close_teardown_started.load(Ordering::Acquire)
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

        timeout(flush_timeout, wait_future).await.map_err(|_| {
            error!(target: LOG_TARGET, "{} timed out", operation_name);
            ZerobusError::StreamClosedError(tonic::Status::deadline_exceeded(format!(
                "{} timed out",
                operation_name
            )))
        })?
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
            // Already closed. If the supervisor closed it on a terminal failure, surface
            // that error rather than reporting success — otherwise the common
            // ingest-then-close() pattern would hide failed batches (retrievable via
            // get_unacked_batches()). A clean prior close() has no stored error.
            if let Some(server_error) = self.server_error_rx.borrow().clone() {
                return Err(server_error);
            }
            if let Some(close_error) = self.close_flush_error.lock().await.clone() {
                return Err(close_error);
            }
            return Ok(());
        }

        info!(
            table_name = %self.table_properties.table_name,
            "Closing Arrow Flight stream"
        );

        // Retain a completed flush result before publishing teardown so retries after
        // teardown starts skip another flush and return the same outcome.
        let flush_result = if close_teardown_started {
            match self.close_flush_error.lock().await.clone() {
                Some(error) => Err(error),
                None => Ok(()),
            }
        } else {
            let result = self.flush().await;
            *self.close_flush_error.lock().await = result.as_ref().err().cloned();
            self.close_teardown_started.store(true, Ordering::Release);
            result
        };
        if let Err(e) = &flush_result {
            warn!(
                "Flush failed during close: {}. Draining pending batches to the failed set.",
                e
            );
        }

        // Reap the supervisor (abort + await) BEFORE clearing the sender, so an in-flight
        // reconnect can't reinstall batch_tx after we clear it, and no ACK processing /
        // reconnect mutates pending_batches or last_acked_records while we drain. Join in
        // place and only clear receiver_task once the join completes, so a close()
        // cancelled during the await doesn't drop the handle — a retry re-joins it.
        {
            let mut task = self.receiver_task.lock().await;
            if let Some(handle) = task.as_mut() {
                handle.abort();
                let _ = handle.await;
            }
            *task = None;
        }

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
        Supervisor::finalize_closed(
            &self.ingest_mutex,
            &self.is_closed,
            &self.pending_batches,
            &self.failed_batches,
            &self.last_acked_records,
        )
        .await;
        self.close_teardown_started.store(false, Ordering::Release);

        flush_result
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
        Supervisor::move_pending_to_failed(
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
    /// releases `proceed` to let recovery finish, or drives a concurrent `close()`
    /// (which reaps the paused supervisor) without releasing it.
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

    /// Test-only: pauses the next recovery after its first replay send and before the
    /// remaining backlog is sent or its pending ACK timestamps are refreshed.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn arm_replay_send_barrier(&self) -> (Arc<Notify>, Arc<Notify>) {
        let reached = Arc::new(Notify::new());
        let proceed = Arc::new(Notify::new());
        *self.replay_send_gate.lock().await = Some(ReplaySendBarrier {
            reached: Arc::clone(&reached),
            proceed: Arc::clone(&proceed),
        });
        (reached, proceed)
    }

    /// Test-only: arms a notify that fires each time the ACK processor applies a non-empty
    /// ack (after storing `last_acked_records`). Lets a test wait until a partial ack has
    /// been processed before proceeding.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn arm_ack_applied_notify(&self) -> Arc<Notify> {
        let notify = Arc::new(Notify::new());
        *self.ack_applied_gate.lock().await = Some(Arc::clone(&notify));
        notify
    }

    /// Test-only: arms a one-shot notification for the next time the ACK processor
    /// enters its no-pending wait with no ACK deadline armed.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn arm_ack_idle_notify(&self) -> Arc<Notify> {
        let notify = Arc::new(Notify::new());
        *self.ack_idle_gate.lock().await = Some(Arc::clone(&notify));
        notify
    }

    /// Test-only: replaces the active batch sender with a sender whose receiver is
    /// already closed, making the next `ingest_batch` exercise its `reserve()` failure.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn replace_batch_sender_with_closed_channel(&self) {
        let _guard = self.ingest_mutex.lock().await;
        let (closed_tx, closed_rx) = mpsc::channel(1);
        drop(closed_rx);
        *self.batch_tx.lock().await = Some(closed_tx);
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
    use std::sync::Arc;

    use super::{ArrowSchema, ArrowTableProperties, DataType, Field};

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
}
