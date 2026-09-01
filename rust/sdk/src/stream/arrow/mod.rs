//! Arrow Flight stream API for high-performance Arrow data ingestion.
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
use tokio::time::{timeout, Duration, Instant};
use tokio_retry::strategy::FixedInterval;
use tokio_retry::RetryIf;
use tracing::{debug, error, info, instrument, warn};

pub use arrow_array::RecordBatch;
pub use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};

use self::batch::{materialize_ipc, PendingBatch};
use self::close::{CloseCoordinator, CloseFinalizer, CloseRequest, CloseState};
use self::connection::RequestBodyRegistry;
pub use self::options::ArrowStreamConfigurationOptions;
use self::supervisor::{Supervisor, SupervisorTaskHandle};
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
mod close;
mod connection;
mod metadata;
mod options;
mod supervisor;

const LOG_TARGET: &str = module_path!();

type BatchSender = Arc<Mutex<Option<mpsc::Sender<Result<RecordBatch, FlightError>>>>>;

struct FlightConnectionParameters<'a> {
    endpoint: &'a str,
    tls_config: &'a Arc<dyn TlsConfig>,
    connector_factory: Option<&'a ConnectorFactory>,
    table_properties: &'a ArrowTableProperties,
    options: &'a ArrowStreamConfigurationOptions,
    headers_provider: &'a Arc<dyn HeadersProvider>,
    sdk_identifier: &'a str,
    request_bodies: &'a RequestBodyRegistry,
    #[cfg(feature = "test-hooks")]
    test_hooks: &'a Arc<TestHooks>,
}

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

#[cfg(feature = "test-hooks")]
type TestBarrierGate = Mutex<Option<TestBarrier>>;

#[cfg(feature = "test-hooks")]
#[derive(Clone)]
struct TestBarrier {
    reached: Arc<Notify>,
    proceed: Arc<Notify>,
}

#[cfg(feature = "test-hooks")]
type TestNotifyGate = Mutex<Option<Arc<Notify>>>;

#[cfg(feature = "test-hooks")]
#[derive(Default)]
struct TestHooks {
    reconnect_rebuild: TestBarrierGate,
    replay_send: TestBarrierGate,
    ack_applied: TestNotifyGate,
    ack_idle: TestNotifyGate,
    failed_enqueue: TestBarrierGate,
    close_finalize: TestBarrierGate,
    request_body_before_batch_poll: TestBarrierGate,
    request_body_shutdown: TestBarrierGate,
    retained_batches_cleared: TestNotifyGate,
    free_shutdown_complete: TestBarrierGate,
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
    /// Rejects new ingests once terminal finalization or a failed enqueue owns admission.
    admission_closed: Arc<AtomicBool>,
    /// Coordinates one resumable explicit-close request with the recovery supervisor.
    close: CloseCoordinator,
    /// Supervisor worker and reaper ownership, consumed once by terminal shutdown.
    supervisor_task: Arc<Mutex<Option<SupervisorTaskHandle>>>,
    /// Once true, FFI destruction must wait until all foreign C Data owners are released.
    #[cfg(feature = "internal-arrow-c-data")]
    has_ingested_c_data: AtomicBool,
    /// Tracks every tonic-owned Flight request body until its queued owners are dropped.
    request_bodies: RequestBodyRegistry,
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
    #[cfg(feature = "test-hooks")]
    test_hooks: Arc<TestHooks>,
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
        configured_deadline(
            validation_started_at,
            Duration::from_millis(options.flush_timeout_ms),
            "flush_timeout_ms",
        )?;

        let (last_ack_tx, _last_ack_rx) = watch::channel(None);
        let is_closed = Arc::new(AtomicBool::new(false));
        let admission_closed = Arc::new(AtomicBool::new(false));
        let pending_batches = Arc::new(Mutex::new(Vec::new()));
        let pending_notify = Arc::new(Notify::new());
        let request_send_failure = Arc::new(acks::RequestSendFailure::default());
        let failed_batches = Arc::new(Mutex::new(Vec::new()));
        let recovery_attempts = Arc::new(AtomicU32::new(0));
        let batch_tx = Arc::new(Mutex::new(None));
        let supervisor_task = Arc::new(Mutex::new(None));
        let request_bodies = RequestBodyRegistry::default();
        let cumulative_records_assigned = Arc::new(AtomicU64::new(0));
        let submitted_records = Arc::new(AtomicU64::new(0));
        let last_acked_records = Arc::new(AtomicU64::new(0));
        let is_paused = Arc::new(AtomicBool::new(false));
        // Capacity mirrors the batch_tx channel so a permit holder always has a slot.
        let inflight = Arc::new(Semaphore::new(options.max_inflight_batches));

        let (server_error_tx, server_error_rx) = watch::channel(None);
        let close = CloseCoordinator::new();

        let stream = Self {
            table_properties,
            options,
            batch_tx,
            offset_generator: OffsetIdGenerator::default(),
            last_ack_tx,
            _last_ack_rx,
            is_closed,
            admission_closed,
            close,
            supervisor_task,
            #[cfg(feature = "internal-arrow-c-data")]
            has_ingested_c_data: AtomicBool::new(false),
            request_bodies,
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
            test_hooks: Arc::new(TestHooks::default()),
        };

        // Initialize the connection with retry logic.
        let endpoint = stream.endpoint.clone();
        let tls_config = Arc::clone(&stream.tls_config);
        let connector_factory = stream.connector_factory.clone();
        let table_properties = stream.table_properties.clone();
        let options = stream.options.clone();
        let headers_provider = Arc::clone(&stream.headers_provider);
        let request_bodies = stream.request_bodies.clone();
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
            let request_bodies = request_bodies.clone();
            #[cfg(feature = "test-hooks")]
            let test_hooks = Arc::clone(&stream.test_hooks);

            async move {
                let parameters = FlightConnectionParameters {
                    endpoint: &endpoint,
                    tls_config: &tls_config,
                    connector_factory: connector_factory.as_ref(),
                    table_properties: &table_properties,
                    options: &options,
                    headers_provider: &headers_provider,
                    sdk_identifier: &sdk_identifier,
                    request_bodies: &request_bodies,
                    #[cfg(feature = "test-hooks")]
                    test_hooks: &test_hooks,
                };
                Self::try_connect(&parameters).await
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
            let mut supervisor_task = stream.supervisor_task.lock().await;
            *supervisor_task = Some(task);
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
    /// * The terminal request-stream error if enqueueing fails with recovery disabled;
    ///   the call waits for terminal finalization before returning that error
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
        if self.is_ingest_admission_closed() {
            return Err(Self::stream_closing_or_closed_error());
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
        if self.is_ingest_admission_closed() {
            return Err(Self::stream_closing_or_closed_error());
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
                return Err(Self::terminal_error_or(&self.server_error_rx, || {
                    "Stream sender is closed".to_string()
                }));
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
                // Withdraw the logical assignment before waking terminal finalization.
                self.cumulative_records_assigned
                    .store(start_record, Ordering::Relaxed);
                self.offset_generator.set_next(offset_id);
                // Claim terminal admission while close is still excluded by ingest_mutex,
                // so the request-send failure cannot be replaced by a successful close.
                self.admission_closed.store(true, Ordering::Release);
                #[cfg(feature = "test-hooks")]
                {
                    let barrier = self.test_hooks.failed_enqueue.lock().await.take();
                    if let Some(barrier) = barrier {
                        barrier.reached.notify_one();
                        barrier.proceed.notified().await;
                    }
                }
                // Finalization must reacquire ingest_mutex before publishing its outcome.
                drop(_guard);
                self.request_send_failure.report();
                return match self.wait_for_terminal_outcome().await {
                    Err(error) => Err(error),
                    // A clean outcome is unreachable after this path claims terminal
                    // admission, but preserve any published cause defensively.
                    Ok(()) => Err(Self::terminal_error_or(&self.server_error_rx, || {
                        "Failed to send batch".to_string()
                    })),
                };
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
        if self.is_ingest_admission_closed() {
            return Err(Self::stream_closing_or_closed_error());
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
                if self.is_closed.load(Ordering::Relaxed) || self.close.has_started() {
                    if let Some(ack_offset) = *offset_rx.borrow_and_update() {
                        if ack_offset >= offset_to_wait {
                            return Ok(());
                        }
                    }
                    return Err(Self::terminal_error_or(&error_rx, || {
                        format!(
                            "Stream closing or closed during {}",
                            operation_name.to_lowercase()
                        )
                    }));
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

    /// Waits after terminal admission is claimed but before finalization publishes its result.
    async fn wait_for_terminal_outcome(&self) -> ZerobusResult<()> {
        let mut close_rx = self.close.subscribe();

        loop {
            if let CloseState::Finalized(result) = close_rx.borrow_and_update().clone() {
                return result;
            }

            if close_rx.changed().await.is_err() {
                return Err(Self::close_coordinator_stopped_error());
            }
        }
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
        // Serialize the snapshot with enqueue assignment and rollback so a concurrent
        // failed enqueue cannot leave this flush waiting on its withdrawn offset.
        let target_offset = {
            let _guard = self.ingest_mutex.lock().await;
            self.offset_generator.last()
        };
        let target_offset = match target_offset {
            Some(offset) => offset,
            None => {
                if self.admission_closed.load(Ordering::Acquire)
                    && matches!(self.close.state(), CloseState::Open)
                {
                    return self.wait_for_terminal_outcome().await;
                }
                // Nothing was ingested: report closure if closed, otherwise nothing to do.
                // Prefer the real terminal error over a generic closed message.
                if self.is_closed.load(Ordering::Relaxed) || self.close.has_started() {
                    return Err(Self::terminal_error_or(&self.server_error_rx, || {
                        "Cannot flush: stream is closing or closed".to_string()
                    }));
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
    /// The first call publishes one close request. While the active transport remains
    /// usable, the supervisor continues ACK processing through the original flush deadline,
    /// then owns transport cleanup and finalization. Close does not start or continue
    /// recovery: a transport failure or an already-running recovery is interrupted, and
    /// unacknowledged batches are retained for retrieval. Repeated calls await the same
    /// request and result. An uncommitted replacement transport is dropped best-effort.
    ///
    /// # Returns
    ///
    /// `Ok(())` after clean teardown.
    ///
    /// # Errors
    ///
    /// Returns a background terminal error or a timeout if the close target is not
    /// acknowledged by the flush deadline. During ordinary active-connection close, a
    /// timely target acknowledgment takes precedence. If close interrupts an already-active
    /// server rotation or an uncommitted recovery attempt, it instead returns that attempt's
    /// trigger even when the close target is durable. Teardown still completes; use
    /// `get_unacked_batches()` to retrieve unacknowledged batches.
    ///
    /// # Cancellation safety
    ///
    /// Once the close request is published, further ingests are rejected. Cancelling the
    /// future does not cancel that request: call `close()` again to await the same original
    /// deadline and final outcome.
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
        self.close_internal().await
    }

    async fn close_internal(&self) -> ZerobusResult<()> {
        info!(
            table_name = %self.table_properties.table_name,
            "Closing Arrow Flight stream"
        );
        let mut close_rx = self.close.subscribe();

        loop {
            let state = { close_rx.borrow_and_update().clone() };
            match state {
                CloseState::Open => {
                    // This mutex makes the target snapshot and request publication atomic
                    // with ingest admission and replacement-sender publication.
                    let guard = self.ingest_mutex.lock().await;
                    match self.close.state() {
                        CloseState::Open if !self.admission_closed.load(Ordering::Acquire) => {
                            let deadline = configured_deadline(
                                Instant::now(),
                                Duration::from_millis(self.options.flush_timeout_ms),
                                "flush_timeout_ms",
                            )?;
                            let request = CloseRequest {
                                target_offset: self.offset_generator.last(),
                                deadline,
                            };
                            self.close.publish(request);
                        }
                        CloseState::Open => {
                            // A failed enqueue or terminal finalization owns admission; both
                            // complete by publishing one shared terminal result.
                            drop(guard);
                            if close_rx.changed().await.is_err() {
                                return Err(Self::close_coordinator_stopped_error());
                            }
                        }
                        CloseState::Requested(_) => {}
                        CloseState::Finalized(result) => return result,
                    }
                }
                CloseState::Requested(_) => {
                    if close_rx.changed().await.is_err() {
                        return Err(Self::close_coordinator_stopped_error());
                    }
                }
                CloseState::Finalized(result) => return result,
            }
        }
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
    /// * `InvalidStateError` - If closure has not been finalized; call `close()` first,
    ///   or call it again to await a previously requested close.
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
        CloseFinalizer::move_pending_to_failed(
            &self.pending_batches,
            &self.failed_batches,
            &self.last_acked_records,
        )
        .await;
        Ok(self.failed_batches.lock().await.clone())
    }

    #[cfg(feature = "internal-arrow-c-data")]
    pub(crate) fn mark_c_data_ingested(&self) {
        self.has_ingested_c_data.store(true, Ordering::Release);
    }

    #[cfg(feature = "internal-arrow-c-data")]
    pub(crate) fn has_ingested_c_data(&self) -> bool {
        self.has_ingested_c_data.load(Ordering::Acquire)
    }

    #[cfg(feature = "internal-arrow-c-data")]
    pub(crate) async fn abort_and_wait(&self) {
        self.admission_closed.store(true, Ordering::Release);
        self.request_bodies.shutdown_all().await;

        let task = self.supervisor_task.lock().await.take();
        if let Some(task) = task {
            task.abort_and_wait().await;
        }

        // A reconnect can register after the first snapshot but not after worker termination.
        self.request_bodies.shutdown_all().await;

        let (pending_batches, failed_batches) = {
            let mut failed = self.failed_batches.lock().await;
            let mut pending = self.pending_batches.lock().await;
            (std::mem::take(&mut *pending), std::mem::take(&mut *failed))
        };
        drop((pending_batches, failed_batches));

        #[cfg(feature = "test-hooks")]
        if let Some(notify) = self.test_hooks.retained_batches_cleared.lock().await.take() {
            notify.notify_one();
        }

        self.request_bodies.wait_for_all_eof().await;

        #[cfg(feature = "test-hooks")]
        {
            let barrier = self.test_hooks.free_shutdown_complete.lock().await.take();
            if let Some(barrier) = barrier {
                barrier.reached.notify_one();
                barrier.proceed.notified().await;
            }
        }
    }

    /// Returns true once supervisor-owned terminal finalization publishes closure.
    pub fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::Relaxed)
    }

    #[cfg(feature = "test-hooks")]
    async fn arm_test_barrier(gate: &TestBarrierGate) -> (Arc<Notify>, Arc<Notify>) {
        let reached = Arc::new(Notify::new());
        let proceed = Arc::new(Notify::new());
        *gate.lock().await = Some(TestBarrier {
            reached: Arc::clone(&reached),
            proceed: Arc::clone(&proceed),
        });
        (reached, proceed)
    }

    #[cfg(feature = "test-hooks")]
    async fn arm_test_notify(gate: &TestNotifyGate) -> Arc<Notify> {
        let notify = Arc::new(Notify::new());
        *gate.lock().await = Some(Arc::clone(&notify));
        notify
    }

    /// Test-only: arms the reconnect rebuild barrier. The next `reconnect` pauses after
    /// establishing the connection but before rebuilding pending ranges/watermark,
    /// firing the returned `reached` notify, then waits on `proceed`. Cancellation drops
    /// the uncommitted replacement transport best-effort.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn arm_reconnect_rebuild_barrier(&self) -> (Arc<Notify>, Arc<Notify>) {
        Self::arm_test_barrier(&self.test_hooks.reconnect_rebuild).await
    }

    /// Test-only: pauses the next recovery after its first replay handoff and before the
    /// remaining backlog or sender publication can be committed.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn arm_replay_send_barrier(&self) -> (Arc<Notify>, Arc<Notify>) {
        Self::arm_test_barrier(&self.test_hooks.replay_send).await
    }

    /// Test-only: arms a notify that fires each time the ACK processor applies a non-empty
    /// ack (after storing `last_acked_records`). Lets a test wait until a partial ack has
    /// been processed before proceeding.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn arm_ack_applied_notify(&self) -> Arc<Notify> {
        Self::arm_test_notify(&self.test_hooks.ack_applied).await
    }

    /// Test-only: arms a one-shot notification for the next time the ACK processor
    /// enters its no-pending wait with no ACK deadline armed.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn arm_ack_idle_notify(&self) -> Arc<Notify> {
        Self::arm_test_notify(&self.test_hooks.ack_idle).await
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

    /// Test-only: parks a recovery-disabled failed enqueue after it claims terminal
    /// admission and before it releases `ingest_mutex` or wakes the supervisor.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn arm_failed_enqueue_barrier(&self) -> (Arc<Notify>, Arc<Notify>) {
        Self::arm_test_barrier(&self.test_hooks.failed_enqueue).await
    }

    /// Test-only: reports whether terminal admission has been claimed.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn admission_closed_for_test(&self) -> bool {
        self.admission_closed.load(Ordering::Acquire)
    }

    /// Test-only: runs the close state machine through a shared reference so tests can
    /// exercise foreign-wrapper concurrency without creating aliased Rust references.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn close_concurrently_for_test(&self) -> ZerobusResult<()> {
        self.close_internal().await
    }

    /// Test-only: parks close finalization after choosing the local outcome and before
    /// moving pending batches into the final failed-batch snapshot.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn arm_close_finalize_barrier(&self) -> (Arc<Notify>, Arc<Notify>) {
        Self::arm_test_barrier(&self.test_hooks.close_finalize).await
    }

    /// Test-only: parks the active request body after it observes forced shutdown but before
    /// it reports EOF or drops transport-owned batches.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn arm_request_body_shutdown_barrier(&self) -> (Arc<Notify>, Arc<Notify>) {
        Self::arm_test_barrier(&self.test_hooks.request_body_shutdown).await
    }

    /// Test-only: parks the active request body before it polls a newly queued batch.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn arm_request_body_before_batch_poll_barrier(&self) -> (Arc<Notify>, Arc<Notify>) {
        Self::arm_test_barrier(&self.test_hooks.request_body_before_batch_poll).await
    }

    /// Test-only: notifies after destructive free clears SDK-retained batch collections.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn arm_retained_batches_cleared_notify(&self) -> Arc<Notify> {
        Self::arm_test_notify(&self.test_hooks.retained_batches_cleared).await
    }

    /// Test-only: marks and retains a foreign-owned batch for destructive-free tests.
    #[cfg(all(feature = "test-hooks", feature = "internal-arrow-c-data"))]
    #[doc(hidden)]
    pub async fn retain_failed_batch_for_test(&self, batch: RecordBatch) {
        self.mark_c_data_ingested();
        self.failed_batches.lock().await.push(batch);
    }

    /// Test-only: reports whether destructive-free batch collection locks are available.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn retained_batch_locks_available_for_test(&self) -> bool {
        let Ok(_failed) = self.failed_batches.try_lock() else {
            return false;
        };
        self.pending_batches.try_lock().is_ok()
    }

    /// Test-only: parks destructive free after all request bodies and retained batches finish.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn arm_free_shutdown_complete_barrier(&self) -> (Arc<Notify>, Arc<Notify>) {
        Self::arm_test_barrier(&self.test_hooks.free_shutdown_complete).await
    }

    /// Test-only: aborts the supervisor worker while leaving its finalizer reaper running.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn abort_supervisor_for_test(&self) {
        if let Some(task) = self.supervisor_task.lock().await.as_ref() {
            task.abort();
        }
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

    fn is_ingest_admission_closed(&self) -> bool {
        self.admission_closed.load(Ordering::Acquire)
            || self.is_closed.load(Ordering::Relaxed)
            || self.close.has_started()
    }

    fn stream_closing_or_closed_error() -> ZerobusError {
        ZerobusError::StreamClosedError(tonic::Status::internal("Stream is closing or closed"))
    }

    fn close_coordinator_stopped_error() -> ZerobusError {
        ZerobusError::StreamClosedError(tonic::Status::internal(
            "Close coordinator stopped unexpectedly",
        ))
    }

    fn terminal_error_or(
        error_rx: &watch::Receiver<Option<ZerobusError>>,
        fallback_message: impl FnOnce() -> String,
    ) -> ZerobusError {
        let terminal_error = error_rx.borrow().clone();
        terminal_error.unwrap_or_else(|| {
            ZerobusError::StreamClosedError(tonic::Status::internal(fallback_message()))
        })
    }
}

impl Drop for ZerobusArrowStream {
    fn drop(&mut self) {
        self.admission_closed.store(true, Ordering::Release);
        self.is_closed.store(true, Ordering::Relaxed);
        self.request_bodies.try_shutdown_all();
        // Best-effort abort the supervisor. Drop does not preserve pending batches for
        // retrieval; call close() or let recovery reach terminal finalization first.
        if let Ok(mut guard) = self.supervisor_task.try_lock() {
            if let Some(task) = guard.take() {
                task.abort();
                // Dropping the reaper JoinHandle intentionally detaches ordinary Drop cleanup.
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
