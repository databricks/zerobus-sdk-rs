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

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::{FlightClient, PutResult};
use arrow_ipc::writer::IpcWriteOptions;
use bytes::Bytes;
use futures::{Stream, StreamExt};
#[cfg(feature = "test-hooks")]
use tokio::sync::Notify;
use tokio::sync::{mpsc, watch, Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::time::{sleep, Duration};
use tokio_retry::strategy::FixedInterval;
use tokio_retry::RetryIf;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tracing::{debug, error, info, instrument, warn};

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

/// Test-only gate: when armed, `process_acks` fires the notify right after applying a
/// non-empty ack (i.e. after storing `last_acked_records`), letting a test confirm a
/// partial ack has landed before it proceeds.
#[cfg(feature = "test-hooks")]
type AckAppliedGate = Arc<Mutex<Option<Arc<Notify>>>>;

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
    /// Retains the first flush failure so resumed close calls return the same outcome.
    close_flush_error: Mutex<Option<ZerobusError>>,
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
    /// Test seam (see [`AckAppliedGate`]); compiled only under `test-hooks`.
    #[cfg(feature = "test-hooks")]
    ack_applied_gate: AckAppliedGate,
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
            ack_applied_gate: Arc::new(Mutex::new(None)),
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

        let (response_stream, tx) = match creation {
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
            Arc::clone(&stream.cumulative_records_assigned),
            Arc::clone(&stream.submitted_records),
            Arc::clone(&stream.last_acked_records),
            Arc::clone(&stream.is_paused),
            Arc::clone(&stream.ingest_mutex),
            response_stream,
            Arc::clone(&stream.sdk_identifier),
            #[cfg(feature = "test-hooks")]
            Arc::clone(&stream.reconnect_rebuild_gate),
            #[cfg(feature = "test-hooks")]
            Arc::clone(&stream.ack_applied_gate),
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
    /// Returns the response stream and batch sender on success.
    async fn try_connect(
        endpoint: &str,
        tls_config: &Arc<dyn TlsConfig>,
        connector_factory: Option<&ConnectorFactory>,
        table_properties: &ArrowTableProperties,
        options: &ArrowStreamConfigurationOptions,
        headers_provider: &Arc<dyn HeadersProvider>,
        sdk_identifier: &str,
    ) -> ZerobusResult<(
        Pin<Box<dyn Stream<Item = Result<PutResult, FlightError>> + Send>>,
        mpsc::Sender<Result<RecordBatch, FlightError>>,
    )> {
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

    /// Starts the Flight stream with the given client.
    /// Returns the response stream and batch sender for use by the supervisor.
    ///
    /// This method waits for the server's "ready" signal (ack_up_to_offset = -1)
    /// to confirm that stream setup succeeded (auth, schema validation, table access).
    /// This allows setup errors to be detected during stream creation rather than
    /// later during batch ingestion.
    async fn start_stream_connection(
        mut client: FlightClient,
        table_properties: &ArrowTableProperties,
        options: &ArrowStreamConfigurationOptions,
    ) -> ZerobusResult<(
        Pin<Box<dyn Stream<Item = Result<PutResult, FlightError>> + Send>>,
        mpsc::Sender<Result<RecordBatch, FlightError>>,
    )> {
        // Create channel for sending RecordBatches.
        let (batch_tx, batch_rx) =
            mpsc::channel::<Result<RecordBatch, FlightError>>(options.max_inflight_batches);

        let ipc_write_options = make_ipc_write_options(options.ipc_compression)?;
        let schema = Arc::clone(&table_properties.schema);
        let batch_stream = tokio_stream::wrappers::ReceiverStream::new(batch_rx);

        // Build the Flight data stream. FlightDataEncoderBuilder handles schema
        // framing, dictionary encoding, and automatic batch chunking at 2 MiB.
        // Each non-schema FlightData message gets a sequential wire offset in
        // its app_metadata (index 0 is the schema message; data messages start at 1).
        let offset_counter = Arc::new(std::sync::atomic::AtomicI64::new(0));
        let offset_counter_clone = Arc::clone(&offset_counter);
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .with_options(ipc_write_options)
            .build(batch_stream)
            .enumerate()
            .map(move |(idx, result)| {
                result.map(|mut flight_data| {
                    // Skip schema message (idx 0); add metadata to data messages.
                    if idx > 0 {
                        let offset =
                            offset_counter_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let metadata = FlightBatchMetadata::new(offset);
                        if let Ok(bytes) = metadata.to_bytes() {
                            flight_data.app_metadata = bytes.into();
                        }
                    }
                    flight_data
                })
            });

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

        Ok((response_stream, batch_tx))
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
        cumulative_records_assigned: Arc<AtomicU64>,
        submitted_records: Arc<AtomicU64>,
        last_acked_records: Arc<AtomicU64>,
        is_paused: Arc<AtomicBool>,
        ingest_mutex: Arc<Mutex<()>>,
        initial_response_stream: Pin<Box<dyn Stream<Item = Result<PutResult, FlightError>> + Send>>,
        sdk_identifier: Arc<str>,
        #[cfg(feature = "test-hooks")] reconnect_rebuild_gate: ReconnectRebuildGate,
        #[cfg(feature = "test-hooks")] ack_applied_gate: AckAppliedGate,
    ) -> tokio::task::JoinHandle<ZerobusResult<()>> {
        tokio::spawn(async move {
            let ack_timeout = Duration::from_millis(options.server_lack_of_ack_timeout_ms);
            let mut response_stream = Some(initial_response_stream);
            // Carries a failed reconnect's real error into the next iteration's handling
            // instead of round-tripping a synthetic error through a dummy stream.
            let mut pending_error: Option<ZerobusError> = None;
            // True when `pending_error` is a reconnect auth rejection: the cached token was
            // invalidated and we want to retry (mint a fresh one) even though auth errors
            // classify as non-retryable — while still surfacing the original error if
            // retries are ultimately exhausted.
            let mut reconnect_auth_retry = false;

            loop {
                if is_closed.load(Ordering::Relaxed) {
                    debug!("Supervisor: Stream closed, exiting");
                    return Ok(());
                }

                // Run process_acks until it returns — unless a prior reconnect attempt
                // failed, in which case carry that real error into the handling below
                // (preserving its message and retry classification).
                let result = if let Some(e) = pending_error.take() {
                    Err(e)
                } else {
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
                        &options,
                        #[cfg(feature = "test-hooks")]
                        Arc::clone(&ack_applied_gate),
                    )
                    .await
                };

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
                        if (error.is_retryable() || reconnect_auth_retry) && options.recovery =>
                    {
                        // Retriable error (or a reconnect auth rejection we've chosen to
                        // retry with re-minted credentials) - attempt recovery.
                        reconnect_auth_retry = false;
                        let attempts = recovery_attempts.fetch_add(1, Ordering::Relaxed);
                        if attempts >= options.recovery_retries {
                            error!(
                                attempts = attempts,
                                max_retries = options.recovery_retries,
                                "Supervisor: Max recovery retries exceeded"
                            );
                            // Publish the terminal error before finalization (so a waiter
                            // checking is_closed right after it already sees the real error;
                            // reconnect-failure errors carried via pending_error are never
                            // pre-published in process_acks) and again after (to wake
                            // already-parked waiters). finalize_closed also drains pending
                            // under ingest_mutex so a concurrent ingest can't be omitted.
                            let _ = server_error_tx.send(Some(error.clone()));
                            Self::finalize_closed(
                                &ingest_mutex,
                                &is_closed,
                                &pending_batches,
                                &failed_batches,
                                &last_acked_records,
                            )
                            .await;
                            let _ = server_error_tx.send(Some(error.clone()));
                            return result;
                        }

                        info!(
                            attempt = attempts + 1,
                            max_retries = options.recovery_retries,
                            error = %error,
                            "Supervisor: Attempting recovery after retriable error"
                        );

                        // Atomically pause ingest and detach the sender under
                        // ingest_mutex, so an in-flight ingest_batch either completes
                        // before the pause or observes is_paused and buffers — it never
                        // sees is_paused=false with a detached sender. Successful replay
                        // lifts the gate; failed attempts remain paused for retry/finalization.
                        Self::pause_and_detach_sender(&ingest_mutex, &is_paused, &batch_tx).await;

                        sleep(Duration::from_millis(options.recovery_backoff_ms)).await;

                        let _ = server_error_tx.send(None);

                        // Share one deadline across reconnect and auth-rejection
                        // invalidation so refresh receives only the remaining recovery
                        // timeout instead of starting a second full timeout.
                        let recovery_deadline = tokio::time::Instant::now()
                            + Duration::from_millis(options.recovery_timeout_ms);
                        let reconnect_result = tokio::time::timeout_at(
                            recovery_deadline,
                            Self::reconnect(
                                &endpoint,
                                &tls_config,
                                connector_factory.as_ref(),
                                &table_properties,
                                &options,
                                &headers_provider,
                                &batch_tx,
                                &pending_batches,
                                &cumulative_records_assigned,
                                &submitted_records,
                                &last_acked_records,
                                &sdk_identifier,
                                &ingest_mutex,
                                &is_paused,
                                #[cfg(feature = "test-hooks")]
                                &reconnect_rebuild_gate,
                            ),
                        )
                        .await;

                        match reconnect_result {
                            Ok(Ok(new_response_stream)) => {
                                info!("Supervisor: Recovery successful, resuming");
                                recovery_attempts.store(0, Ordering::Relaxed);
                                // is_paused was already cleared inside reconnect().
                                response_stream = Some(new_response_stream);
                            }
                            Ok(Err(e)) => {
                                warn!("Supervisor: Reconnection failed: {}", e);
                                // Ask the provider to invalidate cached authentication
                                // state after an auth rejection, then retry even though
                                // such errors are otherwise non-retryable. Preserve this
                                // reconnect error if refresh or later recovery cannot proceed.
                                if e.is_auth_rejection() {
                                    match tokio::time::timeout_at(
                                        recovery_deadline,
                                        headers_provider.invalidate(),
                                    )
                                    .await
                                    {
                                        Ok(()) => reconnect_auth_retry = true,
                                        Err(_) => {
                                            warn!(
                                                timeout_ms = options.recovery_timeout_ms,
                                                "Recovery deadline reached while invalidating \
                                                 the headers provider; terminating recovery"
                                            );
                                            // A custom provider must not stall recovery
                                            // indefinitely. Close with the original auth
                                            // rejection; publish before and after
                                            // finalization for waiter race-freedom.
                                            let _ = server_error_tx.send(Some(e.clone()));
                                            Self::finalize_closed(
                                                &ingest_mutex,
                                                &is_closed,
                                                &pending_batches,
                                                &failed_batches,
                                                &last_acked_records,
                                            )
                                            .await;
                                            let _ = server_error_tx.send(Some(e.clone()));
                                            return Err(e);
                                        }
                                    }
                                }
                                pending_error = Some(e);
                            }
                            Err(_timeout) => {
                                warn!("Supervisor: Reconnection timed out");
                                pending_error = Some(ZerobusError::ConnectionTimeout(format!(
                                    "Reconnection timed out after {}ms",
                                    options.recovery_timeout_ms
                                )));
                            }
                        }
                    }
                    Err(error) => {
                        error!("Supervisor: Non-retriable error, closing stream: {}", error);
                        // Publish the terminal error before finalization (so a waiter
                        // checking is_closed right after it already sees the real error;
                        // reconnect-failure errors carried via pending_error are never
                        // pre-published in process_acks) and again after (to wake
                        // already-parked waiters). finalize_closed drains pending under
                        // ingest_mutex so a concurrent ingest can't be omitted.
                        let _ = server_error_tx.send(Some(error.clone()));
                        Self::finalize_closed(
                            &ingest_mutex,
                            &is_closed,
                            &pending_batches,
                            &failed_batches,
                            &last_acked_records,
                        )
                        .await;
                        let _ = server_error_tx.send(Some(error.clone()));
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
        cumulative_records_assigned: &Arc<AtomicU64>,
        submitted_records: &Arc<AtomicU64>,
        last_acked_records: &Arc<AtomicU64>,
        sdk_identifier: &str,
        ingest_mutex: &Arc<Mutex<()>>,
        is_paused: &Arc<AtomicBool>,
        #[cfg(feature = "test-hooks")] reconnect_rebuild_gate: &ReconnectRebuildGate,
    ) -> ZerobusResult<Pin<Box<dyn Stream<Item = Result<PutResult, FlightError>> + Send>>> {
        // Create new client.
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

        // Create new channel.
        let (tx, batch_rx) =
            mpsc::channel::<Result<RecordBatch, FlightError>>(options.max_inflight_batches);

        let ipc_write_options = make_ipc_write_options(options.ipc_compression)?;
        let schema = Arc::clone(&table_properties.schema);
        let batch_stream = tokio_stream::wrappers::ReceiverStream::new(batch_rx);

        let offset_counter = Arc::new(std::sync::atomic::AtomicI64::new(0));
        let offset_counter_clone = Arc::clone(&offset_counter);
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .with_options(ipc_write_options)
            .build(batch_stream)
            .enumerate()
            .map(move |(idx, result)| {
                result.map(|mut flight_data| {
                    if idx > 0 {
                        let offset =
                            offset_counter_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let metadata = FlightBatchMetadata::new(offset);
                        if let Ok(bytes) = metadata.to_bytes() {
                            flight_data.app_metadata = bytes.into();
                        }
                    }
                    flight_data
                })
            });

        // Start the DoPut stream.
        let mut flight_client = client;
        let mut response_stream = flight_client
            .do_put(flight_data_stream)
            .await
            // `.into()` preserves the inner gRPC code; `Status::from_error` would
            // flatten it to `Unknown` and break auth/retry classification.
            .map_err(|e| ZerobusError::CreateStreamError(e.into()))?;

        // Wait for server's "ready" signal to confirm reconnection succeeded.
        let setup_timeout = Duration::from_millis(options.connection_timeout_ms);
        match tokio::time::timeout(setup_timeout, response_stream.next()).await {
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
                return Err(ZerobusError::ConnectionTimeout(format!(
                    "Timed out waiting for server reconnect confirmation ({}ms)",
                    options.connection_timeout_ms
                )));
            }
        }

        // Store the new sender before taking ingest_mutex for the rebuild. Safe only
        // because is_paused stays true until after replay: a concurrent ingest that
        // observes this new sender still buffers rather than sending out of order.
        {
            let mut tx_guard = batch_tx.lock().await;
            *tx_guard = Some(tx.clone());
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
                barrier.proceed.notified().await;
            }
        }

        // Hold ingest_mutex across the replay so no concurrent ingest interleaves.
        let _ingest_guard = ingest_mutex.lock().await;
        Self::replay_pending_batches(
            &tx,
            pending_batches,
            cumulative_records_assigned,
            submitted_records,
            last_acked_records,
            acked_before_disconnect,
        )
        .await?;

        // Clear the pause gate while still holding ingest_mutex.
        is_paused.store(false, Ordering::Relaxed);

        Ok(response_stream)
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

    /// Advances the monotonic record watermark and removes fully acknowledged batches.
    async fn apply_acknowledgment(
        ack: &FlightAckMetadata,
        submitted_records: &AtomicU64,
        last_acked_records: &AtomicU64,
        pending_batches: &Mutex<Vec<PendingBatch>>,
        last_ack_tx: &watch::Sender<Option<OffsetId>>,
        #[cfg(feature = "test-hooks")] ack_applied_gate: &AckAppliedGate,
    ) -> ZerobusResult<()> {
        let acked_records = ack.ack_up_to_records;
        // `ack_up_to_records` is the durability boundary. Derive completed SDK offsets
        // from local pending ranges so an inconsistent `ack_up_to_offset` cannot advance
        // a waiter; keep the server-provided offset only for diagnostics.
        let (effective_acked_records, max_acked_offset) = {
            // Ingest publishes submitted_records and commits to the active sender while
            // holding this same lock. Validation therefore cannot observe a submitted
            // watermark before its handoff, or a handoff before its watermark.
            let mut pending = pending_batches.lock().await;
            let submitted_records = submitted_records.load(Ordering::Acquire);
            if acked_records > submitted_records {
                return Err(ZerobusError::InvalidStateError(format!(
                    "Acknowledgement claims {acked_records} records, but only {submitted_records} records were submitted"
                )));
            }

            let previous_acked_records =
                last_acked_records.fetch_max(acked_records, Ordering::AcqRel);
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

        #[cfg(feature = "test-hooks")]
        if acked_records > 0 {
            if let Some(notify) = ack_applied_gate.lock().await.as_ref() {
                notify.notify_one();
            }
        }

        if let Some(offset) = max_acked_offset {
            let _ = last_ack_tx.send(Some(offset));
        }

        Ok(())
    }

    /// Processes acknowledgments from the server response stream.
    ///
    /// Uses record-based tracking: the server sends `ack_up_to_records` indicating
    /// the cumulative number of records durably stored. We match this against
    /// pending batches' record ranges to determine which batches are fully acked.
    /// This correctly handles batches that were split into multiple Flight chunks
    /// by `FlightDataEncoderBuilder`.
    #[allow(clippy::too_many_arguments)]
    async fn process_acks(
        mut response_stream: Pin<Box<dyn Stream<Item = Result<PutResult, FlightError>> + Send>>,
        is_closed: Arc<AtomicBool>,
        last_ack_tx: tokio::sync::watch::Sender<Option<OffsetId>>,
        pending_batches: Arc<Mutex<Vec<PendingBatch>>>,
        ack_timeout: Duration,
        server_error_tx: watch::Sender<Option<ZerobusError>>,
        submitted_records: Arc<AtomicU64>,
        last_acked_records: Arc<AtomicU64>,
        is_paused: Arc<AtomicBool>,
        options: &ArrowStreamConfigurationOptions,
        #[cfg(feature = "test-hooks")] ack_applied_gate: AckAppliedGate,
    ) -> ZerobusResult<()> {
        let mut pause_deadline: Option<tokio::time::Instant> = None;

        loop {
            if is_closed.load(Ordering::Relaxed) {
                debug!("Stream closed, stopping ack processor");
                return Ok(());
            }

            // Check pause state: exit when deadline reached or all batches acked.
            // Returns a retriable error to trigger recovery in the supervisor.
            if let Some(deadline) = pause_deadline {
                let now = tokio::time::Instant::now();
                let all_acked = pending_batches.lock().await.is_empty();

                if now >= deadline {
                    info!("Graceful close timeout reached. Triggering recovery.");
                    return Err(ZerobusError::StreamClosedError(tonic::Status::unavailable(
                        "Graceful close timeout reached",
                    )));
                } else if all_acked {
                    info!("All in-flight batches acknowledged during graceful close. Triggering recovery.");
                    return Err(ZerobusError::StreamClosedError(tonic::Status::unavailable(
                        "All in-flight batches acked during graceful close",
                    )));
                }
            }

            // TODO: Anchor this timeout to the oldest pending batch. The current
            // per-response timeout can spend most of its window while idle and restarts
            // whenever any response arrives, even if it makes no ack progress.
            let result = if let Some(deadline) = pause_deadline {
                // TODO: Race graceful close against the active ack deadline and
                // preserve the earliest close deadline when repeated signals arrive.
                tokio::select! {
                    biased;
                    _ = tokio::time::sleep_until(deadline) => {
                        continue;
                    }
                    res = tokio::time::timeout(ack_timeout, response_stream.next()) => res,
                }
            } else {
                tokio::time::timeout(ack_timeout, response_stream.next()).await
            };

            match result {
                Ok(Some(Ok(put_result))) => {
                    match FlightAckMetadata::from_bytes(&put_result.app_metadata) {
                        Ok(ack) => {
                            // Handle close stream signal.
                            if ack.is_close_signal() {
                                if options.recovery {
                                    let server_duration_ms =
                                        ack.close_stream_duration_ms.unwrap_or(0);

                                    let wait_duration_ms = match options
                                        .stream_paused_max_wait_time_ms
                                    {
                                        None => server_duration_ms,
                                        Some(0) => {
                                            info!(
                                                    "Server will close the stream in {}ms. Triggering stream recovery.",
                                                    server_duration_ms
                                                );
                                            return Err(ZerobusError::StreamClosedError(
                                                tonic::Status::unavailable(
                                                    "Immediate recovery on close signal",
                                                ),
                                            ));
                                        }
                                        Some(max_wait) => {
                                            std::cmp::min(max_wait, server_duration_ms)
                                        }
                                    };

                                    if wait_duration_ms == 0 {
                                        info!("Server will close the stream. Triggering immediate recovery.");
                                        return Err(ZerobusError::StreamClosedError(
                                            tonic::Status::unavailable(
                                                "Immediate recovery on close signal",
                                            ),
                                        ));
                                    }

                                    is_paused.store(true, Ordering::Relaxed);
                                    pause_deadline = Some(
                                        tokio::time::Instant::now()
                                            + Duration::from_millis(wait_duration_ms),
                                    );
                                    info!(
                                        "Server will close the stream in {}ms. Entering graceful close period (waiting up to {}ms for in-flight acks).",
                                        server_duration_ms, wait_duration_ms
                                    );
                                }
                                // Process any ack data that came with the close signal.
                                // Fall through to ack processing below only if there's
                                // meaningful ack data (non-zero records count).
                                if ack.ack_up_to_records == 0 {
                                    continue;
                                }
                            }

                            Self::apply_acknowledgment(
                                &ack,
                                &submitted_records,
                                &last_acked_records,
                                &pending_batches,
                                &last_ack_tx,
                                #[cfg(feature = "test-hooks")]
                                &ack_applied_gate,
                            )
                            .await?;
                        }
                        Err(e) => {
                            warn!("Failed to parse ack metadata: {}", e);
                        }
                    }
                }
                Ok(Some(Err(e))) => {
                    // A stream error while paused ends the graceful-close wait and
                    // triggers recovery.
                    if pause_deadline.is_some() {
                        info!(
                            "Stream error during graceful close period, triggering recovery: {}",
                            e
                        );
                        return Err(ZerobusError::StreamClosedError(tonic::Status::unavailable(
                            "Stream error during graceful close",
                        )));
                    }
                    error!("Flight stream error: {}", e);
                    let status: tonic::Status = e.into();
                    let error = ZerobusError::StreamClosedError(status);
                    let _ = server_error_tx.send(Some(error.clone()));
                    return Err(error);
                }
                Ok(None) => {
                    // During graceful close, stream end is expected.
                    // Return retriable error to trigger recovery.
                    if pause_deadline.is_some() {
                        info!("Server closed stream during graceful close period, triggering recovery.");
                        return Err(ZerobusError::StreamClosedError(tonic::Status::unavailable(
                            "Server closed stream during graceful close",
                        )));
                    }
                    debug!("Server closed the stream");
                    let error = ZerobusError::StreamClosedError(tonic::Status::unknown(
                        "Server closed the stream",
                    ));
                    // Returned to the supervisor, which publishes it (before + after
                    // finalization) in its terminal branch.
                    return Err(error);
                }
                Err(_timeout) => {
                    // During graceful close, ack timeout is not an error.
                    if pause_deadline.is_some() {
                        continue;
                    }
                    // Check if there are pending acks that should have been received.
                    let pending = pending_batches.lock().await;
                    if !pending.is_empty() {
                        error!(
                            pending_count = pending.len(),
                            "Server ack timeout with pending batches"
                        );
                        let error = ZerobusError::StreamClosedError(
                            tonic::Status::deadline_exceeded("Server ack timeout"),
                        );
                        // Returned to the supervisor, which publishes it (before + after
                        // finalization) in its terminal branch.
                        return Err(error);
                    }
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
        // reconnect can't reinstall batch_tx after we clear it, and no process_acks /
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
        Self::finalize_closed(
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

    /// Test-only: arms a notify that fires each time `process_acks` applies a non-empty
    /// ack (after storing `last_acked_records`). Lets a test wait until a partial ack has
    /// been processed before proceeding.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn arm_ack_applied_notify(&self) -> Arc<Notify> {
        let notify = Arc::new(Notify::new());
        *self.ack_applied_gate.lock().await = Some(Arc::clone(&notify));
        notify
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
            Arc::new(AtomicBool::new(false)),
            &ArrowStreamConfigurationOptions::default(),
            #[cfg(feature = "test-hooks")]
            Arc::new(Mutex::new(None)),
        )
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
        assert_eq!((pending[0].start_record, pending[0].end_record), (0, 10));
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
            &ArrowStreamConfigurationOptions::default(),
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
            &ArrowStreamConfigurationOptions::default(),
            #[cfg(feature = "test-hooks")]
            Arc::new(Mutex::new(None)),
        )
        .await;

        let acked_before_disconnect = last_acked_records.load(Ordering::Acquire);
        assert_eq!(acked_before_disconnect, 5);
        let cumulative_records_assigned = Arc::new(AtomicU64::new(10));
        let submitted_records = Arc::new(AtomicU64::new(10));
        let (tx, mut rx) = mpsc::channel::<Result<RecordBatch, FlightError>>(1);
        ZerobusArrowStream::replay_pending_batches(
            &tx,
            &pending_batches,
            &cumulative_records_assigned,
            &submitted_records,
            &last_acked_records,
            acked_before_disconnect,
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

        let res = ZerobusArrowStream::replay_pending_batches(
            &tx,
            &pending,
            &cumulative,
            &submitted,
            &last_acked,
            0,
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

        let res = ZerobusArrowStream::replay_pending_batches(
            &tx,
            &pending,
            &cumulative,
            &submitted,
            &last_acked,
            0,
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

        let res = ZerobusArrowStream::replay_pending_batches(
            &tx,
            &pending,
            &cumulative,
            &submitted,
            &last_acked,
            4,
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
