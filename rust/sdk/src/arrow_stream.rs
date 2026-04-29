//! Arrow Flight stream implementation for high-performance Arrow data ingestion.
//!
//! **Experimental/Unsupported**: This module is experimental and not yet supported
//! for production use. The API may change in future releases.
//!
//! This module provides `ZerobusArrowStream`, a client for ingesting Arrow `RecordBatch`
//! data into Databricks Delta tables using the Arrow Flight protocol.
//! Native Rust callers use `ingest_batch` with `RecordBatch` values; FFI callers
//! (Go, Python, Java, TypeScript) can use `ingest_ipc_batch` with pre-serialised
//! Arrow IPC bytes to avoid an extra deserialisation round-trip.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use arrow_flight::error::FlightError;
use arrow_flight::{FlightClient, FlightData, PutResult, SchemaAsIpc};
use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use tokio::sync::{mpsc, watch, Mutex};
use tokio::time::{sleep, Duration};
use tokio_retry::strategy::FixedInterval;
use tokio_retry::RetryIf;
use tonic::transport::Channel;
use tracing::{debug, error, info, instrument, trace, warn};

// Re-export arrow types for public API
pub use arrow_array::RecordBatch;
pub use arrow_schema::{DataType, Field, Schema as ArrowSchema};

use crate::arrow_configuration::ArrowStreamConfigurationOptions;
use crate::arrow_metadata::{FlightAckMetadata, FlightBatchMetadata};
use crate::errors::ZerobusError;
use crate::headers_provider::HeadersProvider;
use crate::offset_generator::{OffsetId, OffsetIdGenerator};
use crate::tls_config::TlsConfig;
use crate::ZerobusResult;

/// Type alias for the batch sender channel, wrapped for thread-safe sharing.
type BatchSender = Arc<Mutex<Option<mpsc::Sender<Result<FlightData, FlightError>>>>>;

/// Payload stored in pending batches — either raw IPC bytes (from FFI callers) or a
/// materialised RecordBatch (from native Rust callers).
#[derive(Clone)]
enum ArrowPayload {
    /// Raw Arrow IPC stream bytes from FFI callers.
    Ipc(Bytes),
    /// A materialised RecordBatch from native Rust callers.
    Batch(RecordBatch),
}

impl ArrowPayload {
    /// Converts this payload to a [`RecordBatch`].
    /// For `Ipc` variants this deserialises the IPC bytes.
    #[allow(clippy::result_large_err)]
    fn materialize(&self) -> ZerobusResult<RecordBatch> {
        match self {
            ArrowPayload::Batch(b) => Ok(b.clone()),
            ArrowPayload::Ipc(bytes) => materialize_ipc(bytes),
        }
    }
}

/// Properties for an Arrow Flight ingestion table.
///
/// **Do not construct this directly.** Configure Arrow streams via the builder API:
/// `sdk.stream_builder().table("catalog.schema.table").arrow(schema)`.
#[derive(Debug, Clone)]
pub struct ArrowTableProperties {
    /// The fully qualified table name (e.g., "catalog.schema.table").
    pub table_name: String,
    /// The Arrow schema for the data being ingested.
    /// This is used to validate RecordBatches before sending and is sent
    /// as the first message in the Flight stream.
    pub schema: Arc<ArrowSchema>,
}

/// A pending batch waiting for acknowledgment.
#[derive(Clone)]
struct PendingBatch {
    payload: ArrowPayload,
    /// Logical offset ID assigned by the client for this batch.
    offset_id: OffsetId,
    /// Cumulative record count before this batch.
    start_record: u64,
    /// Cumulative record count after this batch.
    /// Batch is fully acked when `acked_records >= end_record`.
    end_record: u64,
}

/// Returns the portion of a batch that needs to be replayed after recovery.
///
/// - If batch is fully acked: returns `None`
/// - If batch is partially acked: returns sliced batch with only un-acked records
/// - If batch is fully un-acked: returns the full batch
#[allow(clippy::result_large_err)]
fn slice_batch_for_recovery(
    pb: &PendingBatch,
    acked_before_disconnect: u64,
) -> ZerobusResult<Option<ArrowPayload>> {
    if pb.start_record >= acked_before_disconnect {
        return Ok(Some(pb.payload.clone()));
    }

    let total_rows = pb.end_record - pb.start_record;
    let records_already_acked = (acked_before_disconnect - pb.start_record).min(total_rows);
    let remaining_rows = total_rows.saturating_sub(records_already_acked);

    if remaining_rows == 0 {
        // Fully acked
        Ok(None)
    } else if records_already_acked == 0 {
        // No records acked (shouldn't happen given first check, but be safe)
        Ok(Some(pb.payload.clone()))
    } else {
        debug!(
            offset_id = pb.offset_id,
            total_rows = total_rows,
            records_already_acked = records_already_acked,
            remaining_rows = remaining_rows,
            "Slicing partially-acked batch for recovery"
        );
        match &pb.payload {
            ArrowPayload::Batch(b) => Ok(Some(ArrowPayload::Batch(
                b.slice(records_already_acked as usize, remaining_rows as usize),
            ))),
            ArrowPayload::Ipc(bytes) => {
                // Rare path: partially-acked IPC batch must be deserialised and sliced.
                // TODO: zero-copy partial-ack recovery — slice IPC bytes at buffer level
                // instead of materializing (tracked in #147).
                let b = materialize_ipc(bytes).map_err(|e| {
                    ZerobusError::InvalidArgument(format!(
                        "IPC batch could not be deserialised for partial recovery (offset_id={}): {e}",
                        pb.offset_id
                    ))
                })?;
                Ok(Some(ArrowPayload::Batch(b.slice(
                    records_already_acked as usize,
                    remaining_rows as usize,
                ))))
            }
        }
    }
}

/// Deserialises Arrow IPC stream bytes into a [`RecordBatch`].
/// Enforces the same single-batch contract as [`ipc_bytes_to_flight_data`].
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

/// Result of parsing raw Arrow IPC stream bytes: the extracted schema, row count,
/// and FlightData messages (dictionary batches followed by the record batch).
struct ParsedIpcBatch {
    /// The Arrow schema extracted from the IPC stream.
    schema: ArrowSchema,
    /// Number of rows in the record batch.
    num_rows: u64,
    /// FlightData messages: dictionary batches (if any) followed by the record batch.
    flight_data: Vec<FlightData>,
}

/// Converts raw Arrow IPC stream bytes into [`FlightData`] messages and metadata.
///
/// Parses the IPC stream without materialising Arrow arrays (zero-copy). Handles
/// dictionary messages between the schema and the record batch, and enforces the
/// single-batch contract (exactly one RecordBatch in the stream).
///
/// All offsets are rounded to 8-byte boundaries per the Arrow IPC encapsulated
/// message format specification.
#[allow(clippy::result_large_err)]
fn ipc_bytes_to_flight_data(ipc_bytes: &Bytes) -> ZerobusResult<ParsedIpcBatch> {
    let bytes = &ipc_bytes[..];

    /// Round up to next 8-byte boundary (Arrow IPC alignment requirement).
    fn align8(n: usize) -> usize {
        (n + 7) & !7
    }

    #[allow(clippy::result_large_err)]
    fn read_meta_range(bytes: &[u8], mut p: usize) -> ZerobusResult<(usize, usize)> {
        // Optional continuation token (0xFFFFFFFF).
        if p + 4 <= bytes.len() && bytes[p..p + 4] == [0xFF, 0xFF, 0xFF, 0xFF] {
            p += 4;
        }
        if p + 4 > bytes.len() {
            return Err(ZerobusError::InvalidArgument(
                "IPC: truncated at length field".into(),
            ));
        }
        let meta_len = i32::from_le_bytes([bytes[p], bytes[p + 1], bytes[p + 2], bytes[p + 3]]);
        if meta_len <= 0 {
            return Err(ZerobusError::InvalidArgument(
                "IPC: invalid metadata length".into(),
            ));
        }
        let meta_start = p + 4;
        let meta_end = meta_start + meta_len as usize;
        if meta_end > bytes.len() {
            return Err(ZerobusError::InvalidArgument(
                "IPC: truncated metadata".into(),
            ));
        }
        Ok((meta_start, meta_end))
    }

    // Parse Schema message
    let (ms, me) = read_meta_range(bytes, 0)?;
    let schema_msg = arrow_ipc::root_as_message(&bytes[ms..me])
        .map_err(|e| ZerobusError::InvalidArgument(format!("IPC flatbuffer: {e}")))?;
    let fb_schema = schema_msg.header_as_schema().ok_or_else(|| {
        ZerobusError::InvalidArgument("IPC: first message is not a Schema".into())
    })?;
    let schema = arrow_ipc::convert::fb_to_schema(fb_schema);
    let after_schema = align8(me + schema_msg.bodyLength().max(0) as usize);
    if after_schema > bytes.len() {
        return Err(ZerobusError::InvalidArgument(
            "IPC: truncated schema body".into(),
        ));
    }

    // Walk remaining messages: collect dictionary batches, find the RecordBatch
    let mut pos = after_schema;
    let mut flight_data_messages: Vec<FlightData> = Vec::new();
    let mut num_rows: Option<u64> = None;

    while pos < bytes.len() {
        // Check for end-of-stream marker (continuation token + zero-length metadata).
        if pos + 8 <= bytes.len()
            && bytes[pos..pos + 4] == [0xFF, 0xFF, 0xFF, 0xFF]
            && bytes[pos + 4..pos + 8] == [0x00, 0x00, 0x00, 0x00]
        {
            break; // End-of-stream marker
        }

        let (msg_ms, msg_me) = match read_meta_range(bytes, pos) {
            Ok(r) => r,
            Err(_) => {
                debug!(pos, "IPC: ignoring trailing bytes");
                break;
            }
        };
        let msg = arrow_ipc::root_as_message(&bytes[msg_ms..msg_me])
            .map_err(|e| ZerobusError::InvalidArgument(format!("IPC flatbuffer: {e}")))?;
        let body_end = align8(msg_me + msg.bodyLength().max(0) as usize);
        if body_end > bytes.len() {
            return Err(ZerobusError::InvalidArgument(
                "IPC: truncated message body".into(),
            ));
        }

        match msg.header_type() {
            arrow_ipc::MessageHeader::DictionaryBatch => {
                flight_data_messages.push(FlightData {
                    data_header: ipc_bytes.slice(msg_ms..msg_me),
                    data_body: ipc_bytes.slice(msg_me..body_end),
                    ..Default::default()
                });
            }
            arrow_ipc::MessageHeader::RecordBatch => {
                if num_rows.is_some() {
                    return Err(ZerobusError::InvalidArgument(
                        "IPC stream must contain exactly one RecordBatch (found extra batch)"
                            .into(),
                    ));
                }
                let rb = msg.header_as_record_batch().ok_or_else(|| {
                    ZerobusError::InvalidArgument(
                        "IPC: RecordBatch header could not be parsed".into(),
                    )
                })?;
                num_rows = Some(rb.length().max(0) as u64);
                flight_data_messages.push(FlightData {
                    data_header: ipc_bytes.slice(msg_ms..msg_me),
                    data_body: ipc_bytes.slice(msg_me..body_end),
                    ..Default::default()
                });
            }
            _ => {
                return Err(ZerobusError::InvalidArgument(format!(
                    "IPC: unexpected message type {:?}",
                    msg.header_type()
                )));
            }
        }

        pos = body_end;
    }

    let num_rows = num_rows.ok_or_else(|| {
        ZerobusError::InvalidArgument("IPC stream contains no RecordBatch".into())
    })?;

    Ok(ParsedIpcBatch {
        schema,
        num_rows,
        flight_data: flight_data_messages,
    })
}

/// Encodes a schema into the first [`FlightData`] message for a DoPut stream.
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

fn schema_to_flight_data(schema: &ArrowSchema, opts: &IpcWriteOptions) -> FlightData {
    SchemaAsIpc::new(schema, opts).into()
}

/// Serialises a [`RecordBatch`] into [`FlightData`] messages.
///
/// Returns dictionary FlightData messages (if the batch contains dictionary-encoded
/// columns) followed by the record batch FlightData.
#[allow(clippy::result_large_err)]
fn record_batch_to_flight_data(
    batch: &RecordBatch,
    opts: &IpcWriteOptions,
) -> ZerobusResult<Vec<FlightData>> {
    let data_gen = IpcDataGenerator::default();
    let mut dict_tracker = DictionaryTracker::new(true);
    // Register dictionary IDs from the schema so encoded_batch can find them.
    let _ = data_gen.schema_to_bytes_with_dictionary_tracker(
        batch.schema_ref(),
        &mut dict_tracker,
        opts,
    );
    let (dict_batches, encoded) = data_gen
        .encoded_batch(batch, &mut dict_tracker, opts)
        .map_err(|e| ZerobusError::InvalidArgument(format!("Failed to encode RecordBatch: {e}")))?;
    let mut flight_data: Vec<FlightData> = dict_batches.into_iter().map(Into::into).collect();
    flight_data.push(encoded.into());
    Ok(flight_data)
}

/// An Arrow Flight stream for ingesting Arrow RecordBatches into a Delta table.
///
/// This stream provides a high-performance interface for streaming Arrow data
/// to Databricks Delta tables using the Arrow Flight protocol.
///
/// # Lifecycle
///
/// 1. Create a stream via `ZerobusSdk::create_arrow_stream()`
/// 2. Ingest RecordBatches with `ingest_batch()` and await acknowledgments
/// 3. Optionally call `flush()` to ensure all batches are persisted
/// 4. Close the stream with `close()` to release resources
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
/// # async fn example(mut stream: ZerobusArrowStream, batch: RecordBatch) -> Result<(), ZerobusError> {
/// // Ingest a single RecordBatch
/// let offset = stream.ingest_batch(batch).await?;
/// println!("Batch queued at offset: {}", offset);
///
/// // Wait for acknowledgment
/// stream.wait_for_offset(offset).await?;
/// println!("Batch acknowledged at offset: {}", offset);
///
/// // Close the stream gracefully
/// stream.close().await?;
/// # Ok(())
/// # }
/// ```
pub struct ZerobusArrowStream {
    /// Table properties including name and schema.
    pub(crate) table_properties: ArrowTableProperties,
    /// Configuration options for this stream.
    pub(crate) options: ArrowStreamConfigurationOptions,
    /// Channel to send FlightData to the encoder task.
    batch_tx: BatchSender,
    /// Generator for logical offset IDs.
    offset_generator: OffsetIdGenerator,
    /// Watch channel for tracking the last acknowledged offset.
    last_ack_tx: tokio::sync::watch::Sender<Option<OffsetId>>,
    /// Receiver for the watch channel (kept alive to prevent sender errors).
    _last_ack_rx: tokio::sync::watch::Receiver<Option<OffsetId>>,
    /// Flag indicating if the stream has been closed.
    is_closed: Arc<AtomicBool>,
    /// Handle to the receiver task processing server responses.
    receiver_task: Arc<Mutex<Option<tokio::task::JoinHandle<ZerobusResult<()>>>>>,
    /// Batches that have been sent but not yet acknowledged (for recovery).
    pending_batches: Arc<Mutex<Vec<PendingBatch>>>,
    /// Batches that failed and couldn't be recovered.
    failed_batches: Arc<Mutex<Vec<ArrowPayload>>>,
    /// Count of recovery attempts.
    recovery_attempts: Arc<AtomicU32>,
    /// Connection details for recovery.
    endpoint: String,
    /// TLS configuration for the connection.
    tls_config: Arc<dyn TlsConfig>,
    headers_provider: Arc<dyn HeadersProvider>,
    /// Synchronization mutex for serializing ingest operations.
    ingest_mutex: Arc<Mutex<()>>,
    /// Last error received from the server (watch channel for race-free access).
    /// When process_acks receives a server error, it sends to this channel.
    /// When ingest_batch has a send failure, it can immediately check the current value.
    server_error_tx: watch::Sender<Option<ZerobusError>>,
    server_error_rx: watch::Receiver<Option<ZerobusError>>,
    /// Cumulative count of records sent (for record-based ack tracking).
    cumulative_records_sent: Arc<AtomicU64>,
    /// Last acknowledged cumulative record count (for recovery slicing).
    last_acked_records: Arc<AtomicU64>,
    /// Flag indicating the stream is paused due to a server close signal.
    /// When true, new `ingest_batch()` calls are still accepted and buffered,
    /// but the receiver continues draining in-flight acks before triggering recovery.
    is_paused: Arc<AtomicBool>,
}

impl ZerobusArrowStream {
    /// Creates a new Arrow Flight stream.
    ///
    /// This is typically called internally by `ZerobusSdk::create_arrow_stream()`.
    ///
    /// If `recovery` is enabled in options, initial connection will be retried
    /// up to `recovery_retries` times with `recovery_backoff_ms` delay between attempts.
    #[instrument(level = "debug", skip_all, fields(table_name = %table_properties.table_name))]
    pub(crate) async fn new(
        endpoint: &str,
        tls_config: Arc<dyn TlsConfig>,
        table_properties: ArrowTableProperties,
        headers_provider: Arc<dyn HeadersProvider>,
        options: ArrowStreamConfigurationOptions,
    ) -> ZerobusResult<Self> {
        let (last_ack_tx, _last_ack_rx) = tokio::sync::watch::channel(None);
        let is_closed = Arc::new(AtomicBool::new(false));
        let pending_batches = Arc::new(Mutex::new(Vec::new()));
        let failed_batches = Arc::new(Mutex::new(Vec::new()));
        let recovery_attempts = Arc::new(AtomicU32::new(0));
        let batch_tx = Arc::new(Mutex::new(None));
        let receiver_task = Arc::new(Mutex::new(None));
        let cumulative_records_sent = Arc::new(AtomicU64::new(0));
        let last_acked_records = Arc::new(AtomicU64::new(0));
        let is_paused = Arc::new(AtomicBool::new(false));

        let (server_error_tx, server_error_rx) = watch::channel(None);

        let stream = Self {
            table_properties,
            options,
            batch_tx,
            offset_generator: OffsetIdGenerator::default(),
            last_ack_tx,
            _last_ack_rx,
            is_closed,
            receiver_task,
            pending_batches,
            failed_batches,
            recovery_attempts,
            endpoint: endpoint.to_string(),
            tls_config,
            headers_provider,
            ingest_mutex: Arc::new(Mutex::new(())),
            server_error_tx,
            server_error_rx,
            cumulative_records_sent,
            last_acked_records,
            is_paused,
        };

        // Initialize the connection with retry logic.
        let endpoint = stream.endpoint.clone();
        let tls_config = Arc::clone(&stream.tls_config);
        let table_properties = stream.table_properties.clone();
        let options = stream.options.clone();
        let headers_provider = Arc::clone(&stream.headers_provider);
        let strategy = FixedInterval::from_millis(options.recovery_backoff_ms)
            .take(options.recovery_retries as usize);

        let create_attempt = || {
            let endpoint = endpoint.clone();
            let tls_config = Arc::clone(&tls_config);
            let table_properties = table_properties.clone();
            let options = options.clone();
            let headers_provider = Arc::clone(&headers_provider);

            async move {
                tokio::time::timeout(
                    Duration::from_millis(options.recovery_timeout_ms),
                    Self::try_connect(
                        &endpoint,
                        &tls_config,
                        &table_properties,
                        &options,
                        &headers_provider,
                    ),
                )
                .await
                .map_err(|_| {
                    ZerobusError::CreateStreamError(tonic::Status::deadline_exceeded(
                        "Stream creation timed out",
                    ))
                })?
            }
        };
        let should_retry = |e: &ZerobusError| options.recovery && e.is_retryable();
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
            Arc::clone(&stream.cumulative_records_sent),
            Arc::clone(&stream.last_acked_records),
            Arc::clone(&stream.is_paused),
            response_stream,
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
        table_properties: &ArrowTableProperties,
        options: &ArrowStreamConfigurationOptions,
        headers_provider: &Arc<dyn HeadersProvider>,
    ) -> ZerobusResult<(
        Pin<Box<dyn Stream<Item = Result<PutResult, FlightError>> + Send>>,
        mpsc::Sender<Result<FlightData, FlightError>>,
    )> {
        let client = Self::create_flight_client(
            endpoint,
            tls_config,
            table_properties,
            options,
            headers_provider,
        )
        .await?;

        Self::start_stream_connection(client, table_properties, options).await
    }

    /// Creates a Flight client connected to the endpoint.
    async fn create_flight_client(
        endpoint: &str,
        tls_config: &Arc<dyn TlsConfig>,
        table_properties: &ArrowTableProperties,
        options: &ArrowStreamConfigurationOptions,
        headers_provider: &Arc<dyn HeadersProvider>,
    ) -> ZerobusResult<FlightClient> {
        let connection_timeout = Duration::from_millis(options.connection_timeout_ms);

        let base_endpoint = Channel::from_shared(endpoint.to_string())
            .map_err(|e| ZerobusError::ChannelCreationError(e.to_string()))?
            .connect_timeout(connection_timeout)
            .timeout(connection_timeout);

        let channel = tls_config.configure_endpoint(base_endpoint)?.connect_lazy();

        let mut client = FlightClient::new(channel);

        // Add headers from the provider first, filtering out reserved headers.
        // The table name header is authoritative and must not be overridden.
        const TABLE_NAME_HEADER: &str = "x-databricks-zerobus-table-name";
        let headers = headers_provider.get_headers().await?;
        for (key, value) in headers {
            if key.eq_ignore_ascii_case(TABLE_NAME_HEADER) {
                warn!(
                    "HeadersProvider attempted to set reserved header '{}', ignoring",
                    TABLE_NAME_HEADER
                );
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
        mpsc::Sender<Result<FlightData, FlightError>>,
    )> {
        // Create channel for sending pre-encoded FlightData.
        // Metadata (offset IDs) is set by the sender before enqueueing, so the
        // stream simply forwards messages as-is. Dictionary FlightData messages
        // carry empty app_metadata; only the RecordBatch FlightData has offset info.
        let (batch_tx, batch_rx) =
            mpsc::channel::<Result<FlightData, FlightError>>(options.max_inflight_batches);

        let ipc_write_options = make_ipc_write_options(options.ipc_compression)?;
        let schema_fd = schema_to_flight_data(&table_properties.schema, &ipc_write_options);
        let data_stream = tokio_stream::wrappers::ReceiverStream::new(batch_rx);

        let flight_data_stream =
            futures::stream::once(futures::future::ready(Ok(schema_fd))).chain(data_stream);

        // Start the DoPut stream.
        let mut response_stream = client
            .do_put(flight_data_stream)
            .await
            .map_err(|e| ZerobusError::CreateStreamError(tonic::Status::from_error(Box::new(e))))?;

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
                return Err(ZerobusError::CreateStreamError(tonic::Status::from_error(
                    Box::new(flight_error),
                )));
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
    /// 3. Continues until stream is closed or max retries exceeded
    #[allow(clippy::too_many_arguments)]
    fn spawn_supervisor_task(
        endpoint: String,
        tls_config: Arc<dyn TlsConfig>,
        table_properties: ArrowTableProperties,
        options: ArrowStreamConfigurationOptions,
        headers_provider: Arc<dyn HeadersProvider>,
        batch_tx: BatchSender,
        is_closed: Arc<AtomicBool>,
        last_ack_tx: tokio::sync::watch::Sender<Option<OffsetId>>,
        pending_batches: Arc<Mutex<Vec<PendingBatch>>>,
        failed_batches: Arc<Mutex<Vec<ArrowPayload>>>,
        recovery_attempts: Arc<AtomicU32>,
        server_error_tx: watch::Sender<Option<ZerobusError>>,
        cumulative_records_sent: Arc<AtomicU64>,
        last_acked_records: Arc<AtomicU64>,
        is_paused: Arc<AtomicBool>,
        initial_response_stream: Pin<Box<dyn Stream<Item = Result<PutResult, FlightError>> + Send>>,
    ) -> tokio::task::JoinHandle<ZerobusResult<()>> {
        tokio::spawn(async move {
            let ack_timeout = Duration::from_millis(options.server_lack_of_ack_timeout_ms);
            let mut response_stream = initial_response_stream;

            loop {
                if is_closed.load(Ordering::Relaxed) {
                    debug!("Supervisor: Stream closed, exiting");
                    return Ok(());
                }

                // Run process_acks until it returns (error or stream closed).
                let result = Self::process_acks(
                    response_stream,
                    Arc::clone(&is_closed),
                    last_ack_tx.clone(),
                    Arc::clone(&pending_batches),
                    ack_timeout,
                    server_error_tx.clone(),
                    Arc::clone(&last_acked_records),
                    Arc::clone(&is_paused),
                    &options,
                )
                .await;

                // Capture whether this was a graceful close recovery before resetting.
                // Graceful close recoveries should not consume the retry budget.
                let was_graceful_close = is_paused.load(Ordering::Relaxed);

                // Reset paused state after process_acks returns (before recovery).
                is_paused.store(false, Ordering::Relaxed);

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
                    Err(ref error) if error.is_retryable() && options.recovery => {
                        // Retriable error - attempt recovery.
                        // Graceful close recoveries don't count against the retry budget.
                        let attempts = if was_graceful_close {
                            recovery_attempts.load(Ordering::Relaxed)
                        } else {
                            recovery_attempts.fetch_add(1, Ordering::Relaxed)
                        };
                        if attempts >= options.recovery_retries {
                            error!(
                                attempts = attempts,
                                max_retries = options.recovery_retries,
                                "Supervisor: Max recovery retries exceeded"
                            );
                            is_closed.store(true, Ordering::Relaxed);
                            // Move pending batches to failed and fail the ack futures.
                            Self::move_pending_to_failed(&pending_batches, &failed_batches).await;
                            return result;
                        }

                        info!(
                            attempt = attempts + 1,
                            max_retries = options.recovery_retries,
                            error = %error,
                            "Supervisor: Attempting recovery after retriable error"
                        );

                        // Backoff before retry.
                        sleep(Duration::from_millis(options.recovery_backoff_ms)).await;

                        // Clear the server error.
                        let _ = server_error_tx.send(None);

                        // Close old sender.
                        {
                            let mut tx_guard = batch_tx.lock().await;
                            *tx_guard = None;
                        }

                        // Create new connection.
                        let reconnect_result = tokio::time::timeout(
                            Duration::from_millis(options.recovery_timeout_ms),
                            Self::reconnect(
                                &endpoint,
                                &tls_config,
                                &table_properties,
                                &options,
                                &headers_provider,
                                &batch_tx,
                                &pending_batches,
                                &cumulative_records_sent,
                                &last_acked_records,
                            ),
                        )
                        .await;

                        match reconnect_result {
                            Ok(Ok(new_response_stream)) => {
                                info!("Supervisor: Recovery successful, resuming");
                                recovery_attempts.store(0, Ordering::Relaxed);
                                response_stream = new_response_stream;
                                // Loop continues with new stream.
                            }
                            Ok(Err(e)) => {
                                warn!("Supervisor: Reconnection failed: {}", e);
                                // Loop continues, will retry if retries remain.
                                // Create a dummy stream that immediately errors.
                                response_stream = Box::pin(futures::stream::once(async move {
                                    Err(FlightError::Tonic(Box::new(tonic::Status::unavailable(
                                        "Reconnection failed",
                                    ))))
                                }));
                            }
                            Err(_timeout) => {
                                warn!("Supervisor: Reconnection timed out");
                                // Loop continues, will retry if retries remain.
                                response_stream = Box::pin(futures::stream::once(async move {
                                    Err(FlightError::Tonic(Box::new(
                                        tonic::Status::deadline_exceeded("Reconnection timed out"),
                                    )))
                                }));
                            }
                        }
                    }
                    Err(error) => {
                        // Non-retriable error or recovery disabled.
                        error!("Supervisor: Non-retriable error, closing stream: {}", error);
                        is_closed.store(true, Ordering::Relaxed);
                        // Move pending batches to failed and fail the ack futures.
                        Self::move_pending_to_failed(&pending_batches, &failed_batches).await;
                        return Err(error);
                    }
                }
            }
        })
    }

    /// Reconnects to the server and replays pending batches.
    #[allow(clippy::too_many_arguments)]
    async fn reconnect(
        endpoint: &str,
        tls_config: &Arc<dyn TlsConfig>,
        table_properties: &ArrowTableProperties,
        options: &ArrowStreamConfigurationOptions,
        headers_provider: &Arc<dyn HeadersProvider>,
        batch_tx: &BatchSender,
        pending_batches: &Arc<Mutex<Vec<PendingBatch>>>,
        cumulative_records_sent: &Arc<AtomicU64>,
        last_acked_records: &Arc<AtomicU64>,
    ) -> ZerobusResult<Pin<Box<dyn Stream<Item = Result<PutResult, FlightError>> + Send>>> {
        // Create new client.
        let client = Self::create_flight_client(
            endpoint,
            tls_config,
            table_properties,
            options,
            headers_provider,
        )
        .await?;

        // Create new channel.
        let (tx, batch_rx) =
            mpsc::channel::<Result<FlightData, FlightError>>(options.max_inflight_batches);

        let ipc_write_options = make_ipc_write_options(options.ipc_compression)?;
        let schema_fd = schema_to_flight_data(&table_properties.schema, &ipc_write_options);
        let data_stream = tokio_stream::wrappers::ReceiverStream::new(batch_rx);

        let flight_data_stream =
            futures::stream::once(futures::future::ready(Ok(schema_fd))).chain(data_stream);

        // Start the DoPut stream.
        let mut flight_client = client;
        let mut response_stream = flight_client
            .do_put(flight_data_stream)
            .await
            .map_err(|e| ZerobusError::CreateStreamError(tonic::Status::from_error(Box::new(e))))?;

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
                return Err(ZerobusError::CreateStreamError(tonic::Status::from_error(
                    Box::new(flight_error),
                )));
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

        // Store the new sender.
        {
            let mut tx_guard = batch_tx.lock().await;
            *tx_guard = Some(tx.clone());
        }

        // Get the last acked record count before the disconnect.
        // This tells us how many records were durably stored.
        let acked_before_disconnect = last_acked_records.load(Ordering::Relaxed);
        // Reset for the new connection to avoid reusing stale values.
        last_acked_records.store(0, Ordering::Relaxed);

        // Reset cumulative_records_sent for the new connection.
        // It will be recalculated as we replay batches.
        cumulative_records_sent.store(0, Ordering::Relaxed);

        // Replay pending batches, slicing partially-acked ones if present.
        // We rebuild the pending list to drop fully-acked batches.
        {
            let mut pending = pending_batches.lock().await;
            if !pending.is_empty() {
                info!(
                    batch_count = pending.len(),
                    acked_records = acked_before_disconnect,
                    "Replaying pending batches after recovery"
                );

                let mut new_pending = Vec::with_capacity(pending.len());
                let mut new_cumulative: u64 = 0;
                let mut replay_offset: i64 = 0;

                for pb in pending.drain(..) {
                    let payload = match slice_batch_for_recovery(&pb, acked_before_disconnect)? {
                        None => {
                            debug!(offset_id = pb.offset_id, "Skipping fully-acked batch");
                            continue;
                        }
                        Some(p) => p,
                    };

                    let (flight_data_messages, num_records) = match &payload {
                        ArrowPayload::Batch(b) => (
                            record_batch_to_flight_data(b, &ipc_write_options).map_err(|e| {
                                ZerobusError::InvalidArgument(format!(
                                    "Failed to encode batch for replay: {e}"
                                ))
                            })?,
                            b.num_rows() as u64,
                        ),
                        ArrowPayload::Ipc(bytes) => {
                            let parsed = ipc_bytes_to_flight_data(bytes).map_err(|e| {
                                ZerobusError::InvalidArgument(format!(
                                    "Failed to encode batch for replay: {e}"
                                ))
                            })?;
                            (parsed.flight_data, parsed.num_rows)
                        }
                    };

                    let fd_count = flight_data_messages.len();
                    for (i, mut fd) in flight_data_messages.into_iter().enumerate() {
                        if i == fd_count - 1 {
                            let metadata = FlightBatchMetadata::new(replay_offset);
                            replay_offset += 1;
                            if let Ok(bytes) = metadata.to_bytes() {
                                fd.app_metadata = bytes.into();
                            }
                        }
                        if tx.send(Ok(fd)).await.is_err() {
                            return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                                "Failed to replay batch during recovery",
                            )));
                        }
                    }
                    let start_record = new_cumulative;
                    let end_record = new_cumulative + num_records;
                    new_cumulative = end_record;

                    new_pending.push(PendingBatch {
                        payload,
                        offset_id: pb.offset_id,
                        start_record,
                        end_record,
                    });
                }

                *pending = new_pending;
                cumulative_records_sent.store(new_cumulative, Ordering::Relaxed);
            }
        }

        Ok(response_stream)
    }

    /// Moves all pending batches to the failed batches list.
    async fn move_pending_to_failed(
        pending_batches: &Arc<Mutex<Vec<PendingBatch>>>,
        failed_batches: &Arc<Mutex<Vec<ArrowPayload>>>,
    ) {
        let pending: Vec<PendingBatch> = {
            let mut pending_guard = pending_batches.lock().await;
            std::mem::take(&mut *pending_guard)
        };
        let mut failed = failed_batches.lock().await;
        for pb in pending {
            failed.push(pb.payload);
        }
    }

    /// Processes acknowledgments from the server response stream.
    ///
    /// Uses record-based tracking: the server sends `ack_up_to_records` indicating
    /// the cumulative number of records durably stored. We match this against
    /// pending batches' record ranges to determine which batches are fully acked.
    /// This correctly handles Arrow Flight's automatic batch chunking.
    #[allow(clippy::too_many_arguments)]
    async fn process_acks(
        mut response_stream: Pin<Box<dyn Stream<Item = Result<PutResult, FlightError>> + Send>>,
        is_closed: Arc<AtomicBool>,
        last_ack_tx: tokio::sync::watch::Sender<Option<OffsetId>>,
        pending_batches: Arc<Mutex<Vec<PendingBatch>>>,
        ack_timeout: Duration,
        server_error_tx: watch::Sender<Option<ZerobusError>>,
        last_acked_records: Arc<AtomicU64>,
        is_paused: Arc<AtomicBool>,
        options: &ArrowStreamConfigurationOptions,
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

            let result = if let Some(deadline) = pause_deadline {
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

                            let acked_records = ack.ack_up_to_records;
                            debug!(
                                ack_up_to_offset = ack.ack_up_to_offset,
                                ack_up_to_records = acked_records,
                                "Received acknowledgment"
                            );

                            // Update last_acked_records for recovery slicing.
                            last_acked_records.store(acked_records, Ordering::Relaxed);

                            // Find and remove batches that are fully acknowledged.
                            // A batch is fully acked when ack_up_to_records >= batch.end_record.
                            let mut max_acked_offset: Option<OffsetId> = None;
                            {
                                let mut pending = pending_batches.lock().await;
                                pending.retain(|pb| {
                                    if acked_records >= pb.end_record {
                                        // Batch is fully acknowledged
                                        max_acked_offset = Some(
                                            max_acked_offset
                                                .map_or(pb.offset_id, |o| o.max(pb.offset_id)),
                                        );
                                        false // Remove from pending
                                    } else {
                                        true // Keep in pending
                                    }
                                });
                            }

                            // Notify waiters of the highest acknowledged logical offset.
                            if let Some(offset) = max_acked_offset {
                                let _ = last_ack_tx.send(Some(offset));
                            }
                        }
                        Err(e) => {
                            warn!("Failed to parse ack metadata: {}", e);
                        }
                    }
                }
                Ok(Some(Err(e))) => {
                    // During graceful close, errors are expected (server closes after grace period).
                    // Return retriable error to trigger recovery.
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
                        return Err(error);
                    }
                }
            }
        }
    }

    /// Shared send path for both `ingest_batch` and `ingest_ipc_batch`.
    ///
    /// Sends all `flight_data_messages` (dictionary batches followed by the record batch)
    /// in order. Caller must hold `ingest_mutex` and must have already updated
    /// `cumulative_records_sent`.
    async fn send_flight_data_internal(
        &self,
        payload: ArrowPayload,
        flight_data_messages: Vec<FlightData>,
        offset_id: OffsetId,
        start_record: u64,
        end_record: u64,
    ) -> ZerobusResult<OffsetId> {
        {
            let mut pending = self.pending_batches.lock().await;
            pending.push(PendingBatch {
                payload,
                offset_id,
                start_record,
                end_record,
            });
        }

        let sender = {
            let guard = self.batch_tx.lock().await;
            guard.clone()
        };

        let sender = match sender {
            Some(s) => s,
            None => {
                if let Some(server_error) = self.server_error_rx.borrow().clone() {
                    return Err(server_error);
                }
                return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                    "Stream sender is closed",
                )));
            }
        };

        // Assign offset metadata only to the last message (the RecordBatch).
        // Dictionary FlightData messages (if any) are sent with empty app_metadata.
        let msg_count = flight_data_messages.len();
        for (i, mut flight_data) in flight_data_messages.into_iter().enumerate() {
            if i == msg_count - 1 {
                let metadata = FlightBatchMetadata::new(offset_id);
                if let Ok(bytes) = metadata.to_bytes() {
                    flight_data.app_metadata = bytes.into();
                }
            }
            if let Err(e) = sender.send(Ok(flight_data)).await {
                warn!("Send failed: {}", e);
                if self.options.recovery {
                    debug!(
                        offset_id = offset_id,
                        "Send failed but recovery enabled - supervisor will handle recovery"
                    );
                    return Ok(offset_id);
                } else {
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
            }
        }

        Ok(offset_id)
    }

    /// Ingests a single Arrow RecordBatch into the stream.
    ///
    /// This method queues the batch for transmission and returns the assigned logical offset
    /// immediately. Use `wait_for_offset()` to explicitly wait for server acknowledgment
    /// of this batch when needed.
    ///
    /// # Arguments
    ///
    /// * `batch` - An Arrow RecordBatch to ingest
    ///
    /// # Returns
    ///
    /// The logical offset ID assigned to this batch.
    ///
    /// # Errors
    ///
    /// * `StreamClosedError` - If the stream has been closed
    /// * `InvalidArgument` - If the batch schema doesn't match the stream schema
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # use arrow_array::RecordBatch;
    /// # async fn example(stream: ZerobusArrowStream, batch: RecordBatch) -> Result<(), ZerobusError> {
    /// // Ingest and get offset immediately
    /// let offset = stream.ingest_batch(batch).await?;
    ///
    /// // Later, wait for acknowledgment
    /// stream.wait_for_offset(offset).await?;
    /// println!("Batch at offset {} has been acknowledged", offset);
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(level = "debug", skip_all, fields(table_name = %self.table_properties.table_name))]
    pub async fn ingest_batch(&self, batch: RecordBatch) -> ZerobusResult<OffsetId> {
        if self.is_closed.load(Ordering::Relaxed) {
            return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                "Stream is closed",
            )));
        }

        // Validate schema matches.
        if batch.schema() != self.table_properties.schema {
            return Err(ZerobusError::InvalidArgument(format!(
                "RecordBatch schema does not match stream schema. Expected: {:?}, Got: {:?}",
                self.table_properties.schema,
                batch.schema()
            )));
        }

        let _guard = self.ingest_mutex.lock().await;

        let record_count = batch.num_rows() as u64;
        let offset_id = self.offset_generator.next();
        let start_record = self
            .cumulative_records_sent
            .fetch_add(record_count, Ordering::Relaxed);
        let end_record = start_record + record_count;

        let flight_data_messages = record_batch_to_flight_data(
            &batch,
            &make_ipc_write_options(self.options.ipc_compression)?,
        )?;

        debug!(offset_id = offset_id, "Batch queued for ingestion");
        self.send_flight_data_internal(
            ArrowPayload::Batch(batch),
            flight_data_messages,
            offset_id,
            start_record,
            end_record,
        )
        .await
    }

    /// Ingests a single Arrow RecordBatch supplied as raw Arrow IPC stream bytes.
    ///
    /// Preferred entry point for FFI callers (Go, Python, Java, TypeScript) that already
    /// hold IPC-serialised bytes. Forwards bytes directly to the Flight wire format
    /// without deserialising to a [`RecordBatch`] — eliminating one IPC round-trip
    /// compared to calling `ingest_batch`.
    ///
    /// The `ipc_bytes` must be a valid Arrow IPC *stream* containing exactly one
    /// RecordBatch (i.e. the output of `pyarrow.RecordBatch.serialize()`,
    /// `tableToIPC(table, 'stream')`, etc.). Dictionary messages between the schema and
    /// the RecordBatch are supported. Trailing stream metadata (such as an end-of-stream
    /// marker after `finish()`) is allowed after that batch.
    ///
    /// # Compression
    ///
    /// This method forwards raw IPC bytes as-is. If the stream is configured with
    /// `ipc_compression`, this method will return an error since the raw IPC bytes
    /// would not match the expected compression codec.
    #[instrument(level = "debug", skip_all, fields(table_name = %self.table_properties.table_name))]
    pub async fn ingest_ipc_batch(&self, ipc_bytes: Bytes) -> ZerobusResult<OffsetId> {
        if self.is_closed.load(Ordering::Relaxed) {
            return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                "Stream is closed",
            )));
        }

        // Raw IPC bytes are forwarded as-is; they cannot match a compression codec.
        if let Some(codec) = self.options.ipc_compression {
            return Err(ZerobusError::InvalidArgument(format!(
                "ingest_ipc_batch cannot be used when ipc_compression is enabled ({codec:?}). \
                 Use ingest_batch instead, or disable compression for this stream."
            )));
        }

        let parsed = ipc_bytes_to_flight_data(&ipc_bytes)
            .map_err(|e| ZerobusError::InvalidArgument(format!("Invalid Arrow IPC bytes: {e}")))?;

        // Validate schema matches the stream schema.
        if parsed.schema != *self.table_properties.schema {
            return Err(ZerobusError::InvalidArgument(format!(
                "IPC batch schema does not match stream schema. Expected: {:?}, Got: {:?}",
                self.table_properties.schema, parsed.schema
            )));
        }

        let _guard = self.ingest_mutex.lock().await;

        let offset_id = self.offset_generator.next();
        let start_record = self
            .cumulative_records_sent
            .fetch_add(parsed.num_rows, Ordering::Relaxed);
        let end_record = start_record + parsed.num_rows;

        debug!(offset_id = offset_id, "IPC batch queued for ingestion");
        self.send_flight_data_internal(
            ArrowPayload::Ipc(ipc_bytes),
            parsed.flight_data,
            offset_id,
            start_record,
            end_record,
        )
        .await
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
                if self.is_closed.load(Ordering::Relaxed) {
                    return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                        format!("Stream closed during {}", operation_name.to_lowercase()),
                    )));
                }

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
                    trace!(
                        current_ack = ack_offset,
                        target_offset = offset_to_wait,
                        "Waiting for more acks"
                    );
                }

                // Race between offset updates and server errors
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
                        // Loop continues to check new offset value
                    }
                    _ = error_rx.changed() => {
                        // Server error occurred - return it immediately if stream is closed
                        if let Some(server_error) = error_rx.borrow().clone() {
                            if self.is_closed.load(Ordering::Relaxed) {
                                return Err(server_error);
                            }
                            // Stream still active, recovery might succeed - keep waiting
                        }
                        // Error channel updated but no error (cleared by recovery) - continue waiting
                    }
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
    /// This method captures the current highest offset and waits until all batches up to
    /// that offset have been acknowledged by the server. Batches ingested during the flush
    /// operation are not included in this flush.
    ///
    /// # Returns
    ///
    /// `Ok(())` when all pending batches at the time of the call have been acknowledged.
    ///
    /// # Errors
    ///
    /// * `StreamClosedError` - If the stream is closed or times out
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
        // Check if stream is closed first, before checking for batches.
        if self.is_closed.load(Ordering::Relaxed) {
            return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                "Cannot flush: stream is closed",
            )));
        }

        let target_offset = match self.offset_generator.last() {
            Some(offset) => offset,
            None => {
                debug!("No batches to flush");
                return Ok(());
            }
        };

        self.wait_for_offset_internal(target_offset, "Flush").await
    }

    /// Waits for server acknowledgment of a specific logical offset.
    ///
    /// This method blocks until the server has acknowledged the batch at the
    /// specified offset. Use this with offsets returned from `ingest_batch()` to
    /// explicitly control when to wait for acknowledgments.
    ///
    /// # Arguments
    ///
    /// * `offset` - The logical offset ID to wait for (returned from `ingest_batch()`)
    ///
    /// # Returns
    ///
    /// `Ok(())` when the batch at the specified offset has been acknowledged.
    ///
    /// # Errors
    ///
    /// * `StreamClosedError` - If the stream is closed or times out while waiting
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # use arrow_array::RecordBatch;
    /// # async fn example(stream: ZerobusArrowStream, batches: Vec<RecordBatch>) -> Result<(), ZerobusError> {
    /// // Ingest multiple batches and collect their offsets
    /// let mut offsets = Vec::new();
    /// for batch in batches {
    ///     let offset = stream.ingest_batch(batch).await?;
    ///     offsets.push(offset);
    /// }
    ///
    /// // Wait for specific offsets
    /// for offset in offsets {
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

    /// Closes the stream gracefully after flushing all pending batches.
    ///
    /// This method first calls `flush()` to ensure all pending batches are acknowledged,
    /// then shuts down the stream and releases all resources.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the stream was closed successfully after flushing all batches.
    ///
    /// # Errors
    ///
    /// Returns any errors from the flush operation. If flush fails, some batches
    /// may not have been acknowledged. Use `get_unacked_batches()` to retrieve them.
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
        if self.is_closed.load(Ordering::Relaxed) {
            return Ok(());
        }

        info!(
            table_name = %self.table_properties.table_name,
            "Closing Arrow Flight stream"
        );

        // Flush pending batches.
        if let Err(e) = self.flush().await {
            warn!(
                "Flush failed during close: {}. Moving pending batches to failed.",
                e
            );
            // Move pending batches to failed (drain to avoid duplicates in get_unacked_batches).
            Self::move_pending_to_failed(&self.pending_batches, &self.failed_batches).await;
        }

        // Mark as closed.
        self.is_closed.store(true, Ordering::Relaxed);

        // Drop the batch sender to signal end of stream.
        {
            let mut tx = self.batch_tx.lock().await;
            *tx = None;
        }

        // Abort the receiver task.
        {
            let mut task = self.receiver_task.lock().await;
            if let Some(t) = task.take() {
                t.abort();
            }
        }

        Ok(())
    }

    /// Returns all batches that were ingested but not acknowledged by the server.
    ///
    /// This method should only be called after a stream has failed or been closed.
    /// It's useful for implementing custom retry logic or persisting failed batches.
    ///
    /// # Returns
    ///
    /// A vector of `RecordBatch` items that were not acknowledged.
    ///
    /// # Errors
    ///
    /// * `InvalidStateError` - If the stream is still active
    /// * `InvalidArgument` - If an IPC-backed batch cannot be deserialised (e.g. corrupt bytes)
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::*;
    /// # async fn example(sdk: ZerobusSdk, mut stream: ZerobusArrowStream) -> Result<(), ZerobusError> {
    /// match stream.flush().await {
    ///     Err(_) => {
    ///         let failed_batches = stream.get_unacked_batches().await?;
    ///         println!("Failed to send {} batches", failed_batches.len());
    ///         // You can recreate the stream and retry these batches
    ///         let new_stream = sdk.recreate_arrow_stream(&stream).await?;
    ///         for batch in failed_batches {
    ///             new_stream.ingest_batch(batch).await?;
    ///         }
    ///     }
    ///     Ok(_) => println!("All batches acknowledged"),
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

        // Combine pending and failed batches, materialising RecordBatches from ArrowPayload.
        let mut result = Vec::new();

        {
            let pending = self.pending_batches.lock().await;
            for pb in pending.iter() {
                result.push(pb.payload.materialize().map_err(|e| {
                    ZerobusError::InvalidArgument(format!(
                        "unacked batch at offset_id {} could not be materialised: {e}",
                        pb.offset_id
                    ))
                })?);
            }
        }

        {
            let failed = self.failed_batches.lock().await;
            for (i, payload) in failed.iter().enumerate() {
                result.push(payload.materialize().map_err(|e| {
                    ZerobusError::InvalidArgument(format!(
                        "failed batch at index {i} could not be materialised: {e}"
                    ))
                })?);
            }
        }

        Ok(result)
    }

    /// Returns whether the stream has been closed.
    pub fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::Relaxed)
    }

    /// Returns the table name for this stream.
    pub fn table_name(&self) -> &str {
        &self.table_properties.table_name
    }

    /// Returns the Arrow schema for this stream.
    pub fn schema(&self) -> &Arc<ArrowSchema> {
        &self.table_properties.schema
    }

    /// Returns the table properties for this stream.
    pub fn table_properties(&self) -> &ArrowTableProperties {
        &self.table_properties
    }

    /// Returns the configuration options for this stream.
    pub fn options(&self) -> &ArrowStreamConfigurationOptions {
        &self.options
    }

    /// Returns the headers provider for this stream (for recreation).
    pub(crate) fn headers_provider(&self) -> Arc<dyn HeadersProvider> {
        Arc::clone(&self.headers_provider)
    }
}

impl Drop for ZerobusArrowStream {
    fn drop(&mut self) {
        self.is_closed.store(true, Ordering::Relaxed);
        // Abort the background supervisor task to prevent zombie tasks.
        // This is a hard abort, but outstanding oneshot receivers will get
        // RecvError when their senders are dropped, and pending batches can
        // still be retrieved via get_unacked_batches() before drop.
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
    use arrow_schema::{DataType, Field};

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
