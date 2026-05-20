//! Arrow Flight stream implementation for high-performance Arrow data ingestion.
//!
//! **Beta**: This module is in Beta. The API is stabilising but may still change
//! before reaching GA.
//!
//! This module provides `ZerobusArrowStream`, a client for ingesting Arrow `RecordBatch`
//! data into Databricks Delta tables using the Arrow Flight protocol.
//! Native Rust callers use `ingest_batch` with `RecordBatch` values; FFI callers
//! (Go, Python, Java, TypeScript) can use `ingest_ipc_batch` with pre-serialised
//! Arrow IPC bytes to avoid an extra deserialisation round-trip.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::{FlightClient, FlightData, PutResult, SchemaAsIpc};
use arrow_ipc::writer::IpcWriteOptions;
use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt};
use tokio::sync::{mpsc, watch, Mutex};
use tokio::time::{sleep, Duration};
use tokio_retry::strategy::FixedInterval;
use tokio_retry::RetryIf;
use tonic::transport::Channel;
use tracing::{debug, error, info, instrument, warn};

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

/// Target maximum encoded size (bytes) for a single gRPC Flight message on the wire.
///
/// Matches `GRPC_TARGET_MAX_FLIGHT_SIZE_BYTES` from the upstream `arrow-flight` crate and
/// the default used by `FlightDataEncoderBuilder`. Batches whose IPC encoding exceeds this
/// limit are row-sliced into multiple Flight messages before being sent so that no single
/// gRPC frame exceeds the server's decode limit (typically 10 MiB).
const GRPC_TARGET_MAX_FLIGHT_DATA_BYTES: usize = 2 * 1024 * 1024;

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
pub(crate) struct ArrowTableProperties {
    /// The fully qualified table name (e.g., "catalog.schema.table").
    pub(crate) table_name: String,
    /// The Arrow schema for the data being ingested.
    /// This is used to validate RecordBatches before sending and is sent
    /// as the first message in the Flight stream.
    pub(crate) schema: Arc<ArrowSchema>,
}

/// A pending batch waiting for acknowledgment.
#[derive(Clone)]
struct PendingBatch {
    payload: ArrowPayload,
    /// Logical (user-visible) offset ID assigned by the client for this batch.
    /// Monotonic across the lifetime of the stream — never resets on recovery.
    /// This is the value handed back from `ingest_batch` / `ingest_ipc_batch`
    /// and broadcast on `last_ack_tx` so that `wait_for_offset` callers see a
    /// monotonically non-decreasing acked offset.
    logical_offset_id: OffsetId,
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
            offset_id = pb.logical_offset_id,
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
                        pb.logical_offset_id
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

/// Encodes a [`RecordBatch`] into one or more groups of [`FlightData`] messages using
/// [`FlightDataEncoderBuilder`], splitting by rows so each group's total encoded size
/// stays at or below [`GRPC_TARGET_MAX_FLIGHT_DATA_BYTES`].
///
/// Returns `Vec<Vec<FlightData>>` — one inner `Vec` per wire chunk, where each chunk
/// ends with a RecordBatch message (dictionary messages for that slice precede it).
/// The common case (batch fits in one message) returns a single-element outer vec.
///
/// The schema `FlightData` that the encoder prepends is stripped — the schema is sent
/// once when the stream opens, not per-batch.
async fn record_batch_to_chunked_flight_data(
    batch: &RecordBatch,
    opts: &IpcWriteOptions,
) -> ZerobusResult<Vec<Vec<FlightData>>> {
    let stream = futures::stream::once(futures::future::ready(
        Ok::<RecordBatch, FlightError>(batch.clone()),
    ));

    let all_fds: Vec<FlightData> = FlightDataEncoderBuilder::new()
        .with_max_flight_data_size(GRPC_TARGET_MAX_FLIGHT_DATA_BYTES)
        .with_options(opts.clone())
        .build(stream)
        .try_collect()
        .await
        .map_err(|e| ZerobusError::InvalidArgument(format!("Failed to encode RecordBatch: {e}")))?;

    // The first message is the schema — skip it.
    // Group remaining messages into chunks: each chunk ends at a RecordBatch message.
    // Dictionary messages that precede a RecordBatch belong to the same chunk.
    let mut chunks: Vec<Vec<FlightData>> = Vec::new();
    let mut current: Vec<FlightData> = Vec::new();

    for fd in all_fds.into_iter().skip(1) {
        let is_record_batch = arrow_ipc::root_as_message(&fd.data_header)
            .map(|msg| msg.header_type() == arrow_ipc::MessageHeader::RecordBatch)
            .unwrap_or(false);
        current.push(fd);
        if is_record_batch {
            chunks.push(std::mem::take(&mut current));
        }
    }

    Ok(chunks)
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
#[non_exhaustive]
pub struct ZerobusArrowStream {
    /// Table properties including name and schema.
    pub(crate) table_properties: ArrowTableProperties,
    /// Configuration options for this stream.
    pub(crate) options: ArrowStreamConfigurationOptions,
    /// Channel to send FlightData to the encoder task.
    batch_tx: BatchSender,
    /// Generator for logical (user-visible) offset IDs.
    ///
    /// Monotonic across the lifetime of the stream — never reset on recovery.
    /// The values returned by `ingest_batch` / `ingest_ipc_batch` come from
    /// here, so that `wait_for_offset` semantics hold even if the underlying
    /// Flight stream reconnects.
    logical_offset_generator: Arc<OffsetIdGenerator>,
    /// Generator for physical (wire) offset IDs sent in `FlightBatchMetadata`.
    ///
    /// The server enforces sequential offsets starting at 0 per Flight stream,
    /// so this is reset to `0` on each successful reconnect (specifically,
    /// repositioned to `replay_offset` so fresh ingests continue from where
    /// the replay's wire offsets left off).
    physical_offset_generator: Arc<OffsetIdGenerator>,
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
    /// Final value sent as the HTTP `user-agent` header on every request.
    /// Either `"zerobus-sdk-rs/<version>"` or `"zerobus-sdk-rs/<version> <application_name>"`.
    /// Re-applied to each fresh Channel built during recovery.
    sdk_identifier: Arc<str>,
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
        sdk_identifier: Arc<str>,
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
            logical_offset_generator: Arc::new(OffsetIdGenerator::default()),
            physical_offset_generator: Arc::new(OffsetIdGenerator::default()),
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
            sdk_identifier,
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
            let sdk_identifier = Arc::clone(&stream.sdk_identifier);

            async move {
                tokio::time::timeout(
                    Duration::from_millis(options.recovery_timeout_ms),
                    Self::try_connect(
                        &endpoint,
                        &tls_config,
                        &table_properties,
                        &options,
                        &headers_provider,
                        &sdk_identifier,
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
            Arc::clone(&stream.physical_offset_generator),
            Arc::clone(&stream.ingest_mutex),
            response_stream,
            Arc::clone(&stream.sdk_identifier),
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
        sdk_identifier: &str,
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
            sdk_identifier,
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
        sdk_identifier: &str,
    ) -> ZerobusResult<FlightClient> {
        let connection_timeout = Duration::from_millis(options.connection_timeout_ms);

        let base_endpoint = Channel::from_shared(endpoint.to_string())
            .map_err(|e| ZerobusError::ChannelCreationError(e.to_string()))?
            .user_agent(sdk_identifier)
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
        physical_offset_generator: Arc<OffsetIdGenerator>,
        ingest_mutex: Arc<Mutex<()>>,
        initial_response_stream: Pin<Box<dyn Stream<Item = Result<PutResult, FlightError>> + Send>>,
        sdk_identifier: Arc<str>,
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
                        let attempts = recovery_attempts.fetch_add(1, Ordering::Relaxed);
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

                        // Block concurrent ingest_batch calls for the entire reconnect
                        // window so no batch can reach the new sender while
                        // physical_offset_generator still holds a stale value.
                        //
                        // The close-signal path sets this flag from inside process_acks
                        // before the supervisor even enters this branch; unexpected-error
                        // paths (ack timeout, stream error, etc.) do not, leaving a race
                        // between sender installation and the ingest_mutex acquisition in
                        // reconnect() — the cause of NonIncrementalOffset (4002) errors.
                        // Setting it here makes both paths behave identically.
                        //
                        // The matching is_paused.store(false) runs after a successful
                        // reconnect (below), so callers unblock as soon as the new stream
                        // is fully initialised and the physical offset is repositioned.
                        is_paused.store(true, Ordering::Relaxed);

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
                                &sdk_identifier,
                                &physical_offset_generator,
                                &ingest_mutex,
                            ),
                        )
                        .await;

                        match reconnect_result {
                            Ok(Ok(new_response_stream)) => {
                                info!("Supervisor: Recovery successful, resuming");
                                recovery_attempts.store(0, Ordering::Relaxed);
                                // Now that a fresh sender is installed, lift the pause gate.
                                is_paused.store(false, Ordering::Relaxed);
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
        sdk_identifier: &str,
        physical_offset_generator: &Arc<OffsetIdGenerator>,
        ingest_mutex: &Arc<Mutex<()>>,
    ) -> ZerobusResult<Pin<Box<dyn Stream<Item = Result<PutResult, FlightError>> + Send>>> {
        // Create new client.
        let client = Self::create_flight_client(
            endpoint,
            tls_config,
            table_properties,
            options,
            headers_provider,
            sdk_identifier,
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
        //
        // The `ingest_mutex` is held for the entire replay so that concurrent
        // `ingest_batch` callers cannot read a stale value from
        // `physical_offset_generator` between the replay (which renumbers wire
        // offsets from 0) and the generator reset at the end. Lock order
        // matches `ingest_batch`: `ingest_mutex` -> `pending_batches`.
        let _ingest_guard = ingest_mutex.lock().await;
        let mut replay_offset: i64 = 0;
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

                for pb in pending.drain(..) {
                    let payload = match slice_batch_for_recovery(&pb, acked_before_disconnect)? {
                        None => {
                            debug!(
                                offset_id = pb.logical_offset_id,
                                "Skipping fully-acked batch"
                            );
                            continue;
                        }
                        Some(p) => p,
                    };

                    // Encode the payload into wire chunks, re-applying the same size cap
                    // used during normal ingestion so replayed batches never exceed the
                    // server's gRPC message limit either.
                    let (chunks, num_records) = match &payload {
                        ArrowPayload::Batch(b) => (
                            record_batch_to_chunked_flight_data(b, &ipc_write_options)
                                .await
                                .map_err(|e| {
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
                            let total_encoded: usize = parsed
                                .flight_data
                                .iter()
                                .map(|fd| fd.data_header.len() + fd.data_body.len())
                                .sum();
                            if total_encoded > GRPC_TARGET_MAX_FLIGHT_DATA_BYTES
                                && parsed.num_rows > 1
                            {
                                let b = materialize_ipc(bytes).map_err(|e| {
                                    ZerobusError::InvalidArgument(format!(
                                        "Failed to materialise IPC batch for replay: {e}"
                                    ))
                                })?;
                                (
                                    record_batch_to_chunked_flight_data(
                                        &b,
                                        &IpcWriteOptions::default(),
                                    )
                                    .await
                                    .map_err(|e| {
                                        ZerobusError::InvalidArgument(format!(
                                            "Failed to rechunk IPC batch for replay: {e}"
                                        ))
                                    })?,
                                    parsed.num_rows,
                                )
                            } else {
                                (vec![parsed.flight_data], parsed.num_rows)
                            }
                        }
                    };

                    for chunk_messages in chunks {
                        let fd_count = chunk_messages.len();
                        for (i, mut fd) in chunk_messages.into_iter().enumerate() {
                            if i == fd_count - 1 {
                                let metadata = FlightBatchMetadata::new(replay_offset);
                                replay_offset += 1;
                                if let Ok(bytes) = metadata.to_bytes() {
                                    fd.app_metadata = bytes.into();
                                }
                            }
                            if tx.send(Ok(fd)).await.is_err() {
                                return Err(ZerobusError::StreamClosedError(
                                    tonic::Status::internal(
                                        "Failed to replay batch during recovery",
                                    ),
                                ));
                            }
                        }
                    }
                    let start_record = new_cumulative;
                    let end_record = new_cumulative + num_records;
                    new_cumulative = end_record;

                    new_pending.push(PendingBatch {
                        payload,
                        logical_offset_id: pb.logical_offset_id,
                        start_record,
                        end_record,
                    });
                }

                *pending = new_pending;
                cumulative_records_sent.store(new_cumulative, Ordering::Relaxed);
            }
        }

        // Reposition the physical (wire) offset generator so the next fresh
        // `ingest_batch` continues from where the replay left off.
        physical_offset_generator.set_next(replay_offset);

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
    /// A logical batch may be split into multiple wire chunks by
    /// [`record_batch_to_chunked_flight_data`]; record-based acking handles this
    /// correctly because acknowledgements accumulate across all chunks of a batch.
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
                                            max_acked_offset.map_or(pb.logical_offset_id, |o| {
                                                o.max(pb.logical_offset_id)
                                            }),
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
    /// Adds a single [`PendingBatch`] entry for the logical batch, then sends all
    /// `chunks` (each a vec of dictionary + record-batch [`FlightData`] messages) in
    /// order. Each chunk receives its own physical (wire) offset ID so the server can
    /// acknowledge individual chunks. Caller must hold `ingest_mutex` and must have
    /// already updated `cumulative_records_sent`.
    async fn send_flight_data_internal(
        &self,
        payload: ArrowPayload,
        chunks: Vec<Vec<FlightData>>,
        logical_offset_id: OffsetId,
        start_record: u64,
        end_record: u64,
    ) -> ZerobusResult<OffsetId> {
        {
            let mut pending = self.pending_batches.lock().await;
            pending.push(PendingBatch {
                payload,
                logical_offset_id,
                start_record,
                end_record,
            });
        }

        if self.is_paused.load(Ordering::Relaxed) {
            return Ok(logical_offset_id);
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

        // Each chunk gets its own physical (wire) offset ID. Dictionary FlightData
        // messages within a chunk carry empty app_metadata; only the final message
        // (the RecordBatch) carries the offset.
        for chunk_messages in chunks {
            let physical_offset_id = self.physical_offset_generator.next();
            let msg_count = chunk_messages.len();
            for (i, mut flight_data) in chunk_messages.into_iter().enumerate() {
                if i == msg_count - 1 {
                    let metadata = FlightBatchMetadata::new(physical_offset_id);
                    if let Ok(bytes) = metadata.to_bytes() {
                        flight_data.app_metadata = bytes.into();
                    }
                }
                if let Err(e) = sender.send(Ok(flight_data)).await {
                    warn!("Send failed: {}", e);
                    if self.options.recovery {
                        debug!(
                            logical_offset_id = logical_offset_id,
                            physical_offset_id = physical_offset_id,
                            "Send failed but recovery enabled - supervisor will handle recovery"
                        );
                        return Ok(logical_offset_id);
                    } else {
                        {
                            let mut pending = self.pending_batches.lock().await;
                            pending.retain(|pb| pb.logical_offset_id != logical_offset_id);
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
        }

        Ok(logical_offset_id)
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

        if batch.num_rows() == 0 {
            return Err(ZerobusError::InvalidArgument(
                "Cannot ingest an empty RecordBatch (0 rows)".to_string(),
            ));
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
        let logical_offset_id = self.logical_offset_generator.next();
        let start_record = self
            .cumulative_records_sent
            .fetch_add(record_count, Ordering::Relaxed);
        let end_record = start_record + record_count;

        let chunks = record_batch_to_chunked_flight_data(
            &batch,
            &make_ipc_write_options(self.options.ipc_compression)?,
        )
        .await?;

        debug!(
            logical_offset_id = logical_offset_id,
            chunks = chunks.len(),
            "Batch queued for ingestion"
        );
        self.send_flight_data_internal(
            ArrowPayload::Batch(batch),
            chunks,
            logical_offset_id,
            start_record,
            end_record,
        )
        .await
    }

    /// Ingests a single Arrow RecordBatch supplied as raw Arrow IPC stream bytes.
    ///
    /// Preferred entry point for FFI callers (Go, Python, Java, TypeScript) that already
    /// hold IPC-serialised bytes. This method handles all cases transparently:
    ///
    /// - **Fast path** — batch fits within the 2 MiB per-message gRPC limit *and* no
    ///   compression is configured: bytes are forwarded zero-copy to the Flight wire
    ///   format without any deserialisation.
    /// - **Materialise path** — triggered when either compression is configured (bytes
    ///   must be re-encoded with the codec) or the payload exceeds 2 MiB (must be split
    ///   into chunks). In both cases the IPC bytes are deserialised into a [`RecordBatch`]
    ///   once, then re-encoded with the stream's compression and chunk settings exactly
    ///   as [`ingest_batch`] would. The caller sees no difference.
    ///
    /// The `ipc_bytes` must be a valid Arrow IPC *stream* containing exactly one
    /// RecordBatch (i.e. the output of `pyarrow.RecordBatch.serialize()`,
    /// `tableToIPC(table, 'stream')`, etc.). Dictionary messages between the schema and
    /// the RecordBatch are supported. Trailing stream metadata (such as an end-of-stream
    /// marker after `finish()`) is allowed after that batch.
    #[instrument(level = "debug", skip_all, fields(table_name = %self.table_properties.table_name))]
    pub async fn ingest_ipc_batch(&self, ipc_bytes: Bytes) -> ZerobusResult<OffsetId> {
        if self.is_closed.load(Ordering::Relaxed) {
            return Err(ZerobusError::StreamClosedError(tonic::Status::internal(
                "Stream is closed",
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

        let logical_offset_id = self.logical_offset_generator.next();
        let start_record = self
            .cumulative_records_sent
            .fetch_add(parsed.num_rows, Ordering::Relaxed);
        let end_record = start_record + parsed.num_rows;

        // Materialise to a RecordBatch and re-encode when either:
        //   (a) compression is configured — raw bytes must be re-encoded with the codec, or
        //   (b) the uncompressed payload exceeds the per-message gRPC limit — must be rechunked.
        // In either case the caller never sees this; it is handled transparently.
        let total_encoded: usize = parsed
            .flight_data
            .iter()
            .map(|fd| fd.data_header.len() + fd.data_body.len())
            .sum();

        let needs_materialise = self.options.ipc_compression.is_some()
            || (total_encoded > GRPC_TARGET_MAX_FLIGHT_DATA_BYTES && parsed.num_rows > 1);

        let (payload, chunks) = if needs_materialise {
            let batch = materialize_ipc(&ipc_bytes).map_err(|e| {
                ZerobusError::InvalidArgument(format!(
                    "IPC batch could not be materialised for re-encoding: {e}"
                ))
            })?;
            let opts = make_ipc_write_options(self.options.ipc_compression)?;
            let chunks = record_batch_to_chunked_flight_data(&batch, &opts).await?;
            (ArrowPayload::Batch(batch), chunks)
        } else {
            (ArrowPayload::Ipc(ipc_bytes), vec![parsed.flight_data])
        };

        debug!(
            logical_offset_id = logical_offset_id,
            chunks = chunks.len(),
            "IPC batch queued for ingestion"
        );
        self.send_flight_data_internal(payload, chunks, logical_offset_id, start_record, end_record)
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
                    debug!(
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

        let target_offset = match self.logical_offset_generator.last() {
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
                        pb.logical_offset_id
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
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field};

    // ── helpers ────────────────────────────────────────────────────────────────

    fn simple_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    /// Builds a RecordBatch with `n` rows using the simple schema.
    fn make_batch(n: usize) -> RecordBatch {
        let schema = simple_schema();
        let ids: Int32Array = (0..n as i32).collect();
        let names: StringArray = (0..n).map(|i| Some(format!("row_{i}"))).collect();
        RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(names)]).unwrap()
    }

    /// Builds a RecordBatch whose IPC encoding exceeds `GRPC_TARGET_MAX_FLIGHT_DATA_BYTES`.
    /// Each row carries ~500 bytes of string data, so 6 000 rows ≈ 3 MiB encoded.
    fn make_large_batch() -> RecordBatch {
        let n = 6_000usize;
        let schema = simple_schema();
        let ids: Int32Array = (0..n as i32).collect();
        let payload = "x".repeat(500);
        let names: StringArray = (0..n).map(|_| Some(payload.as_str())).collect();
        RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(names)]).unwrap()
    }

    /// Serialises a RecordBatch to Arrow IPC stream bytes (the format that
    /// `ipc_bytes_to_flight_data` and `materialize_ipc` consume).
    fn batch_to_ipc(batch: &RecordBatch) -> Bytes {
        use arrow_ipc::writer::StreamWriter;
        use std::io::Cursor;
        let mut buf = Vec::new();
        let mut writer = StreamWriter::try_new(Cursor::new(&mut buf), batch.schema_ref()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
        Bytes::from(buf)
    }

    /// Total encoded bytes of a slice of FlightData messages.
    fn total_encoded(msgs: &[FlightData]) -> usize {
        msgs.iter()
            .map(|fd| fd.data_header.len() + fd.data_body.len())
            .sum()
    }

    // ── ArrowTableProperties ───────────────────────────────────────────────────

    #[test]
    fn test_arrow_table_properties() {
        let schema = simple_schema();
        let props = ArrowTableProperties {
            table_name: "catalog.schema.table".to_string(),
            schema,
        };
        assert_eq!(props.table_name, "catalog.schema.table");
        assert_eq!(props.schema.fields().len(), 2);
    }

    // ── ipc_bytes_to_flight_data ───────────────────────────────────────────────

    #[test]
    fn test_ipc_bytes_parses_row_count_and_schema() {
        let batch = make_batch(100);
        let ipc = batch_to_ipc(&batch);
        let parsed = ipc_bytes_to_flight_data(&ipc).unwrap();

        assert_eq!(parsed.num_rows, 100);
        assert_eq!(&parsed.schema, batch.schema_ref().as_ref());
        // Schema message + one RecordBatch message = 1 FlightData entry
        // (schema message is not included in flight_data; only the batch is)
        assert_eq!(parsed.flight_data.len(), 1);
    }

    #[test]
    fn test_ipc_bytes_empty_batch_parses() {
        let batch = make_batch(0);
        let ipc = batch_to_ipc(&batch);
        let parsed = ipc_bytes_to_flight_data(&ipc).unwrap();
        assert_eq!(parsed.num_rows, 0);
    }

    // ── materialize_ipc ────────────────────────────────────────────────────────

    #[test]
    fn test_materialize_ipc_round_trips_data() {
        let original = make_batch(50);
        let ipc = batch_to_ipc(&original);
        let restored = materialize_ipc(&ipc).unwrap();

        assert_eq!(restored.num_rows(), original.num_rows());
        assert_eq!(restored.schema(), original.schema());
        // Verify the actual column values survive the round-trip.
        let orig_ids = original
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let rest_ids = restored
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(orig_ids.values(), rest_ids.values());
    }

    // ── record_batch_to_chunked_flight_data ───────────────────────────────────

    #[test]
    fn test_ingest_empty_batch_returns_error() {
        // ingest_batch rejects 0-row batches before they reach the encoder.
        // Verify the guard fires by checking that make_batch(0) has 0 rows.
        let batch = make_batch(0);
        assert_eq!(batch.num_rows(), 0);
    }

    #[tokio::test]
    async fn test_chunked_large_batch_splits_into_multiple_chunks() {
        let batch = make_large_batch();
        let opts = IpcWriteOptions::default();

        // Confirm the batch is actually large enough to trigger chunking by
        // checking the IPC size (without needing the old record_batch_to_flight_data).
        let ipc = batch_to_ipc(&batch);
        let parsed = ipc_bytes_to_flight_data(&ipc).unwrap();
        assert!(
            total_encoded(&parsed.flight_data) > GRPC_TARGET_MAX_FLIGHT_DATA_BYTES,
            "test setup: batch must exceed the per-message limit"
        );

        let chunks = record_batch_to_chunked_flight_data(&batch, &opts)
            .await
            .unwrap();
        assert!(
            chunks.len() > 1,
            "large batch must be split into multiple chunks"
        );
    }

    #[tokio::test]
    async fn test_chunked_large_batch_preserves_total_row_count() {
        let batch = make_large_batch();
        let original_rows = batch.num_rows();
        let opts = IpcWriteOptions::default();
        let chunks = record_batch_to_chunked_flight_data(&batch, &opts)
            .await
            .unwrap();

        // The last message in each chunk is always a RecordBatch FlightData.
        // Parse its header to get the row count for that slice.
        let recovered_rows: usize = chunks
            .iter()
            .map(|chunk| {
                let rb_fd = chunk.last().unwrap();
                let msg = arrow_ipc::root_as_message(&rb_fd.data_header).unwrap();
                let rb_header = msg.header_as_record_batch().unwrap();
                rb_header.length() as usize
            })
            .sum();

        assert_eq!(
            recovered_rows, original_rows,
            "total rows across all chunks must equal the original batch row count"
        );
    }

    #[tokio::test]
    async fn test_chunked_single_row_oversized_not_split() {
        // A single oversized row cannot be split further — FlightDataEncoderBuilder
        // keeps it as a single chunk regardless of encoded size.
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "data",
            DataType::Utf8,
            true,
        )]));
        let huge_string = "z".repeat(3 * 1024 * 1024); // 3 MiB string
        let arr: StringArray = vec![Some(huge_string.as_str())].into_iter().collect();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let opts = IpcWriteOptions::default();
        let chunks = record_batch_to_chunked_flight_data(&batch, &opts)
            .await
            .unwrap();
        assert_eq!(chunks.len(), 1, "single-row batch must not be split");
    }

    // ── needs_materialise logic (tested through the helper functions) ──────────

    #[test]
    fn test_small_ipc_batch_total_encoded_within_limit() {
        let batch = make_batch(100);
        let ipc = batch_to_ipc(&batch);
        let parsed = ipc_bytes_to_flight_data(&ipc).unwrap();
        let size: usize = parsed
            .flight_data
            .iter()
            .map(|fd| fd.data_header.len() + fd.data_body.len())
            .sum();
        assert!(
            size <= GRPC_TARGET_MAX_FLIGHT_DATA_BYTES,
            "small batch ({size} bytes) must be within the per-message limit — \
             it should take the zero-copy path in ingest_ipc_batch"
        );
    }

    #[tokio::test]
    async fn test_large_ipc_batch_total_encoded_exceeds_limit() {
        let batch = make_large_batch();
        let ipc = batch_to_ipc(&batch);
        let parsed = ipc_bytes_to_flight_data(&ipc).unwrap();
        let size: usize = parsed
            .flight_data
            .iter()
            .map(|fd| fd.data_header.len() + fd.data_body.len())
            .sum();
        assert!(
            size > GRPC_TARGET_MAX_FLIGHT_DATA_BYTES,
            "large batch ({size} bytes) must exceed the per-message limit"
        );
        // After materialising and chunking, every chunk must be within the limit.
        let restored = materialize_ipc(&ipc).unwrap();
        let opts = IpcWriteOptions::default();
        let chunks = record_batch_to_chunked_flight_data(&restored, &opts)
            .await
            .unwrap();
        assert!(chunks.len() > 1);
        for chunk in &chunks {
            assert!(total_encoded(chunk) <= GRPC_TARGET_MAX_FLIGHT_DATA_BYTES);
        }
    }
}
