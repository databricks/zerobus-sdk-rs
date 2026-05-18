//! Zerobus TypeScript SDK — NAPI-RS bindings.
//!
//! This module is the low-level FFI layer. The published TS API lives in
//! `src/index.ts`; treat the names exported here as **internal**. They can be
//! reshaped freely as long as the TS facade stays stable.
//!
//! Design notes:
//! - The SDK sets its identity via `ZerobusSdkBuilder::sdk_identifier`
//!   (`zerobus-sdk-ts/<version>`) at construction time. Headers providers do
//!   not need to inject the SDK marker; the tonic endpoint owns `user-agent`.
//! - `NoTlsConfig` (feature `testing` on the Rust SDK) is always enabled here
//!   so the TS SDK can connect to plaintext local endpoints.
//! - Arrow ingestion uses the zero-copy `ingest_ipc_batch` path whenever
//!   compression is off; the SDK falls back to the parsed-RecordBatch path
//!   when a codec is set (raw bytes can't satisfy a codec on the wire).

#![deny(clippy::all)]

use async_trait::async_trait;
use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ErrorStrategy, ThreadsafeFunction};
use napi::{Env, JsFunction, JsGlobal, JsObject, JsString, JsUnknown, ValueType};
use napi_derive::napi;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use databricks_zerobus_ingest_sdk::databricks::zerobus::RecordType as RustRecordType;
use databricks_zerobus_ingest_sdk::{
    EncodedRecord as RustRecordPayload, HeadersProvider as RustHeadersProvider, NoTlsConfig,
    SecureTlsConfig, TlsConfig, ZerobusError as RustZerobusError,
    ZerobusResult as RustZerobusResult, ZerobusSdk as RustZerobusSdk,
    ZerobusStream as RustZerobusStream,
};

#[cfg(feature = "arrow-flight")]
use bytes::Bytes;
#[cfg(feature = "arrow-flight")]
use databricks_zerobus_ingest_sdk::{
    ArrowSchema as RustArrowSchema, DataType as RustDataType, Field as RustField,
    RecordBatch as RustRecordBatch, ZerobusArrowStream as RustZerobusArrowStream,
};

/// SDK identifier prefix used as the `user-agent` HTTP header (after the
/// Rust SDK appends an optional application name). Set per-SDK at build time
/// so server-side telemetry can attribute traffic to the TypeScript wrapper.
const TS_SDK_IDENTIFIER: &str = concat!("zerobus-sdk-ts/", env!("CARGO_PKG_VERSION"));

// =============================================================================
// Shared option/type structs (passed across the FFI boundary as JS objects)
// =============================================================================

/// Record serialization format for gRPC streams.
#[napi]
pub enum RecordType {
    Json = 0,
    Proto = 1,
}

/// Wire-level options for a JSON/proto stream. All fields are optional; the
/// Rust SDK supplies defaults.
#[napi(object)]
#[derive(Debug, Clone, Default)]
pub struct StreamConfigurationOptions {
    pub max_inflight_requests: Option<u32>,
    pub recovery: Option<bool>,
    pub recovery_timeout_ms: Option<u32>,
    pub recovery_backoff_ms: Option<u32>,
    pub recovery_retries: Option<u32>,
    pub flush_timeout_ms: Option<u32>,
    pub server_lack_of_ack_timeout_ms: Option<u32>,
    pub record_type: Option<i32>,
    pub stream_paused_max_wait_time_ms: Option<u32>,
}

#[napi(object)]
#[derive(Debug, Clone)]
pub struct TableProperties {
    pub table_name: String,
    /// Base64-encoded `DescriptorProto` for protobuf-format streams. Required
    /// when `recordType = Proto`; ignored for JSON.
    pub descriptor_proto: Option<String>,
}

/// JS-side callback that yields headers as `[name, value]` tuples. The
/// `authorization` and `x-databricks-zerobus-table-name` headers are required.
#[napi(object)]
pub struct JsHeadersProvider {
    #[napi(ts_type = "() => Promise<Array<[string, string]>>")]
    pub get_headers_callback: JsFunction,
}

/// Top-level SDK configuration. Constructed once per process; cheap to clone
/// internally (handles share a single tonic channel + tokio runtime).
#[napi(object)]
#[derive(Debug, Clone, Default)]
pub struct SdkOptions {
    pub endpoint: String,
    pub unity_catalog_url: Option<String>,
    /// `true` (default) → system CAs via `SecureTlsConfig`. `false` →
    /// `NoTlsConfig` for plaintext `http://` endpoints (local dev / sidecar
    /// proxy).
    pub use_tls: Option<bool>,
    /// Appended after the SDK prefix in the `user-agent` HTTP header. Common
    /// pattern: `"<product>/<version>"`.
    pub application_name: Option<String>,
    /// Replaces the SDK prefix in the `user-agent` header. Defaults to
    /// `zerobus-sdk-ts/<version>`; rarely overridden by callers.
    pub sdk_identifier: Option<String>,
}

#[napi]
pub struct ZerobusError {
    message: String,
    is_retryable: bool,
}

#[napi]
impl ZerobusError {
    #[napi(getter)]
    pub fn is_retryable(&self) -> bool {
        self.is_retryable
    }

    #[napi(getter)]
    pub fn message(&self) -> String {
        self.message.clone()
    }
}

// =============================================================================
// Helpers
// =============================================================================

/// Convert a JS `BigInt` to `i64`, erroring if the value cannot be represented
/// without loss. Used for offset IDs that can exceed `Number.MAX_SAFE_INTEGER`
/// (2^53 - 1) yet must fit an `i64` on the Rust side.
fn bigint_to_i64(value: BigInt) -> Result<i64> {
    let (n, lossless) = value.get_i64();
    if !lossless {
        return Err(Error::from_reason(
            "offsetId exceeds i64 range; cannot be represented without loss",
        ));
    }
    Ok(n)
}

fn convert_js_to_record_payload(env: &Env, payload: Unknown) -> Result<RustRecordPayload> {
    let value_type = payload.get_type()?;

    match value_type {
        ValueType::Object => {
            let js_value: JsUnknown = payload.try_into()?;
            // Buffer (Node `Buffer`) is the historical fast path.
            if js_value.is_buffer()? {
                let buffer: Buffer = Buffer::from_unknown(js_value)?;
                return Ok(RustRecordPayload::Proto(buffer.to_vec()));
            }
            // Accept `Uint8Array` too. The other typed-array variants
            // (Int8Array, Int16Array, …) carry semantically different element
            // widths so we reject them rather than reinterpret silently.
            if js_value.is_typedarray()? {
                let arr = Uint8Array::from_unknown(js_value).map_err(|_| {
                    Error::from_reason(
                        "payload must be a Buffer or Uint8Array; other typed arrays \
                         (Int8Array, Int16Array, …) carry differently-sized elements \
                         and are rejected to avoid silent reinterpretation",
                    )
                })?;
                return Ok(RustRecordPayload::Proto(arr.to_vec()));
            }

            let obj: JsObject = JsObject::from_unknown(js_value)?;
            let constructor: JsFunction = obj.get_named_property("constructor")?;
            let constructor_obj = JsObject::from_unknown(constructor.into_unknown())?;

            if constructor_obj.has_named_property("encode")? {
                let encode_fn: JsFunction = constructor_obj.get_named_property("encode")?;
                let obj_as_unknown = obj.into_unknown();
                let encode_result: JsUnknown =
                    encode_fn.call::<JsUnknown>(Some(&constructor_obj), &[obj_as_unknown])?;
                let encode_obj = JsObject::from_unknown(encode_result)?;

                if encode_obj.has_named_property("finish")? {
                    let finish_fn: JsFunction = encode_obj.get_named_property("finish")?;
                    let buffer_result: JsUnknown =
                        finish_fn.call::<JsUnknown>(Some(&encode_obj), &[])?;
                    let buffer: Buffer = Buffer::from_unknown(buffer_result)?;
                    Ok(RustRecordPayload::Proto(buffer.to_vec()))
                } else {
                    Err(Error::from_reason(
                        "Protobuf message .encode() must return an object with .finish()",
                    ))
                }
            } else {
                let global: JsGlobal = env.get_global()?;
                let json_obj: JsObject = global.get_named_property("JSON")?;
                let stringify: JsFunction = json_obj.get_named_property("stringify")?;
                let obj_as_unknown = obj.into_unknown();
                let str_result: JsUnknown =
                    stringify.call::<JsUnknown>(Some(&json_obj), &[obj_as_unknown])?;
                let js_string = JsString::from_unknown(str_result)?;
                let json_string = js_string.into_utf8()?.as_str()?.to_string();

                Ok(RustRecordPayload::Json(json_string))
            }
        }
        ValueType::String => {
            let js_value: JsUnknown = payload.try_into()?;
            let js_string = JsString::from_unknown(js_value)?;
            let json_string = js_string.into_utf8()?.as_str()?.to_string();
            Ok(RustRecordPayload::Json(json_string))
        }
        _ => Err(Error::from_reason(
            "Payload must be a Buffer, string, protobuf message, or plain object",
        )),
    }
}

fn base64_decode(input: &str) -> std::result::Result<Vec<u8>, String> {
    use base64::{engine::general_purpose::STANDARD, Engine};
    STANDARD
        .decode(input)
        .map_err(|e| format!("Base64 decode error: {}", e))
}

fn decode_descriptor_proto(b64: &str) -> Result<prost_types::DescriptorProto> {
    let bytes =
        base64_decode(b64).map_err(|e| Error::from_reason(format!("descriptor: {}", e)))?;
    prost::Message::decode(&bytes[..])
        .map_err(|e| Error::from_reason(format!("Failed to parse descriptor proto: {}", e)))
}

fn create_headers_tsfn(
    js_func: JsFunction,
) -> Result<ThreadsafeFunction<(), ErrorStrategy::Fatal>> {
    js_func.create_threadsafe_function(0, |ctx| Ok(vec![ctx.value]))
}

async fn call_headers_tsfn(
    tsfn: ThreadsafeFunction<(), ErrorStrategy::Fatal>,
) -> Result<Vec<(String, String)>> {
    // The JS callback is expected to return a Promise<Array<[string, string]>>.
    // napi-rs 2.x doesn't auto-await Promises, so we ask for `Promise<...>` and
    // await it ourselves.
    let promise: Promise<Vec<Vec<String>>> = tsfn
        .call_async(())
        .await
        .map_err(|e| Error::from_reason(format!("Headers callback failed: {}", e)))?;
    let raw_headers = promise
        .await
        .map_err(|e| Error::from_reason(format!("Headers callback rejected: {}", e)))?;

    // Validate every pair is exactly [name, value]. Silently dropping
    // malformed entries used to lead to confusing "missing authorization"
    // errors downstream when the user just typo'd a tuple shape.
    raw_headers
        .into_iter()
        .map(|pair| {
            if pair.len() != 2 {
                return Err(Error::from_reason(format!(
                    "headers callback returned a tuple with {} elements; expected exactly [name, value]",
                    pair.len()
                )));
            }
            let mut it = pair.into_iter();
            Ok((it.next().unwrap(), it.next().unwrap()))
        })
        .collect()
}

/// Canonical headers the Rust SDK requires. Interned as `&'static str` so
/// the common case doesn't pay the `Box::leak` price on every stream open.
const HEADER_AUTHORIZATION: &str = "authorization";
const HEADER_TABLE_NAME: &str = "x-databricks-zerobus-table-name";

/// Canonicalize a header name to its `&'static str` interned form when known,
/// otherwise leak the lowercased name. HTTP/gRPC metadata is case-insensitive
/// on the wire so we lowercase before comparison.
fn canonical_header_name(name: &str) -> &'static str {
    let lower = name.to_ascii_lowercase();
    match lower.as_str() {
        "authorization" => HEADER_AUTHORIZATION,
        "x-databricks-zerobus-table-name" => HEADER_TABLE_NAME,
        // Uncommon: a custom header the SDK happens to forward. Leak the
        // lowercased name so it has the `'static` lifetime the trait wants.
        _ => Box::leak(lower.into_boxed_str()),
    }
}

/// Internal headers provider that returns a fixed set of headers — the result
/// of calling the user-supplied JS callback exactly once at stream creation.
/// The SDK reconnects do not re-invoke the callback because the
/// `HeadersProvider` trait is per-stream, not per-attempt.
struct StaticHeadersProvider {
    headers: HashMap<&'static str, String>,
}

impl StaticHeadersProvider {
    fn new(headers: Vec<(String, String)>) -> RustZerobusResult<Self> {
        let mut map = HashMap::new();
        for (k, v) in headers {
            map.insert(canonical_header_name(&k), v);
        }
        if !map.contains_key(HEADER_AUTHORIZATION) {
            return Err(RustZerobusError::InvalidArgument(
                "HeadersProvider must include 'authorization' header".to_string(),
            ));
        }
        if !map.contains_key(HEADER_TABLE_NAME) {
            return Err(RustZerobusError::InvalidArgument(
                "HeadersProvider must include 'x-databricks-zerobus-table-name' header"
                    .to_string(),
            ));
        }
        Ok(Self { headers: map })
    }
}

#[async_trait]
impl RustHeadersProvider for StaticHeadersProvider {
    async fn get_headers(&self) -> RustZerobusResult<HashMap<&'static str, String>> {
        Ok(self.headers.clone())
    }
}

// =============================================================================
// ZerobusStream — gRPC stream (JSON / protobuf)
// =============================================================================

/// A bidirectional gRPC ingestion stream. Open via `ZerobusSdk.createStream`.
#[napi]
pub struct ZerobusStream {
    inner: Arc<Mutex<Option<RustZerobusStream>>>,
}

#[napi]
impl ZerobusStream {
    /// Queue one record and resolve to the offset ID as soon as it enters the
    /// SDK's landing zone. The promise resolves before the server ack.
    #[napi(ts_return_type = "Promise<bigint>")]
    pub fn ingest_record_offset(&self, env: Env, payload: Unknown) -> Result<JsObject> {
        let record_payload = convert_js_to_record_payload(&env, payload)?;
        let stream = self.inner.clone();

        env.execute_tokio_future(
            async move {
                let mut guard = stream.lock().await;
                let stream_ref = guard
                    .as_mut()
                    .ok_or_else(|| napi::Error::from_reason("Stream has been closed"))?;
                stream_ref
                    .ingest_record_offset(record_payload)
                    .await
                    .map_err(|e| napi::Error::from_reason(format!("Failed to ingest record: {}", e)))
            },
            |env, offset_id| {
                let global: JsGlobal = env.get_global()?;
                let bigint_ctor: JsFunction = global.get_named_property("BigInt")?;
                let js_str = env.create_string(&offset_id.to_string())?;
                bigint_ctor.call(None, &[js_str.into_unknown()])
            },
        )
    }

    /// Queue many records atomically. Resolves to the offset ID of the batch,
    /// or null for an empty batch.
    #[napi(ts_return_type = "Promise<bigint | null>")]
    pub fn ingest_records_offset(&self, env: Env, records: Vec<Unknown>) -> Result<JsObject> {
        let record_payloads: Result<Vec<RustRecordPayload>> = records
            .into_iter()
            .map(|p| convert_js_to_record_payload(&env, p))
            .collect();
        let record_payloads = record_payloads?;
        let stream = self.inner.clone();

        env.execute_tokio_future(
            async move {
                let mut guard = stream.lock().await;
                let stream_ref = guard
                    .as_mut()
                    .ok_or_else(|| napi::Error::from_reason("Stream has been closed"))?;
                stream_ref
                    .ingest_records_offset(record_payloads)
                    .await
                    .map_err(|e| napi::Error::from_reason(format!("Failed to ingest batch: {}", e)))
            },
            |env, result| match result {
                Some(offset_id) => {
                    let global: JsGlobal = env.get_global()?;
                    let bigint_ctor: JsFunction = global.get_named_property("BigInt")?;
                    let js_str = env.create_string(&offset_id.to_string())?;
                    let bigint = bigint_ctor.call(None, &[js_str.into_unknown()])?;
                    Ok(bigint.into_unknown())
                }
                None => env.get_null().map(|v| v.into_unknown()),
            },
        )
    }

    /// Block until the server acks `offsetId`. Errors that close the stream
    /// short-circuit the wait.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn wait_for_offset(&self, env: Env, offset_id: BigInt) -> Result<JsObject> {
        let offset = bigint_to_i64(offset_id)?;
        let stream = self.inner.clone();
        env.execute_tokio_future(
            async move {
                let guard = stream.lock().await;
                let stream_ref = guard
                    .as_ref()
                    .ok_or_else(|| napi::Error::from_reason("Stream has been closed"))?;
                stream_ref
                    .wait_for_offset(offset)
                    .await
                    .map_err(|e| napi::Error::from_reason(format!("Failed to wait for offset: {}", e)))
            },
            |_env, _| Ok(()),
        )
    }

    #[napi]
    pub async fn flush(&self) -> Result<()> {
        let guard = self.inner.lock().await;
        let stream = guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Stream has been closed"))?;
        stream
            .flush()
            .await
            .map_err(|e| Error::from_reason(format!("Failed to flush stream: {}", e)))
    }

    #[napi]
    pub async fn close(&self) -> Result<()> {
        let mut guard = self.inner.lock().await;
        if let Some(mut stream) = guard.take() {
            stream
                .close()
                .await
                .map_err(|e| Error::from_reason(format!("Failed to close stream: {}", e)))?;
        }
        Ok(())
    }

    /// Return unacked records as IPC-encoded buffers (for recovery). Each
    /// inner Buffer is one record, ready to be re-ingested.
    #[napi]
    pub async fn get_unacked_records(&self) -> Result<Vec<Buffer>> {
        let guard = self.inner.lock().await;
        let stream = guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Stream has been closed"))?;
        let records = stream
            .get_unacked_records()
            .await
            .map_err(|e| Error::from_reason(format!("Failed to get unacked records: {}", e)))?;
        Ok(records
            .into_iter()
            .map(|r| match r {
                RustRecordPayload::Proto(b) => Buffer::from(b),
                RustRecordPayload::Json(s) => Buffer::from(s.into_bytes()),
            })
            .collect())
    }

    /// Return unacked batches grouped by batch. Each outer entry is one
    /// batch; each inner Buffer is one record within that batch.
    #[napi]
    pub async fn get_unacked_batches(&self) -> Result<Vec<Vec<Buffer>>> {
        let guard = self.inner.lock().await;
        let stream = guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Stream has been closed"))?;
        let batches = stream
            .get_unacked_batches()
            .await
            .map_err(|e| Error::from_reason(format!("Failed to get unacked batches: {}", e)))?;
        Ok(batches
            .into_iter()
            .map(|batch| {
                batch
                    .into_iter()
                    .map(|r| match r {
                        RustRecordPayload::Proto(b) => Buffer::from(b),
                        RustRecordPayload::Json(s) => Buffer::from(s.into_bytes()),
                    })
                    .collect()
            })
            .collect())
    }
}

// =============================================================================
// Arrow Flight (Beta) — feature `arrow-flight`
// =============================================================================

#[cfg(feature = "arrow-flight")]
#[napi]
pub enum IpcCompressionType {
    Lz4Frame = 0,
    Zstd = 1,
}

#[cfg(feature = "arrow-flight")]
#[napi(object)]
#[derive(Debug, Clone, Default)]
pub struct ArrowStreamConfigurationOptions {
    pub max_inflight_batches: Option<u32>,
    pub recovery: Option<bool>,
    pub recovery_timeout_ms: Option<u32>,
    pub recovery_backoff_ms: Option<u32>,
    pub recovery_retries: Option<u32>,
    pub server_lack_of_ack_timeout_ms: Option<u32>,
    pub flush_timeout_ms: Option<u32>,
    pub connection_timeout_ms: Option<u32>,
    pub stream_paused_max_wait_time_ms: Option<u32>,
    /// Arrow IPC compression. `Some(Lz4Frame)` or `Some(Zstd)` to enable;
    /// `None` (default) leaves the SDK on the zero-copy IPC forwarding path.
    pub ipc_compression: Option<i32>,
}

#[cfg(feature = "arrow-flight")]
#[napi]
pub enum ArrowDataType {
    Boolean = 0,
    Int8 = 1,
    Int16 = 2,
    Int32 = 3,
    Int64 = 4,
    UInt8 = 5,
    UInt16 = 6,
    UInt32 = 7,
    UInt64 = 8,
    Float32 = 9,
    Float64 = 10,
    Utf8 = 11,
    LargeUtf8 = 12,
    Binary = 13,
    LargeBinary = 14,
    Date32 = 15,
    Date64 = 16,
    TimestampMicros = 17,
    TimestampNanos = 18,
}

#[cfg(feature = "arrow-flight")]
fn convert_arrow_data_type(dt: i32) -> RustDataType {
    match dt {
        0 => RustDataType::Boolean,
        1 => RustDataType::Int8,
        2 => RustDataType::Int16,
        3 => RustDataType::Int32,
        4 => RustDataType::Int64,
        5 => RustDataType::UInt8,
        6 => RustDataType::UInt16,
        7 => RustDataType::UInt32,
        8 => RustDataType::UInt64,
        9 => RustDataType::Float32,
        10 => RustDataType::Float64,
        11 => RustDataType::Utf8,
        12 => RustDataType::LargeUtf8,
        13 => RustDataType::Binary,
        14 => RustDataType::LargeBinary,
        15 => RustDataType::Date32,
        16 => RustDataType::Date64,
        17 => RustDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
        18 => RustDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some("UTC".into())),
        _ => RustDataType::Utf8,
    }
}

#[cfg(feature = "arrow-flight")]
#[napi(object)]
#[derive(Debug, Clone)]
pub struct ArrowField {
    pub name: String,
    /// `ArrowDataType` enum value, passed as i32 across the FFI boundary.
    pub data_type: i32,
    pub nullable: Option<bool>,
}

#[cfg(feature = "arrow-flight")]
#[napi(object)]
#[derive(Debug, Clone)]
pub struct ArrowTableProperties {
    pub table_name: String,
    pub schema_fields: Vec<ArrowField>,
}

#[cfg(feature = "arrow-flight")]
fn build_arrow_schema(fields: &[ArrowField]) -> Arc<RustArrowSchema> {
    let rust_fields: Vec<RustField> = fields
        .iter()
        .map(|f| {
            RustField::new(
                &f.name,
                convert_arrow_data_type(f.data_type),
                f.nullable.unwrap_or(true),
            )
        })
        .collect();
    Arc::new(RustArrowSchema::new(rust_fields))
}

#[cfg(feature = "arrow-flight")]
fn map_compression(value: Option<i32>) -> Option<arrow_ipc::CompressionType> {
    match value {
        Some(0) => Some(arrow_ipc::CompressionType::LZ4_FRAME),
        Some(1) => Some(arrow_ipc::CompressionType::ZSTD),
        _ => None,
    }
}

#[cfg(feature = "arrow-flight")]
#[napi]
pub struct ZerobusArrowStream {
    inner: Arc<Mutex<Option<RustZerobusArrowStream>>>,
    schema: Arc<RustArrowSchema>,
    /// True when ipc_compression was configured. Forces the parse-then-encode
    /// path because raw IPC bytes can't satisfy a codec on the wire.
    has_compression: bool,
}

#[cfg(feature = "arrow-flight")]
fn parse_arrow_ipc_to_batch(
    ipc_buffer: &[u8],
    _expected_schema: &RustArrowSchema,
) -> Result<RustRecordBatch> {
    use arrow_ipc::reader::StreamReader;
    use std::io::Cursor;

    let cursor = Cursor::new(ipc_buffer);
    let mut reader = StreamReader::try_new(cursor, None)
        .map_err(|e| Error::from_reason(format!("Failed to parse Arrow IPC: {}", e)))?;

    // Exactly one RecordBatch per call. The Rust SDK's `ingest_batch` ingests
    // a single RecordBatch; previously we silently dropped subsequent batches
    // and swallowed per-batch decode errors via `filter_map(.ok())`, which
    // hid both data loss and corruption.
    let first = match reader.next() {
        Some(batch) => batch.map_err(|e| {
            Error::from_reason(format!("Failed to decode Arrow IPC record batch: {}", e))
        })?,
        None => {
            return Err(Error::from_reason(
                "Arrow IPC buffer contains no record batches",
            ));
        }
    };

    if let Some(next) = reader.next() {
        // Drain to surface a decode error on the trailing batch if there is
        // one, otherwise complain that the IPC contains too many batches.
        next.map_err(|e| {
            Error::from_reason(format!(
                "Failed to decode trailing Arrow IPC record batch: {}",
                e
            ))
        })?;
        return Err(Error::from_reason(
            "Arrow IPC buffer contains more than one record batch. \
             Call ingestBatch() once per RecordBatch — pass each batch \
             as a separate IPC stream.",
        ));
    }

    Ok(first)
}

#[cfg(feature = "arrow-flight")]
fn serialize_batch_to_ipc(batch: &RustRecordBatch) -> Result<Vec<u8>> {
    use arrow_ipc::writer::StreamWriter;
    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, batch.schema().as_ref())
            .map_err(|e| Error::from_reason(format!("IPC writer: {}", e)))?;
        writer
            .write(batch)
            .map_err(|e| Error::from_reason(format!("IPC write: {}", e)))?;
        writer
            .finish()
            .map_err(|e| Error::from_reason(format!("IPC finish: {}", e)))?;
    }
    Ok(buffer)
}

#[cfg(feature = "arrow-flight")]
#[napi]
impl ZerobusArrowStream {
    /// Ingest one RecordBatch supplied as an Arrow IPC stream
    /// (`tableToIPC(table, 'stream')`).
    ///
    /// Uses the Rust SDK's zero-copy `ingest_ipc_batch` path when the stream
    /// was opened without compression; otherwise parses → re-encodes so the
    /// SDK can apply the configured codec.
    #[napi(ts_return_type = "Promise<bigint>")]
    pub fn ingest_batch(&self, env: Env, ipc_buffer: Buffer) -> Result<JsObject> {
        let schema = self.schema.clone();
        let stream = self.inner.clone();
        let use_zero_copy = !self.has_compression;
        let buffer_vec = ipc_buffer.to_vec();

        env.execute_tokio_future(
            async move {
                let mut guard = stream.lock().await;
                let stream_ref = guard
                    .as_mut()
                    .ok_or_else(|| napi::Error::from_reason("Arrow stream has been closed"))?;

                let offset = if use_zero_copy {
                    stream_ref.ingest_ipc_batch(Bytes::from(buffer_vec)).await
                } else {
                    let batch = parse_arrow_ipc_to_batch(&buffer_vec, &schema)?;
                    stream_ref.ingest_batch(batch).await
                };

                offset.map_err(|e| napi::Error::from_reason(format!("Failed to ingest batch: {}", e)))
            },
            |env, offset_id| {
                let global: JsGlobal = env.get_global()?;
                let bigint_ctor: JsFunction = global.get_named_property("BigInt")?;
                let js_str = env.create_string(&offset_id.to_string())?;
                bigint_ctor.call(None, &[js_str.into_unknown()])
            },
        )
    }

    #[napi(ts_return_type = "Promise<void>")]
    pub fn wait_for_offset(&self, env: Env, offset_id: BigInt) -> Result<JsObject> {
        let offset = bigint_to_i64(offset_id)?;
        let stream = self.inner.clone();
        env.execute_tokio_future(
            async move {
                let guard = stream.lock().await;
                let stream_ref = guard
                    .as_ref()
                    .ok_or_else(|| napi::Error::from_reason("Arrow stream has been closed"))?;
                stream_ref
                    .wait_for_offset(offset)
                    .await
                    .map_err(|e| napi::Error::from_reason(format!("Failed to wait for offset: {}", e)))
            },
            |_env, _| Ok(()),
        )
    }

    #[napi]
    pub async fn flush(&self) -> Result<()> {
        let guard = self.inner.lock().await;
        let stream = guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Arrow stream has been closed"))?;
        stream
            .flush()
            .await
            .map_err(|e| Error::from_reason(format!("Failed to flush arrow stream: {}", e)))
    }

    #[napi]
    pub async fn close(&self) -> Result<()> {
        let mut guard = self.inner.lock().await;
        if let Some(mut stream) = guard.take() {
            stream
                .close()
                .await
                .map_err(|e| Error::from_reason(format!("Failed to close arrow stream: {}", e)))?;
        }
        Ok(())
    }

    #[napi(getter)]
    pub fn is_closed(&self) -> bool {
        match self.inner.try_lock() {
            Ok(guard) => guard.is_none(),
            Err(_) => false,
        }
    }

    #[napi(getter)]
    pub fn table_name(&self) -> Result<String> {
        let guard = self
            .inner
            .try_lock()
            .map_err(|_| Error::from_reason("Stream is busy"))?;
        let stream = guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Stream has been closed"))?;
        Ok(stream.table_name().to_string())
    }

    /// Unacked batches as IPC-encoded buffers for recovery.
    #[napi]
    pub async fn get_unacked_batches(&self) -> Result<Vec<Buffer>> {
        let guard = self.inner.lock().await;
        let stream = guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Arrow stream has been closed"))?;

        let batches = stream
            .get_unacked_batches()
            .await
            .map_err(|e| Error::from_reason(format!("Failed to get unacked batches: {}", e)))?;

        batches
            .iter()
            .map(|batch| {
                let ipc_bytes = serialize_batch_to_ipc(batch)?;
                Ok(ipc_bytes.into())
            })
            .collect()
    }
}

// =============================================================================
// ZerobusSdk — entry point
// =============================================================================

#[napi]
pub struct ZerobusSdk {
    inner: Arc<RustZerobusSdk>,
}

#[napi]
impl ZerobusSdk {
    /// Construct an SDK handle. See [`SdkOptions`] for fields.
    #[napi(constructor)]
    pub fn new(options: SdkOptions) -> Result<Self> {
        if options.endpoint.is_empty() {
            return Err(Error::from_reason("SdkOptions.endpoint is required"));
        }

        let mut builder = RustZerobusSdk::builder().endpoint(&options.endpoint);
        if let Some(url) = options.unity_catalog_url.as_deref() {
            if !url.is_empty() {
                builder = builder.unity_catalog_url(url);
            }
        }

        // SDK identification. The TS SDK always claims its own identity unless
        // the caller explicitly overrides; downstream the Rust SDK appends the
        // optional application_name.
        let sdk_id = options
            .sdk_identifier
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or(TS_SDK_IDENTIFIER);
        builder = builder.sdk_identifier(sdk_id);
        if let Some(app) = options.application_name.as_deref() {
            if !app.is_empty() {
                builder = builder.application_name(app);
            }
        }

        // TLS. Default to secure; allow opt-out for local/sidecar setups.
        let use_tls = options.use_tls.unwrap_or(true);
        let tls: Arc<dyn TlsConfig> = if use_tls {
            Arc::new(SecureTlsConfig::new())
        } else {
            Arc::new(NoTlsConfig)
        };
        builder = builder.tls_config(tls);

        let inner = builder
            .build()
            .map_err(|e| Error::from_reason(format!("Failed to create SDK: {}", e)))?;

        Ok(ZerobusSdk {
            inner: Arc::new(inner),
        })
    }

    /// Open a JSON / protobuf stream.
    ///
    /// Exactly one of `clientId`/`clientSecret` (OAuth) or `headersProvider`
    /// (custom auth) must be supplied. To run against a no-auth local server,
    /// pass a `headersProvider` whose callback returns the required headers
    /// with a placeholder bearer token.
    #[napi(ts_return_type = "Promise<ZerobusStream>")]
    pub fn create_stream(
        &self,
        env: Env,
        table_properties: TableProperties,
        client_id: String,
        client_secret: String,
        options: Option<StreamConfigurationOptions>,
        headers_provider: Option<JsHeadersProvider>,
    ) -> Result<JsObject> {
        let opts = options.unwrap_or_default();
        let descriptor_proto = table_properties
            .descriptor_proto
            .as_deref()
            .map(decode_descriptor_proto)
            .transpose()?;

        let record_type = match opts.record_type {
            Some(0) => RustRecordType::Json,
            Some(1) => RustRecordType::Proto,
            _ => {
                // Default to Proto when a descriptor is supplied; JSON otherwise.
                if descriptor_proto.is_some() {
                    RustRecordType::Proto
                } else {
                    RustRecordType::Json
                }
            }
        };

        let headers_tsfn = match headers_provider {
            Some(JsHeadersProvider {
                get_headers_callback,
            }) => Some(create_headers_tsfn(get_headers_callback)?),
            None => None,
        };

        let sdk = self.inner.clone();
        let table_name = table_properties.table_name.clone();

        env.execute_tokio_future(
            async move {
                let headers_provider_arc: Option<Arc<dyn RustHeadersProvider>> =
                    if let Some(tsfn) = headers_tsfn {
                        let headers = call_headers_tsfn(tsfn).await?;
                        let provider = StaticHeadersProvider::new(headers).map_err(|e| {
                            napi::Error::from_reason(format!("Invalid headers: {}", e))
                        })?;
                        Some(Arc::new(provider))
                    } else {
                        None
                    };

                let mut builder = sdk.stream_builder().table(table_name.clone());

                if let Some(provider) = headers_provider_arc {
                    builder = builder.headers_provider(provider);
                } else {
                    builder = builder.oauth(client_id, client_secret);
                }

                // Apply config — only setters for values the caller actually
                // supplied so SDK defaults survive otherwise.
                if let Some(v) = opts.recovery {
                    builder = builder.recovery(v);
                }
                if let Some(v) = opts.recovery_timeout_ms {
                    builder = builder.recovery_timeout_ms(v as u64);
                }
                if let Some(v) = opts.recovery_backoff_ms {
                    builder = builder.recovery_backoff_ms(v as u64);
                }
                if let Some(v) = opts.recovery_retries {
                    builder = builder.recovery_retries(v);
                }
                if let Some(v) = opts.server_lack_of_ack_timeout_ms {
                    builder = builder.server_lack_of_ack_timeout_ms(v as u64);
                }
                if let Some(v) = opts.flush_timeout_ms {
                    builder = builder.flush_timeout_ms(v as u64);
                }
                if let Some(v) = opts.max_inflight_requests {
                    builder = builder.max_inflight_requests(v as usize);
                }
                if let Some(v) = opts.stream_paused_max_wait_time_ms {
                    builder = builder.stream_paused_max_wait_time_ms(Some(v as u64));
                }

                let builder = match record_type {
                    RustRecordType::Json => builder.json(),
                    RustRecordType::Proto | RustRecordType::Unspecified => {
                        let desc = descriptor_proto.ok_or_else(|| {
                            napi::Error::from_reason(
                                "Proto record type requires descriptor_proto on TableProperties",
                            )
                        })?;
                        builder.compiled_proto(desc)
                    }
                };

                let stream = builder
                    .build()
                    .await
                    .map_err(|e| napi::Error::from_reason(format!("Failed to create stream: {}", e)))?;

                Ok(ZerobusStream {
                    inner: Arc::new(Mutex::new(Some(stream))),
                })
            },
            |_env, stream| Ok(stream),
        )
    }

    /// Recreate a closed/failed gRPC stream with the same configuration. Any
    /// unacked records are re-ingested on the new stream.
    #[napi]
    pub async fn recreate_stream(&self, stream: &ZerobusStream) -> Result<ZerobusStream> {
        let inner_guard = stream.inner.lock().await;
        let rust_stream = inner_guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Stream has been closed"))?;

        let new_rust_stream = self
            .inner
            .recreate_stream(rust_stream)
            .await
            .map_err(|e| Error::from_reason(format!("Failed to recreate stream: {}", e)))?;

        Ok(ZerobusStream {
            inner: Arc::new(Mutex::new(Some(new_rust_stream))),
        })
    }
}

// =============================================================================
// ZerobusSdk — Arrow-flight extensions
// =============================================================================

#[cfg(feature = "arrow-flight")]
#[napi]
impl ZerobusSdk {
    /// Open an Arrow Flight stream.
    #[napi(ts_return_type = "Promise<ZerobusArrowStream>")]
    pub fn create_arrow_stream(
        &self,
        env: Env,
        table_properties: ArrowTableProperties,
        client_id: String,
        client_secret: String,
        options: Option<ArrowStreamConfigurationOptions>,
        headers_provider: Option<JsHeadersProvider>,
    ) -> Result<JsObject> {
        let opts = options.unwrap_or_default();
        let arrow_schema = build_arrow_schema(&table_properties.schema_fields);
        let schema = arrow_schema.clone();
        let table_name = table_properties.table_name.clone();
        let ipc_compression = map_compression(opts.ipc_compression);
        let has_compression = ipc_compression.is_some();

        let headers_tsfn = match headers_provider {
            Some(JsHeadersProvider {
                get_headers_callback,
            }) => Some(create_headers_tsfn(get_headers_callback)?),
            None => None,
        };

        let sdk = self.inner.clone();

        env.execute_tokio_future(
            async move {
                let headers_provider_arc: Option<Arc<dyn RustHeadersProvider>> =
                    if let Some(tsfn) = headers_tsfn {
                        let headers = call_headers_tsfn(tsfn).await?;
                        let provider = StaticHeadersProvider::new(headers).map_err(|e| {
                            napi::Error::from_reason(format!("Invalid headers: {}", e))
                        })?;
                        Some(Arc::new(provider))
                    } else {
                        None
                    };

                let mut builder = sdk.stream_builder().table(table_name);

                if let Some(provider) = headers_provider_arc {
                    builder = builder.headers_provider(provider);
                } else {
                    builder = builder.oauth(client_id, client_secret);
                }

                builder = builder.arrow(arrow_schema);

                if let Some(v) = opts.recovery {
                    builder = builder.recovery(v);
                }
                if let Some(v) = opts.recovery_timeout_ms {
                    builder = builder.recovery_timeout_ms(v as u64);
                }
                if let Some(v) = opts.recovery_backoff_ms {
                    builder = builder.recovery_backoff_ms(v as u64);
                }
                if let Some(v) = opts.recovery_retries {
                    builder = builder.recovery_retries(v);
                }
                if let Some(v) = opts.server_lack_of_ack_timeout_ms {
                    builder = builder.server_lack_of_ack_timeout_ms(v as u64);
                }
                if let Some(v) = opts.flush_timeout_ms {
                    builder = builder.flush_timeout_ms(v as u64);
                }
                if let Some(v) = opts.max_inflight_batches {
                    builder = builder.max_inflight_batches(v as usize);
                }
                if let Some(v) = opts.connection_timeout_ms {
                    builder = builder.connection_timeout_ms(v as u64);
                }
                if let Some(v) = opts.stream_paused_max_wait_time_ms {
                    builder = builder.stream_paused_max_wait_time_ms(Some(v as u64));
                }
                builder = builder.ipc_compression(ipc_compression);

                let stream = builder.build_arrow().await.map_err(|e| {
                    napi::Error::from_reason(format!("Failed to create arrow stream: {}", e))
                })?;

                Ok(ZerobusArrowStream {
                    inner: Arc::new(Mutex::new(Some(stream))),
                    schema,
                    has_compression,
                })
            },
            |_env, stream| Ok(stream),
        )
    }

    /// Recreate a closed/failed Arrow Flight stream. Unacked batches are
    /// re-ingested on the new stream.
    #[napi]
    pub async fn recreate_arrow_stream(
        &self,
        stream: &ZerobusArrowStream,
    ) -> Result<ZerobusArrowStream> {
        let inner_guard = stream.inner.lock().await;
        let rust_stream = inner_guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Arrow stream has been closed"))?;

        let new_rust_stream = self
            .inner
            .recreate_arrow_stream(rust_stream)
            .await
            .map_err(|e| Error::from_reason(format!("Failed to recreate arrow stream: {}", e)))?;

        Ok(ZerobusArrowStream {
            inner: Arc::new(Mutex::new(Some(new_rust_stream))),
            schema: stream.schema.clone(),
            has_compression: stream.has_compression,
        })
    }
}
