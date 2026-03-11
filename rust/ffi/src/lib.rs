// Allow clippy warnings for FFI code where unsafe operations are unavoidable
#![allow(clippy::not_unsafe_ptr_arg_deref)]
#![allow(clippy::type_complexity)]

use once_cell::sync::Lazy;
use std::collections::{HashMap, HashSet};
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use tokio::runtime::Runtime;
use tracing_subscriber::{fmt, EnvFilter};
extern crate libc;

use arrow_ipc::{reader::StreamReader, writer::StreamWriter, CompressionType};
use async_trait::async_trait;
use databricks_zerobus_ingest_sdk::databricks::zerobus::RecordType;
use databricks_zerobus_ingest_sdk::{
    ArrowStreamConfigurationOptions, ArrowTableProperties, RecordBatch, ZerobusArrowStream,
};
use databricks_zerobus_ingest_sdk::{
    EncodedRecord, HeadersProvider, NoTlsConfig, StreamConfigurationOptions, TableProperties,
    ZerobusError, ZerobusResult, ZerobusSdk, ZerobusStream,
};
use prost::Message;
use std::sync::Arc;

// Test module
#[cfg(test)]
mod tests;

// ============================================================================
// Arrow Flight FFI
// ============================================================================

/// Opaque handle for an Arrow Flight stream.
#[repr(C)]
pub struct CArrowStream {
    _private: [u8; 0],
}

/// Configuration options for Arrow Flight streams.
///
/// `ipc_compression`: -1 = None, 0 = LZ4_FRAME, 1 = ZSTD
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CArrowStreamConfigurationOptions {
    pub max_inflight_batches: usize,
    pub recovery: bool,
    pub recovery_timeout_ms: u64,
    pub recovery_backoff_ms: u64,
    pub recovery_retries: u32,
    pub server_lack_of_ack_timeout_ms: u64,
    pub flush_timeout_ms: u64,
    pub connection_timeout_ms: u64,
    /// -1 = None, 0 = LZ4_FRAME, 1 = ZSTD
    pub ipc_compression: i32,
}

impl From<CArrowStreamConfigurationOptions> for ArrowStreamConfigurationOptions {
    fn from(c_opts: CArrowStreamConfigurationOptions) -> Self {
        let ipc_compression = match c_opts.ipc_compression {
            0 => Some(CompressionType::LZ4_FRAME),
            1 => Some(CompressionType::ZSTD),
            _ => None,
        };
        ArrowStreamConfigurationOptions {
            max_inflight_batches: c_opts.max_inflight_batches,
            recovery: c_opts.recovery,
            recovery_timeout_ms: c_opts.recovery_timeout_ms,
            recovery_backoff_ms: c_opts.recovery_backoff_ms,
            recovery_retries: c_opts.recovery_retries,
            server_lack_of_ack_timeout_ms: c_opts.server_lack_of_ack_timeout_ms,
            flush_timeout_ms: c_opts.flush_timeout_ms,
            connection_timeout_ms: c_opts.connection_timeout_ms,
            ipc_compression,
        }
    }
}

/// An array of Arrow IPC-encoded batches, returned by `zerobus_arrow_stream_get_unacked_batches`.
/// Must be freed with `zerobus_arrow_free_batch_array`.
#[repr(C)]
pub struct CArrowBatchArray {
    /// Array of pointers to IPC-encoded batch bytes.
    pub batches: *mut *mut u8,
    /// Array of byte lengths, one per batch.
    pub lengths: *mut usize,
    /// Number of batches.
    pub count: usize,
}

// ---- Arrow pointer validation helpers ----

fn validate_arrow_stream_ptr<'a>(
    stream: *mut CArrowStream,
) -> Result<&'a ZerobusArrowStream, &'static str> {
    if stream.is_null() {
        return Err("Arrow stream pointer is null");
    }
    unsafe { Ok(&*(stream as *const ZerobusArrowStream)) }
}

fn validate_arrow_stream_ptr_mut<'a>(
    stream: *mut CArrowStream,
) -> Result<&'a mut ZerobusArrowStream, &'static str> {
    if stream.is_null() {
        return Err("Arrow stream pointer is null");
    }
    unsafe { Ok(&mut *(stream as *mut ZerobusArrowStream)) }
}

// ---- Arrow IPC helpers ----

/// Deserializes an `Arc<ArrowSchema>` from Arrow IPC stream bytes (schema-only stream).
fn ipc_bytes_to_schema(
    bytes: &[u8],
) -> ZerobusResult<std::sync::Arc<databricks_zerobus_ingest_sdk::ArrowSchema>> {
    use std::io::Cursor;
    let cursor = Cursor::new(bytes);
    let reader = StreamReader::try_new(cursor, None).map_err(|e| {
        ZerobusError::InvalidArgument(format!("Failed to parse Arrow IPC schema: {e}"))
    })?;
    Ok(reader.schema().clone())
}

/// Deserializes the first `RecordBatch` from Arrow IPC stream bytes.
fn ipc_bytes_to_record_batch(bytes: &[u8]) -> ZerobusResult<RecordBatch> {
    use std::io::Cursor;
    let cursor = Cursor::new(bytes);
    let mut reader = StreamReader::try_new(cursor, None).map_err(|e| {
        ZerobusError::InvalidArgument(format!("Failed to parse Arrow IPC stream: {e}"))
    })?;
    reader
        .next()
        .ok_or_else(|| ZerobusError::InvalidArgument("No record batch in IPC stream".to_string()))?
        .map_err(|e| ZerobusError::InvalidArgument(format!("Failed to read Arrow IPC batch: {e}")))
}

/// Serializes a `RecordBatch` to Arrow IPC stream bytes (schema + one batch).
fn record_batch_to_ipc_bytes(batch: &RecordBatch) -> ZerobusResult<Vec<u8>> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref()).map_err(|e| {
        ZerobusError::InvalidArgument(format!("Failed to create Arrow IPC writer: {e}"))
    })?;
    writer.write(batch).map_err(|e| {
        ZerobusError::InvalidArgument(format!("Failed to write Arrow IPC batch: {e}"))
    })?;
    writer.finish().map_err(|e| {
        ZerobusError::InvalidArgument(format!("Failed to finish Arrow IPC stream: {e}"))
    })?;
    Ok(buf)
}

// ---- Arrow FFI functions ----

/// Creates an Arrow Flight stream authenticated with OAuth client credentials.
///
/// `schema_ipc_bytes` must point to Arrow IPC stream bytes encoding only the schema
/// (write an empty IPC stream with just the schema message).
#[no_mangle]
pub extern "C" fn zerobus_sdk_create_arrow_stream(
    sdk: *mut CZerobusSdk,
    table_name: *const c_char,
    schema_ipc_bytes: *const u8,
    schema_ipc_len: usize,
    client_id: *const c_char,
    client_secret: *const c_char,
    options: *const CArrowStreamConfigurationOptions,
    result: *mut CResult,
) -> *mut CArrowStream {
    let sdk_ref = match validate_sdk_ptr(sdk) {
        Ok(s) => s,
        Err(msg) => {
            write_error_result(result, msg, false);
            return ptr::null_mut();
        }
    };

    let res = RUNTIME.block_on(async {
        let table_name_str = unsafe { c_str_to_string(table_name).map_err(|e| e.to_string())? };
        let client_id_str = unsafe { c_str_to_string(client_id).map_err(|e| e.to_string())? };
        let client_secret_str =
            unsafe { c_str_to_string(client_secret).map_err(|e| e.to_string())? };

        if schema_ipc_bytes.is_null() || schema_ipc_len == 0 {
            return Err("Schema IPC bytes are required for Arrow stream".to_string());
        }
        let schema_bytes = unsafe { std::slice::from_raw_parts(schema_ipc_bytes, schema_ipc_len) };
        let schema = ipc_bytes_to_schema(schema_bytes).map_err(|e| e.to_string())?;

        let table_props = ArrowTableProperties {
            table_name: table_name_str,
            schema,
        };
        let stream_options = if !options.is_null() {
            Some(unsafe { (*options).into() })
        } else {
            None
        };

        let stream = sdk_ref
            .create_arrow_stream(
                table_props,
                client_id_str,
                client_secret_str,
                stream_options,
            )
            .await
            .map_err(|e| e.to_string())?;

        let boxed = Box::new(stream);
        Ok::<*mut CArrowStream, String>(Box::into_raw(boxed) as *mut CArrowStream)
    });

    match res {
        Ok(ptr) => {
            write_success_result(result);
            ptr
        }
        Err(err) => {
            write_error_result(result, &err, false);
            ptr::null_mut()
        }
    }
}

/// Creates an Arrow Flight stream with a custom headers provider callback.
///
/// `schema_ipc_bytes` must point to Arrow IPC stream bytes encoding only the schema.
#[no_mangle]
pub extern "C" fn zerobus_sdk_create_arrow_stream_with_headers_provider(
    sdk: *mut CZerobusSdk,
    table_name: *const c_char,
    schema_ipc_bytes: *const u8,
    schema_ipc_len: usize,
    headers_callback: HeadersProviderCallback,
    user_data: *mut std::ffi::c_void,
    options: *const CArrowStreamConfigurationOptions,
    result: *mut CResult,
) -> *mut CArrowStream {
    let sdk_ref = match validate_sdk_ptr(sdk) {
        Ok(s) => s,
        Err(msg) => {
            write_error_result(result, msg, false);
            return ptr::null_mut();
        }
    };

    let res = RUNTIME.block_on(async {
        let table_name_str = unsafe { c_str_to_string(table_name).map_err(|e| e.to_string())? };

        if schema_ipc_bytes.is_null() || schema_ipc_len == 0 {
            return Err("Schema IPC bytes are required for Arrow stream".to_string());
        }
        let schema_bytes = unsafe { std::slice::from_raw_parts(schema_ipc_bytes, schema_ipc_len) };
        let schema = ipc_bytes_to_schema(schema_bytes).map_err(|e| e.to_string())?;

        let table_props = ArrowTableProperties {
            table_name: table_name_str,
            schema,
        };
        let stream_options = if !options.is_null() {
            Some(unsafe { (*options).into() })
        } else {
            None
        };

        let headers_provider = Arc::new(CallbackHeadersProvider::new(headers_callback, user_data));

        let stream = sdk_ref
            .create_arrow_stream_with_headers_provider(
                table_props,
                headers_provider,
                stream_options,
            )
            .await
            .map_err(|e| e.to_string())?;

        let boxed = Box::new(stream);
        Ok::<*mut CArrowStream, String>(Box::into_raw(boxed) as *mut CArrowStream)
    });

    match res {
        Ok(ptr) => {
            write_success_result(result);
            ptr
        }
        Err(err) => {
            write_error_result(result, &err, false);
            ptr::null_mut()
        }
    }
}

/// Frees an Arrow Flight stream instance.
#[no_mangle]
pub extern "C" fn zerobus_arrow_stream_free(stream: *mut CArrowStream) {
    if !stream.is_null() {
        unsafe {
            let _ = Box::from_raw(stream as *mut ZerobusArrowStream);
        }
    }
}

/// Ingests one Arrow RecordBatch supplied as Arrow IPC stream bytes.
///
/// `ipc_bytes` must be a valid Arrow IPC stream (schema + one record batch).
/// Returns the logical offset assigned to this batch, or -1 on error.
#[no_mangle]
pub extern "C" fn zerobus_arrow_stream_ingest_batch(
    stream: *mut CArrowStream,
    ipc_bytes: *const u8,
    ipc_len: usize,
    result: *mut CResult,
) -> i64 {
    if ipc_bytes.is_null() || ipc_len == 0 {
        write_error_result(result, "IPC bytes are required", false);
        return -1;
    }

    let stream_ref = match validate_arrow_stream_ptr(stream) {
        Ok(s) => s,
        Err(msg) => {
            write_error_result(result, msg, false);
            return -1;
        }
    };

    let bytes = unsafe { std::slice::from_raw_parts(ipc_bytes, ipc_len) };

    let offset_res = RUNTIME.block_on(async {
        let batch = ipc_bytes_to_record_batch(bytes)?;
        stream_ref.ingest_batch(batch).await
    });

    match offset_res {
        Ok(offset) => {
            write_success_result(result);
            offset
        }
        Err(err) => {
            if !result.is_null() {
                unsafe {
                    *result = CResult::error(err);
                }
            }
            -1
        }
    }
}

/// Waits until the server acknowledges the batch at the given logical offset.
#[no_mangle]
pub extern "C" fn zerobus_arrow_stream_wait_for_offset(
    stream: *mut CArrowStream,
    offset: i64,
    result: *mut CResult,
) -> bool {
    let stream_ref = match validate_arrow_stream_ptr(stream) {
        Ok(s) => s,
        Err(msg) => {
            write_error_result(result, msg, false);
            return false;
        }
    };

    let res = RUNTIME.block_on(async { stream_ref.wait_for_offset(offset).await });

    match res {
        Ok(()) => {
            write_success_result(result);
            true
        }
        Err(err) => {
            if !result.is_null() {
                unsafe {
                    *result = CResult::error(err);
                }
            }
            false
        }
    }
}

/// Flushes all pending batches and waits for their acknowledgment.
#[no_mangle]
pub extern "C" fn zerobus_arrow_stream_flush(
    stream: *mut CArrowStream,
    result: *mut CResult,
) -> bool {
    let stream_ref = match validate_arrow_stream_ptr(stream) {
        Ok(s) => s,
        Err(msg) => {
            write_error_result(result, msg, false);
            return false;
        }
    };

    let res = RUNTIME.block_on(async { stream_ref.flush().await });

    match res {
        Ok(()) => {
            write_success_result(result);
            true
        }
        Err(err) => {
            if !result.is_null() {
                unsafe {
                    *result = CResult::error(err);
                }
            }
            false
        }
    }
}

/// Gracefully closes the stream, flushing all pending batches first.
#[no_mangle]
pub extern "C" fn zerobus_arrow_stream_close(
    stream: *mut CArrowStream,
    result: *mut CResult,
) -> bool {
    let stream_ref = match validate_arrow_stream_ptr_mut(stream) {
        Ok(s) => s,
        Err(msg) => {
            write_error_result(result, msg, false);
            return false;
        }
    };

    let res = RUNTIME.block_on(async { stream_ref.close().await });

    match res {
        Ok(()) => {
            write_success_result(result);
            true
        }
        Err(err) => {
            if !result.is_null() {
                unsafe {
                    *result = CResult::error(err);
                }
            }
            false
        }
    }
}

/// Returns all unacknowledged batches from a closed or failed stream as Arrow IPC bytes.
///
/// Each batch is serialized as a self-contained Arrow IPC stream (schema + one batch).
/// The returned array must be freed with `zerobus_arrow_free_batch_array`.
#[no_mangle]
pub extern "C" fn zerobus_arrow_stream_get_unacked_batches(
    stream: *mut CArrowStream,
    result: *mut CResult,
) -> CArrowBatchArray {
    let empty = CArrowBatchArray {
        batches: ptr::null_mut(),
        lengths: ptr::null_mut(),
        count: 0,
    };

    let stream_ref = match validate_arrow_stream_ptr(stream) {
        Ok(s) => s,
        Err(msg) => {
            write_error_result(result, msg, false);
            return empty;
        }
    };

    let batches_res = RUNTIME.block_on(async { stream_ref.get_unacked_batches().await });

    match batches_res {
        Ok(batches) => {
            if batches.is_empty() {
                write_success_result(result);
                return empty;
            }

            let count = batches.len();
            let mut batch_ptrs: Vec<*mut u8> = Vec::with_capacity(count);
            let mut batch_lens: Vec<usize> = Vec::with_capacity(count);

            for batch in &batches {
                match record_batch_to_ipc_bytes(batch) {
                    Ok(bytes) => {
                        let len = bytes.len();
                        let ptr = Box::into_raw(bytes.into_boxed_slice()) as *mut u8;
                        batch_ptrs.push(ptr);
                        batch_lens.push(len);
                    }
                    Err(e) => {
                        // Free already-allocated batches before returning error.
                        for (&ptr, &len) in batch_ptrs.iter().zip(batch_lens.iter()) {
                            if !ptr.is_null() && len > 0 {
                                unsafe {
                                    let _ = Vec::from_raw_parts(ptr, len, len);
                                }
                            }
                        }
                        write_error_result(result, &e.to_string(), false);
                        return empty;
                    }
                }
            }

            let ptrs_ptr = batch_ptrs.as_mut_ptr();
            let lens_ptr = batch_lens.as_mut_ptr();
            std::mem::forget(batch_ptrs);
            std::mem::forget(batch_lens);

            write_success_result(result);
            CArrowBatchArray {
                batches: ptrs_ptr,
                lengths: lens_ptr,
                count,
            }
        }
        Err(err) => {
            if !result.is_null() {
                unsafe {
                    *result = CResult::error(err);
                }
            }
            empty
        }
    }
}

/// Frees a `CArrowBatchArray` returned by `zerobus_arrow_stream_get_unacked_batches`.
#[no_mangle]
pub extern "C" fn zerobus_arrow_free_batch_array(array: CArrowBatchArray) {
    if array.count == 0 {
        return;
    }
    unsafe {
        if !array.batches.is_null() && !array.lengths.is_null() {
            let ptrs = Vec::from_raw_parts(array.batches, array.count, array.count);
            let lens = Vec::from_raw_parts(array.lengths, array.count, array.count);
            for (&ptr, &len) in ptrs.iter().zip(lens.iter()) {
                if !ptr.is_null() && len > 0 {
                    let _ = Vec::from_raw_parts(ptr, len, len);
                }
            }
        }
    }
}

/// Returns whether the Arrow stream has been closed.
#[no_mangle]
pub extern "C" fn zerobus_arrow_stream_is_closed(stream: *mut CArrowStream) -> bool {
    match validate_arrow_stream_ptr(stream) {
        Ok(s) => s.is_closed(),
        Err(_) => true,
    }
}

/// Returns the default Arrow stream configuration options.
#[no_mangle]
pub extern "C" fn zerobus_arrow_get_default_config() -> CArrowStreamConfigurationOptions {
    let d = ArrowStreamConfigurationOptions::default();
    let ipc_compression = match d.ipc_compression {
        Some(ct) if ct == CompressionType::LZ4_FRAME => 0,
        Some(ct) if ct == CompressionType::ZSTD => 1,
        _ => -1,
    };
    CArrowStreamConfigurationOptions {
        max_inflight_batches: d.max_inflight_batches,
        recovery: d.recovery,
        recovery_timeout_ms: d.recovery_timeout_ms,
        recovery_backoff_ms: d.recovery_backoff_ms,
        recovery_retries: d.recovery_retries,
        server_lack_of_ack_timeout_ms: d.server_lack_of_ack_timeout_ms,
        flush_timeout_ms: d.flush_timeout_ms,
        connection_timeout_ms: d.connection_timeout_ms,
        ipc_compression,
    }
}

// Global Tokio runtime for handling async Rust calls
static RUNTIME: Lazy<Runtime> =
    Lazy::new(|| Runtime::new().expect("Failed to create Tokio runtime"));

// Flag to track if logging has been initialized
static LOGGING_INITIALIZED: AtomicBool = AtomicBool::new(false);

/// Initialize tracing subscriber for Rust logs
/// Can be controlled via RUST_LOG environment variable
/// Examples:
///   RUST_LOG=info           - Show info and above
///   RUST_LOG=debug          - Show debug and above
///   RUST_LOG=trace          - Show all logs
///   RUST_LOG=databricks_zerobus_ingest_sdk=debug - Show only SDK logs at debug level
fn init_logging() {
    if LOGGING_INITIALIZED.swap(true, Ordering::SeqCst) {
        return;
    }

    let _ = fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .try_init();
}

// Global cache for header keys to prevent memory leaks
// Header keys are typically a small set of constant strings (e.g., "Authorization", "Content-Type")
// We intern them once to avoid leaking memory on every callback
static HEADER_KEY_CACHE: Lazy<Mutex<HashSet<&'static str>>> =
    Lazy::new(|| Mutex::new(HashSet::new()));

/// Intern a header key string to prevent memory leaks
/// Only leaks memory for unique keys, not on every call
pub(crate) fn intern_header_key(key: String) -> &'static str {
    let mut cache = HEADER_KEY_CACHE.lock().unwrap();

    // Check if we already have this key
    if let Some(&existing) = cache.iter().find(|&&k| k == key.as_str()) {
        return existing;
    }

    // Only leak if it's a new key (typically happens once per unique header name)
    let static_key: &'static str = Box::leak(key.into_boxed_str());
    cache.insert(static_key);
    static_key
}

// Opaque types for Go
#[repr(C)]
pub struct CZerobusSdk {
    _private: [u8; 0],
}

#[repr(C)]
pub struct CZerobusStream {
    _private: [u8; 0],
}

// Result type for FFI calls
#[repr(C)]
pub struct CResult {
    pub success: bool,
    pub error_message: *mut c_char,
    pub is_retryable: bool,
}

/// Represents a single record (either Proto or JSON)
#[repr(C)]
pub struct CRecord {
    pub is_json: bool,
    pub data: *mut u8,
    pub data_len: usize,
}

/// Represents an array of records
#[repr(C)]
pub struct CRecordArray {
    pub records: *mut CRecord,
    pub len: usize,
}

impl CResult {
    fn success() -> Self {
        CResult {
            success: true,
            error_message: ptr::null_mut(),
            is_retryable: false,
        }
    }

    fn error(err: ZerobusError) -> Self {
        let is_retryable = err.is_retryable();
        let message = CString::new(err.to_string())
            .unwrap_or_else(|_| CString::new("Unknown error").unwrap());

        CResult {
            success: false,
            error_message: message.into_raw(),
            is_retryable,
        }
    }
}

// Configuration options
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CStreamConfigurationOptions {
    pub max_inflight_requests: usize,
    pub recovery: bool,
    pub recovery_timeout_ms: u64,
    pub recovery_backoff_ms: u64,
    pub recovery_retries: u32,
    pub server_lack_of_ack_timeout_ms: u64,
    pub flush_timeout_ms: u64,
    pub record_type: i32,
    pub stream_paused_max_wait_time_ms: u64,
    pub has_stream_paused_max_wait_time_ms: bool,
    pub callback_max_wait_time_ms: u64,
    pub has_callback_max_wait_time_ms: bool,
}

impl From<CStreamConfigurationOptions> for StreamConfigurationOptions {
    fn from(c_opts: CStreamConfigurationOptions) -> Self {
        StreamConfigurationOptions {
            max_inflight_requests: c_opts.max_inflight_requests,
            recovery: c_opts.recovery,
            recovery_timeout_ms: c_opts.recovery_timeout_ms,
            recovery_backoff_ms: c_opts.recovery_backoff_ms,
            recovery_retries: c_opts.recovery_retries,
            server_lack_of_ack_timeout_ms: c_opts.server_lack_of_ack_timeout_ms,
            flush_timeout_ms: c_opts.flush_timeout_ms,
            record_type: match c_opts.record_type {
                1 => RecordType::Proto,
                2 => RecordType::Json,
                _ => RecordType::Unspecified,
            },
            stream_paused_max_wait_time_ms: if c_opts.has_stream_paused_max_wait_time_ms {
                Some(c_opts.stream_paused_max_wait_time_ms)
            } else {
                None
            },
            callback_max_wait_time_ms: if c_opts.has_callback_max_wait_time_ms {
                Some(c_opts.callback_max_wait_time_ms)
            } else {
                None
            },
            ack_callback: None,
        }
    }
}

// Helper to convert C string to Rust String
unsafe fn c_str_to_string(c_str: *const c_char) -> Result<String, &'static str> {
    if c_str.is_null() {
        return Err("Null pointer passed");
    }
    CStr::from_ptr(c_str)
        .to_str()
        .map(|s| s.to_string())
        .map_err(|_| "Invalid UTF-8 string")
}

/// A single header key-value pair for C FFI
#[repr(C)]
pub struct CHeader {
    pub key: *mut c_char,
    pub value: *mut c_char,
}

/// A collection of headers returned from Go callback
#[repr(C)]
pub struct CHeaders {
    pub headers: *mut CHeader,
    pub count: usize,
    pub error_message: *mut c_char,
}

/// Function pointer type for the headers provider callback
/// The callback should return a CHeaders struct
/// The caller is responsible for freeing the returned CHeaders using zerobus_free_headers
pub type HeadersProviderCallback = extern "C" fn(user_data: *mut std::ffi::c_void) -> CHeaders;

/// Free headers returned from callback
#[no_mangle]
pub extern "C" fn zerobus_free_headers(headers: CHeaders) {
    if !headers.headers.is_null() {
        unsafe {
            let headers_slice = std::slice::from_raw_parts_mut(headers.headers, headers.count);
            for header in headers_slice {
                if !header.key.is_null() {
                    let _ = CString::from_raw(header.key);
                }
                if !header.value.is_null() {
                    let _ = CString::from_raw(header.value);
                }
            }
            libc::free(headers.headers as *mut std::ffi::c_void);
        }
    }
    if !headers.error_message.is_null() {
        unsafe {
            let _ = CString::from_raw(headers.error_message);
        }
    }
}

/// Rust struct that wraps a Go callback and implements HeadersProvider
pub(crate) struct CallbackHeadersProvider {
    callback: HeadersProviderCallback,
    user_data: *mut std::ffi::c_void,
    in_use: AtomicBool, // Track concurrent access to detect thread-safety issues
}

impl CallbackHeadersProvider {
    pub(crate) fn new(callback: HeadersProviderCallback, user_data: *mut std::ffi::c_void) -> Self {
        Self {
            callback,
            user_data,
            in_use: AtomicBool::new(false),
        }
    }
}

// Safety: We assume the Go callback is thread-safe, but we validate at runtime
unsafe impl Send for CallbackHeadersProvider {}
unsafe impl Sync for CallbackHeadersProvider {}

#[async_trait]
impl HeadersProvider for CallbackHeadersProvider {
    async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
        // Check for concurrent access (indicates thread-safety issue)
        if self.in_use.swap(true, Ordering::SeqCst) {
            return Err(ZerobusError::InvalidArgument(
                "Concurrent headers provider callback detected - Go callback must be thread-safe"
                    .to_string(),
            ));
        }

        // Call the Go callback (synchronous)
        let c_headers = (self.callback)(self.user_data);

        // Release the lock before processing
        self.in_use.store(false, Ordering::SeqCst);

        // Check for error
        if !c_headers.error_message.is_null() {
            let error_str = unsafe {
                CStr::from_ptr(c_headers.error_message)
                    .to_string_lossy()
                    .into_owned()
            };
            zerobus_free_headers(c_headers);
            return Err(ZerobusError::InvalidArgument(format!(
                "Headers provider error: {}",
                error_str
            )));
        }

        // Convert C headers to Rust HashMap
        let mut headers = HashMap::new();
        if !c_headers.headers.is_null() && c_headers.count > 0 {
            unsafe {
                let headers_slice = std::slice::from_raw_parts(c_headers.headers, c_headers.count);
                for header in headers_slice {
                    if !header.key.is_null() && !header.value.is_null() {
                        let key = CStr::from_ptr(header.key).to_string_lossy().into_owned();
                        let value = CStr::from_ptr(header.value).to_string_lossy().into_owned();

                        // Use interned keys to minimize memory leaks
                        // Only unique header names are leaked (typically < 10 strings for lifetime of process)
                        let static_key = intern_header_key(key);
                        headers.insert(static_key, value);
                    }
                }
            }
        }

        zerobus_free_headers(c_headers);
        Ok(headers)
    }
}

// ============================================================================
// SDK Functions
// ============================================================================

/// Safe wrapper to validate SDK pointer
pub(crate) fn validate_sdk_ptr<'a>(sdk: *mut CZerobusSdk) -> Result<&'a ZerobusSdk, &'static str> {
    if sdk.is_null() {
        return Err("SDK pointer is null");
    }
    // Still unsafe, but centralized and validated
    unsafe { Ok(&*(sdk as *const ZerobusSdk)) }
}

/// Safe wrapper to validate mutable SDK pointer
pub(crate) fn validate_sdk_ptr_mut<'a>(
    sdk: *mut CZerobusSdk,
) -> Result<&'a mut ZerobusSdk, &'static str> {
    if sdk.is_null() {
        return Err("SDK pointer is null");
    }
    unsafe { Ok(&mut *(sdk as *mut ZerobusSdk)) }
}

/// Safe wrapper to validate stream pointer
pub(crate) fn validate_stream_ptr<'a>(
    stream: *mut CZerobusStream,
) -> Result<&'a ZerobusStream, &'static str> {
    if stream.is_null() {
        return Err("Stream pointer is null");
    }
    unsafe { Ok(&*(stream as *const ZerobusStream)) }
}

/// Safe wrapper to validate mutable stream pointer
pub(crate) fn validate_stream_ptr_mut<'a>(
    stream: *mut CZerobusStream,
) -> Result<&'a mut ZerobusStream, &'static str> {
    if stream.is_null() {
        return Err("Stream pointer is null");
    }
    unsafe { Ok(&mut *(stream as *mut ZerobusStream)) }
}

/// Helper to write error result
pub(crate) fn write_error_result(result: *mut CResult, message: &str, is_retryable: bool) {
    if !result.is_null() {
        unsafe {
            *result = CResult {
                success: false,
                error_message: CString::new(message)
                    .unwrap_or_else(|_| CString::new("Error message contains null byte").unwrap())
                    .into_raw(),
                is_retryable,
            };
        }
    }
}

/// Helper to write success result
pub(crate) fn write_success_result(result: *mut CResult) {
    if !result.is_null() {
        unsafe {
            *result = CResult::success();
        }
    }
}

/// Create a new ZerobusSdk instance
/// Returns NULL on error. Check the result parameter for error details.
#[no_mangle]
pub extern "C" fn zerobus_sdk_new(
    zerobus_endpoint: *const c_char,
    unity_catalog_url: *const c_char,
    result: *mut CResult,
) -> *mut CZerobusSdk {
    init_logging();

    let res = (|| -> Result<*mut CZerobusSdk, String> {
        let endpoint = unsafe { c_str_to_string(zerobus_endpoint).map_err(|e| e.to_string())? };
        let catalog_url = unsafe { c_str_to_string(unity_catalog_url).map_err(|e| e.to_string())? };

        let mut builder = ZerobusSdk::builder()
            .endpoint(endpoint.clone())
            .unity_catalog_url(catalog_url);
        if endpoint.starts_with("http://") {
            builder = builder.tls_config(Arc::new(NoTlsConfig));
        }
        let sdk = builder.build().map_err(|e| e.to_string())?;
        let boxed = Box::new(sdk);
        Ok(Box::into_raw(boxed) as *mut CZerobusSdk)
    })();

    match res {
        Ok(sdk_ptr) => {
            if !result.is_null() {
                unsafe {
                    *result = CResult::success();
                }
            }
            sdk_ptr
        }
        Err(err) => {
            if !result.is_null() {
                let err_msg =
                    CString::new(err).unwrap_or_else(|_| CString::new("Unknown error").unwrap());
                unsafe {
                    *result = CResult {
                        success: false,
                        error_message: err_msg.into_raw(),
                        is_retryable: false,
                    };
                }
            }
            ptr::null_mut()
        }
    }
}

/// Free the SDK instance
#[no_mangle]
pub extern "C" fn zerobus_sdk_free(sdk: *mut CZerobusSdk) {
    if !sdk.is_null() {
        unsafe {
            let _ = Box::from_raw(sdk as *mut ZerobusSdk);
        }
    }
}

/// Set whether to use TLS for connections.
///
/// Deprecated: This function is a no-op. TLS is now controlled via the `TlsConfig`
/// trait passed to the SDK builder. This function is retained for ABI compatibility.
#[no_mangle]
#[allow(deprecated)]
pub extern "C" fn zerobus_sdk_set_use_tls(sdk: *mut CZerobusSdk, _use_tls: bool) {
    if let Ok(sdk_mut) = validate_sdk_ptr_mut(sdk) {
        sdk_mut.use_tls = _use_tls;
    }
}

/// Create a stream with OAuth authentication
/// descriptor_proto_bytes: protobuf-encoded DescriptorProto (can be NULL for JSON streams)
#[no_mangle]
pub extern "C" fn zerobus_sdk_create_stream(
    sdk: *mut CZerobusSdk,
    table_name: *const c_char,
    descriptor_proto_bytes: *const u8,
    descriptor_proto_len: usize,
    client_id: *const c_char,
    client_secret: *const c_char,
    options: *const CStreamConfigurationOptions,
    result: *mut CResult,
) -> *mut CZerobusStream {
    let sdk_ref = match validate_sdk_ptr(sdk) {
        Ok(s) => s,
        Err(msg) => {
            write_error_result(result, msg, false);
            return ptr::null_mut();
        }
    };

    let res = RUNTIME.block_on(async {
        let table_name_str = unsafe { c_str_to_string(table_name).map_err(|e| e.to_string())? };
        let client_id_str = unsafe { c_str_to_string(client_id).map_err(|e| e.to_string())? };
        let client_secret_str =
            unsafe { c_str_to_string(client_secret).map_err(|e| e.to_string())? };

        // Decode descriptor if provided
        let descriptor_proto = if !descriptor_proto_bytes.is_null() && descriptor_proto_len > 0 {
            let bytes =
                unsafe { std::slice::from_raw_parts(descriptor_proto_bytes, descriptor_proto_len) };
            Some(prost_types::DescriptorProto::decode(bytes).map_err(|e| e.to_string())?)
        } else {
            None
        };

        let table_props = TableProperties {
            table_name: table_name_str,
            descriptor_proto,
        };

        let stream_options = if !options.is_null() {
            Some(unsafe { (*options).into() })
        } else {
            None
        };

        let stream = sdk_ref
            .create_stream(
                table_props,
                client_id_str,
                client_secret_str,
                stream_options,
            )
            .await
            .map_err(|e| e.to_string())?;

        let boxed = Box::new(stream);
        Ok::<*mut CZerobusStream, String>(Box::into_raw(boxed) as *mut CZerobusStream)
    });

    match res {
        Ok(stream_ptr) => {
            write_success_result(result);
            stream_ptr
        }
        Err(err) => {
            write_error_result(result, &err, false);
            ptr::null_mut()
        }
    }
}

/// Create a stream with a custom headers provider callback
/// This allows you to provide custom authentication headers via a Go callback function
#[no_mangle]
pub extern "C" fn zerobus_sdk_create_stream_with_headers_provider(
    sdk: *mut CZerobusSdk,
    table_name: *const c_char,
    descriptor_proto_bytes: *const u8,
    descriptor_proto_len: usize,
    headers_callback: HeadersProviderCallback,
    user_data: *mut std::ffi::c_void,
    options: *const CStreamConfigurationOptions,
    result: *mut CResult,
) -> *mut CZerobusStream {
    let sdk_ref = match validate_sdk_ptr(sdk) {
        Ok(s) => s,
        Err(msg) => {
            write_error_result(result, msg, false);
            return ptr::null_mut();
        }
    };

    let res = RUNTIME.block_on(async {
        let table_name_str = unsafe { c_str_to_string(table_name).map_err(|e| e.to_string())? };

        // Decode descriptor if provided
        let descriptor_proto = if !descriptor_proto_bytes.is_null() && descriptor_proto_len > 0 {
            let bytes =
                unsafe { std::slice::from_raw_parts(descriptor_proto_bytes, descriptor_proto_len) };
            Some(prost_types::DescriptorProto::decode(bytes).map_err(|e| e.to_string())?)
        } else {
            None
        };

        let table_props = TableProperties {
            table_name: table_name_str,
            descriptor_proto,
        };

        let stream_options = if !options.is_null() {
            Some(unsafe { (*options).into() })
        } else {
            None
        };

        // Create the headers provider from the callback with thread-safety validation
        let headers_provider = Arc::new(CallbackHeadersProvider::new(headers_callback, user_data));

        let stream = sdk_ref
            .create_stream_with_headers_provider(table_props, headers_provider, stream_options)
            .await
            .map_err(|e| e.to_string())?;

        let boxed = Box::new(stream);
        Ok::<*mut CZerobusStream, String>(Box::into_raw(boxed) as *mut CZerobusStream)
    });

    match res {
        Ok(stream_ptr) => {
            write_success_result(result);
            stream_ptr
        }
        Err(err) => {
            write_error_result(result, &err, false);
            ptr::null_mut()
        }
    }
}

/// Free a stream instance
#[no_mangle]
pub extern "C" fn zerobus_stream_free(stream: *mut CZerobusStream) {
    if !stream.is_null() {
        unsafe {
            let _ = Box::from_raw(stream as *mut ZerobusStream);
        }
    }
}

/// Ingest a record (protobuf encoded)
/// Returns the offset directly
/// Returns -1 on error
#[no_mangle]
pub extern "C" fn zerobus_stream_ingest_proto_record(
    stream: *mut CZerobusStream,
    data: *const u8,
    data_len: usize,
    result: *mut CResult,
) -> i64 {
    if data.is_null() {
        write_error_result(result, "Invalid data pointer", false);
        return -1;
    }

    let stream_ref = match validate_stream_ptr(stream) {
        Ok(s) => s,
        Err(msg) => {
            write_error_result(result, msg, false);
            return -1;
        }
    };

    let data_slice = unsafe { std::slice::from_raw_parts(data, data_len) };
    let data_vec = data_slice.to_vec();

    // Queue the record and get the offset directly
    let offset_res = RUNTIME.block_on(async {
        let payload = EncodedRecord::Proto(data_vec);
        stream_ref.ingest_record_offset(payload).await
    });

    match offset_res {
        Ok(offset) => {
            write_success_result(result);
            offset
        }
        Err(err) => {
            if !result.is_null() {
                unsafe {
                    *result = CResult::error(err);
                }
            }
            -1
        }
    }
}

/// Ingest a JSON record
/// Returns the offset directly
/// Returns -1 on error
#[no_mangle]
pub extern "C" fn zerobus_stream_ingest_json_record(
    stream: *mut CZerobusStream,
    json_data: *const c_char,
    result: *mut CResult,
) -> i64 {
    let stream_ref = match validate_stream_ptr(stream) {
        Ok(s) => s,
        Err(msg) => {
            write_error_result(result, msg, false);
            return -1;
        }
    };

    let json_str = match unsafe { c_str_to_string(json_data) } {
        Ok(s) => s,
        Err(e) => {
            write_error_result(result, e, false);
            return -1;
        }
    };

    // Queue the record and get the offset directly
    let offset_res = RUNTIME.block_on(async {
        let payload = EncodedRecord::Json(json_str);
        stream_ref.ingest_record_offset(payload).await
    });

    match offset_res {
        Ok(offset) => {
            write_success_result(result);
            offset
        }
        Err(err) => {
            if !result.is_null() {
                unsafe {
                    *result = CResult::error(err);
                }
            }
            -1
        }
    }
}

/// Ingest a batch of protobuf records
/// Returns the offset of the last record in the batch, or -1 on error
/// Returns -2 if batch is empty
#[no_mangle]
pub extern "C" fn zerobus_stream_ingest_proto_records(
    stream: *mut CZerobusStream,
    records: *const *const u8,
    record_lens: *const usize,
    num_records: usize,
    result: *mut CResult,
) -> i64 {
    if records.is_null() || record_lens.is_null() {
        write_error_result(result, "Invalid records pointer", false);
        return -1;
    }

    if num_records == 0 {
        write_success_result(result);
        return -2; // Empty batch
    }

    let stream_ref = match validate_stream_ptr(stream) {
        Ok(s) => s,
        Err(msg) => {
            write_error_result(result, msg, false);
            return -1;
        }
    };

    // Convert array of C pointers to Vec<Vec<u8>>
    let records_vec: Vec<Vec<u8>> = unsafe {
        let records_slice = std::slice::from_raw_parts(records, num_records);
        let lens_slice = std::slice::from_raw_parts(record_lens, num_records);

        records_slice
            .iter()
            .zip(lens_slice.iter())
            .map(|(ptr, len)| {
                let data_slice = std::slice::from_raw_parts(*ptr, *len);
                data_slice.to_vec()
            })
            .collect()
    };

    // Queue the records and get the offset
    let offset_res = RUNTIME.block_on(async {
        let payloads: Vec<EncodedRecord> =
            records_vec.into_iter().map(EncodedRecord::Proto).collect();
        stream_ref.ingest_records_offset(payloads).await
    });

    match offset_res {
        Ok(Some(offset)) => {
            write_success_result(result);
            offset
        }
        Ok(None) => {
            write_success_result(result);
            -2 // Empty batch
        }
        Err(err) => {
            if !result.is_null() {
                unsafe {
                    *result = CResult::error(err);
                }
            }
            -1
        }
    }
}

/// Ingest a batch of JSON records
/// Returns the offset of the last record in the batch, or -1 on error
/// Returns -2 if batch is empty
#[no_mangle]
pub extern "C" fn zerobus_stream_ingest_json_records(
    stream: *mut CZerobusStream,
    json_records: *const *const c_char,
    num_records: usize,
    result: *mut CResult,
) -> i64 {
    if json_records.is_null() {
        write_error_result(result, "Invalid records pointer", false);
        return -1;
    }

    if num_records == 0 {
        write_success_result(result);
        return -2; // Empty batch
    }

    let stream_ref = match validate_stream_ptr(stream) {
        Ok(s) => s,
        Err(msg) => {
            write_error_result(result, msg, false);
            return -1;
        }
    };

    // Convert array of C strings to Vec<String>
    let json_vec: Result<Vec<String>, _> = unsafe {
        let json_slice = std::slice::from_raw_parts(json_records, num_records);
        json_slice.iter().map(|ptr| c_str_to_string(*ptr)).collect()
    };

    let json_vec = match json_vec {
        Ok(v) => v,
        Err(e) => {
            write_error_result(result, e, false);
            return -1;
        }
    };

    // Queue the records and get the offset
    let offset_res = RUNTIME.block_on(async {
        let payloads: Vec<EncodedRecord> = json_vec.into_iter().map(EncodedRecord::Json).collect();
        stream_ref.ingest_records_offset(payloads).await
    });

    match offset_res {
        Ok(Some(offset)) => {
            write_success_result(result);
            offset
        }
        Ok(None) => {
            write_success_result(result);
            -2 // Empty batch
        }
        Err(err) => {
            if !result.is_null() {
                unsafe {
                    *result = CResult::error(err);
                }
            }
            -1
        }
    }
}

/// Wait for a specific offset to be acknowledged by the server
#[no_mangle]
pub extern "C" fn zerobus_stream_wait_for_offset(
    stream: *mut CZerobusStream,
    offset: i64,
    result: *mut CResult,
) -> bool {
    let stream_ref = match validate_stream_ptr(stream) {
        Ok(s) => s,
        Err(msg) => {
            write_error_result(result, msg, false);
            return false;
        }
    };

    let res = RUNTIME.block_on(async { stream_ref.wait_for_offset(offset).await });

    match res {
        Ok(()) => {
            write_success_result(result);
            true
        }
        Err(err) => {
            if !result.is_null() {
                unsafe {
                    *result = CResult::error(err);
                }
            }
            false
        }
    }
}

/// Flush all pending records
#[no_mangle]
pub extern "C" fn zerobus_stream_flush(stream: *mut CZerobusStream, result: *mut CResult) -> bool {
    let stream_ref = match validate_stream_ptr(stream) {
        Ok(s) => s,
        Err(msg) => {
            write_error_result(result, msg, false);
            return false;
        }
    };

    let res = RUNTIME.block_on(async { stream_ref.flush().await });

    match res {
        Ok(_) => {
            write_success_result(result);
            true
        }
        Err(err) => {
            if !result.is_null() {
                unsafe {
                    *result = CResult::error(err);
                }
            }
            false
        }
    }
}

/// Get unacknowledged records from a closed stream
/// Returns a CRecordArray that must be freed with zerobus_free_record_array
#[no_mangle]
pub extern "C" fn zerobus_stream_get_unacked_records(
    stream: *mut CZerobusStream,
    result: *mut CResult,
) -> CRecordArray {
    let stream_ref = match validate_stream_ptr(stream) {
        Ok(s) => s,
        Err(msg) => {
            write_error_result(result, msg, false);
            return CRecordArray {
                records: ptr::null_mut(),
                len: 0,
            };
        }
    };

    let records_res = RUNTIME.block_on(async { stream_ref.get_unacked_records().await });

    match records_res {
        Ok(records_iter) => {
            // Collect into Vec
            let records_vec: Vec<EncodedRecord> = records_iter.collect();
            let len = records_vec.len();

            // Convert to CRecords
            let mut c_records: Vec<CRecord> = records_vec
                .into_iter()
                .map(|record| match record {
                    EncodedRecord::Proto(data) => {
                        let data_len = data.len();
                        let data_ptr = Box::into_raw(data.into_boxed_slice()) as *mut u8;
                        CRecord {
                            is_json: false,
                            data: data_ptr,
                            data_len,
                        }
                    }
                    EncodedRecord::Json(json_str) => {
                        let bytes = json_str.into_bytes();
                        let data_len = bytes.len();
                        let data_ptr = Box::into_raw(bytes.into_boxed_slice()) as *mut u8;
                        CRecord {
                            is_json: true,
                            data: data_ptr,
                            data_len,
                        }
                    }
                })
                .collect();

            let records_ptr = c_records.as_mut_ptr();
            std::mem::forget(c_records); // Don't drop, Go will call free

            write_success_result(result);
            CRecordArray {
                records: records_ptr,
                len,
            }
        }
        Err(err) => {
            if !result.is_null() {
                unsafe {
                    *result = CResult::error(err);
                }
            }
            CRecordArray {
                records: ptr::null_mut(),
                len: 0,
            }
        }
    }
}

/// Free a CRecordArray returned by zerobus_stream_get_unacked_records
#[no_mangle]
pub extern "C" fn zerobus_free_record_array(array: CRecordArray) {
    if array.records.is_null() || array.len == 0 {
        return;
    }

    unsafe {
        let records_vec = Vec::from_raw_parts(array.records, array.len, array.len);
        for record in records_vec {
            if !record.data.is_null() && record.data_len > 0 {
                let _ = Vec::from_raw_parts(record.data, record.data_len, record.data_len);
            }
        }
    }
}

/// Close the stream gracefully
#[no_mangle]
pub extern "C" fn zerobus_stream_close(stream: *mut CZerobusStream, result: *mut CResult) -> bool {
    let stream_ref = match validate_stream_ptr_mut(stream) {
        Ok(s) => s,
        Err(msg) => {
            write_error_result(result, msg, false);
            return false;
        }
    };

    let res = RUNTIME.block_on(async { stream_ref.close().await });

    match res {
        Ok(_) => {
            write_success_result(result);
            true
        }
        Err(err) => {
            if !result.is_null() {
                unsafe {
                    *result = CResult::error(err);
                }
            }
            false
        }
    }
}

/// Free error message string
#[no_mangle]
pub extern "C" fn zerobus_free_error_message(message: *mut c_char) {
    if !message.is_null() {
        unsafe {
            let _ = CString::from_raw(message);
        }
    }
}

/// Get default stream configuration options
#[no_mangle]
pub extern "C" fn zerobus_get_default_config() -> CStreamConfigurationOptions {
    let default_opts = StreamConfigurationOptions::default();
    CStreamConfigurationOptions {
        max_inflight_requests: default_opts.max_inflight_requests,
        recovery: default_opts.recovery,
        recovery_timeout_ms: default_opts.recovery_timeout_ms,
        recovery_backoff_ms: default_opts.recovery_backoff_ms,
        recovery_retries: default_opts.recovery_retries,
        server_lack_of_ack_timeout_ms: default_opts.server_lack_of_ack_timeout_ms,
        flush_timeout_ms: default_opts.flush_timeout_ms,
        record_type: 1,                            // RecordType::Proto
        stream_paused_max_wait_time_ms: 0,         // Not used when has_ is false
        has_stream_paused_max_wait_time_ms: false, // Default is None
        callback_max_wait_time_ms: 0,              // Not used when has_ is false
        has_callback_max_wait_time_ms: false,      // Default is None
    }
}
