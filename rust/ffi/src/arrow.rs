//! Arrow Flight FFI surface.

use crate::arrow_c_data::{CArrowArray, CArrowSchema};
use crate::common::*;
use arrow_ipc::{reader::StreamReader, writer::StreamWriter, CompressionType};
use bytes::Bytes;
use databricks_zerobus_ingest_sdk::internal::abort_arrow_stream_and_wait;
use databricks_zerobus_ingest_sdk::internal::arrow_c_data::{
    import_c_data_record_batch, FFI_ArrowArray, FFI_ArrowSchema,
};
use databricks_zerobus_ingest_sdk::{
    HeadersProvider, RecordBatch, StreamBuilder, ZerobusArrowStream, ZerobusError, ZerobusResult,
};
use std::os::raw::c_char;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::{Arc, Mutex as StdMutex};
use std::thread;
use std::time::Duration;
use tracing::{error, warn};

// ============================================================================
// Arrow Flight FFI
// ============================================================================

const ARROW_STREAM_SHUTDOWN_WARNING_AFTER: Duration = Duration::from_secs(30);

/// Opaque handle for an Arrow Flight stream.
///
/// FFI pointers are `Box<ZerobusArrowStream>` addresses cast to this type.
/// Creation, validation, test-hook casts, and `Box::from_raw` must stay coupled.
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
    /// Maximum acknowledgment wait in milliseconds during graceful stream close.
    /// -1 = use the available server grace period, 0 = no ACK wait, >0 = capped ACK wait.
    /// Bounded transport cleanup still runs with 0; very short server grace periods get a
    /// best-effort local cleanup window even if the server may already have hard-closed.
    pub stream_paused_max_wait_time_ms: i64,
}

fn c_to_compression(value: i32) -> Option<CompressionType> {
    match value {
        0 => Some(CompressionType::LZ4_FRAME),
        1 => Some(CompressionType::ZSTD),
        _ => None,
    }
}

fn c_to_stream_paused_ms(value: i64) -> Option<u64> {
    if value < 0 {
        None
    } else {
        Some(value as u64)
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
#[allow(clippy::result_large_err)]
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

/// Serializes a `RecordBatch` to Arrow IPC stream bytes (schema + one batch).
#[allow(clippy::result_large_err)]
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

fn apply_c_arrow_stream_options<'a>(
    builder: StreamBuilder<'a>,
    c: &CArrowStreamConfigurationOptions,
) -> StreamBuilder<'a> {
    builder
        .max_inflight_batches(c.max_inflight_batches)
        .recovery(c.recovery)
        .recovery_timeout_ms(c.recovery_timeout_ms)
        .recovery_backoff_ms(c.recovery_backoff_ms)
        .recovery_retries(c.recovery_retries)
        .server_lack_of_ack_timeout_ms(c.server_lack_of_ack_timeout_ms)
        .flush_timeout_ms(c.flush_timeout_ms)
        .connection_timeout_ms(c.connection_timeout_ms)
        .ipc_compression(c_to_compression(c.ipc_compression))
        .stream_paused_max_wait_time_ms(c_to_stream_paused_ms(c.stream_paused_max_wait_time_ms))
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
    ffi_guard(result, ptr::null_mut(), move || {
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
            let schema_bytes =
                unsafe { std::slice::from_raw_parts(schema_ipc_bytes, schema_ipc_len) };
            let schema = ipc_bytes_to_schema(schema_bytes).map_err(|e| e.to_string())?;

            let mut builder = sdk_ref
                .stream_builder()
                .table(table_name_str)
                .oauth(client_id_str, client_secret_str)
                .arrow(schema);
            if !options.is_null() {
                builder = apply_c_arrow_stream_options(builder, unsafe { &*options });
            }

            let stream = builder.build_arrow().await.map_err(|e| e.to_string())?;

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
    })
}

/// Creates an Arrow Flight stream with a custom headers provider callback.
///
/// `schema_ipc_bytes` must point to Arrow IPC stream bytes encoding only the schema.
///
/// Ownership of `user_data` / `free_user_data` follows
/// `zerobus_sdk_create_stream_with_headers_provider` — once called the FFI owns
/// `user_data` and invokes `free_user_data` exactly once on every path (on
/// success after any in-flight `get_headers` returns; on failure before
/// returning null). The caller must never free `user_data` itself.
#[no_mangle]
pub extern "C" fn zerobus_sdk_create_arrow_stream_with_headers_provider(
    sdk: *mut CZerobusSdk,
    table_name: *const c_char,
    schema_ipc_bytes: *const u8,
    schema_ipc_len: usize,
    headers_callback: HeadersProviderCallback,
    user_data: *mut std::ffi::c_void,
    // Written inline (not via HeadersProviderFreeCallback) so cbindgen emits a
    // nullable C function pointer instead of an opaque struct.
    free_user_data: Option<extern "C" fn(user_data: *mut std::ffi::c_void)>,
    options: *const CArrowStreamConfigurationOptions,
    result: *mut CResult,
) -> *mut CArrowStream {
    ffi_guard(result, ptr::null_mut(), move || {
        // INVARIANT: construct the provider Arc *before* any fallible work (see
        // the proto path in stream.rs). It owns `user_data`, so its Drop invokes
        // `free_user_data` exactly once on every path — the free-once contract
        // the wrappers rely on. Moving this after an early-return would leak
        // `user_data`, since the wrappers do not free on failure.
        let headers_provider: Arc<dyn HeadersProvider> = Arc::new(CallbackHeadersProvider::new(
            headers_callback,
            user_data,
            free_user_data,
        ));

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
            let schema_bytes =
                unsafe { std::slice::from_raw_parts(schema_ipc_bytes, schema_ipc_len) };
            let schema = ipc_bytes_to_schema(schema_bytes).map_err(|e| e.to_string())?;

            let mut builder = sdk_ref
                .stream_builder()
                .table(table_name_str)
                .headers_provider(headers_provider)
                .arrow(schema);
            if !options.is_null() {
                builder = apply_c_arrow_stream_options(builder, unsafe { &*options });
            }

            let stream = builder.build_arrow().await.map_err(|e| e.to_string())?;

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
    })
}

fn abort_and_drop_arrow_stream(stream: Box<ZerobusArrowStream>) {
    let mut stream = Some(stream);
    let shutdown = catch_unwind(AssertUnwindSafe(|| {
        let stream_ref = stream
            .as_ref()
            .expect("Arrow stream must remain owned during shutdown");
        RUNTIME.block_on(async {
            let shutdown = abort_arrow_stream_and_wait(stream_ref);
            tokio::pin!(shutdown);
            while tokio::time::timeout(ARROW_STREAM_SHUTDOWN_WARNING_AFTER, &mut shutdown)
                .await
                .is_err()
            {
                warn!(
                    threshold_seconds = ARROW_STREAM_SHUTDOWN_WARNING_AFTER.as_secs(),
                    "Arrow stream background shutdown is still running; continuing to wait"
                );
            }
        });
        drop(
            stream
                .take()
                .expect("Arrow stream must remain owned until shutdown completes"),
        );
    }));

    if shutdown.is_err() {
        error!("Arrow stream shutdown or destruction panicked; aborting process");
        std::process::abort();
    }
}

fn abort_and_drop_arrow_stream_on_thread(stream: Box<ZerobusArrowStream>) {
    let stream_slot = Arc::new(StdMutex::new(Some(stream)));
    let worker_slot = Arc::clone(&stream_slot);
    let worker = thread::Builder::new()
        .name("zerobus-arrow-free".to_string())
        .spawn(move || {
            let mut slot = worker_slot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let stream = slot
                .take()
                .expect("Arrow stream shutdown has exactly one owner");
            drop(slot);
            abort_and_drop_arrow_stream(stream);
        });

    let worker = match worker {
        Ok(worker) => worker,
        Err(error) => {
            // Leaking only the stream cannot preserve the callback guarantee: an ACK
            // can already have moved the last SDK-owned batch into the request body,
            // which may release it after this function returns.
            error!(%error, "Unable to start Arrow stream shutdown thread; aborting process");
            std::process::abort();
        }
    };

    if worker.join().is_err() {
        error!("Arrow stream shutdown thread panicked; aborting process");
        std::process::abort();
    }
}

/// Frees an Arrow Flight stream instance.
///
/// This call blocks until background shutdown completes, every Flight request body reaches EOF
/// or is dropped, and all retained Arrow C Data owners are released. If shutdown takes longer
/// than 30 seconds, it logs a warning every 30 seconds while continuing to wait. It does not
/// return on a timeout.
/// When the calling restrictions below are respected, no Arrow C Data release callback for this
/// stream can run after this function returns.
/// An internal shutdown panic, a required helper-thread spawn failure, or a helper-thread panic
/// terminates the process rather than returning without that guarantee.
///
/// Do not race this function with another operation on the same stream handle. Freeing this same
/// stream reentrantly from one of its SDK callbacks is unsupported because shutdown would wait
/// for the callback that is making the call. Freeing a different stream from a callback remains
/// supported.
#[no_mangle]
pub extern "C" fn zerobus_arrow_stream_free(stream: *mut CArrowStream) {
    ffi_guard(ptr::null_mut(), (), move || {
        if !stream.is_null() {
            unsafe {
                let stream = Box::from_raw(stream as *mut ZerobusArrowStream);
                match tokio::runtime::Handle::try_current() {
                    Ok(handle)
                        if handle.runtime_flavor()
                            == tokio::runtime::RuntimeFlavor::MultiThread =>
                    {
                        tokio::task::block_in_place(|| abort_and_drop_arrow_stream(stream));
                    }
                    Ok(_) => abort_and_drop_arrow_stream_on_thread(stream),
                    Err(_) => abort_and_drop_arrow_stream(stream),
                }
            }
        }
    })
}

/// Ingests one Arrow RecordBatch supplied as Arrow IPC stream bytes.
///
/// `ipc_bytes` must be a valid Arrow IPC stream (schema + one record batch).
/// The bytes are deserialised to a RecordBatch internally. Works with all
/// compression settings. Returns the logical offset assigned to this batch, or -1 on error.
#[no_mangle]
pub extern "C" fn zerobus_arrow_stream_ingest_batch(
    stream: *mut CArrowStream,
    ipc_bytes: *const u8,
    ipc_len: usize,
    result: *mut CResult,
) -> i64 {
    ffi_guard(result, -1, move || {
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
            stream_ref
                .ingest_ipc_batch(Bytes::copy_from_slice(bytes))
                .await
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
    })
}

/// Ingests one canonical Arrow C Data Interface RecordBatch.
///
/// When both `array` and `schema` are non-null, this function consumes them on
/// every success or error path. Their release callbacks are cleared before
/// validation, and the imported buffers may remain owned by the stream until
/// acknowledgment, recovery finalization, or stream destruction.
///
/// Every non-null pointer must address a valid, properly aligned canonical
/// `ArrowArray` / `ArrowSchema` structure satisfying the Arrow C Data
/// Interface. All referenced children, dictionaries, buffers, `private_data`,
/// and release callbacks must remain valid for the lifetime required by the
/// producer contract. After ownership transfer, release callbacks may run on
/// any thread that drops the final owner, including SDK runtime/transport
/// threads or the thread calling `zerobus_arrow_stream_free`. They must be
/// thread-safe and must not unwind or throw across the C ABI.
///
/// Malformed, dangling, or malicious structures are caller undefined behavior
/// and cannot be safely validated by this function.
#[no_mangle]
pub extern "C" fn zerobus_arrow_stream_ingest_c_data(
    stream: *mut CArrowStream,
    array: *mut CArrowArray,
    schema: *mut CArrowSchema,
    result: *mut CResult,
) -> i64 {
    ffi_guard(result, -1, move || {
        if array.is_null() || schema.is_null() {
            write_error_result(
                result,
                "ArrowArray and ArrowSchema pointers are required",
                false,
            );
            return -1;
        }

        let array = unsafe { FFI_ArrowArray::from_raw(array.cast::<FFI_ArrowArray>()) };
        let schema = unsafe { FFI_ArrowSchema::from_raw(schema.cast::<FFI_ArrowSchema>()) };

        if array.is_released() || schema.release.is_none() {
            write_error_result(
                result,
                "Arrow C Data input has already been released",
                false,
            );
            return -1;
        }

        let stream_ref = match validate_arrow_stream_ptr(stream) {
            Ok(stream) => stream,
            Err(message) => {
                write_error_result(result, message, false);
                return -1;
            }
        };

        let batch = match unsafe { import_c_data_record_batch(array, schema) } {
            Ok(batch) => batch,
            Err(error) => {
                if !result.is_null() {
                    unsafe { *result = CResult::error(error) };
                }
                return -1;
            }
        };

        match RUNTIME.block_on(stream_ref.ingest_batch(batch)) {
            Ok(offset) => {
                write_success_result(result);
                offset
            }
            Err(error) => {
                if !result.is_null() {
                    unsafe { *result = CResult::error(error) };
                }
                -1
            }
        }
    })
}

/// Ingests one Arrow RecordBatch supplied as Arrow IPC stream bytes.
///
/// Equivalent to `zerobus_arrow_stream_ingest_batch`. Both functions deserialise the IPC
/// bytes to a `RecordBatch` and re-encode with the stream's compression settings, so
/// either works regardless of whether the stream was created with compression.
/// Returns the logical offset assigned to this batch, or -1 on error.
#[no_mangle]
pub extern "C" fn zerobus_arrow_stream_ingest_batch_via_record_batch(
    stream: *mut CArrowStream,
    ipc_bytes: *const u8,
    ipc_len: usize,
    result: *mut CResult,
) -> i64 {
    ffi_guard(result, -1, move || {
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
            stream_ref
                .ingest_ipc_batch(bytes::Bytes::copy_from_slice(bytes))
                .await
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
    })
}

/// Waits until the server acknowledges the batch at the given logical offset.
#[no_mangle]
pub extern "C" fn zerobus_arrow_stream_wait_for_offset(
    stream: *mut CArrowStream,
    offset: i64,
    result: *mut CResult,
) -> bool {
    ffi_guard(result, false, move || {
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
    })
}

/// Flushes all pending batches and waits for their acknowledgment.
#[no_mangle]
pub extern "C" fn zerobus_arrow_stream_flush(
    stream: *mut CArrowStream,
    result: *mut CResult,
) -> bool {
    ffi_guard(result, false, move || {
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
    })
}

/// Gracefully closes the stream, flushing all pending batches first.
#[no_mangle]
pub extern "C" fn zerobus_arrow_stream_close(
    stream: *mut CArrowStream,
    result: *mut CResult,
) -> bool {
    ffi_guard(result, false, move || {
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
    })
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
    ffi_guard(
        result,
        CArrowBatchArray {
            batches: ptr::null_mut(),
            lengths: ptr::null_mut(),
            count: 0,
        },
        move || {
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

                    // Convert to owning boxes first. If `record_batch_to_ipc_bytes`
                    // returns an error — or panics and `ffi_guard` unwinds — every
                    // already-converted batch is freed automatically when `owned`
                    // drops. (A `Vec<*mut u8>` of raw pointers would not: dropping it
                    // leaks the slices those pointers own.)
                    let mut owned: Vec<Box<[u8]>> = Vec::with_capacity(count);
                    for batch in &batches {
                        match record_batch_to_ipc_bytes(batch) {
                            Ok(bytes) => owned.push(bytes.into_boxed_slice()),
                            Err(e) => {
                                // `owned` drops here, freeing already-converted batches.
                                write_error_result(result, &e.to_string(), false);
                                return empty;
                            }
                        }
                    }

                    // The fallible/panic-prone work is done. Everything below is
                    // infallible — pre-sized pushes, `Box::into_raw`, and
                    // `into_boxed_slice` on len==capacity vecs never panic — so no
                    // batch can leak between taking raw ownership and returning.
                    let mut batch_ptrs: Vec<*mut u8> = Vec::with_capacity(count);
                    let mut batch_lens: Vec<usize> = Vec::with_capacity(count);
                    for bytes in owned {
                        batch_lens.push(bytes.len());
                        batch_ptrs.push(Box::into_raw(bytes) as *mut u8);
                    }

                    // into_boxed_slice() shrinks to fit, guaranteeing capacity == len
                    // so the corresponding Box::from_raw in free_batch_array is sound.
                    let ptrs_box = batch_ptrs.into_boxed_slice();
                    let lens_box = batch_lens.into_boxed_slice();
                    let ptrs_ptr = Box::into_raw(ptrs_box) as *mut *mut u8;
                    let lens_ptr = Box::into_raw(lens_box) as *mut usize;

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
        },
    )
}

/// Frees a `CArrowBatchArray` returned by `zerobus_arrow_stream_get_unacked_batches`.
#[no_mangle]
pub extern "C" fn zerobus_arrow_free_batch_array(array: CArrowBatchArray) {
    ffi_guard(ptr::null_mut(), (), move || {
        if array.count == 0 {
            return;
        }
        unsafe {
            if !array.batches.is_null() && !array.lengths.is_null() {
                // Reconstruct as Box<[T]> using the original length. This is safe because
                // the pointers were produced by Box::into_raw(vec.into_boxed_slice()),
                // which guarantees capacity == len.
                let ptrs = Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                    array.batches,
                    array.count,
                ));
                let lens = Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                    array.lengths,
                    array.count,
                ));
                for (&ptr, &len) in ptrs.iter().zip(lens.iter()) {
                    if !ptr.is_null() && len > 0 {
                        // Each batch slice was produced by Box::into_raw(bytes.into_boxed_slice()),
                        // so capacity == len and this reconstruction is sound.
                        let _ = Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, len));
                    }
                }
            }
        }
    })
}

/// Returns whether the Arrow stream has been closed.
#[no_mangle]
pub extern "C" fn zerobus_arrow_stream_is_closed(stream: *mut CArrowStream) -> bool {
    // No CResult out-param; on a caught panic return `true` (treat as closed),
    // matching the answer for an invalid handle.
    ffi_guard(
        ptr::null_mut(),
        true,
        move || match validate_arrow_stream_ptr(stream) {
            Ok(s) => s.is_closed(),
            Err(_) => true,
        },
    )
}

/// Returns the default Arrow stream configuration options.
// Not wrapped in `ffi_guard`: builds a `#[repr(C)]` struct from constants, so
// nothing here can panic.
#[no_mangle]
pub extern "C" fn zerobus_arrow_get_default_config() -> CArrowStreamConfigurationOptions {
    use databricks_zerobus_ingest_sdk::stream_options::defaults;
    CArrowStreamConfigurationOptions {
        max_inflight_batches: 1_000,
        recovery: defaults::RECOVERY,
        recovery_timeout_ms: defaults::RECOVERY_TIMEOUT_MS,
        recovery_backoff_ms: defaults::RECOVERY_BACKOFF_MS,
        recovery_retries: defaults::RECOVERY_RETRIES,
        server_lack_of_ack_timeout_ms: defaults::SERVER_LACK_OF_ACK_TIMEOUT_MS,
        flush_timeout_ms: defaults::FLUSH_TIMEOUT_MS,
        connection_timeout_ms: defaults::CONNECTION_TIMEOUT_MS,
        ipc_compression: -1,
        stream_paused_max_wait_time_ms: -1,
    }
}
