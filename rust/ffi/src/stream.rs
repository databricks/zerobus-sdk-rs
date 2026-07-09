//! Stream creation and record ingestion FFI surface.

use crate::common::*;
use databricks_zerobus_ingest_sdk::databricks::zerobus::RecordType;
use databricks_zerobus_ingest_sdk::{
    EncodedRecord, HeadersProvider, StreamBuilder, ZerobusError, ZerobusStream,
};
use prost::Message;
use std::mem::ManuallyDrop;
use std::ffi::CString;
use std::os::raw::c_char;
use std::ptr;
use std::sync::Arc;

// Builder option application helpers

fn apply_c_stream_options<'a>(
    builder: StreamBuilder<'a>,
    c: &CStreamConfigurationOptions,
) -> StreamBuilder<'a> {
    let builder = builder
        .max_inflight_requests(c.max_inflight_requests)
        .recovery(c.recovery)
        .recovery_timeout_ms(c.recovery_timeout_ms)
        .recovery_backoff_ms(c.recovery_backoff_ms)
        .recovery_retries(c.recovery_retries)
        .server_lack_of_ack_timeout_ms(c.server_lack_of_ack_timeout_ms)
        .flush_timeout_ms(c.flush_timeout_ms)
        .stream_paused_max_wait_time_ms(if c.has_stream_paused_max_wait_time_ms {
            Some(c.stream_paused_max_wait_time_ms)
        } else {
            None
        })
        .callback_max_wait_time_ms(if c.has_callback_max_wait_time_ms {
            Some(c.callback_max_wait_time_ms)
        } else {
            None
        });

    // Register the ack callback only when the caller supplied a function pointer.
    if c.ack_on_ack.is_some() || c.ack_on_error.is_some() {
        builder.ack_callback(Arc::new(CallbackAckCallback::new(
            c.ack_on_ack,
            c.ack_on_error,
            c.ack_user_data,
        )))
    } else {
        builder
    }
}

#[derive(Clone, Copy)]
struct SendPtr<T>(*mut T);

impl<T> SendPtr<T> {
    const fn new(ptr: *mut T) -> Self {
        Self(ptr)
    }

    const fn get(self) -> *mut T {
        self.0
    }
}

// Safety: this wrapper carries an opaque FFI pointer across task boundaries.
// The pointee's lifetime and synchronization remain the caller's contract.
unsafe impl<T> Send for SendPtr<T> {}

enum StreamCreateAuth {
    OAuth {
        client_id: String,
        client_secret: String,
    },
    HeadersProvider {
        headers_callback: HeadersProviderCallback,
        user_data: SendPtr<std::ffi::c_void>,
    },
}

async fn build_stream_from_parts(
    sdk_ref: &databricks_zerobus_ingest_sdk::ZerobusSdk,
    table_name: String,
    descriptor_proto: Option<prost_types::DescriptorProto>,
    auth: StreamCreateAuth,
    options: Option<CStreamConfigurationOptions>,
) -> Result<*mut CZerobusStream, ZerobusError> {
    let record_type = options
        .as_ref()
        .map(|c| c_record_type(c.record_type))
        .unwrap_or(RecordType::Proto);

    let base = match auth {
        StreamCreateAuth::OAuth {
            client_id,
            client_secret,
        } => sdk_ref
            .stream_builder()
            .table(table_name)
            .oauth(client_id, client_secret),
        StreamCreateAuth::HeadersProvider {
            headers_callback,
            user_data,
        } => {
            let headers_provider: Arc<dyn HeadersProvider> =
                Arc::new(CallbackHeadersProvider::new(headers_callback, user_data.get()));
            sdk_ref
                .stream_builder()
                .table(table_name)
                .headers_provider(headers_provider)
        }
    };

    let mut builder = match record_type {
        RecordType::Proto => {
            let desc = descriptor_proto.ok_or_else(|| {
                ZerobusError::InvalidArgument(
                    "Proto descriptor is required for Proto record type".to_string(),
                )
            })?;
            base.compiled_proto(desc)
        }
        RecordType::Json => base.json(),
        RecordType::Unspecified => {
            return Err(ZerobusError::InvalidArgument(
                "Record type is not specified".to_string(),
            ))
        }
    };

    if let Some(c) = options.as_ref() {
        builder = apply_c_stream_options(builder, c);
    }

    let stream = builder.build().await?;
    Ok(Arc::into_raw(Arc::new(stream)) as *mut CZerobusStream)
}

fn invoke_create_stream_async_callback(
    callback: CreateStreamAsyncCallback,
    stream: *mut CZerobusStream,
    result: CResult,
    user_data: *mut std::ffi::c_void,
) {
    let callback_result = result;
    let callback_result_ptr = &callback_result as *const CResult;

    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        callback(stream, callback_result_ptr, user_data)
    }))
    .is_err()
    {
        tracing::error!("async create_stream callback panicked; contained at FFI boundary");
    }

    if !callback_result.error_message.is_null() {
        unsafe {
            let _ = CString::from_raw(callback_result.error_message);
        }
    }
}

fn invoke_offset_async_callback(
    callback: OffsetAsyncCallback,
    offset: i64,
    result: CResult,
    user_data: *mut std::ffi::c_void,
) {
    let callback_result = result;
    let callback_result_ptr = &callback_result as *const CResult;

    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        callback(offset, callback_result_ptr, user_data)
    }))
    .is_err()
    {
        tracing::error!(offset, "async offset callback panicked; contained at FFI boundary");
    }

    if !callback_result.error_message.is_null() {
        unsafe {
            let _ = CString::from_raw(callback_result.error_message);
        }
    }
}

fn invoke_bool_async_callback(
    callback: BoolAsyncCallback,
    value: bool,
    result: CResult,
    user_data: *mut std::ffi::c_void,
) {
    let callback_result = result;
    let callback_result_ptr = &callback_result as *const CResult;

    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        callback(value, callback_result_ptr, user_data)
    }))
    .is_err()
    {
        tracing::error!(value, "async bool callback panicked; contained at FFI boundary");
    }

    if !callback_result.error_message.is_null() {
        unsafe {
            let _ = CString::from_raw(callback_result.error_message);
        }
    }
}

fn invoke_record_array_async_callback(
    callback: RecordArrayAsyncCallback,
    records: CRecordArray,
    result: CResult,
    user_data: *mut std::ffi::c_void,
) {
    let callback_result = result;
    let callback_result_ptr = &callback_result as *const CResult;
    let callback_records = ManuallyDrop::new(records);

    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        callback(ptr::read(&*callback_records), callback_result_ptr, user_data)
    }))
    .is_err();
    if panicked {
        tracing::error!("async record-array callback panicked; contained at FFI boundary");
        zerobus_free_record_array(ManuallyDrop::into_inner(callback_records));
    }

    if !callback_result.error_message.is_null() {
        unsafe {
            let _ = CString::from_raw(callback_result.error_message);
        }
    }
}

fn encoded_records_to_c_array(records_vec: Vec<EncodedRecord>) -> CRecordArray {
    let len = records_vec.len();

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
    std::mem::forget(c_records);

    CRecordArray {
        records: records_ptr,
        len,
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
    ffi_guard(result, ptr::null_mut(), move || {
        let sdk_ref = match validate_sdk_ptr(sdk) {
            Ok(s) => s,
            Err(msg) => {
                write_error_result(result, msg, false);
                return ptr::null_mut();
            }
        };

        let res = RUNTIME.block_on(async {
            let table_name_str = unsafe {
                c_str_to_string(table_name)
                    .map_err(|e| ZerobusError::InvalidArgument(e.to_string()))?
            };
            let client_id_str = unsafe {
                c_str_to_string(client_id)
                    .map_err(|e| ZerobusError::InvalidArgument(e.to_string()))?
            };
            let client_secret_str = unsafe {
                c_str_to_string(client_secret)
                    .map_err(|e| ZerobusError::InvalidArgument(e.to_string()))?
            };

            let descriptor_proto = if !descriptor_proto_bytes.is_null() && descriptor_proto_len > 0
            {
                let bytes = unsafe {
                    std::slice::from_raw_parts(descriptor_proto_bytes, descriptor_proto_len)
                };
                Some(
                    prost_types::DescriptorProto::decode(bytes)
                        .map_err(|e| ZerobusError::InvalidArgument(e.to_string()))?,
                )
            } else {
                None
            };

            let c_opts = if !options.is_null() {
                Some(unsafe { *options })
            } else {
                None
            };

            build_stream_from_parts(
                sdk_ref,
                table_name_str,
                descriptor_proto,
                StreamCreateAuth::OAuth {
                    client_id: client_id_str,
                    client_secret: client_secret_str,
                },
                c_opts,
            )
            .await
        });

        match res {
            Ok(stream_ptr) => {
                write_success_result(result);
                stream_ptr
            }
            Err(err) => {
                if !result.is_null() {
                    unsafe {
                        *result = CResult::error(err);
                    }
                }
                ptr::null_mut()
            }
        }
    })
}

/// Create a stream with OAuth authentication on a background task.
///
/// Returns `true` once the request has been validated and scheduled. The
/// callback is invoked exactly once with either a non-null stream pointer and a
/// success result, or a null stream pointer and a failure result. The SDK
/// handle must remain valid until the callback runs.
#[no_mangle]
pub extern "C" fn zerobus_sdk_create_stream_async(
    sdk: *mut CZerobusSdk,
    table_name: *const c_char,
    descriptor_proto_bytes: *const u8,
    descriptor_proto_len: usize,
    client_id: *const c_char,
    client_secret: *const c_char,
    options: *const CStreamConfigurationOptions,
    callback: CreateStreamAsyncCallback,
    user_data: *mut std::ffi::c_void,
    result: *mut CResult,
) -> bool {
    ffi_guard(result, false, move || {
        if let Err(msg) = validate_sdk_ptr(sdk) {
            write_error_result(result, msg, false);
            return false;
        }

        let table_name_str = match unsafe { c_str_to_string(table_name) } {
            Ok(s) => s,
            Err(e) => {
                write_error_result(result, e, false);
                return false;
            }
        };
        let client_id_str = match unsafe { c_str_to_string(client_id) } {
            Ok(s) => s,
            Err(e) => {
                write_error_result(result, e, false);
                return false;
            }
        };
        let client_secret_str = match unsafe { c_str_to_string(client_secret) } {
            Ok(s) => s,
            Err(e) => {
                write_error_result(result, e, false);
                return false;
            }
        };

        let descriptor_proto = if !descriptor_proto_bytes.is_null() && descriptor_proto_len > 0 {
            let bytes = unsafe {
                std::slice::from_raw_parts(descriptor_proto_bytes, descriptor_proto_len)
            };
            match prost_types::DescriptorProto::decode(bytes) {
                Ok(desc) => Some(desc),
                Err(e) => {
                    write_error_result(result, &e.to_string(), false);
                    return false;
                }
            }
        } else {
            None
        };

        let c_opts = if !options.is_null() {
            Some(unsafe { *options })
        } else {
            None
        };

        let sdk_ptr = SendPtr::new(sdk);
        let callback_user_data = SendPtr::new(user_data);
        RUNTIME.spawn(async move {
            let callback_result = match validate_sdk_ptr(sdk_ptr.get()) {
                Ok(sdk_ref) => match build_stream_from_parts(
                    sdk_ref,
                    table_name_str,
                    descriptor_proto,
                    StreamCreateAuth::OAuth {
                        client_id: client_id_str,
                        client_secret: client_secret_str,
                    },
                    c_opts,
                )
                .await
                {
                    Ok(stream_ptr) => {
                        invoke_create_stream_async_callback(
                            callback,
                            stream_ptr,
                            CResult::success(),
                            callback_user_data.get(),
                        );
                        return;
                    }
                    Err(err) => CResult::error(err),
                },
                Err(msg) => CResult {
                    success: false,
                    error_message: CString::new(msg)
                        .unwrap_or_else(|_| CString::new("SDK pointer is invalid").unwrap())
                        .into_raw(),
                    is_retryable: false,
                },
            };

            invoke_create_stream_async_callback(
                callback,
                ptr::null_mut(),
                callback_result,
                callback_user_data.get(),
            );
        });

        write_success_result(result);
        true
    })
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
    ffi_guard(result, ptr::null_mut(), move || {
        let sdk_ref = match validate_sdk_ptr(sdk) {
            Ok(s) => s,
            Err(msg) => {
                write_error_result(result, msg, false);
                return ptr::null_mut();
            }
        };

        let res = RUNTIME.block_on(async {
            let table_name_str = unsafe {
                c_str_to_string(table_name)
                    .map_err(|e| ZerobusError::InvalidArgument(e.to_string()))?
            };

            let descriptor_proto = if !descriptor_proto_bytes.is_null() && descriptor_proto_len > 0
            {
                let bytes = unsafe {
                    std::slice::from_raw_parts(descriptor_proto_bytes, descriptor_proto_len)
                };
                Some(
                    prost_types::DescriptorProto::decode(bytes)
                        .map_err(|e| ZerobusError::InvalidArgument(e.to_string()))?,
                )
            } else {
                None
            };

            let c_opts = if !options.is_null() {
                Some(unsafe { *options })
            } else {
                None
            };

            build_stream_from_parts(
                sdk_ref,
                table_name_str,
                descriptor_proto,
                StreamCreateAuth::HeadersProvider {
                    headers_callback,
                    user_data: SendPtr::new(user_data),
                },
                c_opts,
            )
            .await
        });

        match res {
            Ok(stream_ptr) => {
                write_success_result(result);
                stream_ptr
            }
            Err(err) => {
                if !result.is_null() {
                    unsafe {
                        *result = CResult::error(err);
                    }
                }
                ptr::null_mut()
            }
        }
    })
}

/// Create a stream with a custom headers provider callback on a background task.
///
/// Returns `true` once the request has been validated and scheduled. The
/// callback is invoked exactly once with either a non-null stream pointer and a
/// success result, or a null stream pointer and a failure result. The SDK
/// handle must remain valid until the callback runs.
#[no_mangle]
pub extern "C" fn zerobus_sdk_create_stream_with_headers_provider_async(
    sdk: *mut CZerobusSdk,
    table_name: *const c_char,
    descriptor_proto_bytes: *const u8,
    descriptor_proto_len: usize,
    headers_callback: HeadersProviderCallback,
    user_data: *mut std::ffi::c_void,
    options: *const CStreamConfigurationOptions,
    callback: CreateStreamAsyncCallback,
    callback_user_data: *mut std::ffi::c_void,
    result: *mut CResult,
) -> bool {
    ffi_guard(result, false, move || {
        if let Err(msg) = validate_sdk_ptr(sdk) {
            write_error_result(result, msg, false);
            return false;
        }

        let table_name_str = match unsafe { c_str_to_string(table_name) } {
            Ok(s) => s,
            Err(e) => {
                write_error_result(result, e, false);
                return false;
            }
        };

        let descriptor_proto = if !descriptor_proto_bytes.is_null() && descriptor_proto_len > 0 {
            let bytes = unsafe {
                std::slice::from_raw_parts(descriptor_proto_bytes, descriptor_proto_len)
            };
            match prost_types::DescriptorProto::decode(bytes) {
                Ok(desc) => Some(desc),
                Err(e) => {
                    write_error_result(result, &e.to_string(), false);
                    return false;
                }
            }
        } else {
            None
        };

        let c_opts = if !options.is_null() {
            Some(unsafe { *options })
        } else {
            None
        };

        let sdk_ptr = SendPtr::new(sdk);
        let stream_user_data = SendPtr::new(user_data);
        let callback_user_data = SendPtr::new(callback_user_data);
        RUNTIME.spawn(async move {
            let callback_result = match validate_sdk_ptr(sdk_ptr.get()) {
                Ok(sdk_ref) => match build_stream_from_parts(
                    sdk_ref,
                    table_name_str,
                    descriptor_proto,
                    StreamCreateAuth::HeadersProvider {
                        headers_callback,
                        user_data: stream_user_data,
                    },
                    c_opts,
                )
                .await
                {
                    Ok(stream_ptr) => {
                        invoke_create_stream_async_callback(
                            callback,
                            stream_ptr,
                            CResult::success(),
                            callback_user_data.get(),
                        );
                        return;
                    }
                    Err(err) => CResult::error(err),
                },
                Err(msg) => CResult {
                    success: false,
                    error_message: CString::new(msg)
                        .unwrap_or_else(|_| CString::new("SDK pointer is invalid").unwrap())
                        .into_raw(),
                    is_retryable: false,
                },
            };

            invoke_create_stream_async_callback(
                callback,
                ptr::null_mut(),
                callback_result,
                callback_user_data.get(),
            );
        });

        write_success_result(result);
        true
    })
}

/// Recreate a stream from an existing stream
/// This is used for recovery scenarios where the stream needs to be re-established
#[no_mangle]
pub extern "C" fn zerobus_sdk_recreate_stream(
    sdk: *mut CZerobusSdk,
    stream: *mut CZerobusStream,
    result: *mut CResult,
) -> *mut CZerobusStream {
    ffi_guard(result, ptr::null_mut(), move || {
        let sdk_ref = match validate_sdk_ptr(sdk) {
            Ok(s) => s,
            Err(msg) => {
                write_error_result(result, msg, false);
                return ptr::null_mut();
            }
        };

        let stream_ref = match validate_stream_ptr(stream) {
            Ok(s) => s,
            Err(msg) => {
                write_error_result(result, msg, false);
                return ptr::null_mut();
            }
        };

        let res = RUNTIME.block_on(async {
            let new_stream = sdk_ref.recreate_stream(stream_ref).await?;

            let arc = Arc::new(new_stream);
            Ok::<*mut CZerobusStream, ZerobusError>(Arc::into_raw(arc) as *mut CZerobusStream)
        });

        match res {
            Ok(stream_ptr) => {
                write_success_result(result);
                stream_ptr
            }
            Err(err) => {
                if !result.is_null() {
                    unsafe {
                        *result = CResult::error(err);
                    }
                }
                ptr::null_mut()
            }
        }
    })
}

/// Recreate a stream from an existing stream on a background task.
#[no_mangle]
pub extern "C" fn zerobus_sdk_recreate_stream_async(
    sdk: *mut CZerobusSdk,
    stream: *mut CZerobusStream,
    callback: CreateStreamAsyncCallback,
    user_data: *mut std::ffi::c_void,
    result: *mut CResult,
) -> bool {
    ffi_guard(result, false, move || {
        if let Err(msg) = validate_sdk_ptr(sdk) {
            write_error_result(result, msg, false);
            return false;
        }
        if let Err(msg) = validate_stream_ptr(stream) {
            write_error_result(result, msg, false);
            return false;
        }

        let sdk_ptr = SendPtr::new(sdk);
        let stream_arc = unsafe { clone_stream_arc(stream) };
        let callback_user_data = SendPtr::new(user_data);
        RUNTIME.spawn(async move {
            let callback_result = match validate_sdk_ptr(sdk_ptr.get()) {
                Ok(sdk_ref) => match sdk_ref.recreate_stream(stream_arc.as_ref()).await {
                    Ok(new_stream) => {
                        let stream_ptr = Arc::into_raw(Arc::new(new_stream)) as *mut CZerobusStream;
                        invoke_create_stream_async_callback(
                            callback,
                            stream_ptr,
                            CResult::success(),
                            callback_user_data.get(),
                        );
                        return;
                    }
                    Err(err) => CResult::error(err),
                },
                Err(msg) => CResult {
                    success: false,
                    error_message: CString::new(msg)
                        .unwrap_or_else(|_| CString::new("SDK pointer is invalid").unwrap())
                        .into_raw(),
                    is_retryable: false,
                },
            };

            invoke_create_stream_async_callback(
                callback,
                ptr::null_mut(),
                callback_result,
                callback_user_data.get(),
            );
        });

        write_success_result(result);
        true
    })
}

/// Free a stream instance
#[no_mangle]
pub extern "C" fn zerobus_stream_free(stream: *mut CZerobusStream) {
    ffi_guard(ptr::null_mut(), (), move || {
        if !stream.is_null() {
            unsafe {
                // Reconstruct the Arc and drop it. If nowait tasks still hold clones,
                // the stream is not freed until the last Arc is dropped.
                let _ = Arc::from_raw(stream as *const ZerobusStream);
            }
        }
    })
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
    ffi_guard(result, -1, move || {
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
    })
}

/// Ingest a protobuf record on a background task and report the assigned offset via callback.
#[no_mangle]
pub extern "C" fn zerobus_stream_ingest_proto_record_async(
    stream: *mut CZerobusStream,
    data: *const u8,
    data_len: usize,
    callback: OffsetAsyncCallback,
    user_data: *mut std::ffi::c_void,
    result: *mut CResult,
) -> bool {
    ffi_guard(result, false, move || {
        if data.is_null() {
            write_error_result(result, "Invalid data pointer", false);
            return false;
        }
        if let Err(msg) = validate_stream_ptr(stream) {
            write_error_result(result, msg, false);
            return false;
        }

        let data_vec = unsafe { std::slice::from_raw_parts(data, data_len) }.to_vec();
        let stream_arc = unsafe { clone_stream_arc(stream) };
        let callback_user_data = SendPtr::new(user_data);
        RUNTIME.spawn(async move {
            match stream_arc
                .ingest_record_offset(EncodedRecord::Proto(data_vec))
                .await
            {
                Ok(offset) => invoke_offset_async_callback(
                    callback,
                    offset,
                    CResult::success(),
                    callback_user_data.get(),
                ),
                Err(err) => invoke_offset_async_callback(
                    callback,
                    -1,
                    CResult::error(err),
                    callback_user_data.get(),
                ),
            }
        });

        write_success_result(result);
        true
    })
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
    ffi_guard(result, -1, move || {
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
    })
}

/// Ingest a JSON record on a background task and report the assigned offset via callback.
#[no_mangle]
pub extern "C" fn zerobus_stream_ingest_json_record_async(
    stream: *mut CZerobusStream,
    json_data: *const c_char,
    callback: OffsetAsyncCallback,
    user_data: *mut std::ffi::c_void,
    result: *mut CResult,
) -> bool {
    ffi_guard(result, false, move || {
        if let Err(msg) = validate_stream_ptr(stream) {
            write_error_result(result, msg, false);
            return false;
        }

        let json_str = match unsafe { c_str_to_string(json_data) } {
            Ok(s) => s,
            Err(e) => {
                write_error_result(result, e, false);
                return false;
            }
        };

        let stream_arc = unsafe { clone_stream_arc(stream) };
        let callback_user_data = SendPtr::new(user_data);
        RUNTIME.spawn(async move {
            match stream_arc.ingest_record_offset(EncodedRecord::Json(json_str)).await {
                Ok(offset) => invoke_offset_async_callback(
                    callback,
                    offset,
                    CResult::success(),
                    callback_user_data.get(),
                ),
                Err(err) => invoke_offset_async_callback(
                    callback,
                    -1,
                    CResult::error(err),
                    callback_user_data.get(),
                ),
            }
        });

        write_success_result(result);
        true
    })
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
    ffi_guard(result, -1, move || {
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
    })
}

/// Ingest a batch of protobuf records on a background task and report the last offset via callback.
#[no_mangle]
pub extern "C" fn zerobus_stream_ingest_proto_records_async(
    stream: *mut CZerobusStream,
    records: *const *const u8,
    record_lens: *const usize,
    num_records: usize,
    callback: OffsetAsyncCallback,
    user_data: *mut std::ffi::c_void,
    result: *mut CResult,
) -> bool {
    ffi_guard(result, false, move || {
        if records.is_null() || record_lens.is_null() {
            write_error_result(result, "Invalid records pointer", false);
            return false;
        }

        let callback_user_data = SendPtr::new(user_data);
        if num_records == 0 {
            RUNTIME.spawn(async move {
                invoke_offset_async_callback(
                    callback,
                    -2,
                    CResult::success(),
                    callback_user_data.get(),
                );
            });
            write_success_result(result);
            return true;
        }

        if let Err(msg) = validate_stream_ptr(stream) {
            write_error_result(result, msg, false);
            return false;
        }

        let records_vec: Vec<Vec<u8>> = unsafe {
            let records_slice = std::slice::from_raw_parts(records, num_records);
            let lens_slice = std::slice::from_raw_parts(record_lens, num_records);
            records_slice
                .iter()
                .zip(lens_slice.iter())
                .map(|(ptr, len)| std::slice::from_raw_parts(*ptr, *len).to_vec())
                .collect()
        };

        let stream_arc = unsafe { clone_stream_arc(stream) };
        RUNTIME.spawn(async move {
            let payloads: Vec<EncodedRecord> =
                records_vec.into_iter().map(EncodedRecord::Proto).collect();
            match stream_arc.ingest_records_offset(payloads).await {
                Ok(Some(offset)) => invoke_offset_async_callback(
                    callback,
                    offset,
                    CResult::success(),
                    callback_user_data.get(),
                ),
                Ok(None) => invoke_offset_async_callback(
                    callback,
                    -2,
                    CResult::success(),
                    callback_user_data.get(),
                ),
                Err(err) => invoke_offset_async_callback(
                    callback,
                    -1,
                    CResult::error(err),
                    callback_user_data.get(),
                ),
            }
        });

        write_success_result(result);
        true
    })
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
    ffi_guard(result, -1, move || {
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
            let payloads: Vec<EncodedRecord> =
                json_vec.into_iter().map(EncodedRecord::Json).collect();
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
    })
}

/// Ingest a batch of JSON records on a background task and report the last offset via callback.
#[no_mangle]
pub extern "C" fn zerobus_stream_ingest_json_records_async(
    stream: *mut CZerobusStream,
    json_records: *const *const c_char,
    num_records: usize,
    callback: OffsetAsyncCallback,
    user_data: *mut std::ffi::c_void,
    result: *mut CResult,
) -> bool {
    ffi_guard(result, false, move || {
        if json_records.is_null() {
            write_error_result(result, "Invalid records pointer", false);
            return false;
        }

        let callback_user_data = SendPtr::new(user_data);
        if num_records == 0 {
            RUNTIME.spawn(async move {
                invoke_offset_async_callback(
                    callback,
                    -2,
                    CResult::success(),
                    callback_user_data.get(),
                );
            });
            write_success_result(result);
            return true;
        }

        if let Err(msg) = validate_stream_ptr(stream) {
            write_error_result(result, msg, false);
            return false;
        }

        let json_vec: Result<Vec<String>, _> = unsafe {
            let json_slice = std::slice::from_raw_parts(json_records, num_records);
            json_slice.iter().map(|ptr| c_str_to_string(*ptr)).collect()
        };
        let json_vec = match json_vec {
            Ok(v) => v,
            Err(e) => {
                write_error_result(result, e, false);
                return false;
            }
        };

        let stream_arc = unsafe { clone_stream_arc(stream) };
        RUNTIME.spawn(async move {
            let payloads: Vec<EncodedRecord> =
                json_vec.into_iter().map(EncodedRecord::Json).collect();
            match stream_arc.ingest_records_offset(payloads).await {
                Ok(Some(offset)) => invoke_offset_async_callback(
                    callback,
                    offset,
                    CResult::success(),
                    callback_user_data.get(),
                ),
                Ok(None) => invoke_offset_async_callback(
                    callback,
                    -2,
                    CResult::success(),
                    callback_user_data.get(),
                ),
                Err(err) => invoke_offset_async_callback(
                    callback,
                    -1,
                    CResult::error(err),
                    callback_user_data.get(),
                ),
            }
        });

        write_success_result(result);
        true
    })
}

/// Clones the `Arc<ZerobusStream>` from a raw `CZerobusStream` pointer without
/// consuming the pointer. The caller retains ownership of the original pointer;
/// the returned `Arc` will keep the stream alive until it is dropped.
///
/// # Safety
/// `stream` must be a non-null pointer produced by `zerobus_sdk_create_stream` or
/// `zerobus_sdk_create_stream_with_headers_provider` and must not have been freed.
unsafe fn clone_stream_arc(stream: *mut CZerobusStream) -> Arc<ZerobusStream> {
    Arc::increment_strong_count(stream as *const ZerobusStream);
    Arc::from_raw(stream as *const ZerobusStream)
}

/// Ingest a protobuf record without waiting for the record to be queued (fire-and-forget).
///
/// Spawns a background task to queue the record and returns immediately.
/// The result only reflects argument validation errors; ingestion errors are silently ignored.
///
/// # Safety
/// The stream must remain valid until all background tasks spawned by this function complete.
#[no_mangle]
pub extern "C" fn zerobus_stream_ingest_proto_record_nowait(
    stream: *mut CZerobusStream,
    data: *const u8,
    data_len: usize,
    result: *mut CResult,
) {
    ffi_guard(result, (), move || {
        if data.is_null() {
            write_error_result(result, "Invalid data pointer", false);
            return;
        }

        if let Err(msg) = validate_stream_ptr(stream) {
            write_error_result(result, msg, false);
            return;
        }

        let data_slice = unsafe { std::slice::from_raw_parts(data, data_len) };
        let data_vec = data_slice.to_vec();
        let stream_arc = unsafe { clone_stream_arc(stream) };

        RUNTIME.spawn(async move {
            let payload = EncodedRecord::Proto(data_vec);
            let _ = stream_arc.ingest_record_offset(payload).await;
        });

        write_success_result(result);
    })
}

/// Ingest a JSON record without waiting for the record to be queued (fire-and-forget).
///
/// Spawns a background task to queue the record and returns immediately.
/// The result only reflects argument validation errors; ingestion errors are silently ignored.
///
/// # Safety
/// The stream must remain valid until all background tasks spawned by this function complete.
#[no_mangle]
pub extern "C" fn zerobus_stream_ingest_json_record_nowait(
    stream: *mut CZerobusStream,
    json_data: *const c_char,
    result: *mut CResult,
) {
    ffi_guard(result, (), move || {
        if let Err(msg) = validate_stream_ptr(stream) {
            write_error_result(result, msg, false);
            return;
        }

        let json_str = match unsafe { c_str_to_string(json_data) } {
            Ok(s) => s,
            Err(e) => {
                write_error_result(result, e, false);
                return;
            }
        };

        let stream_arc = unsafe { clone_stream_arc(stream) };

        RUNTIME.spawn(async move {
            let payload = EncodedRecord::Json(json_str);
            let _ = stream_arc.ingest_record_offset(payload).await;
        });

        write_success_result(result);
    })
}

/// Ingest a batch of protobuf records without waiting (fire-and-forget).
///
/// Copies all record data before spawning the background task, so the caller's
/// memory is safe to release immediately after this function returns.
///
/// # Safety
/// The stream must remain valid until all background tasks spawned by this function complete.
#[no_mangle]
pub extern "C" fn zerobus_stream_ingest_proto_records_nowait(
    stream: *mut CZerobusStream,
    records: *const *const u8,
    record_lens: *const usize,
    num_records: usize,
    result: *mut CResult,
) {
    ffi_guard(result, (), move || {
        if records.is_null() || record_lens.is_null() {
            write_error_result(result, "Invalid records pointer", false);
            return;
        }

        if let Err(msg) = validate_stream_ptr(stream) {
            write_error_result(result, msg, false);
            return;
        }

        if num_records == 0 {
            write_success_result(result);
            return;
        }

        let records_vec: Vec<Vec<u8>> = unsafe {
            let records_slice = std::slice::from_raw_parts(records, num_records);
            let lens_slice = std::slice::from_raw_parts(record_lens, num_records);
            records_slice
                .iter()
                .zip(lens_slice.iter())
                .map(|(ptr, len)| std::slice::from_raw_parts(*ptr, *len).to_vec())
                .collect()
        };

        let stream_arc = unsafe { clone_stream_arc(stream) };

        RUNTIME.spawn(async move {
            let payloads: Vec<EncodedRecord> =
                records_vec.into_iter().map(EncodedRecord::Proto).collect();
            let _ = stream_arc.ingest_records_offset(payloads).await;
        });

        write_success_result(result);
    })
}

/// Ingest a batch of JSON records without waiting (fire-and-forget).
///
/// Copies all strings before spawning the background task, so the caller's
/// memory is safe to release immediately after this function returns.
///
/// # Safety
/// The stream must remain valid until all background tasks spawned by this function complete.
#[no_mangle]
pub extern "C" fn zerobus_stream_ingest_json_records_nowait(
    stream: *mut CZerobusStream,
    json_records: *const *const c_char,
    num_records: usize,
    result: *mut CResult,
) {
    ffi_guard(result, (), move || {
        if json_records.is_null() {
            write_error_result(result, "Invalid records pointer", false);
            return;
        }

        if let Err(msg) = validate_stream_ptr(stream) {
            write_error_result(result, msg, false);
            return;
        }

        if num_records == 0 {
            write_success_result(result);
            return;
        }

        let json_vec: Result<Vec<String>, _> = unsafe {
            let json_slice = std::slice::from_raw_parts(json_records, num_records);
            json_slice.iter().map(|ptr| c_str_to_string(*ptr)).collect()
        };

        let json_vec = match json_vec {
            Ok(v) => v,
            Err(e) => {
                write_error_result(result, e, false);
                return;
            }
        };

        let stream_arc = unsafe { clone_stream_arc(stream) };

        RUNTIME.spawn(async move {
            let payloads: Vec<EncodedRecord> =
                json_vec.into_iter().map(EncodedRecord::Json).collect();
            let _ = stream_arc.ingest_records_offset(payloads).await;
        });

        write_success_result(result);
    })
}

/// Wait for a specific offset to be acknowledged by the server
#[no_mangle]
pub extern "C" fn zerobus_stream_wait_for_offset(
    stream: *mut CZerobusStream,
    offset: i64,
    result: *mut CResult,
) -> bool {
    ffi_guard(result, false, move || {
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
    })
}

/// Wait for an offset on a background task and report completion via callback.
#[no_mangle]
pub extern "C" fn zerobus_stream_wait_for_offset_async(
    stream: *mut CZerobusStream,
    offset: i64,
    callback: BoolAsyncCallback,
    user_data: *mut std::ffi::c_void,
    result: *mut CResult,
) -> bool {
    ffi_guard(result, false, move || {
        if let Err(msg) = validate_stream_ptr(stream) {
            write_error_result(result, msg, false);
            return false;
        }

        let stream_arc = unsafe { clone_stream_arc(stream) };
        let callback_user_data = SendPtr::new(user_data);
        RUNTIME.spawn(async move {
            match stream_arc.wait_for_offset(offset).await {
                Ok(()) => invoke_bool_async_callback(
                    callback,
                    true,
                    CResult::success(),
                    callback_user_data.get(),
                ),
                Err(err) => invoke_bool_async_callback(
                    callback,
                    false,
                    CResult::error(err),
                    callback_user_data.get(),
                ),
            }
        });

        write_success_result(result);
        true
    })
}

/// Flush all pending records
#[no_mangle]
pub extern "C" fn zerobus_stream_flush(stream: *mut CZerobusStream, result: *mut CResult) -> bool {
    ffi_guard(result, false, move || {
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
    })
}

/// Flush all pending records on a background task and report completion via callback.
#[no_mangle]
pub extern "C" fn zerobus_stream_flush_async(
    stream: *mut CZerobusStream,
    callback: BoolAsyncCallback,
    user_data: *mut std::ffi::c_void,
    result: *mut CResult,
) -> bool {
    ffi_guard(result, false, move || {
        if let Err(msg) = validate_stream_ptr(stream) {
            write_error_result(result, msg, false);
            return false;
        }

        let stream_arc = unsafe { clone_stream_arc(stream) };
        let callback_user_data = SendPtr::new(user_data);
        RUNTIME.spawn(async move {
            match stream_arc.flush().await {
                Ok(_) => invoke_bool_async_callback(
                    callback,
                    true,
                    CResult::success(),
                    callback_user_data.get(),
                ),
                Err(err) => invoke_bool_async_callback(
                    callback,
                    false,
                    CResult::error(err),
                    callback_user_data.get(),
                ),
            }
        });

        write_success_result(result);
        true
    })
}

/// Get unacknowledged records from a closed stream
/// Returns a CRecordArray that must be freed with zerobus_free_record_array
#[no_mangle]
pub extern "C" fn zerobus_stream_get_unacked_records(
    stream: *mut CZerobusStream,
    result: *mut CResult,
) -> CRecordArray {
    let empty = CRecordArray {
        records: ptr::null_mut(),
        len: 0,
    };
    ffi_guard(result, empty, move || {
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
                write_success_result(result);
                encoded_records_to_c_array(records_iter.collect())
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
    })
}

/// Get unacknowledged records from a closed stream on a background task.
#[no_mangle]
pub extern "C" fn zerobus_stream_get_unacked_records_async(
    stream: *mut CZerobusStream,
    callback: RecordArrayAsyncCallback,
    user_data: *mut std::ffi::c_void,
    result: *mut CResult,
) -> bool {
    ffi_guard(result, false, move || {
        if let Err(msg) = validate_stream_ptr(stream) {
            write_error_result(result, msg, false);
            return false;
        }

        let stream_arc = unsafe { clone_stream_arc(stream) };
        let callback_user_data = SendPtr::new(user_data);
        RUNTIME.spawn(async move {
            match stream_arc.get_unacked_records().await {
                Ok(records_iter) => invoke_record_array_async_callback(
                    callback,
                    encoded_records_to_c_array(records_iter.collect()),
                    CResult::success(),
                    callback_user_data.get(),
                ),
                Err(err) => invoke_record_array_async_callback(
                    callback,
                    CRecordArray {
                        records: ptr::null_mut(),
                        len: 0,
                    },
                    CResult::error(err),
                    callback_user_data.get(),
                ),
            }
        });

        write_success_result(result);
        true
    })
}

/// Free a CRecordArray returned by zerobus_stream_get_unacked_records
#[no_mangle]
pub extern "C" fn zerobus_free_record_array(array: CRecordArray) {
    ffi_guard(ptr::null_mut(), (), move || {
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
    })
}

/// Close the stream gracefully
#[no_mangle]
pub extern "C" fn zerobus_stream_close(stream: *mut CZerobusStream, result: *mut CResult) -> bool {
    ffi_guard(result, false, move || {
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
    })
}

/// Close the stream gracefully on a background task.
#[no_mangle]
pub extern "C" fn zerobus_stream_close_async(
    stream: *mut CZerobusStream,
    callback: BoolAsyncCallback,
    user_data: *mut std::ffi::c_void,
    result: *mut CResult,
) -> bool {
    ffi_guard(result, false, move || {
        if let Err(msg) = validate_stream_ptr_mut(stream) {
            write_error_result(result, msg, false);
            return false;
        }

        let stream_ptr = SendPtr::new(stream);
        let callback_user_data = SendPtr::new(user_data);
        RUNTIME.spawn(async move {
            let callback_result = match validate_stream_ptr_mut(stream_ptr.get()) {
                Ok(stream_ref) => match stream_ref.close().await {
                    Ok(_) => {
                        invoke_bool_async_callback(
                            callback,
                            true,
                            CResult::success(),
                            callback_user_data.get(),
                        );
                        return;
                    }
                    Err(err) => CResult::error(err),
                },
                Err(msg) => CResult {
                    success: false,
                    error_message: CString::new(msg)
                        .unwrap_or_else(|_| CString::new("Stream pointer is invalid").unwrap())
                        .into_raw(),
                    is_retryable: false,
                },
            };

            invoke_bool_async_callback(
                callback,
                false,
                callback_result,
                callback_user_data.get(),
            );
        });

        write_success_result(result);
        true
    })
}

/// Returns whether the stream has been closed.
#[no_mangle]
pub extern "C" fn zerobus_stream_is_closed(stream: *mut CZerobusStream) -> bool {
    // No CResult out-param; on a caught panic return `true` (treat as closed),
    // matching the answer for an invalid handle.
    ffi_guard(
        ptr::null_mut(),
        true,
        move || match validate_stream_ptr(stream) {
            Ok(s) => s.is_closed(),
            Err(_) => true,
        },
    )
}

/// Free error message string
#[no_mangle]
pub extern "C" fn zerobus_free_error_message(message: *mut c_char) {
    ffi_guard(ptr::null_mut(), (), move || {
        if !message.is_null() {
            unsafe {
                let _ = CString::from_raw(message);
            }
        }
    })
}

/// Get default stream configuration options
// Not wrapped in `ffi_guard`: builds a `#[repr(C)]` struct from constants, so
// nothing here can panic.
#[no_mangle]
pub extern "C" fn zerobus_get_default_config() -> CStreamConfigurationOptions {
    use databricks_zerobus_ingest_sdk::stream_options::defaults;
    CStreamConfigurationOptions {
        max_inflight_requests: 1_000_000,
        recovery: defaults::RECOVERY,
        recovery_timeout_ms: defaults::RECOVERY_TIMEOUT_MS,
        recovery_backoff_ms: defaults::RECOVERY_BACKOFF_MS,
        recovery_retries: defaults::RECOVERY_RETRIES,
        server_lack_of_ack_timeout_ms: defaults::SERVER_LACK_OF_ACK_TIMEOUT_MS,
        flush_timeout_ms: defaults::FLUSH_TIMEOUT_MS,
        record_type: 1, // RecordType::Proto
        stream_paused_max_wait_time_ms: 0,
        has_stream_paused_max_wait_time_ms: false,
        callback_max_wait_time_ms: defaults::CALLBACK_MAX_WAIT_TIME_MS,
        has_callback_max_wait_time_ms: true,
        // No ack callback by default.
        ack_on_ack: None,
        ack_on_error: None,
        ack_user_data: ptr::null_mut(),
    }
}
