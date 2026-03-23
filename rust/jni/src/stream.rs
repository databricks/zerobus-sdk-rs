//! JNI bindings for ZerobusStream.
//!
//! This module provides JNI functions for stream operations including
//! record ingestion, acknowledgment waiting, flushing, and closing.

use crate::async_bridge::{create_completable_future, spawn_and_complete_void};
use crate::errors::{throw_from_zerobus_error, throw_zerobus_exception};
use crate::runtime::block_on;
use databricks_zerobus_ingest_sdk::ZerobusStream;
use databricks_zerobus_ingest_sdk::{EncodedBatch, EncodedRecord};
use jni::objects::{JByteArray, JClass, JList, JObject, JValue};
use jni::sys::{jboolean, jlong, JNI_FALSE, JNI_TRUE};
use jni::JNIEnv;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Native stream handle stored in Java.
pub struct NativeStreamHandle {
    pub stream: Arc<Mutex<Option<ZerobusStream>>>,
    pub client_id: String,
    pub client_secret: String,
}

impl NativeStreamHandle {
    pub fn new(stream: ZerobusStream, client_id: String, client_secret: String) -> Self {
        Self {
            stream: Arc::new(Mutex::new(Some(stream))),
            client_id,
            client_secret,
        }
    }

    pub fn into_raw(self) -> jlong {
        Box::into_raw(Box::new(self)) as jlong
    }

    /// # Safety
    ///
    /// The pointer must have been created by `into_raw` and not freed.
    pub unsafe fn from_raw(ptr: jlong) -> Box<Self> {
        Box::from_raw(ptr as *mut Self)
    }

    /// # Safety
    ///
    /// The pointer must have been created by `into_raw` and not freed.
    pub unsafe fn borrow_from_raw(ptr: jlong) -> &'static Self {
        &*(ptr as *const Self)
    }
}

/// Destroy a stream handle.
///
/// # JNI Signature
/// ```java
/// private static native void nativeDestroy(long handle);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_BaseZerobusStream_nativeDestroy<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    if handle != 0 {
        // Safety: handle was created by createStream
        unsafe {
            let _ = NativeStreamHandle::from_raw(handle);
        }
    }
}

/// Ingest a record and return a CompletableFuture that completes on ack.
///
/// # JNI Signature
/// ```java
/// private native CompletableFuture<Void> nativeIngestRecord(long handle, byte[] payload, boolean isJson);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_BaseZerobusStream_nativeIngestRecord<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
    payload: JByteArray<'local>,
    is_json: jboolean,
) -> JObject<'local> {
    // Create the CompletableFuture to return
    let future = match create_completable_future(&mut env) {
        Ok(f) => f,
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Failed to create future: {}", e));
            return JObject::null();
        }
    };

    // Create a global reference to the future
    let future_ref = match env.new_global_ref(&future) {
        Ok(r) => r,
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Failed to create global ref: {}", e));
            return JObject::null();
        }
    };

    // Extract payload bytes
    let bytes: Vec<u8> = match env.convert_byte_array(payload) {
        Ok(b) => b,
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Invalid payload: {}", e));
            return JObject::null();
        }
    };

    // Get stream handle
    let stream_handle = unsafe { NativeStreamHandle::borrow_from_raw(handle) };
    let stream_arc = stream_handle.stream.clone();

    // Create the encoded record
    let record = if is_json != JNI_FALSE {
        let json_str = match String::from_utf8(bytes) {
            Ok(s) => s,
            Err(e) => {
                throw_zerobus_exception(&mut env, &format!("Invalid UTF-8 in JSON payload: {}", e));
                return JObject::null();
            }
        };
        EncodedRecord::Json(json_str)
    } else {
        EncodedRecord::Proto(bytes)
    };

    // Spawn async operation
    spawn_and_complete_void(future_ref, async move {
        let guard = stream_arc.lock().await;
        let stream = guard.as_ref().ok_or_else(|| {
            databricks_zerobus_ingest_sdk::ZerobusError::InvalidStateError(
                "Stream is closed".to_string(),
            )
        })?;

        let offset = stream.ingest_record_offset(record).await?;
        stream.wait_for_offset(offset).await?;
        Ok(())
    });

    future
}

/// Ingest a record and return the offset immediately (blocking until enqueued).
///
/// # JNI Signature
/// ```java
/// private native long nativeIngestRecordOffset(long handle, byte[] payload, boolean isJson);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_BaseZerobusStream_nativeIngestRecordOffset<
    'local,
>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
    payload: JByteArray<'local>,
    is_json: jboolean,
) -> jlong {
    // Extract payload bytes
    let bytes: Vec<u8> = match env.convert_byte_array(payload) {
        Ok(b) => b,
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Invalid payload: {}", e));
            return -1;
        }
    };

    // Get stream handle
    let stream_handle = unsafe { NativeStreamHandle::borrow_from_raw(handle) };

    // Create the encoded record
    let record = if is_json != JNI_FALSE {
        let json_str = match String::from_utf8(bytes) {
            Ok(s) => s,
            Err(e) => {
                throw_zerobus_exception(&mut env, &format!("Invalid UTF-8 in JSON payload: {}", e));
                return -1;
            }
        };
        EncodedRecord::Json(json_str)
    } else {
        EncodedRecord::Proto(bytes)
    };

    // Block on the async operation
    let result = block_on(async {
        let guard = stream_handle.stream.lock().await;
        let stream = guard.as_ref().ok_or_else(|| {
            databricks_zerobus_ingest_sdk::ZerobusError::InvalidStateError(
                "Stream is closed".to_string(),
            )
        })?;

        stream.ingest_record_offset(record).await
    });

    match result {
        Ok(offset) => offset,
        Err(e) => {
            throw_from_zerobus_error(&mut env, &e);
            -1
        }
    }
}

/// Ingest multiple records and return the offset of the last record (or -1 if empty).
///
/// # JNI Signature
/// ```java
/// private native long nativeIngestRecordsOffset(long handle, List<byte[]> payloads, boolean isJson);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_BaseZerobusStream_nativeIngestRecordsOffset<
    'local,
>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
    payloads: JObject<'local>,
    is_json: jboolean,
) -> jlong {
    // Convert Java List to Vec of byte arrays
    let list = match JList::from_env(&mut env, &payloads) {
        Ok(l) => l,
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Invalid payload list: {}", e));
            return -1;
        }
    };

    let mut records: Vec<EncodedRecord> = Vec::new();

    // Iterate over the list
    let mut iter = match list.iter(&mut env) {
        Ok(i) => i,
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Failed to iterate payload list: {}", e));
            return -1;
        }
    };

    while let Some(obj) = match iter.next(&mut env) {
        Ok(o) => o,
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Failed to get next payload: {}", e));
            return -1;
        }
    } {
        let byte_array: JByteArray = obj.into();
        let bytes: Vec<u8> = match env.convert_byte_array(byte_array) {
            Ok(b) => b,
            Err(e) => {
                throw_zerobus_exception(&mut env, &format!("Invalid payload: {}", e));
                return -1;
            }
        };

        let record = if is_json != JNI_FALSE {
            let json_str = match String::from_utf8(bytes) {
                Ok(s) => s,
                Err(e) => {
                    throw_zerobus_exception(
                        &mut env,
                        &format!("Invalid UTF-8 in JSON payload: {}", e),
                    );
                    return -1;
                }
            };
            EncodedRecord::Json(json_str)
        } else {
            EncodedRecord::Proto(bytes)
        };

        records.push(record);
    }

    if records.is_empty() {
        return -1;
    }

    // Get stream handle
    let stream_handle = unsafe { NativeStreamHandle::borrow_from_raw(handle) };

    // Block on the async operation
    let result = block_on(async {
        let guard = stream_handle.stream.lock().await;
        let stream = guard.as_ref().ok_or_else(|| {
            databricks_zerobus_ingest_sdk::ZerobusError::InvalidStateError(
                "Stream is closed".to_string(),
            )
        })?;

        stream.ingest_records_offset(records).await
    });

    match result {
        Ok(Some(offset)) => offset,
        Ok(None) => -1,
        Err(e) => {
            throw_from_zerobus_error(&mut env, &e);
            -1
        }
    }
}

/// Wait for a specific offset to be acknowledged (blocking).
///
/// # JNI Signature
/// ```java
/// private native void nativeWaitForOffset(long handle, long offset);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_BaseZerobusStream_nativeWaitForOffset<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
    offset: jlong,
) {
    // Get stream handle
    let stream_handle = unsafe { NativeStreamHandle::borrow_from_raw(handle) };

    // Block on the async operation
    let result = block_on(async {
        let guard = stream_handle.stream.lock().await;
        let stream = guard.as_ref().ok_or_else(|| {
            databricks_zerobus_ingest_sdk::ZerobusError::InvalidStateError(
                "Stream is closed".to_string(),
            )
        })?;

        stream.wait_for_offset(offset).await
    });

    if let Err(e) = result {
        throw_from_zerobus_error(&mut env, &e);
    }
}

/// Flush all pending records (blocking).
///
/// # JNI Signature
/// ```java
/// private native void nativeFlush(long handle);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_BaseZerobusStream_nativeFlush<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
) {
    // Get stream handle
    let stream_handle = unsafe { NativeStreamHandle::borrow_from_raw(handle) };

    // Block on the async operation
    let result = block_on(async {
        let guard = stream_handle.stream.lock().await;
        let stream = guard.as_ref().ok_or_else(|| {
            databricks_zerobus_ingest_sdk::ZerobusError::InvalidStateError(
                "Stream is closed".to_string(),
            )
        })?;

        stream.flush().await
    });

    if let Err(e) = result {
        throw_from_zerobus_error(&mut env, &e);
    }
}

/// Close the stream (blocking).
///
/// # JNI Signature
/// ```java
/// private native void nativeClose(long handle);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_BaseZerobusStream_nativeClose<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
) {
    // Get stream handle
    let stream_handle = unsafe { NativeStreamHandle::borrow_from_raw(handle) };

    // Block on the async operation.
    // Close in place (not .take()) so nativeGetUnackedRecords/Batches can still access the stream.
    let result = block_on(async {
        let mut guard = stream_handle.stream.lock().await;
        if let Some(stream) = guard.as_mut() {
            stream.close().await?;
        }
        Ok::<_, databricks_zerobus_ingest_sdk::ZerobusError>(())
    });

    if let Err(e) = result {
        throw_from_zerobus_error(&mut env, &e);
    }
}

/// Check if the stream is closed.
///
/// # JNI Signature
/// ```java
/// private native boolean nativeIsClosed(long handle);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_BaseZerobusStream_nativeIsClosed<'local>(
    _env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
) -> jboolean {
    // Get stream handle
    let stream_handle = unsafe { NativeStreamHandle::borrow_from_raw(handle) };

    let is_closed = block_on(async {
        let guard = stream_handle.stream.lock().await;
        match guard.as_ref() {
            None => true,
            Some(stream) => stream.is_closed(),
        }
    });

    if is_closed {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

/// Get unacked records as a list of byte arrays.
///
/// # JNI Signature
/// ```java
/// private native List<byte[]> nativeGetUnackedRecords(long handle);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_BaseZerobusStream_nativeGetUnackedRecords<
    'local,
>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
) -> JObject<'local> {
    // Get stream handle
    let stream_handle = unsafe { NativeStreamHandle::borrow_from_raw(handle) };

    // Block on the async operation
    let result = block_on(async {
        let guard = stream_handle.stream.lock().await;
        let stream = guard.as_ref().ok_or_else(|| {
            databricks_zerobus_ingest_sdk::ZerobusError::InvalidStateError(
                "Stream is closed".to_string(),
            )
        })?;

        stream.get_unacked_records().await
    });

    let records = match result {
        Ok(r) => r,
        Err(e) => {
            throw_from_zerobus_error(&mut env, &e);
            return JObject::null();
        }
    };

    // Create an ArrayList
    let array_list_class = match env.find_class("java/util/ArrayList") {
        Ok(c) => c,
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Failed to find ArrayList class: {}", e));
            return JObject::null();
        }
    };

    let list = match env.new_object(&array_list_class, "()V", &[]) {
        Ok(l) => l,
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Failed to create ArrayList: {}", e));
            return JObject::null();
        }
    };

    // Add each record to the list
    for record in records {
        let bytes = match record {
            EncodedRecord::Proto(p) => p,
            EncodedRecord::Json(j) => j.into_bytes(),
        };

        let byte_array = match env.byte_array_from_slice(&bytes) {
            Ok(b) => b,
            Err(e) => {
                throw_zerobus_exception(&mut env, &format!("Failed to create byte array: {}", e));
                return JObject::null();
            }
        };

        if let Err(e) = env.call_method(
            &list,
            "add",
            "(Ljava/lang/Object;)Z",
            &[JValue::Object(&byte_array.into())],
        ) {
            throw_zerobus_exception(&mut env, &format!("Failed to add to list: {}", e));
            return JObject::null();
        }
    }

    list
}

/// Get unacked batches as a list of EncodedBatch objects.
///
/// # JNI Signature
/// ```java
/// private native List<EncodedBatch> nativeGetUnackedBatches(long handle);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_BaseZerobusStream_nativeGetUnackedBatches<
    'local,
>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
) -> JObject<'local> {
    // Get stream handle
    let stream_handle = unsafe { NativeStreamHandle::borrow_from_raw(handle) };

    // Block on the async operation
    let result = block_on(async {
        let guard = stream_handle.stream.lock().await;
        let stream = guard.as_ref().ok_or_else(|| {
            databricks_zerobus_ingest_sdk::ZerobusError::InvalidStateError(
                "Stream is closed".to_string(),
            )
        })?;

        stream.get_unacked_batches().await
    });

    let batches = match result {
        Ok(b) => b,
        Err(e) => {
            throw_from_zerobus_error(&mut env, &e);
            return JObject::null();
        }
    };

    // Create an ArrayList
    let array_list_class = match env.find_class("java/util/ArrayList") {
        Ok(c) => c,
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Failed to find ArrayList class: {}", e));
            return JObject::null();
        }
    };

    let encoded_batch_class = match env.find_class("com/databricks/zerobus/EncodedBatch") {
        Ok(c) => c,
        Err(e) => {
            throw_zerobus_exception(
                &mut env,
                &format!("Failed to find EncodedBatch class: {}", e),
            );
            return JObject::null();
        }
    };

    let list = match env.new_object(&array_list_class, "()V", &[]) {
        Ok(l) => l,
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Failed to create ArrayList: {}", e));
            return JObject::null();
        }
    };

    // Add each batch to the list
    for batch in batches {
        let (is_json, records): (bool, Vec<Vec<u8>>) = match batch {
            EncodedBatch::Proto(records) => (false, records.into_iter().collect()),
            EncodedBatch::Json(records) => {
                (true, records.into_iter().map(|r| r.into_bytes()).collect())
            }
        };

        // Create inner ArrayList for records
        let records_list = match env.new_object(&array_list_class, "()V", &[]) {
            Ok(l) => l,
            Err(e) => {
                throw_zerobus_exception(
                    &mut env,
                    &format!("Failed to create inner ArrayList: {}", e),
                );
                return JObject::null();
            }
        };

        for record_bytes in records {
            let byte_array = match env.byte_array_from_slice(&record_bytes) {
                Ok(b) => b,
                Err(e) => {
                    throw_zerobus_exception(
                        &mut env,
                        &format!("Failed to create byte array: {}", e),
                    );
                    return JObject::null();
                }
            };

            if let Err(e) = env.call_method(
                &records_list,
                "add",
                "(Ljava/lang/Object;)Z",
                &[JValue::Object(&byte_array.into())],
            ) {
                throw_zerobus_exception(&mut env, &format!("Failed to add to inner list: {}", e));
                return JObject::null();
            }
        }

        // Create EncodedBatch object
        let batch_obj = match env.new_object(
            &encoded_batch_class,
            "(Ljava/util/List;Z)V",
            &[
                JValue::Object(&records_list),
                JValue::Bool(if is_json { JNI_TRUE } else { JNI_FALSE }),
            ],
        ) {
            Ok(b) => b,
            Err(e) => {
                throw_zerobus_exception(&mut env, &format!("Failed to create EncodedBatch: {}", e));
                return JObject::null();
            }
        };

        if let Err(e) = env.call_method(
            &list,
            "add",
            "(Ljava/lang/Object;)Z",
            &[JValue::Object(&batch_obj)],
        ) {
            throw_zerobus_exception(&mut env, &format!("Failed to add batch to list: {}", e));
            return JObject::null();
        }
    }

    list
}
