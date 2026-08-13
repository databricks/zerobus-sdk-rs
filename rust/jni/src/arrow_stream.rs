//! JNI bindings for ZerobusArrowStream.
//!
//! This module provides JNI functions for Arrow Flight stream operations including
//! batch ingestion, acknowledgment waiting, flushing, and closing.

use crate::class_cache::{as_jclass, get_class_cache};
use crate::errors::{throw_from_zerobus_error, throw_zerobus_exception};
use crate::runtime::block_on;
use databricks_zerobus_ingest_sdk::ZerobusArrowStream;
use jni::objects::{JByteArray, JClass, JObject, JValue};
use jni::sys::{jboolean, jlong, JNI_FALSE, JNI_TRUE};
use jni::JNIEnv;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Native Arrow stream handle stored in Java.
pub struct NativeArrowStreamHandle {
    pub stream: Arc<Mutex<Option<ZerobusArrowStream>>>,
    pub client_id: String,
    pub client_secret: String,
}

impl NativeArrowStreamHandle {
    pub fn new(stream: ZerobusArrowStream, client_id: String, client_secret: String) -> Self {
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

async fn close_native_stream(
    stream_handle: &NativeArrowStreamHandle,
) -> databricks_zerobus_ingest_sdk::ZerobusResult<()> {
    let mut guard = stream_handle.stream.lock().await;
    if let Some(stream) = guard.as_mut() {
        stream.close().await?;
    }
    Ok(())
}

/// Destroy an Arrow stream handle.
///
/// # JNI Signature
/// ```java
/// private static native void nativeDestroy(long handle);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_ZerobusArrowStream_nativeDestroy<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    if handle != 0 {
        // Safety: handle was created by createArrowStream
        unsafe {
            let _ = NativeArrowStreamHandle::from_raw(handle);
        }
    }
}

/// Ingest an Arrow RecordBatch and return the offset immediately.
///
/// The batch is provided as serialized Arrow IPC format bytes.
///
/// # JNI Signature
/// ```java
/// private native long nativeIngestBatch(long handle, byte[] batchData);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_ZerobusArrowStream_nativeIngestBatch<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
    batch_data: JByteArray<'local>,
) -> jlong {
    // Extract batch data bytes
    let bytes: Vec<u8> = match env.convert_byte_array(batch_data) {
        Ok(b) => b,
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Invalid batch data: {}", e));
            return -1;
        }
    };

    // Get stream handle
    let stream_handle = unsafe { NativeArrowStreamHandle::borrow_from_raw(handle) };

    // Block on the async operation
    let result = block_on(async {
        let mut guard = stream_handle.stream.lock().await;
        let stream = guard.as_mut().ok_or_else(|| {
            databricks_zerobus_ingest_sdk::ZerobusError::InvalidStateError(
                "Arrow stream is closed".to_string(),
            )
        })?;

        // Deserialize the RecordBatch from IPC format
        let reader = arrow_ipc::reader::StreamReader::try_new(&bytes[..], None).map_err(|e| {
            databricks_zerobus_ingest_sdk::ZerobusError::InvalidArgument(format!(
                "Failed to parse Arrow IPC data: {}",
                e
            ))
        })?;

        // Read the batch (assuming single batch in the stream)
        let mut batches = Vec::new();
        for batch_result in reader {
            let batch = batch_result.map_err(|e| {
                databricks_zerobus_ingest_sdk::ZerobusError::InvalidArgument(format!(
                    "Failed to read Arrow batch: {}",
                    e
                ))
            })?;
            batches.push(batch);
        }

        if batches.is_empty() {
            return Err(
                databricks_zerobus_ingest_sdk::ZerobusError::InvalidArgument(
                    "No batches found in Arrow IPC data".to_string(),
                ),
            );
        }

        // Ingest the first batch
        stream.ingest_batch(batches.remove(0)).await
    });

    match result {
        Ok(offset) => offset,
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
pub extern "system" fn Java_com_databricks_zerobus_ZerobusArrowStream_nativeWaitForOffset<
    'local,
>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
    offset: jlong,
) {
    // Get stream handle
    let stream_handle = unsafe { NativeArrowStreamHandle::borrow_from_raw(handle) };

    // Block on the async operation
    let result = block_on(async {
        let guard = stream_handle.stream.lock().await;
        let stream = guard.as_ref().ok_or_else(|| {
            databricks_zerobus_ingest_sdk::ZerobusError::InvalidStateError(
                "Arrow stream is closed".to_string(),
            )
        })?;

        stream.wait_for_offset(offset).await
    });

    if let Err(e) = result {
        throw_from_zerobus_error(&mut env, &e);
    }
}

/// Flush all pending batches (blocking).
///
/// # JNI Signature
/// ```java
/// private native void nativeFlush(long handle);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_ZerobusArrowStream_nativeFlush<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
) {
    // Get stream handle
    let stream_handle = unsafe { NativeArrowStreamHandle::borrow_from_raw(handle) };

    // Block on the async operation
    let result = block_on(async {
        let guard = stream_handle.stream.lock().await;
        let stream = guard.as_ref().ok_or_else(|| {
            databricks_zerobus_ingest_sdk::ZerobusError::InvalidStateError(
                "Arrow stream is closed".to_string(),
            )
        })?;

        stream.flush().await
    });

    if let Err(e) = result {
        throw_from_zerobus_error(&mut env, &e);
    }
}

/// Close the Arrow stream (blocking).
///
/// # JNI Signature
/// ```java
/// private native void nativeClose(long handle);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_ZerobusArrowStream_nativeClose<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
) {
    // Get stream handle
    let stream_handle = unsafe { NativeArrowStreamHandle::borrow_from_raw(handle) };

    // Block on the async operation
    let result = block_on(close_native_stream(stream_handle));

    if let Err(e) = result {
        throw_from_zerobus_error(&mut env, &e);
    }
}

/// Check if the Arrow stream is closed.
///
/// # JNI Signature
/// ```java
/// private native boolean nativeIsClosed(long handle);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_ZerobusArrowStream_nativeIsClosed<'local>(
    _env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
) -> jboolean {
    // Get stream handle
    let stream_handle = unsafe { NativeArrowStreamHandle::borrow_from_raw(handle) };

    // Check both: wrapper cleared (nativeClose called) or stream internally closed (error/shutdown)
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

/// Get the table name.
///
/// # JNI Signature
/// ```java
/// private native String nativeGetTableName(long handle);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_ZerobusArrowStream_nativeGetTableName<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
) -> JObject<'local> {
    // Get stream handle
    let stream_handle = unsafe { NativeArrowStreamHandle::borrow_from_raw(handle) };

    // Block on the async operation to get the table name
    let result = block_on(async {
        let guard = stream_handle.stream.lock().await;
        let stream = guard.as_ref().ok_or_else(|| {
            databricks_zerobus_ingest_sdk::ZerobusError::InvalidStateError(
                "Arrow stream is closed".to_string(),
            )
        })?;

        Ok::<_, databricks_zerobus_ingest_sdk::ZerobusError>(stream.table_name().to_string())
    });

    match result {
        Ok(name) => match env.new_string(&name) {
            Ok(s) => s.into(),
            Err(e) => {
                throw_zerobus_exception(&mut env, &format!("Failed to create string: {}", e));
                JObject::null()
            }
        },
        Err(e) => {
            throw_from_zerobus_error(&mut env, &e);
            JObject::null()
        }
    }
}

/// Get unacked batches as serialized Arrow IPC data.
///
/// # JNI Signature
/// ```java
/// private native List<byte[]> nativeGetUnackedBatches(long handle);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_ZerobusArrowStream_nativeGetUnackedBatches<
    'local,
>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
) -> JObject<'local> {
    // Get stream handle
    let stream_handle = unsafe { NativeArrowStreamHandle::borrow_from_raw(handle) };

    // Block on the async operation
    let result = block_on(async {
        let guard = stream_handle.stream.lock().await;
        let stream = guard.as_ref().ok_or_else(|| {
            databricks_zerobus_ingest_sdk::ZerobusError::InvalidStateError(
                "Arrow stream is closed".to_string(),
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
    let array_list_class = as_jclass(&get_class_cache().array_list_class);

    let list = match env.new_object(&array_list_class, "()V", &[]) {
        Ok(l) => l,
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Failed to create ArrayList: {}", e));
            return JObject::null();
        }
    };

    // Serialize each batch to IPC format and add to the list
    for batch in batches {
        // Serialize the batch to IPC format
        let mut buffer = Vec::new();
        {
            let mut writer =
                match arrow_ipc::writer::StreamWriter::try_new(&mut buffer, &batch.schema()) {
                    Ok(w) => w,
                    Err(e) => {
                        throw_zerobus_exception(
                            &mut env,
                            &format!("Failed to create IPC writer: {}", e),
                        );
                        return JObject::null();
                    }
                };

            if let Err(e) = writer.write(&batch) {
                throw_zerobus_exception(&mut env, &format!("Failed to write batch: {}", e));
                return JObject::null();
            }

            if let Err(e) = writer.finish() {
                throw_zerobus_exception(&mut env, &format!("Failed to finish IPC stream: {}", e));
                return JObject::null();
            }
        }

        let byte_array = match env.byte_array_from_slice(&buffer) {
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

#[cfg(test)]
#[path = "arrow_stream/tests.rs"]
mod tests;
