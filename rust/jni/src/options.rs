//! Stream configuration options mapping between Java and Rust.
//!
//! This module provides utilities for converting Java StreamConfigurationOptions
//! objects to Rust StreamConfigurationOptions structs.

use crate::callbacks::JavaAckCallback;
use arrow_ipc::CompressionType;
use databricks_zerobus_ingest_sdk::databricks::zerobus::RecordType;
use databricks_zerobus_ingest_sdk::{
    AckCallback, ArrowStreamConfigurationOptions, StreamConfigurationOptions,
};
use jni::objects::JObject;
use jni::JNIEnv;
use std::sync::Arc;

/// Extract StreamConfigurationOptions from a Java object.
///
/// This function reads all fields from the Java StreamConfigurationOptions object
/// and creates a corresponding Rust StreamConfigurationOptions struct.
pub fn extract_stream_options(
    env: &mut JNIEnv,
    options: &JObject,
    record_type: RecordType,
) -> Result<StreamConfigurationOptions, jni::errors::Error> {
    // Extract max inflight records
    let max_inflight = env
        .call_method(options, "maxInflightRecords", "()I", &[])?
        .i()? as usize;

    // Extract recovery flag
    let recovery = env.call_method(options, "recovery", "()Z", &[])?.z()?;

    // Extract recovery timeout
    let recovery_timeout_ms = env
        .call_method(options, "recoveryTimeoutMs", "()I", &[])?
        .i()? as u64;

    // Extract recovery backoff
    let recovery_backoff_ms = env
        .call_method(options, "recoveryBackoffMs", "()I", &[])?
        .i()? as u64;

    // Extract recovery retries
    let recovery_retries = env
        .call_method(options, "recoveryRetries", "()I", &[])?
        .i()? as u32;

    // Extract flush timeout
    let flush_timeout_ms = env
        .call_method(options, "flushTimeoutMs", "()I", &[])?
        .i()? as u64;

    // Extract server lack of ack timeout
    let server_lack_of_ack_timeout_ms = env
        .call_method(options, "serverLackOfAckTimeoutMs", "()I", &[])?
        .i()? as u64;

    // Extract ack callback (new interface)
    let ack_callback = extract_ack_callback(env, options)?;

    Ok(StreamConfigurationOptions {
        max_inflight_requests: max_inflight,
        recovery,
        recovery_timeout_ms,
        recovery_backoff_ms,
        recovery_retries,
        server_lack_of_ack_timeout_ms,
        flush_timeout_ms,
        record_type,
        stream_paused_max_wait_time_ms: None,
        ack_callback,
        callback_max_wait_time_ms: Some(5000),
    })
}

/// Extract the AckCallback from a Java StreamConfigurationOptions object.
fn extract_ack_callback(
    env: &mut JNIEnv,
    options: &JObject,
) -> Result<Option<Arc<dyn AckCallback>>, jni::errors::Error> {
    // Call getAckCallback() which returns Optional<AckCallback>
    // We need to check if the result contains a new-style AckCallback
    let optional_result =
        env.call_method(options, "getNewAckCallback", "()Ljava/util/Optional;", &[]);

    // If the method doesn't exist (old API), return None
    let optional = match optional_result {
        Ok(val) => val.l()?,
        Err(_) => return Ok(None),
    };

    if optional.is_null() {
        return Ok(None);
    }

    // Check if Optional is present
    let is_present = env.call_method(&optional, "isPresent", "()Z", &[])?.z()?;

    if !is_present {
        return Ok(None);
    }

    // Get the callback object
    let callback_obj = env
        .call_method(&optional, "get", "()Ljava/lang/Object;", &[])?
        .l()?;

    if callback_obj.is_null() {
        return Ok(None);
    }

    // Create a global reference to the callback
    let callback_ref = env.new_global_ref(callback_obj)?;

    Ok(Some(JavaAckCallback::new(callback_ref).into_arc()))
}

/// Extract ArrowStreamConfigurationOptions from a Java object.
pub fn extract_arrow_stream_options(
    env: &mut JNIEnv,
    options: &JObject,
) -> Result<ArrowStreamConfigurationOptions, jni::errors::Error> {
    // Extract max inflight batches
    let max_inflight_batches = env
        .call_method(options, "maxInflightBatches", "()I", &[])?
        .i()? as usize;

    // Extract recovery flag
    let recovery = env.call_method(options, "recovery", "()Z", &[])?.z()?;

    // Extract recovery timeout
    let recovery_timeout_ms = env
        .call_method(options, "recoveryTimeoutMs", "()J", &[])?
        .j()? as u64;

    // Extract recovery backoff
    let recovery_backoff_ms = env
        .call_method(options, "recoveryBackoffMs", "()J", &[])?
        .j()? as u64;

    // Extract recovery retries
    let recovery_retries = env
        .call_method(options, "recoveryRetries", "()I", &[])?
        .i()? as u32;

    // Extract server lack of ack timeout
    let server_lack_of_ack_timeout_ms = env
        .call_method(options, "serverLackOfAckTimeoutMs", "()J", &[])?
        .j()? as u64;

    // Extract flush timeout
    let flush_timeout_ms = env
        .call_method(options, "flushTimeoutMs", "()J", &[])?
        .j()? as u64;

    // Extract connection timeout
    let connection_timeout_ms = env
        .call_method(options, "connectionTimeoutMs", "()J", &[])?
        .j()? as u64;

    // Extract IPC compression type from the Java enum via ordinal().
    // IPCCompressionType: NONE=0, LZ4_FRAME=1, ZSTD=2
    let compression_enum = env
        .call_method(
            options,
            "ipcCompression",
            "()Lcom/databricks/zerobus/IPCCompressionType;",
            &[],
        )?
        .l()?;
    let compression_ordinal = env
        .call_method(&compression_enum, "ordinal", "()I", &[])?
        .i()?;
    let ipc_compression = match compression_ordinal {
        1 => Some(CompressionType::LZ4_FRAME),
        2 => Some(CompressionType::ZSTD),
        _ => None, // 0 (NONE) or any unknown value
    };

    Ok(ArrowStreamConfigurationOptions {
        max_inflight_batches,
        recovery,
        recovery_timeout_ms,
        recovery_backoff_ms,
        recovery_retries,
        server_lack_of_ack_timeout_ms,
        flush_timeout_ms,
        connection_timeout_ms,
        ipc_compression,
    })
}

/// Create default StreamConfigurationOptions for the given record type.
pub fn default_stream_options(record_type: RecordType) -> StreamConfigurationOptions {
    StreamConfigurationOptions {
        record_type,
        ..Default::default()
    }
}

/// Create default ArrowStreamConfigurationOptions.
pub fn default_arrow_stream_options() -> ArrowStreamConfigurationOptions {
    ArrowStreamConfigurationOptions::default()
}
