//! Stream configuration options extracted from Java objects.
//!
//! Values are extracted on the JNI thread into JNI-private structs (so they're `Send`),
//! then applied to a `StreamBuilder` inside the async task that builds the stream.

use crate::callbacks::JavaAckCallback;
use arrow_ipc::CompressionType;
use databricks_zerobus_ingest_sdk::{AckCallback, StreamBuilder};
use jni::objects::JObject;
use jni::JNIEnv;
use std::sync::Arc;

/// Extracted JSON/protobuf stream options, ready to apply to a `StreamBuilder`.
pub struct ExtractedStreamOptions {
    pub max_inflight_requests: usize,
    pub recovery: bool,
    pub recovery_timeout_ms: u64,
    pub recovery_backoff_ms: u64,
    pub recovery_retries: u32,
    pub server_lack_of_ack_timeout_ms: u64,
    pub flush_timeout_ms: u64,
    pub ack_callback: Option<Arc<dyn AckCallback>>,
}

/// Extracted Arrow Flight stream options, ready to apply to a `StreamBuilder`.
pub struct ExtractedArrowStreamOptions {
    pub max_inflight_batches: usize,
    pub recovery: bool,
    pub recovery_timeout_ms: u64,
    pub recovery_backoff_ms: u64,
    pub recovery_retries: u32,
    pub server_lack_of_ack_timeout_ms: u64,
    pub flush_timeout_ms: u64,
    pub connection_timeout_ms: u64,
    pub ipc_compression: Option<CompressionType>,
    pub stream_paused_max_wait_time_ms: Option<u64>,
}

/// Read JSON/protobuf stream options from a Java options object.
pub fn extract_stream_options(
    env: &mut JNIEnv,
    options: &JObject,
) -> Result<ExtractedStreamOptions, jni::errors::Error> {
    Ok(ExtractedStreamOptions {
        max_inflight_requests: env
            .call_method(options, "maxInflightRecords", "()I", &[])?
            .i()? as usize,
        recovery: env.call_method(options, "recovery", "()Z", &[])?.z()?,
        recovery_timeout_ms: env
            .call_method(options, "recoveryTimeoutMs", "()I", &[])?
            .i()? as u64,
        recovery_backoff_ms: env
            .call_method(options, "recoveryBackoffMs", "()I", &[])?
            .i()? as u64,
        recovery_retries: env
            .call_method(options, "recoveryRetries", "()I", &[])?
            .i()? as u32,
        flush_timeout_ms: env
            .call_method(options, "flushTimeoutMs", "()I", &[])?
            .i()? as u64,
        server_lack_of_ack_timeout_ms: env
            .call_method(options, "serverLackOfAckTimeoutMs", "()I", &[])?
            .i()? as u64,
        ack_callback: extract_ack_callback(env, options)?,
    })
}

/// Apply extracted JSON/protobuf stream options to a builder.
pub fn apply_stream_options<'a>(
    builder: StreamBuilder<'a>,
    opts: ExtractedStreamOptions,
) -> StreamBuilder<'a> {
    let mut builder = builder
        .max_inflight_requests(opts.max_inflight_requests)
        .recovery(opts.recovery)
        .recovery_timeout_ms(opts.recovery_timeout_ms)
        .recovery_backoff_ms(opts.recovery_backoff_ms)
        .recovery_retries(opts.recovery_retries)
        .server_lack_of_ack_timeout_ms(opts.server_lack_of_ack_timeout_ms)
        .flush_timeout_ms(opts.flush_timeout_ms)
        .stream_paused_max_wait_time_ms(None)
        .callback_max_wait_time_ms(Some(5000));
    if let Some(cb) = opts.ack_callback {
        builder = builder.ack_callback(cb);
    }
    builder
}

/// Extract the AckCallback from a Java StreamConfigurationOptions object.
fn extract_ack_callback(
    env: &mut JNIEnv,
    options: &JObject,
) -> Result<Option<Arc<dyn AckCallback>>, jni::errors::Error> {
    let optional_result =
        env.call_method(options, "getNewAckCallback", "()Ljava/util/Optional;", &[]);

    let optional = match optional_result {
        Ok(val) => val.l()?,
        Err(_) => return Ok(None),
    };

    if optional.is_null() {
        return Ok(None);
    }

    let is_present = env.call_method(&optional, "isPresent", "()Z", &[])?.z()?;
    if !is_present {
        return Ok(None);
    }

    let callback_obj = env
        .call_method(&optional, "get", "()Ljava/lang/Object;", &[])?
        .l()?;
    if callback_obj.is_null() {
        return Ok(None);
    }

    let callback_ref = env.new_global_ref(callback_obj)?;
    Ok(Some(JavaAckCallback::new(callback_ref).into_arc()))
}

/// Read Arrow Flight stream options from a Java options object.
pub fn extract_arrow_stream_options(
    env: &mut JNIEnv,
    options: &JObject,
) -> Result<ExtractedArrowStreamOptions, jni::errors::Error> {
    let compression_enum = env
        .call_method(
            options,
            "ipcCompression",
            "()Lcom/databricks/zerobus/IPCCompressionType;",
            &[],
        )?
        .l()?;
    let compression_jstring = env
        .call_method(&compression_enum, "name", "()Ljava/lang/String;", &[])?
        .l()?;
    let compression_name: String = env.get_string((&compression_jstring).into())?.into();
    let ipc_compression = match compression_name.as_str() {
        "LZ4_FRAME" => Some(CompressionType::LZ4_FRAME),
        "ZSTD" => Some(CompressionType::ZSTD),
        _ => None,
    };

    let stream_paused_max_wait_time_raw = env
        .call_method(options, "streamPausedMaxWaitTimeMs", "()J", &[])?
        .j()?;
    let stream_paused_max_wait_time_ms = if stream_paused_max_wait_time_raw < 0 {
        None
    } else {
        Some(stream_paused_max_wait_time_raw as u64)
    };

    Ok(ExtractedArrowStreamOptions {
        max_inflight_batches: env
            .call_method(options, "maxInflightBatches", "()I", &[])?
            .i()? as usize,
        recovery: env.call_method(options, "recovery", "()Z", &[])?.z()?,
        recovery_timeout_ms: env
            .call_method(options, "recoveryTimeoutMs", "()J", &[])?
            .j()? as u64,
        recovery_backoff_ms: env
            .call_method(options, "recoveryBackoffMs", "()J", &[])?
            .j()? as u64,
        recovery_retries: env
            .call_method(options, "recoveryRetries", "()I", &[])?
            .i()? as u32,
        server_lack_of_ack_timeout_ms: env
            .call_method(options, "serverLackOfAckTimeoutMs", "()J", &[])?
            .j()? as u64,
        flush_timeout_ms: env
            .call_method(options, "flushTimeoutMs", "()J", &[])?
            .j()? as u64,
        connection_timeout_ms: env
            .call_method(options, "connectionTimeoutMs", "()J", &[])?
            .j()? as u64,
        ipc_compression,
        stream_paused_max_wait_time_ms,
    })
}

/// Apply extracted Arrow Flight stream options to a builder.
pub fn apply_arrow_stream_options<'a>(
    builder: StreamBuilder<'a>,
    opts: ExtractedArrowStreamOptions,
) -> StreamBuilder<'a> {
    builder
        .max_inflight_batches(opts.max_inflight_batches)
        .recovery(opts.recovery)
        .recovery_timeout_ms(opts.recovery_timeout_ms)
        .recovery_backoff_ms(opts.recovery_backoff_ms)
        .recovery_retries(opts.recovery_retries)
        .server_lack_of_ack_timeout_ms(opts.server_lack_of_ack_timeout_ms)
        .flush_timeout_ms(opts.flush_timeout_ms)
        .connection_timeout_ms(opts.connection_timeout_ms)
        .ipc_compression(opts.ipc_compression)
        .stream_paused_max_wait_time_ms(opts.stream_paused_max_wait_time_ms)
}
