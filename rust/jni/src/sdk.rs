//! JNI bindings for ZerobusSdk.
//!
//! This module provides JNI functions for creating and managing ZerobusSdk instances.

use crate::arrow_stream::NativeArrowStreamHandle;
use crate::async_bridge::{create_completable_future, spawn_and_complete};
use crate::errors::{throw_from_zerobus_error, throw_zerobus_exception};
use crate::headers_provider::JavaHeadersProvider;
use crate::options::{
    apply_arrow_stream_options, apply_stream_options, extract_arrow_stream_options,
    extract_stream_options,
};
use crate::runtime::block_on;
use crate::stream::NativeStreamHandle;
use databricks_zerobus_ingest_sdk::databricks::zerobus::RecordType;
use databricks_zerobus_ingest_sdk::ZerobusSdk;
use jni::objects::{JByteArray, JClass, JObject, JString};
use jni::sys::{jboolean, jlong, JNI_FALSE};
use jni::JNIEnv;
use std::sync::Arc;

/// Native SDK handle stored in Java.
pub struct NativeSdkHandle {
    pub sdk: Arc<ZerobusSdk>,
}

impl NativeSdkHandle {
    pub fn new(sdk: ZerobusSdk) -> Self {
        Self { sdk: Arc::new(sdk) }
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

/// Create a new ZerobusSdk instance.
///
/// # JNI Signature
/// ```java
/// private static native long nativeCreate(String serverEndpoint, String unityCatalogEndpoint, String applicationName);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_ZerobusSdk_nativeCreate<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    server_endpoint: JString<'local>,
    unity_catalog_endpoint: JString<'local>,
    application_name: JString<'local>,
) -> jlong {
    // Extract the endpoint strings
    let server_endpoint: String = match env.get_string(&server_endpoint) {
        Ok(s) => s.into(),
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Invalid server endpoint: {}", e));
            return 0;
        }
    };

    let unity_catalog_endpoint: String = match env.get_string(&unity_catalog_endpoint) {
        Ok(s) => s.into(),
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Invalid Unity Catalog endpoint: {}", e));
            return 0;
        }
    };

    let application_name: Option<String> = if application_name.is_null() {
        None
    } else {
        match env.get_string(&application_name) {
            Ok(s) => Some(s.into()),
            Err(e) => {
                throw_zerobus_exception(&mut env, &format!("Invalid application name: {}", e));
                return 0;
            }
        }
    };

    // Create the SDK
    let sdk_identifier = format!("zerobus-sdk-java/{}", env!("CARGO_PKG_VERSION"));
    let builder = ZerobusSdk::builder()
        .endpoint(server_endpoint)
        .unity_catalog_url(unity_catalog_endpoint)
        .sdk_identifier(sdk_identifier);
    let builder = match application_name {
        Some(name) => builder.application_name(name),
        None => builder,
    };
    match builder.build() {
        Ok(sdk) => NativeSdkHandle::new(sdk).into_raw(),
        Err(e) => {
            throw_from_zerobus_error(&mut env, &e);
            0
        }
    }
}

/// Destroy a ZerobusSdk instance.
///
/// # JNI Signature
/// ```java
/// private static native void nativeDestroy(long handle);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_ZerobusSdk_nativeDestroy<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    if handle != 0 {
        // Safety: handle was created by nativeCreate
        unsafe {
            let _ = NativeSdkHandle::from_raw(handle);
        }
    }
}

/// Create a new stream.
///
/// # JNI Signature
/// ```java
/// private native CompletableFuture<Long> nativeCreateStream(
///     long sdkHandle,
///     String tableName,
///     byte[] descriptorProto,
///     String clientId,
///     String clientSecret,
///     Object options,
///     boolean isJson
/// );
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_ZerobusSdk_nativeCreateStream<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    sdk_handle: jlong,
    table_name: JString<'local>,
    descriptor_proto: JByteArray<'local>,
    client_id: JString<'local>,
    client_secret: JString<'local>,
    headers_provider: JObject<'local>,
    options: JObject<'local>,
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

    // Create a global reference to the future for use in async code
    let future_ref = match env.new_global_ref(&future) {
        Ok(r) => r,
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Failed to create global ref: {}", e));
            return JObject::null();
        }
    };

    // Extract parameters
    let table_name: String = match env.get_string(&table_name) {
        Ok(s) => s.into(),
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Invalid table name: {}", e));
            return JObject::null();
        }
    };

    let client_id: String = match env.get_string(&client_id) {
        Ok(s) => s.into(),
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Invalid client ID: {}", e));
            return JObject::null();
        }
    };

    let client_secret: String = match env.get_string(&client_secret) {
        Ok(s) => s.into(),
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Invalid client secret: {}", e));
            return JObject::null();
        }
    };

    let headers_provider = if headers_provider.is_null() {
        None
    } else {
        match env.new_global_ref(&headers_provider) {
            Ok(provider_ref) => Some(JavaHeadersProvider::new(provider_ref).into_arc()),
            Err(e) => {
                throw_zerobus_exception(
                    &mut env,
                    &format!("Failed to retain headers provider: {}", e),
                );
                return JObject::null();
            }
        }
    };

    // Extract descriptor proto bytes (may be null for JSON)
    let descriptor_proto_bytes: Option<Vec<u8>> = if descriptor_proto.is_null() {
        None
    } else {
        match env.convert_byte_array(descriptor_proto) {
            Ok(bytes) => Some(bytes),
            Err(e) => {
                throw_zerobus_exception(&mut env, &format!("Invalid descriptor proto: {}", e));
                return JObject::null();
            }
        }
    };

    // Determine record type
    let record_type = if is_json != JNI_FALSE {
        RecordType::Json
    } else {
        RecordType::Proto
    };

    let extracted_options = if options.is_null() {
        None
    } else {
        match extract_stream_options(&mut env, &options) {
            Ok(opts) => Some(opts),
            Err(e) => {
                throw_zerobus_exception(&mut env, &format!("Invalid options: {}", e));
                return JObject::null();
            }
        }
    };

    // Get SDK handle
    let sdk = unsafe { NativeSdkHandle::borrow_from_raw(sdk_handle) };
    let sdk_arc = sdk.sdk.clone();

    // Store credentials for later use in stream recreation
    let credentials = (client_id, client_secret);

    // Spawn async operation to create stream
    spawn_and_complete(
        future_ref,
        async move {
            // Parse descriptor proto if provided
            let descriptor_proto = if let Some(bytes) = descriptor_proto_bytes {
                Some(prost::Message::decode(bytes.as_slice()).map_err(|e| {
                    databricks_zerobus_ingest_sdk::ZerobusError::InvalidArgument(format!(
                        "Failed to parse descriptor proto: {}",
                        e
                    ))
                })?)
            } else {
                None
            };

            let base = sdk_arc.stream_builder().table(table_name);
            let base = match headers_provider {
                Some(provider) => base.headers_provider(provider),
                None => base.oauth(credentials.0.clone(), credentials.1.clone()),
            };
            let mut builder = match record_type {
                RecordType::Proto => {
                    let desc = descriptor_proto.ok_or_else(|| {
                        databricks_zerobus_ingest_sdk::ZerobusError::InvalidArgument(
                            "Proto descriptor is required for Proto record type".to_string(),
                        )
                    })?;
                    base.compiled_proto(desc)
                }
                RecordType::Json => base.json(),
                RecordType::Unspecified => {
                    return Err(
                        databricks_zerobus_ingest_sdk::ZerobusError::InvalidArgument(
                            "Record type is not specified".to_string(),
                        ),
                    );
                }
            };
            if let Some(opts) = extracted_options {
                builder = apply_stream_options(builder, opts);
            }

            let stream = builder.build().await?;

            Ok(NativeStreamHandle::new(
                stream,
                credentials.0,
                credentials.1,
            ))
        },
        |handle| handle.into_raw(),
    );

    future
}

/// Recreate a stream from a failed stream.
///
/// # JNI Signature
/// ```java
/// private native CompletableFuture<Long> nativeRecreateStream(long sdkHandle, long streamHandle);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_ZerobusSdk_nativeRecreateStream<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    sdk_handle: jlong,
    stream_handle: jlong,
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

    // Get SDK and stream handles
    let sdk = unsafe { NativeSdkHandle::borrow_from_raw(sdk_handle) };
    let sdk_arc = sdk.sdk.clone();

    let stream = unsafe { NativeStreamHandle::borrow_from_raw(stream_handle) };
    let client_id = stream.client_id.clone();
    let client_secret = stream.client_secret.clone();

    // Take the stream out of the handle for recreation
    let old_stream = {
        let mut guard = block_on(stream.stream.lock());
        guard.take()
    };

    let old_stream = match old_stream {
        Some(s) => s,
        None => {
            throw_zerobus_exception(&mut env, "Stream already closed or recreated");
            return JObject::null();
        }
    };

    // Spawn async operation
    spawn_and_complete(
        future_ref,
        async move {
            let new_stream = sdk_arc.recreate_stream(&old_stream).await?;
            Ok(NativeStreamHandle::new(
                new_stream,
                client_id,
                client_secret,
            ))
        },
        |handle| handle.into_raw(),
    );

    future
}

/// Create a new Arrow stream.
///
/// # JNI Signature
/// ```java
/// private native CompletableFuture<Long> nativeCreateArrowStream(
///     long sdkHandle,
///     String tableName,
///     byte[] arrowSchema,
///     String clientId,
///     String clientSecret,
///     Object options
/// );
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_ZerobusSdk_nativeCreateArrowStream<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    sdk_handle: jlong,
    table_name: JString<'local>,
    arrow_schema: JByteArray<'local>,
    client_id: JString<'local>,
    client_secret: JString<'local>,
    headers_provider: JObject<'local>,
    options: JObject<'local>,
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

    // Extract parameters
    let table_name: String = match env.get_string(&table_name) {
        Ok(s) => s.into(),
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Invalid table name: {}", e));
            return JObject::null();
        }
    };

    let client_id: String = match env.get_string(&client_id) {
        Ok(s) => s.into(),
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Invalid client ID: {}", e));
            return JObject::null();
        }
    };

    let client_secret: String = match env.get_string(&client_secret) {
        Ok(s) => s.into(),
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Invalid client secret: {}", e));
            return JObject::null();
        }
    };

    let headers_provider = if headers_provider.is_null() {
        None
    } else {
        match env.new_global_ref(&headers_provider) {
            Ok(provider_ref) => Some(JavaHeadersProvider::new(provider_ref).into_arc()),
            Err(e) => {
                throw_zerobus_exception(
                    &mut env,
                    &format!("Failed to retain headers provider: {}", e),
                );
                return JObject::null();
            }
        }
    };

    // Extract Arrow schema bytes
    let schema_bytes: Vec<u8> = match env.convert_byte_array(arrow_schema) {
        Ok(bytes) => bytes,
        Err(e) => {
            throw_zerobus_exception(&mut env, &format!("Invalid Arrow schema: {}", e));
            return JObject::null();
        }
    };

    let extracted_options = if options.is_null() {
        None
    } else {
        match extract_arrow_stream_options(&mut env, &options) {
            Ok(opts) => Some(opts),
            Err(e) => {
                throw_zerobus_exception(&mut env, &format!("Invalid options: {}", e));
                return JObject::null();
            }
        }
    };

    // Get SDK handle
    let sdk = unsafe { NativeSdkHandle::borrow_from_raw(sdk_handle) };
    let sdk_arc = sdk.sdk.clone();

    let credentials = (client_id, client_secret);

    // Spawn async operation
    spawn_and_complete(
        future_ref,
        async move {
            // Deserialize Arrow schema from IPC format using StreamReader
            let reader = arrow_ipc::reader::StreamReader::try_new(&schema_bytes[..], None)
                .map_err(|e| {
                    databricks_zerobus_ingest_sdk::ZerobusError::InvalidArgument(format!(
                        "Failed to parse Arrow schema: {}",
                        e
                    ))
                })?;

            let schema = reader.schema();

            let builder = sdk_arc.stream_builder().table(table_name);
            let builder = match headers_provider {
                Some(provider) => builder.headers_provider(provider),
                None => builder.oauth(credentials.0.clone(), credentials.1.clone()),
            };
            let mut builder = builder.arrow(schema);
            if let Some(opts) = extracted_options {
                builder = apply_arrow_stream_options(builder, opts);
            }

            let stream = builder.build_arrow().await?;

            Ok(NativeArrowStreamHandle::new(
                stream,
                credentials.0,
                credentials.1,
            ))
        },
        |handle| handle.into_raw(),
    );

    future
}

/// Recreate an Arrow stream from a failed stream.
///
/// # JNI Signature
/// ```java
/// private native CompletableFuture<Long> nativeRecreateArrowStream(long sdkHandle, long streamHandle);
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_ZerobusSdk_nativeRecreateArrowStream<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    sdk_handle: jlong,
    stream_handle: jlong,
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

    // Get SDK and stream handles
    let sdk = unsafe { NativeSdkHandle::borrow_from_raw(sdk_handle) };
    let sdk_arc = sdk.sdk.clone();

    let stream = unsafe { NativeArrowStreamHandle::borrow_from_raw(stream_handle) };
    let client_id = stream.client_id.clone();
    let client_secret = stream.client_secret.clone();

    // Take the stream out of the handle for recreation
    let old_stream = {
        let mut guard = block_on(stream.stream.lock());
        guard.take()
    };

    let old_stream = match old_stream {
        Some(s) => s,
        None => {
            throw_zerobus_exception(&mut env, "Arrow stream already closed or recreated");
            return JObject::null();
        }
    };

    // Spawn async operation
    spawn_and_complete(
        future_ref,
        async move {
            let new_stream = sdk_arc.recreate_arrow_stream(&old_stream).await?;
            Ok(NativeArrowStreamHandle::new(
                new_stream,
                client_id,
                client_secret,
            ))
        },
        |handle| handle.into_raw(),
    );

    future
}
