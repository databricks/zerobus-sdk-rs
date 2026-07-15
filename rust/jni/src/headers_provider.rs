//! JNI bridge for the Java HeadersProvider interface.

use crate::runtime::get_jvm;
use async_trait::async_trait;
use databricks_zerobus_ingest_sdk::{HeadersProvider, ZerobusError, ZerobusResult};
use jni::errors::Error as JniError;
use jni::objects::{GlobalRef, JMap, JObject, JString};
use jni::JNIEnv;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};

static HEADER_NAMES: OnceLock<Mutex<HashSet<&'static str>>> = OnceLock::new();

struct LocalFrameError(ZerobusError);

impl From<JniError> for LocalFrameError {
    fn from(error: JniError) -> Self {
        Self(provider_error(format!("JNI local frame failed: {error}")))
    }
}

impl From<ZerobusError> for LocalFrameError {
    fn from(error: ZerobusError) -> Self {
        Self(error)
    }
}

pub struct JavaHeadersProvider {
    provider_ref: GlobalRef,
}

impl JavaHeadersProvider {
    pub fn new(provider_ref: GlobalRef) -> Self {
        Self { provider_ref }
    }

    pub fn into_arc(self) -> Arc<dyn HeadersProvider> {
        Arc::new(self)
    }
}

#[async_trait]
impl HeadersProvider for JavaHeadersProvider {
    async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
        let provider_ref = self.provider_ref.clone();
        tokio::task::spawn_blocking(move || get_headers_blocking(provider_ref))
            .await
            .map_err(|error| provider_error(format!("callback task failed: {error}")))?
    }

    async fn invalidate(&self) {
        let provider_ref = self.provider_ref.clone();
        if let Err(error) =
            tokio::task::spawn_blocking(move || invalidate_blocking(provider_ref)).await
        {
            tracing::error!("HeadersProvider.invalidate callback task failed: {error}");
        }
    }
}

fn get_headers_blocking(provider_ref: GlobalRef) -> ZerobusResult<HashMap<&'static str, String>> {
    let jvm = get_jvm();
    let mut env = jvm
        .attach_current_thread_as_daemon()
        .map_err(|error| provider_error(format!("failed to attach to JVM: {error}")))?;

    env.with_local_frame(32, |env| -> Result<_, LocalFrameError> {
        let map_object = env
            .call_method(
                provider_ref.as_obj(),
                "getHeaders",
                "()Ljava/util/Map;",
                &[],
            )
            .and_then(|value| value.l())
            .map_err(|error| java_call_error(env, "getHeaders", error))?;

        if map_object.is_null() {
            return Err(provider_error("getHeaders returned null").into());
        }

        Ok(extract_headers(env, &map_object)?)
    })
    .map_err(|error| error.0)
}

fn invalidate_blocking(provider_ref: GlobalRef) {
    let jvm = get_jvm();
    let mut env = match jvm.attach_current_thread_as_daemon() {
        Ok(env) => env,
        Err(error) => {
            tracing::error!("Failed to attach to JVM for HeadersProvider.invalidate: {error}");
            return;
        }
    };

    if let Err(error) = env.with_local_frame(16, |env| -> Result<_, LocalFrameError> {
        env.call_method(provider_ref.as_obj(), "invalidate", "()V", &[])
            .map_err(|error| java_call_error(env, "invalidate", error))?;
        Ok(())
    }) {
        let error = error.0;
        tracing::error!("{error}");
    }
}

fn extract_headers(
    env: &mut JNIEnv<'_>,
    map_object: &JObject<'_>,
) -> ZerobusResult<HashMap<&'static str, String>> {
    let map = JMap::from_env(env, map_object)
        .map_err(|error| java_call_error(env, "read headers map", error))?;
    let mut iterator = map
        .iter(env)
        .map_err(|error| java_call_error(env, "iterate headers map", error))?;
    let mut headers = HashMap::new();

    loop {
        let entry = iterator
            .next(env)
            .map_err(|error| java_call_error(env, "iterate headers map", error))?;
        let Some((key, value)) = entry else {
            break;
        };

        if key.is_null() || value.is_null() {
            return Err(provider_error("header names and values must not be null"));
        }

        let key_string = JString::from(key);
        let value_string = JString::from(value);
        let key: String = env
            .get_string(&key_string)
            .map_err(|error| java_call_error(env, "read header name", error))?
            .into();
        let value: String = env
            .get_string(&value_string)
            .map_err(|error| java_call_error(env, "read header value", error))?
            .into();

        headers.insert(intern_header_name(key), value);
    }

    Ok(headers)
}

fn intern_header_name(name: String) -> &'static str {
    let names = HEADER_NAMES.get_or_init(|| Mutex::new(HashSet::new()));
    let mut names = names
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(existing) = names.get(name.as_str()) {
        return existing;
    }

    let leaked = Box::leak(name.into_boxed_str());
    names.insert(leaked);
    leaked
}

fn java_call_error(env: &mut JNIEnv<'_>, method: &str, error: JniError) -> ZerobusError {
    let detail = if matches!(error, JniError::JavaException) {
        take_java_exception_message(env).unwrap_or_else(|| error.to_string())
    } else {
        error.to_string()
    };
    provider_error(format!("{method} failed: {detail}"))
}

fn take_java_exception_message(env: &mut JNIEnv<'_>) -> Option<String> {
    let throwable = env.exception_occurred().ok()?;
    env.exception_clear().ok()?;
    let message = env
        .call_method(&throwable, "toString", "()Ljava/lang/String;", &[])
        .ok()?
        .l()
        .ok()?;
    let message = JString::from(message);
    env.get_string(&message).ok().map(Into::into)
}

fn provider_error(message: impl Into<String>) -> ZerobusError {
    ZerobusError::InvalidArgument(format!("Java HeadersProvider error: {}", message.into()))
}

unsafe impl Send for JavaHeadersProvider {}
unsafe impl Sync for JavaHeadersProvider {}
