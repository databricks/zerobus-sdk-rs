//! JNI bridge for the Java HeadersProvider interface.

use crate::runtime::get_jvm;
use crate::{as_jclass, get_class_cache};
use async_trait::async_trait;
use databricks_zerobus_ingest_sdk::{HeadersProvider, ZerobusError, ZerobusResult};
use jni::errors::Error as JniError;
use jni::objects::{GlobalRef, JMap, JObject, JString};
use jni::JNIEnv;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};

static HEADER_NAMES: OnceLock<Mutex<HashSet<&'static str>>> = OnceLock::new();
const MAX_INTERNED_HEADER_NAMES: usize = 1024;

struct LocalFrameError(ZerobusError);

impl From<JniError> for LocalFrameError {
    fn from(error: JniError) -> Self {
        Self(provider_retryable_error(format!(
            "JNI local frame failed: {error}"
        )))
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
            .map_err(|error| provider_retryable_error(format!("callback task failed: {error}")))?
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

pub(crate) fn get_headers_blocking(
    provider_ref: GlobalRef,
) -> ZerobusResult<HashMap<&'static str, String>> {
    let jvm = get_jvm();
    let mut env = jvm
        .attach_current_thread_as_daemon()
        .map_err(|error| provider_retryable_error(format!("failed to attach to JVM: {error}")))?;

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
            return Err(provider_invalid_argument("getHeaders returned null").into());
        }

        Ok(extract_headers(env, &map_object)?)
    })
    .map_err(|error| error.0)
}

pub(crate) fn invalidate_blocking(provider_ref: GlobalRef) {
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
            return Err(provider_invalid_argument(
                "header names and values must not be null",
            ));
        }
        let key_is_string = env
            .is_instance_of(&key, "java/lang/String")
            .map_err(|error| java_call_error(env, "validate header name", error))?;
        let value_is_string = env
            .is_instance_of(&value, "java/lang/String")
            .map_err(|error| java_call_error(env, "validate header value", error))?;
        if !key_is_string || !value_is_string {
            return Err(provider_invalid_argument(
                "header names and values must be java.lang.String",
            ));
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

        env.delete_local_ref(key_string)
            .map_err(|error| java_call_error(env, "release header name", error))?;
        env.delete_local_ref(value_string)
            .map_err(|error| java_call_error(env, "release header value", error))?;

        headers.insert(intern_header_name(key)?, value);
    }

    Ok(headers)
}

fn intern_header_name(name: String) -> ZerobusResult<&'static str> {
    let name = normalize_header_name(name)?;
    let names = HEADER_NAMES.get_or_init(|| Mutex::new(HashSet::new()));
    let mut names = names
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(existing) = names.get(name.as_str()) {
        return Ok(existing);
    }
    if names.len() >= MAX_INTERNED_HEADER_NAMES {
        return Err(provider_invalid_argument(format!(
            "too many distinct header names; use at most {MAX_INTERNED_HEADER_NAMES} fixed names"
        )));
    }

    let leaked = Box::leak(name.into_boxed_str());
    names.insert(leaked);
    Ok(leaked)
}

fn normalize_header_name(mut name: String) -> ZerobusResult<String> {
    name.make_ascii_lowercase();
    if name.is_empty()
        || name.ends_with("-bin")
        || !name.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"-_.".contains(&byte)
        })
    {
        return Err(provider_invalid_argument(format!(
            "invalid gRPC metadata header name: {name}"
        )));
    }
    Ok(name)
}

fn java_call_error(env: &mut JNIEnv<'_>, method: &str, error: JniError) -> ZerobusError {
    if matches!(error, JniError::JavaException) {
        let exception = take_java_exception(env);
        let detail = exception
            .as_ref()
            .and_then(|exception| exception.message.as_deref())
            .unwrap_or("Java exception was thrown");
        let message = format!("{method} failed: {detail}");
        if exception.is_some_and(|exception| exception.non_retryable) {
            provider_invalid_argument(message)
        } else {
            provider_retryable_error(message)
        }
    } else {
        provider_retryable_error(format!("{method} failed: {error}"))
    }
}

struct JavaException {
    message: Option<String>,
    non_retryable: bool,
}

fn take_java_exception(env: &mut JNIEnv<'_>) -> Option<JavaException> {
    let throwable = match env.exception_occurred() {
        Ok(throwable) => throwable,
        Err(_) => {
            let _ = env.exception_clear();
            return None;
        }
    };
    let _ = env.exception_clear();

    let non_retryable = env
        .is_instance_of(
            &throwable,
            as_jclass(&get_class_cache().non_retriable_exception_class),
        )
        .unwrap_or(false);
    let _ = env.exception_clear();
    let message = extract_throwable_string(env, &throwable);
    let _ = env.exception_clear();

    Some(JavaException {
        message,
        non_retryable,
    })
}

fn extract_throwable_string(env: &mut JNIEnv<'_>, throwable: &JObject<'_>) -> Option<String> {
    let message = env
        .call_method(throwable, "toString", "()Ljava/lang/String;", &[])
        .ok()?
        .l()
        .ok()?;
    let message = JString::from(message);
    env.get_string(&message).ok().map(Into::into)
}

fn provider_invalid_argument(message: impl Into<String>) -> ZerobusError {
    ZerobusError::InvalidArgument(format!("Java HeadersProvider error: {}", message.into()))
}

fn provider_retryable_error(message: impl Into<String>) -> ZerobusError {
    ZerobusError::TokenFetchError(format!("Java HeadersProvider error: {}", message.into()))
}
