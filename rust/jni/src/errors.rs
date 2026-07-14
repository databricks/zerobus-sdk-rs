//! Error handling and conversion between Rust and Java exceptions.
//!
//! This module provides utilities for converting Zerobus SDK errors to
//! appropriate Java exceptions.

use crate::class_cache::{as_jclass, get_class_cache};
use databricks_zerobus_ingest_sdk::ZerobusError;
use jni::objects::{GlobalRef, JObject, JString, JThrowable, JValue};
use jni::JNIEnv;

/// Throw a ZerobusException in Java.
///
/// This function creates and throws a Java ZerobusException with the given message.
pub fn throw_zerobus_exception(env: &mut JNIEnv, message: &str) {
    let cache = get_class_cache();
    match create_exception(env, &cache.zerobus_exception_class, message) {
        Some(exc) => {
            if let Err(e) = env.throw(exc) {
                tracing::error!("Failed to throw ZerobusException: {}", e);
            }
        }
        None => {
            tracing::error!("Failed to create ZerobusException");
        }
    }
}

/// Throw a NonRetriableException in Java.
///
/// This function creates and throws a Java NonRetriableException with the given message.
pub fn throw_non_retriable_exception(env: &mut JNIEnv, message: &str) {
    let cache = get_class_cache();
    match create_exception(env, &cache.non_retriable_exception_class, message) {
        Some(exc) => {
            if let Err(e) = env.throw(exc) {
                tracing::error!("Failed to throw NonRetriableException: {}", e);
            }
        }
        None => {
            tracing::error!("Failed to create NonRetriableException");
        }
    }
}

/// Convert a ZerobusError to a Java exception and throw it.
///
/// This function maps Rust error types to appropriate Java exception types
/// based on whether the error is retryable or not.
pub fn throw_from_zerobus_error(env: &mut JNIEnv, error: &ZerobusError) {
    let message = error.to_string();

    if error.is_retryable() {
        throw_zerobus_exception(env, &message);
    } else {
        throw_non_retriable_exception(env, &message);
    }
}

/// Create a Java exception object from a ZerobusError.
///
/// This function creates but does not throw the exception, useful for
/// completing CompletableFutures exceptionally.
pub fn create_exception_from_error<'local>(
    env: &mut JNIEnv<'local>,
    error: &ZerobusError,
) -> Option<JThrowable<'local>> {
    let cache = get_class_cache();
    let message = error.to_string();
    let class_ref = if error.is_retryable() {
        &cache.zerobus_exception_class
    } else {
        &cache.non_retriable_exception_class
    };

    create_exception(env, class_ref, &message)
}

/// Create a Java exception object with the given cached class reference and message.
pub fn create_exception<'local>(
    env: &mut JNIEnv<'local>,
    class_ref: &GlobalRef,
    message: &str,
) -> Option<JThrowable<'local>> {
    let class = as_jclass(class_ref);

    // Create the message string
    let j_message = match env.new_string(message) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("Failed to create message string: {}", e);
            return None;
        }
    };

    // Create the exception instance
    match env.new_object(
        class,
        "(Ljava/lang/String;)V",
        &[JValue::Object(&j_message.into())],
    ) {
        Ok(obj) => Some(JThrowable::from(obj)),
        Err(e) => {
            tracing::error!("Failed to create exception instance: {}", e);
            None
        }
    }
}

/// Create a ZerobusException Java object.
pub fn create_zerobus_exception<'local>(
    env: &mut JNIEnv<'local>,
    message: &str,
) -> Option<JThrowable<'local>> {
    let cache = get_class_cache();
    create_exception(env, &cache.zerobus_exception_class, message)
}

/// Create a NonRetriableException Java object.
pub fn create_non_retriable_exception<'local>(
    env: &mut JNIEnv<'local>,
    message: &str,
) -> Option<JThrowable<'local>> {
    let cache = get_class_cache();
    create_exception(env, &cache.non_retriable_exception_class, message)
}

/// Check if a Java exception is pending and clear it.
///
/// Returns the exception message if one was pending.
pub fn check_and_clear_exception(env: &mut JNIEnv) -> Option<String> {
    if env.exception_check().unwrap_or(false) {
        if let Ok(exc) = env.exception_occurred() {
            let _ = env.exception_clear();
            let exc = env.auto_local(JObject::from(exc));

            // Try to get the exception message
            let message = env
                .call_method(exc.as_ref(), "getMessage", "()Ljava/lang/String;", &[])
                .ok()
                .and_then(|value| value.l().ok())
                .and_then(|message_obj| {
                    let message_obj = env.auto_local(message_obj);
                    if message_obj.as_ref().is_null() {
                        return None;
                    }
                    let jstr: &JString = message_obj.as_ref().into();
                    env.get_string(jstr).ok().map(|value| value.into())
                });
            if env.exception_check().unwrap_or(false) {
                let _ = env.exception_clear();
            }
            return Some(message.unwrap_or_else(|| "Unknown exception".to_string()));
        }
    }
    None
}

/// A result type alias for JNI operations that may throw Java exceptions.
pub type JniResult<T> = Result<T, JniError>;

/// Error type for JNI operations.
#[derive(Debug, thiserror::Error)]
pub enum JniError {
    #[error("JNI error: {0}")]
    Jni(#[from] jni::errors::Error),

    #[error("Zerobus error: {0}")]
    Zerobus(#[from] ZerobusError),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Null pointer")]
    NullPointer,
}
