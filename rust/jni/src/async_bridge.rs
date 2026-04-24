//! Async bridge for completing Java CompletableFutures from Rust async code.
//!
//! This module provides utilities for bridging Rust async operations to Java
//! CompletableFutures, allowing async results to be propagated back to Java.

use crate::class_cache::{as_jclass, get_class_cache};
use crate::errors::{create_exception_from_error, create_zerobus_exception};
use crate::runtime::{get_jvm, spawn};
use databricks_zerobus_ingest_sdk::ZerobusError;
use jni::objects::{GlobalRef, JObject, JValue};
use jni::JNIEnv;
use std::future::Future;

/// Complete a Java CompletableFuture with a successful result.
///
/// This function completes the future on the current thread. The value
/// must be a valid Java object reference.
pub fn complete_future<'local>(
    env: &mut JNIEnv<'local>,
    future: &JObject<'local>,
    value: JObject<'local>,
) -> Result<(), jni::errors::Error> {
    env.call_method(
        future,
        "complete",
        "(Ljava/lang/Object;)Z",
        &[JValue::Object(&value)],
    )?;
    Ok(())
}

/// Complete a Java CompletableFuture with a Void result (null).
pub fn complete_future_void<'local>(
    env: &mut JNIEnv<'local>,
    future: &JObject<'local>,
) -> Result<(), jni::errors::Error> {
    env.call_method(
        future,
        "complete",
        "(Ljava/lang/Object;)Z",
        &[JValue::Object(&JObject::null())],
    )?;
    Ok(())
}

/// Complete a Java CompletableFuture with an exception.
pub fn complete_future_exceptionally<'local>(
    env: &mut JNIEnv<'local>,
    future: &JObject<'local>,
    exception: JObject<'local>,
) -> Result<(), jni::errors::Error> {
    env.call_method(
        future,
        "completeExceptionally",
        "(Ljava/lang/Throwable;)Z",
        &[JValue::Object(&exception)],
    )?;
    Ok(())
}

/// Spawn an async operation that will complete a Java CompletableFuture with a Long value.
///
/// This function takes ownership of a GlobalRef to the CompletableFuture and
/// spawns an async task that will complete the future when the operation finishes.
/// The result is boxed as a Java Long object.
///
/// The future is completed on Tokio's blocking thread pool (via `spawn_blocking`)
/// to ensure that any Java callbacks (like `thenApply`) can safely call back into
/// JNI methods that use `block_on`.
///
/// # Type Parameters
///
/// * `F` - The future type that produces the result
/// * `T` - The result type that can be converted to a jlong
/// * `C` - A closure that converts the result to a jlong
pub fn spawn_and_complete<F, T, C>(future_ref: GlobalRef, async_fn: F, convert_result: C)
where
    F: Future<Output = Result<T, ZerobusError>> + Send + 'static,
    T: Send + 'static,
    C: FnOnce(T) -> jni::sys::jlong + Send + 'static,
{
    spawn(async move {
        let result = async_fn.await;

        // Complete the future on the blocking thread pool to avoid issues when
        // Java callbacks call back into JNI methods that use block_on.
        let handle = tokio::task::spawn_blocking(move || {
            // Attach to the JVM to complete the future
            let jvm = get_jvm();
            let mut env = match jvm.attach_current_thread_as_daemon() {
                Ok(env) => env,
                Err(e) => {
                    tracing::error!("Failed to attach to JVM: {}", e);
                    return;
                }
            };

            let future = future_ref.as_obj();

            match result {
                Ok(value) => {
                    let handle_value = convert_result(value);
                    // Box as Java Long using cached class reference
                    let long_class = as_jclass(&get_class_cache().long_class);
                    match env.new_object(long_class, "(J)V", &[JValue::Long(handle_value)]) {
                        Ok(java_value) => {
                            if let Err(e) = complete_future(&mut env, future, java_value) {
                                tracing::error!("Failed to complete future: {}", e);
                            }
                        }
                        Err(e) => {
                            tracing::error!("Failed to create Long object: {}", e);
                            if let Some(exc) = create_zerobus_exception(&mut env, &e.to_string()) {
                                let _ = complete_future_exceptionally(&mut env, future, exc.into());
                            } else {
                                // Last resort: complete with null to avoid hanging the future
                                let _ = complete_future_void(&mut env, future);
                            }
                        }
                    }
                }
                Err(error) => {
                    if let Some(exc) = create_exception_from_error(&mut env, &error) {
                        if let Err(e) = complete_future_exceptionally(&mut env, future, exc.into())
                        {
                            tracing::error!("Failed to complete future exceptionally: {}", e);
                        }
                    } else {
                        // Last resort: complete with null to avoid hanging the future
                        tracing::error!(
                            "Failed to create exception for error: {}. Completing future with null.",
                            error
                        );
                        let _ = complete_future_void(&mut env, future);
                    }
                }
            }
        });
        if let Err(e) = handle.await {
            tracing::error!("Blocking task panicked: {}", e);
        }
    });
}

/// Spawn an async operation that will complete a Java CompletableFuture<Void>.
///
/// This is a convenience function for operations that don't return a value.
///
/// The future is completed on Tokio's blocking thread pool (via `spawn_blocking`)
/// to ensure that any Java callbacks can safely call back into JNI methods.
pub fn spawn_and_complete_void<F>(future_ref: GlobalRef, async_fn: F)
where
    F: Future<Output = Result<(), ZerobusError>> + Send + 'static,
{
    spawn(async move {
        let result = async_fn.await;

        // Complete the future on the blocking thread pool to avoid issues when
        // Java callbacks call back into JNI methods that use block_on.
        let handle = tokio::task::spawn_blocking(move || {
            // Attach to the JVM to complete the future
            let jvm = get_jvm();
            let mut env = match jvm.attach_current_thread_as_daemon() {
                Ok(env) => env,
                Err(e) => {
                    tracing::error!("Failed to attach to JVM: {}", e);
                    return;
                }
            };

            let future = future_ref.as_obj();

            match result {
                Ok(()) => {
                    if let Err(e) = complete_future_void(&mut env, future) {
                        tracing::error!("Failed to complete future: {}", e);
                    }
                }
                Err(error) => {
                    if let Some(exc) = create_exception_from_error(&mut env, &error) {
                        if let Err(e) = complete_future_exceptionally(&mut env, future, exc.into())
                        {
                            tracing::error!("Failed to complete future exceptionally: {}", e);
                        }
                    } else {
                        // Last resort: complete with null to avoid hanging the future
                        tracing::error!(
                            "Failed to create exception for error: {}. Completing future with null.",
                            error
                        );
                        let _ = complete_future_void(&mut env, future);
                    }
                }
            }
        });
        if let Err(e) = handle.await {
            tracing::error!("Blocking task panicked: {}", e);
        }
    });
}

/// Create a new Java CompletableFuture object.
pub fn create_completable_future<'local>(
    env: &mut JNIEnv<'local>,
) -> Result<JObject<'local>, jni::errors::Error> {
    let class = as_jclass(&get_class_cache().completable_future_class);
    env.new_object(class, "()V", &[])
}
