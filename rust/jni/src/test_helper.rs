//! Native test helpers for classloader isolation testing.
//!
//! These JNI functions are used by `ClassLoaderIsolationTest` to verify that
//! class references work correctly from daemon threads, even when the system
//! classloader cannot see the SDK classes.

use crate::class_cache::{as_jclass, get_class_cache};
use crate::runtime::{get_jvm, get_runtime};
use jni::objects::{JClass, JObject, JString};
use jni::JNIEnv;

/// Test finding a class from a Tokio daemon thread using direct `find_class`.
///
/// Returns `"OK"` if the class is found, or an error message string.
/// In an isolated classloader environment, this is expected to FAIL because
/// daemon threads use the system classloader which cannot see SDK classes.
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_NativeTestHelper_nativeTestFindClassFromDaemonThread<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    class_name: JString<'local>,
) -> JObject<'local> {
    let class_name_str: String = match env.get_string(&class_name) {
        Ok(s) => s.into(),
        Err(_) => return JObject::null(),
    };

    let result = get_runtime().block_on(async {
        tokio::task::spawn_blocking(move || {
            let jvm = get_jvm();
            let mut env = match jvm.attach_current_thread_as_daemon() {
                Ok(env) => env,
                Err(e) => return format!("ERROR: Failed to attach thread: {}", e),
            };
            match env.find_class(&class_name_str) {
                Ok(_) => "OK".to_string(),
                Err(e) => {
                    // Capture the actual Java exception details before clearing
                    let exception_detail = if env.exception_check().unwrap_or(false) {
                        if let Ok(exc) = env.exception_occurred() {
                            let _ = env.exception_clear();
                            // Get exception class name
                            let class_name = env
                                .get_object_class(&exc)
                                .ok()
                                .and_then(|cls| {
                                    env.call_method(cls, "getName", "()Ljava/lang/String;", &[])
                                        .ok()
                                })
                                .and_then(|v| v.l().ok())
                                .filter(|o| !o.is_null())
                                .and_then(|o| {
                                    let jstr: jni::objects::JString = o.into();
                                    env.get_string(&jstr).ok().map(String::from)
                                })
                                .unwrap_or_default();
                            // Get exception message
                            let message = env
                                .call_method(&exc, "getMessage", "()Ljava/lang/String;", &[])
                                .ok()
                                .and_then(|v| v.l().ok())
                                .filter(|o| !o.is_null())
                                .and_then(|o| {
                                    let jstr: jni::objects::JString = o.into();
                                    env.get_string(&jstr).ok().map(String::from)
                                })
                                .unwrap_or_default();
                            format!("{}: {}", class_name, message)
                        } else {
                            let _ = env.exception_clear();
                            String::new()
                        }
                    } else {
                        String::new()
                    };
                    format!("ERROR: {} [{}]", e, exception_detail)
                }
            }
        })
        .await
        .unwrap_or_else(|e| format!("ERROR: Task panicked: {}", e))
    });

    match env.new_string(&result) {
        Ok(s) => s.into(),
        Err(_) => JObject::null(),
    }
}

/// Test finding a class from a Tokio daemon thread using the cached `GlobalRef`.
///
/// Returns `"OK"` if the cached class reference is valid, or an error message.
/// This is expected to SUCCEED even in isolated classloader environments because
/// the `GlobalRef` was populated during `JNI_OnLoad` with the correct classloader.
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_NativeTestHelper_nativeTestFindClassFromDaemonThreadCached<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    class_name: JString<'local>,
) -> JObject<'local> {
    let class_name_str: String = match env.get_string(&class_name) {
        Ok(s) => s.into(),
        Err(_) => return JObject::null(),
    };

    let result = get_runtime().block_on(async {
        tokio::task::spawn_blocking(move || {
            let jvm = get_jvm();
            let _env = match jvm.attach_current_thread_as_daemon() {
                Ok(env) => env,
                Err(e) => return format!("ERROR: Failed to attach thread: {}", e),
            };

            let cache = get_class_cache();
            let cached_ref = match class_name_str.as_str() {
                "com/databricks/zerobus/ZerobusException" => Some(&cache.zerobus_exception_class),
                "com/databricks/zerobus/NonRetriableException" => {
                    Some(&cache.non_retriable_exception_class)
                }
                "java/lang/Long" => Some(&cache.long_class),
                "java/util/concurrent/CompletableFuture" => Some(&cache.completable_future_class),
                "java/util/ArrayList" => Some(&cache.array_list_class),
                "com/databricks/zerobus/EncodedBatch" => Some(&cache.encoded_batch_class),
                _ => None,
            };

            match cached_ref {
                Some(global_ref) => {
                    let jclass = as_jclass(global_ref);
                    if jclass.is_null() {
                        "ERROR: cached class reference is null".to_string()
                    } else {
                        "OK".to_string()
                    }
                }
                None => format!("ERROR: class {} not found in cache", class_name_str),
            }
        })
        .await
        .unwrap_or_else(|e| format!("ERROR: Task panicked: {}", e))
    });

    match env.new_string(&result) {
        Ok(s) => s.into(),
        Err(_) => JObject::null(),
    }
}
