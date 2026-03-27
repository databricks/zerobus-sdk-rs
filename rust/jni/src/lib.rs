//! JNI bindings for the Zerobus Rust SDK.
//!
//! This crate provides JNI bindings to allow Java applications to use the Zerobus
//! Rust SDK for high-performance data ingestion into Databricks Delta tables.

mod arrow_stream;
mod async_bridge;
mod callbacks;
mod class_cache;
mod errors;
mod options;
mod runtime;
mod sdk;
mod stream;
mod test_helper;

use jni::sys::jint;
use jni::JavaVM;
use std::ffi::c_void;

pub use arrow_stream::*;
pub use async_bridge::*;
pub use callbacks::*;
pub use class_cache::*;
pub use errors::*;
pub use options::*;
pub use runtime::*;
pub use sdk::*;
pub use stream::*;
pub use test_helper::*;

/// JNI version required by this library.
const JNI_VERSION: jint = jni::sys::JNI_VERSION_1_8;

/// Called by the JVM when the native library is loaded.
///
/// This function initializes the Tokio runtime that will be used for all async
/// operations in the Zerobus SDK.
#[no_mangle]
pub extern "system" fn JNI_OnLoad(vm: JavaVM, _reserved: *mut c_void) -> jint {
    // Initialize the global Tokio runtime
    runtime::init_runtime();

    // Cache class references as GlobalRefs while the correct classloader is active.
    // JNI_OnLoad runs on a Java thread whose classloader loaded the native library.
    // Caching here ensures classes are resolvable from daemon threads that only
    // see the system classloader (e.g., Spring Boot's LaunchedURLClassLoader).
    {
        let mut env = match vm.get_env() {
            Ok(env) => env,
            Err(e) => {
                tracing::error!("Failed to get JNI env for class cache: {}", e);
                return jni::sys::JNI_ERR;
            }
        };
        if let Err(e) = class_cache::init_class_cache(&mut env) {
            tracing::error!("Failed to initialize class cache: {}", e);
            return jni::sys::JNI_ERR;
        }
    }

    // Cache the JVM reference for later use in async callbacks
    if let Err(e) = runtime::set_jvm(vm) {
        tracing::error!("Failed to cache JVM reference: {}", e);
        return jni::sys::JNI_ERR;
    }

    tracing::info!("Zerobus JNI library loaded successfully");
    JNI_VERSION
}

/// Called by the JVM when the native library is unloaded.
#[no_mangle]
pub extern "system" fn JNI_OnUnload(_vm: JavaVM, _reserved: *mut c_void) {
    tracing::info!("Zerobus JNI library unloading");
    // The runtime will be dropped when the process exits
}
