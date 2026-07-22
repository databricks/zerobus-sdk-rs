//! Tokio runtime management for JNI.
//!
//! This module manages the global Tokio runtime used for all async operations
//! in the Zerobus SDK. The runtime is initialized when the JNI library is loaded
//! and persists for the lifetime of the JVM.

use std::sync::OnceLock;

use jni::JavaVM;
use tokio::runtime::Runtime;

/// Global Tokio runtime for async operations.
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Global JVM reference for attaching threads in async callbacks.
static JVM: OnceLock<JavaVM> = OnceLock::new();

/// Number of worker threads for the Tokio runtime.
const WORKER_THREADS: usize = 4;

/// Initialize the global Tokio runtime.
///
/// This function is idempotent - calling it multiple times has no effect after
/// the first successful initialization.
pub fn init_runtime() {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(WORKER_THREADS)
            .enable_all()
            .thread_name("zerobus-tokio")
            .build()
            .expect("Failed to create Tokio runtime")
    });
}

/// Get a reference to the global Tokio runtime.
///
/// # Panics
///
/// Panics if the runtime has not been initialized (JNI_OnLoad not called).
pub fn get_runtime() -> &'static Runtime {
    RUNTIME
        .get()
        .expect("Tokio runtime not initialized - JNI_OnLoad must be called first")
}

/// Set the global JVM reference.
///
/// # Errors
///
/// Returns an error if the JVM has already been set.
pub fn set_jvm(vm: JavaVM) -> Result<(), &'static str> {
    JVM.set(vm).map_err(|_| "JVM already set")
}

/// Get a reference to the global JVM.
///
/// # Panics
///
/// Panics if the JVM has not been set (JNI_OnLoad not called).
pub fn get_jvm() -> &'static JavaVM {
    JVM.get()
        .expect("JVM not set - JNI_OnLoad must be called first")
}

/// Execute a blocking operation on the Tokio runtime.
///
/// This function blocks the current thread until the future completes.
pub fn block_on<F, T>(future: F) -> T
where
    F: std::future::Future<Output = T>,
{
    get_runtime().block_on(future)
}

/// Spawn a future on the Tokio runtime.
///
/// The future will run asynchronously on the runtime's thread pool.
pub fn spawn<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    get_runtime().spawn(future)
}
