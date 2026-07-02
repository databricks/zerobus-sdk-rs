//! Shared FFI types and helpers used across the FFI surface modules.

use async_trait::async_trait;
use databricks_zerobus_ingest_sdk::databricks::zerobus::RecordType;
use databricks_zerobus_ingest_sdk::{
    HeadersProvider, ZerobusError, ZerobusResult, ZerobusSdk, ZerobusStream,
};
use once_cell::sync::Lazy;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use tokio::runtime::Runtime;
use tracing_subscriber::{fmt, EnvFilter};

// Global Tokio runtime for handling async Rust calls
pub(crate) static RUNTIME: Lazy<Runtime> =
    Lazy::new(|| Runtime::new().expect("Failed to create Tokio runtime"));

// Flag to track if logging has been initialized
static LOGGING_INITIALIZED: AtomicBool = AtomicBool::new(false);

/// Initialize tracing subscriber for Rust logs
/// Can be controlled via RUST_LOG environment variable
/// Examples:
///   RUST_LOG=info           - Show info and above
///   RUST_LOG=debug          - Show debug and above
///   RUST_LOG=trace          - Show all logs
///   RUST_LOG=databricks_zerobus_ingest_sdk=debug - Show only SDK logs at debug level
pub(crate) fn init_logging() {
    if LOGGING_INITIALIZED.swap(true, Ordering::SeqCst) {
        return;
    }

    let _ = fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .try_init();
}

// Global cache for header keys to prevent memory leaks
// Header keys are typically a small set of constant strings (e.g., "Authorization", "Content-Type")
// We intern them once to avoid leaking memory on every callback
static HEADER_KEY_CACHE: Lazy<Mutex<HashSet<&'static str>>> =
    Lazy::new(|| Mutex::new(HashSet::new()));

/// Intern a header key string to prevent memory leaks
/// Only leaks memory for unique keys, not on every call
pub(crate) fn intern_header_key(key: String) -> &'static str {
    // Recover from a poisoned lock instead of propagating the panic: the
    // interned keys remain valid, so reusing the set after a panic is safe.
    let mut cache = HEADER_KEY_CACHE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    // Check if we already have this key
    if let Some(&existing) = cache.iter().find(|&&k| k == key.as_str()) {
        return existing;
    }

    // Only leak if it's a new key (typically happens once per unique header name)
    let static_key: &'static str = Box::leak(key.into_boxed_str());
    cache.insert(static_key);
    static_key
}

// Opaque types for Go
#[repr(C)]
pub struct CZerobusSdk {
    _private: [u8; 0],
}

#[repr(C)]
pub struct CZerobusStream {
    _private: [u8; 0],
}

// Result type for FFI calls
#[repr(C)]
pub struct CResult {
    pub success: bool,
    pub error_message: *mut c_char,
    pub is_retryable: bool,
}

/// Represents a single record (either Proto or JSON)
#[repr(C)]
pub struct CRecord {
    pub is_json: bool,
    pub data: *mut u8,
    pub data_len: usize,
}

/// Represents an array of records
#[repr(C)]
pub struct CRecordArray {
    pub records: *mut CRecord,
    pub len: usize,
}

impl CResult {
    pub(crate) fn success() -> Self {
        CResult {
            success: true,
            error_message: ptr::null_mut(),
            is_retryable: false,
        }
    }

    pub(crate) fn error(err: ZerobusError) -> Self {
        let is_retryable = err.is_retryable();
        let message = CString::new(err.to_string())
            .unwrap_or_else(|_| CString::new("Unknown error").unwrap());

        CResult {
            success: false,
            error_message: message.into_raw(),
            is_retryable,
        }
    }
}

// Configuration options
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CStreamConfigurationOptions {
    pub max_inflight_requests: usize,
    pub recovery: bool,
    pub recovery_timeout_ms: u64,
    pub recovery_backoff_ms: u64,
    pub recovery_retries: u32,
    pub server_lack_of_ack_timeout_ms: u64,
    pub flush_timeout_ms: u64,
    pub record_type: i32,
    pub stream_paused_max_wait_time_ms: u64,
    pub has_stream_paused_max_wait_time_ms: bool,
    pub callback_max_wait_time_ms: u64,
    pub has_callback_max_wait_time_ms: bool,
}

// Helper to convert C string to Rust String
pub(crate) unsafe fn c_str_to_string(c_str: *const c_char) -> Result<String, &'static str> {
    if c_str.is_null() {
        return Err("Null pointer passed");
    }
    CStr::from_ptr(c_str)
        .to_str()
        .map(|s| s.to_string())
        .map_err(|_| "Invalid UTF-8 string")
}

pub(crate) fn c_record_type(value: i32) -> RecordType {
    match value {
        1 => RecordType::Proto,
        2 => RecordType::Json,
        _ => RecordType::Unspecified,
    }
}

/// A single header key-value pair for C FFI
#[repr(C)]
pub struct CHeader {
    pub key: *mut c_char,
    pub value: *mut c_char,
}

/// A collection of headers returned from Go callback
#[repr(C)]
pub struct CHeaders {
    pub headers: *mut CHeader,
    pub count: usize,
    pub error_message: *mut c_char,
}

/// Function pointer type for the headers provider callback
/// The callback should return a CHeaders struct
/// The caller is responsible for freeing the returned CHeaders using zerobus_free_headers
pub type HeadersProviderCallback = extern "C" fn(user_data: *mut std::ffi::c_void) -> CHeaders;

/// Rust struct that wraps a Go callback and implements HeadersProvider
pub(crate) struct CallbackHeadersProvider {
    callback: HeadersProviderCallback,
    user_data: *mut std::ffi::c_void,
    in_use: AtomicBool, // Track concurrent access to detect thread-safety issues
}

impl CallbackHeadersProvider {
    pub(crate) fn new(callback: HeadersProviderCallback, user_data: *mut std::ffi::c_void) -> Self {
        Self {
            callback,
            user_data,
            in_use: AtomicBool::new(false),
        }
    }
}

// Safety: We assume the Go callback is thread-safe, but we validate at runtime
unsafe impl Send for CallbackHeadersProvider {}
unsafe impl Sync for CallbackHeadersProvider {}

#[async_trait]
impl HeadersProvider for CallbackHeadersProvider {
    async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
        // Check for concurrent access (indicates thread-safety issue)
        if self.in_use.swap(true, Ordering::SeqCst) {
            return Err(ZerobusError::InvalidArgument(
                "Concurrent headers provider callback detected - Go callback must be thread-safe"
                    .to_string(),
            ));
        }

        // Call the Go callback (synchronous)
        let c_headers = (self.callback)(self.user_data);

        // Release the lock before processing
        self.in_use.store(false, Ordering::SeqCst);

        // Check for error
        if !c_headers.error_message.is_null() {
            let error_str = unsafe {
                CStr::from_ptr(c_headers.error_message)
                    .to_string_lossy()
                    .into_owned()
            };
            crate::zerobus_free_headers(c_headers);
            return Err(ZerobusError::InvalidArgument(format!(
                "Headers provider error: {}",
                error_str
            )));
        }

        // Convert C headers to Rust HashMap
        let mut headers = HashMap::new();
        if !c_headers.headers.is_null() && c_headers.count > 0 {
            unsafe {
                let headers_slice = std::slice::from_raw_parts(c_headers.headers, c_headers.count);
                for header in headers_slice {
                    if !header.key.is_null() && !header.value.is_null() {
                        let key = CStr::from_ptr(header.key).to_string_lossy().into_owned();
                        let value = CStr::from_ptr(header.value).to_string_lossy().into_owned();

                        // Use interned keys to minimize memory leaks
                        // Only unique header names are leaked (typically < 10 strings for lifetime of process)
                        let static_key = intern_header_key(key);
                        headers.insert(static_key, value);
                    }
                }
            }
        }

        crate::zerobus_free_headers(c_headers);
        Ok(headers)
    }
}

/// Safe wrapper to validate SDK pointer
pub(crate) fn validate_sdk_ptr<'a>(sdk: *mut CZerobusSdk) -> Result<&'a ZerobusSdk, &'static str> {
    if sdk.is_null() {
        return Err("SDK pointer is null");
    }
    // Still unsafe, but centralized and validated
    unsafe { Ok(&*(sdk as *const ZerobusSdk)) }
}

/// Safe wrapper to validate stream pointer
pub(crate) fn validate_stream_ptr<'a>(
    stream: *mut CZerobusStream,
) -> Result<&'a ZerobusStream, &'static str> {
    if stream.is_null() {
        return Err("Stream pointer is null");
    }
    unsafe { Ok(&*(stream as *const ZerobusStream)) }
}

/// Safe wrapper to validate mutable stream pointer
pub(crate) fn validate_stream_ptr_mut<'a>(
    stream: *mut CZerobusStream,
) -> Result<&'a mut ZerobusStream, &'static str> {
    if stream.is_null() {
        return Err("Stream pointer is null");
    }
    unsafe { Ok(&mut *(stream as *mut ZerobusStream)) }
}

/// Helper to write error result
pub(crate) fn write_error_result(result: *mut CResult, message: &str, is_retryable: bool) {
    if !result.is_null() {
        unsafe {
            // Free any message left by a prior write before overwriting. This closes a
            // leak on the panic path: a guarded body can populate `result` via
            // `write_error_result` and then panic, after which `ffi_guard`'s panic arm
            // writes again. Callers pass a zero-initialized `CResult` (error_message
            // NULL), so the first write frees nothing.
            if !(*result).error_message.is_null() {
                drop(CString::from_raw((*result).error_message));
            }
            *result = CResult {
                success: false,
                error_message: CString::new(message)
                    .unwrap_or_else(|_| CString::new("Error message contains null byte").unwrap())
                    .into_raw(),
                is_retryable,
            };
        }
    }
}

/// Helper to write success result
pub(crate) fn write_success_result(result: *mut CResult) {
    if !result.is_null() {
        unsafe {
            *result = CResult::success();
        }
    }
}

/// Best-effort human-readable message extracted from a caught panic payload.
fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        format!("Rust panic caught at FFI boundary: {s}")
    } else if let Some(s) = payload.downcast_ref::<String>() {
        format!("Rust panic caught at FFI boundary: {s}")
    } else {
        "Rust panic caught at FFI boundary".to_string()
    }
}

/// Runs an FFI entry point's body, catching any panic so it cannot cross the
/// `extern "C"` boundary. On a caught panic it writes a non-retryable error to
/// `result` and returns `sentinel`, the function's normal failure value. Pass a
/// null `result` for entry points that have no `CResult` out-parameter.
pub(crate) fn ffi_guard<R>(result: *mut CResult, sentinel: R, body: impl FnOnce() -> R) -> R {
    // AssertUnwindSafe is sound here: on a caught panic the call is abandoned
    // and the sentinel returned, so partially-updated state is never observed.
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(value) => value,
        Err(payload) => {
            write_error_result(result, &panic_message(payload.as_ref()), false);
            sentinel
        }
    }
}

#[cfg(test)]
mod common_tests {
    use super::*;

    /// `intern_header_key` must keep working after its lock is poisoned by a
    /// panic. Lives here to reach the private `HEADER_KEY_CACHE`.
    #[test]
    fn test_intern_header_key_recovers_from_poisoned_lock() {
        // Poison the global mutex: lock it and panic while holding the guard.
        let poisoned = std::thread::spawn(|| {
            let _guard = HEADER_KEY_CACHE.lock().unwrap();
            panic!("poison HEADER_KEY_CACHE on purpose");
        })
        .join();
        assert!(poisoned.is_err(), "the spawned thread should have panicked");
        assert!(
            HEADER_KEY_CACHE.lock().is_err(),
            "the lock should now be poisoned"
        );

        // Interning must still work despite the poison — no panic, correct value,
        // and still interned (same pointer on a second call).
        let k1 = intern_header_key("X-Poison-Recovery-Test".to_string());
        assert_eq!(k1, "X-Poison-Recovery-Test");
        let k2 = intern_header_key("X-Poison-Recovery-Test".to_string());
        assert_eq!(k1.as_ptr(), k2.as_ptr());
    }
}
