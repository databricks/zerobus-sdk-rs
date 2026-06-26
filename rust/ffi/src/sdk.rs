//! Top-level SDK lifecycle FFI surface.

use crate::common::*;
use databricks_zerobus_ingest_sdk::ZerobusSdk;
use std::os::raw::c_char;
use std::ptr;

/// Creates a new ZerobusSdk with default user-agent and TLS settings.
///
/// Retained for ABI back-compat with v1.2.x; new code should use the
/// `zerobus_sdk_builder_*` API. Does not infer TLS state from the endpoint
/// scheme — callers needing a plain-HTTP channel must use the builder API.
///
/// Returns NULL on error; see `result` for details.
#[no_mangle]
pub extern "C" fn zerobus_sdk_new(
    zerobus_endpoint: *const c_char,
    unity_catalog_url: *const c_char,
    result: *mut CResult,
) -> *mut CZerobusSdk {
    ffi_guard(result, ptr::null_mut(), move || {
        let builder = crate::zerobus_sdk_builder_new();
        crate::zerobus_sdk_builder_endpoint(builder, zerobus_endpoint);
        crate::zerobus_sdk_builder_unity_catalog_url(builder, unity_catalog_url);
        crate::zerobus_sdk_builder_build(builder, result)
    })
}

/// Free the SDK instance
#[no_mangle]
pub extern "C" fn zerobus_sdk_free(sdk: *mut CZerobusSdk) {
    ffi_guard(ptr::null_mut(), (), move || {
        if !sdk.is_null() {
            unsafe {
                let _ = Box::from_raw(sdk as *mut ZerobusSdk);
            }
        }
    })
}

/// Set whether to use TLS for connections.
///
/// Deprecated: This function is a no-op. TLS is now controlled via the `TlsConfig`
/// trait passed to the SDK builder. This function is retained for ABI compatibility.
#[no_mangle]
pub extern "C" fn zerobus_sdk_set_use_tls(_sdk: *mut CZerobusSdk, _use_tls: bool) {}
