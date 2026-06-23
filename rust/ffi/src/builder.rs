//! `ZerobusSdkBuilder` FFI surface.

use crate::common::*;
use databricks_zerobus_ingest_sdk::{NoTlsConfig, ZerobusSdk, ZerobusSdkBuilder};
use std::os::raw::c_char;
use std::ptr;
use std::sync::Arc;

// ============================================================================
// ZerobusSdkBuilder FFI
// ============================================================================
//
// C-builder mirroring the Rust `ZerobusSdkBuilder`. New options are added as
// additive setter functions — no ABI breaks.
//
// Lifecycle: `_new` → zero or more `_<setter>` calls → `_build` (consumes) or
// `_free` (abandon). Single-owner; not safe to share across threads.

/// Opaque handle for an SDK builder. Allocated by `_new`, consumed by
/// `_build`, or dropped by `_free`. Must not be used after either finalizer.
#[repr(C)]
pub struct CZerobusSdkBuilder {
    _private: [u8; 0],
}

/// Concrete type behind `*mut CZerobusSdkBuilder`. All cast sites in this
/// module must agree on this alias.
type SdkBuilderAlloc = ZerobusSdkBuilder;

/// SAFETY: `b` must be a valid pointer from `_new` that hasn't been consumed
/// or freed. `mem::take` keeps the slot valid even if `f` panics.
unsafe fn with_builder<F>(b: *mut CZerobusSdkBuilder, f: F)
where
    F: FnOnce(ZerobusSdkBuilder) -> ZerobusSdkBuilder,
{
    if b.is_null() {
        return;
    }
    let slot = &mut *(b as *mut SdkBuilderAlloc);
    let taken = std::mem::take(slot);
    *slot = f(taken);
}

/// Allocates a new SDK builder. Must be terminated by exactly one of
/// `_build` or `_free`.
#[no_mangle]
pub extern "C" fn zerobus_sdk_builder_new() -> *mut CZerobusSdkBuilder {
    init_logging();
    let boxed: Box<SdkBuilderAlloc> = Box::new(ZerobusSdk::builder());
    Box::into_raw(boxed) as *mut CZerobusSdkBuilder
}

/// Sets the Zerobus gRPC endpoint URL (required). No-op on null.
#[no_mangle]
pub extern "C" fn zerobus_sdk_builder_endpoint(
    builder: *mut CZerobusSdkBuilder,
    value: *const c_char,
) {
    if value.is_null() {
        return;
    }
    let s = match unsafe { c_str_to_string(value) } {
        Ok(s) => s,
        Err(_) => return,
    };
    unsafe { with_builder(builder, |b| b.endpoint(s)) }
}

/// Sets the Unity Catalog URL. Optional with a custom headers provider.
/// No-op on null.
#[no_mangle]
pub extern "C" fn zerobus_sdk_builder_unity_catalog_url(
    builder: *mut CZerobusSdkBuilder,
    value: *const c_char,
) {
    if value.is_null() {
        return;
    }
    let s = match unsafe { c_str_to_string(value) } {
        Ok(s) => s,
        Err(_) => return,
    };
    unsafe { with_builder(builder, |b| b.unity_catalog_url(s)) }
}

/// Overrides the SDK prefix of the `user-agent` header (default
/// `zerobus-sdk-rs/<version>`). Wrappers pass their own identifier here.
/// Null and empty values are no-ops.
#[no_mangle]
pub extern "C" fn zerobus_sdk_builder_sdk_identifier(
    builder: *mut CZerobusSdkBuilder,
    value: *const c_char,
) {
    if value.is_null() {
        return;
    }
    let s = match unsafe { c_str_to_string(value) } {
        Ok(s) if !s.is_empty() => s,
        _ => return,
    };
    unsafe { with_builder(builder, |b| b.sdk_identifier(s)) }
}

/// Appends an application identifier to the `user-agent` header. Wire value
/// becomes `<sdk_identifier> <application_name>`. Null and empty values are
/// no-ops.
#[no_mangle]
pub extern "C" fn zerobus_sdk_builder_application_name(
    builder: *mut CZerobusSdkBuilder,
    value: *const c_char,
) {
    if value.is_null() {
        return;
    }
    let s = match unsafe { c_str_to_string(value) } {
        Ok(s) if !s.is_empty() => s,
        _ => return,
    };
    unsafe { with_builder(builder, |b| b.application_name(s)) }
}

/// Selects a no-TLS gRPC channel. TLS is on by default.
#[no_mangle]
pub extern "C" fn zerobus_sdk_builder_disable_tls(builder: *mut CZerobusSdkBuilder) {
    unsafe { with_builder(builder, |b| b.tls_config(Arc::new(NoTlsConfig))) }
}

/// Consumes the builder and returns a `CZerobusSdk*`, or NULL on error.
/// Frees the builder on both paths — any further use of the pointer is
/// undefined behavior. Null `builder` writes an error to `result`.
#[no_mangle]
pub extern "C" fn zerobus_sdk_builder_build(
    builder: *mut CZerobusSdkBuilder,
    result: *mut CResult,
) -> *mut CZerobusSdk {
    if builder.is_null() {
        write_error_result(result, "Builder pointer is null", false);
        return ptr::null_mut();
    }
    // Reclaim ownership of the builder Box so it is dropped on every path,
    // mirroring the Rust builder's consume-on-build semantics.
    let inner = *unsafe { Box::from_raw(builder as *mut SdkBuilderAlloc) };
    match inner.build() {
        Ok(sdk) => {
            write_success_result(result);
            Box::into_raw(Box::new(sdk)) as *mut CZerobusSdk
        }
        Err(err) => {
            if !result.is_null() {
                unsafe {
                    *result = CResult::error(err);
                }
            }
            ptr::null_mut()
        }
    }
}

/// Drops an unconsumed builder. No-op on null.
#[no_mangle]
pub extern "C" fn zerobus_sdk_builder_free(builder: *mut CZerobusSdkBuilder) {
    if !builder.is_null() {
        unsafe {
            let _ = Box::from_raw(builder as *mut SdkBuilderAlloc);
        }
    }
}
