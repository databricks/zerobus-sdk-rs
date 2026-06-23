// Allow clippy warnings for FFI code where unsafe operations are unavoidable
#![allow(clippy::not_unsafe_ptr_arg_deref)]
#![allow(clippy::type_complexity)]

extern crate libc;

// Module declaration order is load-bearing. cbindgen emits functions in source
// walk order (module declaration order, then top-to-bottom within each module),
// and `ffi/zerobus.h` must stay byte-identical for the Go/Java ABI. The exported
// functions must concatenate as: arrow fns + `zerobus_free_headers` (arrow) →
// builder fns → sdk fns → stream fns ending with `zerobus_free_error_message`
// and `zerobus_get_default_config` → proto fns. `common` declares the shared
// types and has no exported functions.
//
// A comment between each declaration breaks rustfmt's contiguous-`mod` grouping,
// so it cannot alphabetize them and reorder the generated header.
mod common;
// arrow must follow common and precede builder/sdk/stream/proto_schema.
mod arrow;
// builder follows arrow.
mod builder;
// sdk follows builder.
mod sdk;
// stream follows sdk.
mod stream;
// proto_schema must be declared last.
mod proto_schema;

// Test module
#[cfg(test)]
mod tests;

pub use arrow::*;
pub use builder::*;
pub use common::*;
pub use proto_schema::*;
pub use sdk::*;
pub use stream::*;

// Re-exported SDK types referenced via `crate::` paths by the test module.
#[cfg(test)]
pub(crate) use databricks_zerobus_ingest_sdk::databricks::zerobus::RecordType;
#[cfg(test)]
pub(crate) use databricks_zerobus_ingest_sdk::ZerobusError;
