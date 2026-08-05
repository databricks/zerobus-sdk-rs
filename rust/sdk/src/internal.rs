//! Unsupported implementation details shared by Zerobus-maintained bindings.
//!
//! Items in this module are not covered by the public Rust SDK stability
//! contract and may change without deprecation.

/// Arrow C Data conversion used by Zerobus native bindings.
#[doc(hidden)]
pub mod arrow_c_data {
    pub use crate::stream::arrow_c_data::{
        import_c_data_record_batch, FFI_ArrowArray, FFI_ArrowSchema,
    };
}
