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

/// Stops Arrow stream background work without flushing and waits for shutdown to complete.
#[cfg(feature = "internal-arrow-c-data")]
#[doc(hidden)]
pub async fn abort_arrow_stream_and_wait(stream: &crate::ZerobusArrowStream) {
    stream.abort_and_wait().await;
}

/// Marks that an Arrow stream may retain foreign Arrow C Data owners.
#[cfg(feature = "internal-arrow-c-data")]
#[doc(hidden)]
pub fn mark_arrow_stream_c_data_ingested(stream: &crate::ZerobusArrowStream) {
    stream.mark_c_data_ingested();
}

/// Returns whether Arrow stream destruction must wait for foreign C Data owners.
#[cfg(feature = "internal-arrow-c-data")]
#[doc(hidden)]
pub fn arrow_stream_has_ingested_c_data(stream: &crate::ZerobusArrowStream) -> bool {
    stream.has_ingested_c_data()
}
