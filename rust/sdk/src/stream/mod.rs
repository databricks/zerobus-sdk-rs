//! Ingestion streams, organized per transport.
//!
//! [`grpc`] holds JSON and Protocol Buffer streaming.
//! [`arrow`] holds Arrow Flight streaming.
//!
//! TODO: lift the transport-agnostic parts of `grpc/` (callbacks,
//! `flush` / `wait_for_offset` / `close`, `ingest_*`) into a shared core.

#[cfg(feature = "arrow-flight")]
mod arrow;
mod grpc;

#[cfg(feature = "internal-arrow-c-data")]
pub(crate) use arrow::c_data as arrow_c_data;

#[cfg(feature = "arrow-flight")]
pub(crate) use arrow::ArrowTableProperties;
#[cfg(feature = "arrow-flight")]
pub use arrow::{
    ArrowSchema, ArrowStreamConfigurationOptions, DataType, Field, RecordBatch, TimeUnit,
    ZerobusArrowStream,
};

pub use grpc::ZerobusStream;

#[cfg(feature = "testing")]
pub use grpc::CallbackHandlerHarness;
#[cfg(feature = "testing")]
pub(crate) use grpc::StreamShutdownHandle;
