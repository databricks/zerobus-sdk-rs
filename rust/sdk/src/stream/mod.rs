//! Ingestion streams, organized per transport.
//!
//! [`grpc`] holds proto / JSON streaming over a bidirectional gRPC stream.
//! [`arrow`] holds Arrow Flight streaming (Beta).
//!
//! TODO: lift the transport-agnostic parts of `grpc/` (callbacks,
//! `flush` / `wait_for_offset` / `close`, `ingest_*`) into a shared core.

#[cfg(feature = "arrow-flight")]
mod arrow;
mod grpc;

#[cfg(feature = "arrow-flight")]
pub(crate) use arrow::ArrowTableProperties;
#[cfg(feature = "arrow-flight")]
pub use arrow::{ArrowSchema, DataType, Field, RecordBatch, TimeUnit, ZerobusArrowStream};

pub use grpc::ZerobusStream;

#[cfg(feature = "testing")]
pub use grpc::CallbackHandlerHarness;
