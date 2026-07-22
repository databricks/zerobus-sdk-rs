//! Ingestion streams, organized per transport.
//!
//! [`grpc`] holds proto / JSON streaming over a bidirectional gRPC stream.
//! Arrow Flight (Beta) still lives in the top-level `arrow_stream` module.
//!
//! TODO: move Arrow Flight under here and lift the transport-agnostic parts
//! of `grpc/` (callbacks, `flush` / `wait_for_offset` / `close`, `ingest_*`)
//! into a shared core.

mod grpc;

#[cfg(feature = "testing")]
pub use grpc::CallbackHandlerHarness;
pub use grpc::ZerobusStream;
