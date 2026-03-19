//! Marker types for the stream builder's typestate pattern.
//!
//! These types enforce at compile time that a stream builder has both a format
//! and authentication configured before `build()` can be called.

mod sealed {
    pub trait Sealed {}
}

// Format markers

/// Initial state: no format has been chosen yet.
pub struct NoFormat;

/// JSON record format.
pub struct Json;

/// Compiled protobuf record format.
pub struct CompiledProto;

/// Arrow Flight record format.
#[cfg(feature = "arrow-flight")]
pub struct Arrow;

/// Sealed trait implemented by all resolved format markers.
///
/// `build()` is only available when `F: StreamFormat`.
pub trait StreamFormat: sealed::Sealed {}

impl sealed::Sealed for Json {}
impl sealed::Sealed for CompiledProto {}
impl StreamFormat for Json {}
impl StreamFormat for CompiledProto {}

/// Sealed trait for gRPC-based formats ([`Json`] and [`CompiledProto`]).
///
/// gRPC-only configuration setters (e.g., `max_inflight_requests`,
/// `ack_callback`) are only available when `F: GrpcFormat`.
pub trait GrpcFormat: StreamFormat {}
impl GrpcFormat for Json {}
impl GrpcFormat for CompiledProto {}

#[cfg(feature = "arrow-flight")]
impl sealed::Sealed for Arrow {}
#[cfg(feature = "arrow-flight")]
impl StreamFormat for Arrow {}

// Auth markers

/// Initial state: no authentication has been configured yet.
pub struct NoAuth;

/// Authentication has been configured (OAuth or custom headers provider).
pub struct HasAuth;

/// Sealed trait implemented only by [`HasAuth`].
///
/// `build()` is only available when `A: AuthReady`.
pub trait AuthReady: sealed::Sealed {}

impl sealed::Sealed for HasAuth {}
impl AuthReady for HasAuth {}
