//! Shared configuration options for stream creation and operation.
//!
//! This module provides common configuration constants shared between JSON/protobuf
//! and Arrow Flight streams.

/// Default values for stream configuration options.
/// These are shared between JSON/protobuf and Arrow Flight streams.
pub mod defaults {
    /// Default: enable automatic stream recovery
    pub const RECOVERY: bool = true;
    /// Default: 15 seconds per recovery attempt
    pub const RECOVERY_TIMEOUT_MS: u64 = 15_000;
    /// Default: 2 seconds backoff between retries
    pub const RECOVERY_BACKOFF_MS: u64 = 2_000;
    /// Default: 4 retry attempts
    pub const RECOVERY_RETRIES: u32 = 4;
    /// Default: 60 seconds lack of ack timeout
    pub const SERVER_LACK_OF_ACK_TIMEOUT_MS: u64 = 60_000;
    /// Default: 5 minutes flush timeout
    pub const FLUSH_TIMEOUT_MS: u64 = 300_000;
    /// Default: 30 seconds connection timeout
    #[cfg(feature = "arrow-flight")]
    pub const CONNECTION_TIMEOUT_MS: u64 = 30_000;
    /// Default: 5 seconds callback timeout
    pub const CALLBACK_MAX_WAIT_TIME_MS: u64 = 5_000;
    /// Default: max ingest payload, slightly below the 10 MiB server limit to leave envelope headroom
    pub const MAX_INGEST_PAYLOAD_BYTES: usize = 10 * 1024 * 1024 - 64 * 1024;
}
