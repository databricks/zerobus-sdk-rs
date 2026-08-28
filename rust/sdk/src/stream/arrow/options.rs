//! Configuration options for Arrow Flight streams.
//!
use crate::stream_options::defaults;
use arrow_ipc::CompressionType;

/// Configuration options for Arrow Flight stream creation and operation.
///
/// These options control the behavior of Arrow Flight ingestion streams, including
/// backpressure limits, timeout settings, and recovery policies.
///
/// **Do not construct this directly.** Configure Arrow streams via the builder API:
///
/// ```rust,ignore
/// let stream = sdk
///     .stream_builder()
///     .table("catalog.schema.table")
///     .oauth("client-id", "client-secret")
///     .arrow(schema)
///     .max_inflight_batches(100)
///     .server_lack_of_ack_timeout_ms(30_000)
///     .recovery(true)
///     .build_arrow()
///     .await?;
/// ```
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ArrowStreamConfigurationOptions {
    /// Maximum number of batches that can be in-flight (sent but not acknowledged).
    ///
    /// This limit controls memory usage and backpressure. When this limit is reached,
    /// `ingest_batch()` calls will block until acknowledgments free up space.
    ///
    /// Default: 1,000
    pub max_inflight_batches: usize,

    /// Whether to enable automatic stream recovery on failure.
    ///
    /// When enabled, the SDK will automatically attempt to reconnect and recover
    /// the stream when encountering retryable errors.
    ///
    /// Default: `true`
    pub recovery: bool,

    /// Timeout in milliseconds for each stream recovery attempt.
    ///
    /// If a recovery attempt takes longer than this, it will be retried.
    /// Values whose absolute deadline cannot be represented by the platform's
    /// monotonic clock are rejected when the stream is built.
    ///
    /// For OAuth-authenticated streams this also caps a proactive token refresh at
    /// half its value, but only when the cached token has more life left than that
    /// cap, so a stalled endpoint falls back to the cached token before the attempt
    /// deadline. When too little of the token's life remains, the refresh runs
    /// unbounded, like a cold miss.
    ///
    /// Default: 15,000 (15 seconds)
    pub recovery_timeout_ms: u64,

    /// Backoff time in milliseconds between stream recovery retry attempts.
    ///
    /// The SDK will wait this duration before attempting another recovery after a failure.
    ///
    /// Default: 2,000 (2 seconds)
    pub recovery_backoff_ms: u64,

    /// Maximum number of recovery retry attempts before giving up.
    ///
    /// After this many failed attempts, the stream will close and return an error.
    ///
    /// Default: 4
    pub recovery_retries: u32,

    /// Maximum time in milliseconds that a batch may remain pending during normal stream
    /// operation without being fully acknowledged on the active connection.
    ///
    /// No timer runs while there are no pending batches. A batch's absolute deadline starts
    /// when it becomes pending; responses and partial acknowledgments do not refresh it.
    /// Configure this timeout together with `max_inflight_batches` so the server can
    /// acknowledge a full allowed backlog in time. Replayed batches receive a fresh deadline
    /// after the full replay has completed and ACK processing can resume on the recovered
    /// connection. Expiry fails the stream and triggers recovery when recovery is enabled.
    /// Values whose absolute deadline cannot be represented by the platform's
    /// monotonic clock are rejected when the stream is built.
    ///
    /// Default: 60,000 (60 seconds)
    pub server_lack_of_ack_timeout_ms: u64,

    /// Timeout in milliseconds for flush operations.
    ///
    /// If a `flush()` call cannot complete within this time, it will return a timeout error.
    /// Values whose absolute deadline cannot be represented by the platform's
    /// monotonic clock are rejected when the stream is built.
    ///
    /// Default: 300,000 (5 minutes)
    pub flush_timeout_ms: u64,

    /// Timeout in milliseconds for stream connection establishment.
    ///
    /// If the Arrow Flight stream cannot be established within this time,
    /// stream creation will fail.
    ///
    /// Default: 30,000 (30 seconds)
    pub connection_timeout_ms: u64,

    /// Optional Arrow IPC compression for Flight payloads.
    ///
    /// Supported compression types from `arrow_ipc::CompressionType`:
    /// - `CompressionType::LZ4_FRAME` - LZ4 frame compression
    /// - `CompressionType::ZSTD` - Zstandard compression
    ///
    /// Default: `None`
    pub ipc_compression: Option<CompressionType>,

    /// Maximum time in milliseconds to wait for acknowledgments during server-initiated
    /// graceful stream rotation.
    ///
    /// When the server sends a close stream signal indicating it will close the stream,
    /// the SDK enters a "paused" state where it:
    /// - Continues accepting and buffering new `ingest_batch()` calls
    /// - Stops sending buffered batches to the server
    /// - Continues processing acknowledgments for in-flight batches
    /// - Waits for batches already sent at the time of the signal to be acknowledged,
    ///   or for the configured grace period to expire
    /// - Half-closes the request, then shares up to 500ms between observing request EOF
    ///   and draining the response so the peer can send `END_STREAM` before recovery
    ///
    /// Configuration values:
    /// - `None`: Use the available server grace period for acknowledgments
    /// - `Some(0)`: Do not wait for acknowledgments
    /// - `Some(x)`: Wait up to `x` milliseconds for acknowledgments, further capped by
    ///   the server grace period after reserving transport-cleanup time
    ///
    /// The SDK reserves time inside the server grace period for bounded request/response
    /// transport cleanup. This cleanup still runs when the configured ACK wait is zero.
    /// If the server advertises less than 500ms (including zero), the SDK skips the ACK
    /// wait and makes a best-effort local cleanup attempt for up to 500ms; the server may
    /// already have hard-closed, so clean peer shutdown cannot be guaranteed in that case.
    /// Server-advertised grace periods longer than one year are capped at one year.
    /// Close signals are honored even when recovery is disabled: the SDK performs transport
    /// cleanup and terminates without reconnecting. Batches accepted while paused remain
    /// available through `get_unacked_batches()`.
    ///
    /// The clean half-close guarantee applies to the active connection. Explicit close
    /// during an already-active rotation or recovery retains that attempt's trigger even if
    /// the explicit close target is already acknowledged. Any uncommitted replacement request
    /// is dropped best-effort.
    ///
    /// Default: `None` (use the available server grace period)
    pub stream_paused_max_wait_time_ms: Option<u64>,
}

impl Default for ArrowStreamConfigurationOptions {
    fn default() -> Self {
        Self {
            max_inflight_batches: 1_000,
            recovery: defaults::RECOVERY,
            recovery_timeout_ms: defaults::RECOVERY_TIMEOUT_MS,
            recovery_backoff_ms: defaults::RECOVERY_BACKOFF_MS,
            recovery_retries: defaults::RECOVERY_RETRIES,
            server_lack_of_ack_timeout_ms: defaults::SERVER_LACK_OF_ACK_TIMEOUT_MS,
            flush_timeout_ms: defaults::FLUSH_TIMEOUT_MS,
            connection_timeout_ms: defaults::CONNECTION_TIMEOUT_MS,
            ipc_compression: None,
            stream_paused_max_wait_time_ms: None,
        }
    }
}
