use crate::databricks::zerobus::RecordType;

/// Configuration options for stream creation, recovery of broken streams and flushing.
///
/// These options control the behavior of ingestion streams, including memory limits,
/// recovery policies, and timeout settings.
///
/// # Examples
///
/// ```
/// use databricks_zerobus_ingest_sdk::StreamConfigurationOptions;
///
/// let options = StreamConfigurationOptions {
///     max_inflight_records: 50000,
///     recovery: true,
///     recovery_timeout_ms: 20000,
///     recovery_retries: 5,
///     ..Default::default()
/// };
/// ```
#[derive(Clone)]
pub struct StreamConfigurationOptions {
    /// Maximum number of records that can be sending or pending acknowledgement at any given time.
    ///
    /// This limit controls memory usage and backpressure. When this limit is reached,
    /// `ingest_record()` calls will block until acknowledgments free up space.
    ///
    /// Default: 1,000,000
    pub max_inflight_records: usize,

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

    /// Timeout in milliseconds for waiting for server acknowledgements.
    ///
    /// If no acknowledgement is received within this time (and there are pending records),
    /// the stream will be considered failed and recovery will be triggered.
    ///
    /// Default: 60,000 (60 seconds)
    pub server_lack_of_ack_timeout_ms: u64,

    /// Timeout in milliseconds for flush operations.
    ///
    /// If a flush() call cannot complete within this time, it will return a timeout error.
    ///
    /// Default: 300,000 (5 minutes)
    pub flush_timeout_ms: u64,
    /// Type of record to ingest.
    ///
    /// Supported values:
    /// - RecordType::Proto
    /// - RecordType::Json
    /// - RecordType::Unspecified
    ///
    /// Default: RecordType::Proto
    pub record_type: RecordType,
}

impl Default for StreamConfigurationOptions {
    fn default() -> Self {
        Self {
            max_inflight_records: 1_000_000,
            recovery: true,
            recovery_timeout_ms: 15000,
            recovery_backoff_ms: 2000,
            recovery_retries: 4,
            server_lack_of_ack_timeout_ms: 60000,
            flush_timeout_ms: 300000,
            record_type: RecordType::Proto,
        }
    }
}
