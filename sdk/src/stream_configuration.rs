/// Configuration options for stream creation, recovery of broken streams and flushing.
#[derive(Clone)]
pub struct StreamConfigurationOptions {
    /// Maximum number of records that can be sending or pending acknowledgement at any given time.
    pub max_inflight_records: usize,
    /// Whether to enable stream recovery.
    pub recovery: bool,
    /// Timeout for stream recovery attempt.
    pub recovery_timeout_ms: u64,
    /// Backoff time in case of stream recovery failure.
    pub recovery_backoff_ms: u64,
    /// Number of retries for stream recovery.
    pub recovery_retries: u32,
    /// Timeout for server lack of acknowledgement.
    pub server_lack_of_ack_timeout_ms: u64,
    /// Timeout for flush.
    pub flush_timeout_ms: u64,
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
        }
    }
}
