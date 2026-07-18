namespace Databricks.Zerobus;

/// <summary>
/// Configuration options for Zerobus Arrow Flight streams (Beta).
/// Extends gRPC stream options with Arrow-specific settings.
/// </summary>
public sealed class ArrowStreamConfigurationOptions
{
    /// <summary>
    /// Default: 1,000 batches.
    /// </summary>
    public const int DefaultMaxInflightBatches = 1_000;

    /// <summary>
    /// Default: 30,000 ms (30 seconds).
    /// </summary>
    public const int DefaultConnectionTimeoutMs = 30_000;

    /// <summary>
    /// Default: -1 (wait full server-specified duration).
    /// </summary>
    public const long DefaultStreamPausedMaxWaitTimeMs = -1;

    /// <summary>
    /// Maximum number of Arrow batches that can be in flight.
    /// </summary>
    public int MaxInflightBatches { get; }

    /// <summary>
    /// Whether automatic recovery is enabled. Default: true.
    /// </summary>
    public bool Recovery { get; }

    /// <summary>
    /// Recovery timeout in milliseconds.
    /// </summary>
    public int RecoveryTimeoutMs { get; }

    /// <summary>
    /// Recovery backoff delay in milliseconds.
    /// </summary>
    public int RecoveryBackoffMs { get; }

    /// <summary>
    /// Maximum number of recovery attempts.
    /// </summary>
    public int RecoveryRetries { get; }

    /// <summary>
    /// Server lack-of-ack timeout in milliseconds.
    /// </summary>
    public int ServerLackOfAckTimeoutMs { get; }

    /// <summary>
    /// Flush timeout in milliseconds.
    /// </summary>
    public int FlushTimeoutMs { get; }

    /// <summary>
    /// Connection timeout in milliseconds.
    /// </summary>
    public int ConnectionTimeoutMs { get; }

    /// <summary>
    /// IPC compression type. Default: None.
    /// </summary>
    public IPCCompressionType IpcCompression { get; }

    /// <summary>
    /// Maximum wait time when the stream is paused, in milliseconds.
    /// A negative value means "wait the full server-specified duration."
    /// </summary>
    public long StreamPausedMaxWaitTimeMs { get; }

    private ArrowStreamConfigurationOptions(Builder builder)
    {
        MaxInflightBatches = builder.MaxInflightBatches;
        Recovery = builder.Recovery;
        RecoveryTimeoutMs = builder.RecoveryTimeoutMs;
        RecoveryBackoffMs = builder.RecoveryBackoffMs;
        RecoveryRetries = builder.RecoveryRetries;
        ServerLackOfAckTimeoutMs = builder.ServerLackOfAckTimeoutMs;
        FlushTimeoutMs = builder.FlushTimeoutMs;
        ConnectionTimeoutMs = builder.ConnectionTimeoutMs;
        IpcCompression = builder.IpcCompression;
        StreamPausedMaxWaitTimeMs = builder.StreamPausedMaxWaitTimeMs;
    }

    /// <summary>
    /// Returns the default Arrow stream configuration options.
    /// </summary>
    public static ArrowStreamConfigurationOptions Default => new(new Builder());

    /// <summary>
    /// Creates a new builder for ArrowStreamConfigurationOptions.
    /// </summary>
    public static Builder NewBuilder() => new();

    /// <summary>
    /// Builder for creating <see cref="ArrowStreamConfigurationOptions"/> instances.
    /// </summary>
    public sealed class Builder
    {
        internal int MaxInflightBatches { get; set; } = DefaultMaxInflightBatches;
        internal bool Recovery { get; set; } = StreamConfigurationOptions.DefaultRecovery;
        internal int RecoveryTimeoutMs { get; set; } = StreamConfigurationOptions.DefaultRecoveryTimeoutMs;
        internal int RecoveryBackoffMs { get; set; } = StreamConfigurationOptions.DefaultRecoveryBackoffMs;
        internal int RecoveryRetries { get; set; } = StreamConfigurationOptions.DefaultRecoveryRetries;
        internal int ServerLackOfAckTimeoutMs { get; set; } = StreamConfigurationOptions.DefaultServerLackOfAckTimeoutMs;
        internal int FlushTimeoutMs { get; set; } = StreamConfigurationOptions.DefaultFlushTimeoutMs;
        internal int ConnectionTimeoutMs { get; set; } = DefaultConnectionTimeoutMs;
        internal IPCCompressionType IpcCompression { get; set; } = IPCCompressionType.None;
        internal long StreamPausedMaxWaitTimeMs { get; set; } = DefaultStreamPausedMaxWaitTimeMs;

        internal Builder() { }

        /// <summary>
        /// Sets the maximum number of in-flight Arrow batches. Must be positive.
        /// </summary>
        public Builder SetMaxInflightBatches(int maxInflightBatches)
        {
            if (maxInflightBatches <= 0)
                throw new ArgumentException("maxInflightBatches must be positive", nameof(maxInflightBatches));
            MaxInflightBatches = maxInflightBatches;
            return this;
        }

        /// <summary>
        /// Enables or disables automatic recovery.
        /// </summary>
        public Builder SetRecovery(bool recovery) { Recovery = recovery; return this; }

        /// <summary>
        /// Sets the recovery timeout in milliseconds. Must be non-negative.
        /// </summary>
        public Builder SetRecoveryTimeoutMs(int ms)
        {
            if (ms < 0) throw new ArgumentException("Must be non-negative", nameof(ms));
            RecoveryTimeoutMs = ms; return this;
        }

        /// <summary>
        /// Sets the recovery backoff in milliseconds. Must be non-negative.
        /// </summary>
        public Builder SetRecoveryBackoffMs(int ms)
        {
            if (ms < 0) throw new ArgumentException("Must be non-negative", nameof(ms));
            RecoveryBackoffMs = ms; return this;
        }

        /// <summary>
        /// Sets the maximum number of recovery retries. Must be non-negative.
        /// </summary>
        public Builder SetRecoveryRetries(int retries)
        {
            if (retries < 0) throw new ArgumentException("Must be non-negative", nameof(retries));
            RecoveryRetries = retries; return this;
        }

        /// <summary>
        /// Sets the server lack-of-ack timeout in milliseconds. Must be positive.
        /// </summary>
        public Builder SetServerLackOfAckTimeoutMs(int ms)
        {
            if (ms <= 0) throw new ArgumentException("Must be positive", nameof(ms));
            ServerLackOfAckTimeoutMs = ms; return this;
        }

        /// <summary>
        /// Sets the flush timeout in milliseconds. Must be positive.
        /// </summary>
        public Builder SetFlushTimeoutMs(int ms)
        {
            if (ms <= 0) throw new ArgumentException("Must be positive", nameof(ms));
            FlushTimeoutMs = ms; return this;
        }

        /// <summary>
        /// Sets the connection timeout in milliseconds. Must be positive.
        /// </summary>
        public Builder SetConnectionTimeoutMs(int ms)
        {
            if (ms <= 0) throw new ArgumentException("Must be positive", nameof(ms));
            ConnectionTimeoutMs = ms; return this;
        }

        /// <summary>
        /// Sets the IPC compression type.
        /// </summary>
        public Builder SetIpcCompression(IPCCompressionType compression)
        {
            IpcCompression = compression; return this;
        }

        /// <summary>
        /// Sets the maximum wait time when the stream is paused.
        /// A negative value means "wait the full server-specified duration."
        /// </summary>
        public Builder SetStreamPausedMaxWaitTimeMs(long ms)
        {
            StreamPausedMaxWaitTimeMs = ms; return this;
        }

        /// <summary>
        /// Builds the <see cref="ArrowStreamConfigurationOptions"/> instance.
        /// </summary>
        public ArrowStreamConfigurationOptions Build() => new(this);
    }
}
