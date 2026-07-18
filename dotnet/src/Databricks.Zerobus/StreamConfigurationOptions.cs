namespace Databricks.Zerobus;

/// <summary>
/// Configuration options for Zerobus gRPC streams (JSON and Protocol Buffers).
/// Controls performance tuning, error handling, and callback configuration.
/// </summary>
public sealed class StreamConfigurationOptions
{
    /// <summary>
    /// Default: 1,000,000 records.
    /// </summary>
    public const int DefaultMaxInflightRecords = 1_000_000;

    /// <summary>
    /// Default: true (enabled).
    /// </summary>
    public const bool DefaultRecovery = true;

    /// <summary>
    /// Default: 15,000 ms.
    /// </summary>
    public const int DefaultRecoveryTimeoutMs = 15_000;

    /// <summary>
    /// Default: 2,000 ms.
    /// </summary>
    public const int DefaultRecoveryBackoffMs = 2_000;

    /// <summary>
    /// Default: 4 retries.
    /// </summary>
    public const int DefaultRecoveryRetries = 4;

    /// <summary>
    /// Default: 300,000 ms (5 minutes).
    /// </summary>
    public const int DefaultFlushTimeoutMs = 300_000;

    /// <summary>
    /// Default: 60,000 ms (1 minute).
    /// </summary>
    public const int DefaultServerLackOfAckTimeoutMs = 60_000;

    /// <summary>
    /// Default: 5,000 ms (5 seconds).
    /// </summary>
    public const int DefaultCallbackMaxWaitTimeMs = 5_000;

    /// <summary>
    /// Maximum number of records that can be in flight.
    /// Higher values improve throughput but use more memory.
    /// </summary>
    public int MaxInflightRecords { get; }

    /// <summary>
    /// Whether automatic recovery is enabled.
    /// </summary>
    public bool Recovery { get; }

    /// <summary>
    /// Maximum time to wait for a recovery operation (milliseconds).
    /// </summary>
    public int RecoveryTimeoutMs { get; }

    /// <summary>
    /// Delay between consecutive recovery attempts (milliseconds).
    /// </summary>
    public int RecoveryBackoffMs { get; }

    /// <summary>
    /// Maximum number of recovery attempts before giving up.
    /// </summary>
    public int RecoveryRetries { get; }

    /// <summary>
    /// Maximum time to wait for a flush operation (milliseconds).
    /// </summary>
    public int FlushTimeoutMs { get; }

    /// <summary>
    /// Maximum time to wait for server acknowledgment (milliseconds).
    /// </summary>
    public int ServerLackOfAckTimeoutMs { get; }

    /// <summary>
    /// Maximum time to wait when the stream is paused (milliseconds).
    /// A negative value means "wait the full server-specified duration."
    /// Null means use the default.
    /// </summary>
    public long? StreamPausedMaxWaitTimeMs { get; }

    /// <summary>
    /// Maximum wait time for acknowledgment callbacks (milliseconds).
    /// Null means use the default.
    /// </summary>
    public long? CallbackMaxWaitTimeMs { get; }

    /// <summary>
    /// Callback invoked when records are durably acknowledged.
    /// </summary>
    public AckOnAckDelegate? OnAck { get; }

    /// <summary>
    /// Callback invoked when an error occurs for a specific offset.
    /// </summary>
    public AckOnErrorDelegate? OnError { get; }

    /// <summary>
    /// Opaque user data passed through to ack callbacks.
    /// </summary>
    public object? AckUserData { get; }

    private StreamConfigurationOptions(Builder builder)
    {
        MaxInflightRecords = builder.MaxInflightRecords;
        Recovery = builder.Recovery;
        RecoveryTimeoutMs = builder.RecoveryTimeoutMs;
        RecoveryBackoffMs = builder.RecoveryBackoffMs;
        RecoveryRetries = builder.RecoveryRetries;
        FlushTimeoutMs = builder.FlushTimeoutMs;
        ServerLackOfAckTimeoutMs = builder.ServerLackOfAckTimeoutMs;
        StreamPausedMaxWaitTimeMs = builder.StreamPausedMaxWaitTimeMs;
        CallbackMaxWaitTimeMs = builder.CallbackMaxWaitTimeMs;
        OnAck = builder.OnAck;
        OnError = builder.OnError;
        AckUserData = builder.AckUserData;
    }

    /// <summary>
    /// Returns the default stream configuration options.
    /// </summary>
    public static StreamConfigurationOptions Default => new(new Builder());

    /// <summary>
    /// Creates a new builder for StreamConfigurationOptions.
    /// </summary>
    public static Builder NewBuilder() => new();

    /// <summary>
    /// Builder for creating <see cref="StreamConfigurationOptions"/> instances.
    /// All parameters have sensible defaults if not specified.
    /// </summary>
    public sealed class Builder
    {
        internal int MaxInflightRecords { get; set; } = DefaultMaxInflightRecords;
        internal bool Recovery { get; set; } = DefaultRecovery;
        internal int RecoveryTimeoutMs { get; set; } = DefaultRecoveryTimeoutMs;
        internal int RecoveryBackoffMs { get; set; } = DefaultRecoveryBackoffMs;
        internal int RecoveryRetries { get; set; } = DefaultRecoveryRetries;
        internal int FlushTimeoutMs { get; set; } = DefaultFlushTimeoutMs;
        internal int ServerLackOfAckTimeoutMs { get; set; } = DefaultServerLackOfAckTimeoutMs;
        internal long? StreamPausedMaxWaitTimeMs { get; set; }
        internal long? CallbackMaxWaitTimeMs { get; set; } = DefaultCallbackMaxWaitTimeMs;
        internal AckOnAckDelegate? OnAck { get; set; }
        internal AckOnErrorDelegate? OnError { get; set; }
        internal object? AckUserData { get; set; }

        internal Builder() { }

        /// <summary>
        /// Sets the maximum number of in-flight records. Must be positive.
        /// </summary>
        public Builder SetMaxInflightRecords(int maxInflightRecords)
        {
            if (maxInflightRecords <= 0)
                throw new ArgumentException("maxInflightRecords must be positive", nameof(maxInflightRecords));
            MaxInflightRecords = maxInflightRecords;
            return this;
        }

        /// <summary>
        /// Enables or disables automatic recovery.
        /// </summary>
        public Builder SetRecovery(bool recovery)
        {
            Recovery = recovery;
            return this;
        }

        /// <summary>
        /// Sets the recovery timeout in milliseconds. Must be non-negative.
        /// </summary>
        public Builder SetRecoveryTimeoutMs(int recoveryTimeoutMs)
        {
            if (recoveryTimeoutMs < 0)
                throw new ArgumentException("recoveryTimeoutMs must be non-negative", nameof(recoveryTimeoutMs));
            RecoveryTimeoutMs = recoveryTimeoutMs;
            return this;
        }

        /// <summary>
        /// Sets the recovery backoff in milliseconds. Must be non-negative.
        /// </summary>
        public Builder SetRecoveryBackoffMs(int recoveryBackoffMs)
        {
            if (recoveryBackoffMs < 0)
                throw new ArgumentException("recoveryBackoffMs must be non-negative", nameof(recoveryBackoffMs));
            RecoveryBackoffMs = recoveryBackoffMs;
            return this;
        }

        /// <summary>
        /// Sets the maximum number of recovery retries. Must be non-negative.
        /// </summary>
        public Builder SetRecoveryRetries(int recoveryRetries)
        {
            if (recoveryRetries < 0)
                throw new ArgumentException("recoveryRetries must be non-negative", nameof(recoveryRetries));
            RecoveryRetries = recoveryRetries;
            return this;
        }

        /// <summary>
        /// Sets the flush timeout in milliseconds. Must be positive.
        /// </summary>
        public Builder SetFlushTimeoutMs(int flushTimeoutMs)
        {
            if (flushTimeoutMs <= 0)
                throw new ArgumentException("flushTimeoutMs must be positive", nameof(flushTimeoutMs));
            FlushTimeoutMs = flushTimeoutMs;
            return this;
        }

        /// <summary>
        /// Sets the server lack-of-ack timeout in milliseconds. Must be positive.
        /// </summary>
        public Builder SetServerLackOfAckTimeoutMs(int serverLackOfAckTimeoutMs)
        {
            if (serverLackOfAckTimeoutMs <= 0)
                throw new ArgumentException("serverLackOfAckTimeoutMs must be positive",
                    nameof(serverLackOfAckTimeoutMs));
            ServerLackOfAckTimeoutMs = serverLackOfAckTimeoutMs;
            return this;
        }

        /// <summary>
        /// Sets the maximum wait time when the stream is paused, in milliseconds.
        /// A negative value means "wait the full server-specified duration."
        /// </summary>
        public Builder SetStreamPausedMaxWaitTimeMs(long? streamPausedMaxWaitTimeMs)
        {
            StreamPausedMaxWaitTimeMs = streamPausedMaxWaitTimeMs;
            return this;
        }

        /// <summary>
        /// Sets the maximum wait time for acknowledgment callbacks.
        /// </summary>
        public Builder SetCallbackMaxWaitTimeMs(long? callbackMaxWaitTimeMs)
        {
            CallbackMaxWaitTimeMs = callbackMaxWaitTimeMs;
            return this;
        }

        /// <summary>
        /// Sets the acknowledgment callback. The onAck delegate is called
        /// when records are durably acknowledged; onError is called for errors.
        /// </summary>
        public Builder SetAckCallback(AckOnAckDelegate? onAck, AckOnErrorDelegate? onError, object? userData = null)
        {
            OnAck = onAck;
            OnError = onError;
            AckUserData = userData;
            return this;
        }

        /// <summary>
        /// Builds the <see cref="StreamConfigurationOptions"/> instance.
        /// </summary>
        public StreamConfigurationOptions Build() => new(this);
    }
}
