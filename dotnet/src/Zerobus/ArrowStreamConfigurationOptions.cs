namespace Databricks.Zerobus;

/// <summary>
/// Configuration options for Arrow Flight ingestion streams.
/// Start with <see cref="Default"/> and override only the fields you need.
/// </summary>
/// <example>
/// <code>
/// var options = ArrowStreamConfigurationOptions.Default with
/// {
///     MaxInflightBatches = 5_000,
///     IpcCompression = IPCCompressionType.Zstd,
/// };
/// </code>
/// </example>
public sealed record ArrowStreamConfigurationOptions
{
    /// <summary>
    /// Maximum number of Arrow batches that can be in-flight (pending acknowledgment) at once.
    /// <c>null</c> uses the SDK default.
    /// Default: 1,000.
    /// </summary>
    public uint? MaxInflightBatches { get; init; }

    /// <summary>
    /// Enable automatic stream recovery on retryable failures.
    /// Default: true.
    /// </summary>
    public bool Recovery { get; init; } = true;

    /// <summary>
    /// Timeout for each recovery attempt in milliseconds.
    /// <c>null</c> uses the SDK default.
    /// Default: 15,000 (15 seconds).
    /// </summary>
    public ulong? RecoveryTimeoutMs { get; init; }

    /// <summary>
    /// Backoff delay between recovery attempts in milliseconds.
    /// <c>null</c> uses the SDK default.
    /// Default: 2,000 (2 seconds).
    /// </summary>
    public ulong? RecoveryBackoffMs { get; init; }

    /// <summary>
    /// Maximum number of recovery retry attempts.
    /// <c>null</c> uses the SDK default.
    /// Default: 4.
    /// </summary>
    public uint? RecoveryRetries { get; init; }

    /// <summary>
    /// Server acknowledgment timeout in milliseconds.
    /// <c>null</c> uses the SDK default.
    /// Default: 60,000 (60 seconds).
    /// </summary>
    public ulong? ServerLackOfAckTimeoutMs { get; init; }

    /// <summary>
    /// Flush operation timeout in milliseconds.
    /// <c>null</c> uses the SDK default.
    /// Default: 300,000 (5 minutes).
    /// </summary>
    public ulong? FlushTimeoutMs { get; init; }

    /// <summary>
    /// Connection timeout in milliseconds.
    /// <c>null</c> uses the SDK default.
    /// Default: 30,000 (30 seconds).
    /// </summary>
    public ulong? ConnectionTimeoutMs { get; init; }

    /// <summary>
    /// IPC compression type for Arrow Flight messages.
    /// Default: <see cref="IPCCompressionType.None"/>.
    /// </summary>
    public IPCCompressionType IpcCompression { get; init; } = IPCCompressionType.None;

    /// <summary>
    /// Maximum time in milliseconds to wait during graceful stream close
    /// when the server sends a CloseStreamSignal.
    /// <list type="bullet">
    ///   <item><c>-1</c> — Wait for the full server-specified duration (most graceful, default).</item>
    ///   <item><c>0</c> — Immediate recovery, close stream right away.</item>
    ///   <item>A positive value — Wait up to <c>min(value, serverDuration)</c> milliseconds.</item>
    /// </list>
    /// Default: -1.
    /// </summary>
    public long StreamPausedMaxWaitTimeMs { get; init; } = -1;

    /// <summary>
    /// Returns the default Arrow stream configuration options.
    /// </summary>
    public static ArrowStreamConfigurationOptions Default => new()
    {
        MaxInflightBatches = 1_000,
        ConnectionTimeoutMs = 30_000,
    };
}
