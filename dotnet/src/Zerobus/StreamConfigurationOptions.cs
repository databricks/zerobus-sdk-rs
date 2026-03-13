namespace Databricks.Zerobus;

/// <summary>
/// Configuration options for creating a Zerobus stream.
/// Start with <see cref="Default"/> and override only the fields you need.
/// </summary>
/// <example>
/// <code>
/// var options = StreamConfigurationOptions.Default with
/// {
///     MaxInflightRequests = 50_000,
///     RecordType = RecordType.Json,
/// };
/// </code>
/// </example>
public sealed record StreamConfigurationOptions
{
    /// <summary>
    /// Maximum number of requests that can be in-flight (pending acknowledgment) at once.
    /// Default: 1,000,000.
    /// </summary>
    public ulong MaxInflightRequests { get; init; } = 1_000_000;

    /// <summary>
    /// Enable automatic stream recovery on retryable failures.
    /// Default: true.
    /// </summary>
    public bool Recovery { get; init; } = true;

    /// <summary>
    /// Timeout for each recovery attempt in milliseconds.
    /// Default: 15,000 (15 seconds).
    /// </summary>
    public ulong RecoveryTimeoutMs { get; init; } = 15_000;

    /// <summary>
    /// Backoff delay between recovery attempts in milliseconds.
    /// Default: 2,000 (2 seconds).
    /// </summary>
    public ulong RecoveryBackoffMs { get; init; } = 2_000;

    /// <summary>
    /// Maximum number of recovery retry attempts.
    /// Default: 4.
    /// </summary>
    public uint RecoveryRetries { get; init; } = 4;

    /// <summary>
    /// Server acknowledgment timeout in milliseconds.
    /// Default: 60,000 (60 seconds).
    /// </summary>
    public ulong ServerLackOfAckTimeoutMs { get; init; } = 60_000;

    /// <summary>
    /// Flush operation timeout in milliseconds.
    /// Default: 300,000 (5 minutes).
    /// </summary>
    public ulong FlushTimeoutMs { get; init; } = 300_000;

    /// <summary>
    /// Type of record to ingest (Proto, Json, or Unspecified).
    /// Default: <see cref="Zerobus.RecordType.Proto"/>.
    /// </summary>
    public RecordType RecordType { get; init; } = RecordType.Proto;

    /// <summary>
    /// Maximum time in milliseconds to wait during graceful stream close
    /// when the server sends a CloseStreamSignal.
    /// <list type="bullet">
    ///   <item><c>null</c> — Wait for the full server-specified duration (most graceful, default).</item>
    ///   <item><c>0</c> — Immediate recovery, close stream right away.</item>
    ///   <item>A positive value — Wait up to <c>min(value, serverDuration)</c> milliseconds.</item>
    /// </list>
    /// Default: null.
    /// </summary>
    public ulong? StreamPausedMaxWaitTimeMs { get; init; }

    /// <summary>
    /// Returns the default configuration options.
    /// This is the idiomatic starting point — use C# record <c>with</c> expressions to override.
    /// </summary>
    public static StreamConfigurationOptions Default => new();
}
