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
///     RecoveryRetries = 8,
/// };
/// </code>
/// </example>
public sealed record StreamConfigurationOptions
{
    /// <summary>
    /// Maximum number of requests that can be in-flight (pending acknowledgment) at once.
    /// <c>null</c> uses the SDK default.
    /// Default: 1,000,000.
    /// </summary>
    public ulong? MaxInflightRequests { get; init; }

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
    /// Type of record to ingest (Proto, Json, or Unspecified).
    /// Typed factories such as <see cref="ZerobusSdk.CreateJsonStream(string, string, string, StreamConfigurationOptions?)"/>
    /// and <see cref="ZerobusSdk.CreateProtoStream(string, byte[], string, string, StreamConfigurationOptions?)"/>
    /// set this automatically.
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
    public static StreamConfigurationOptions Default => new()
    {
        MaxInflightRequests = 1_000_000,
        RecoveryTimeoutMs = 15_000,
        RecoveryBackoffMs = 2_000,
        RecoveryRetries = 4,
        ServerLackOfAckTimeoutMs = 60_000,
        FlushTimeoutMs = 300_000,
    };
}
