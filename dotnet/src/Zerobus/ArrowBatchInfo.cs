namespace Databricks.Zerobus;

/// <summary>
/// Represents an unacknowledged Arrow IPC-encoded batch retrieved from a closed or failed stream.
/// </summary>
/// <param name="Data">The raw IPC-encoded batch bytes.</param>
public sealed record ArrowBatchInfo(byte[] Data);
