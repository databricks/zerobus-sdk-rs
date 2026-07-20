namespace Databricks.Zerobus;

/// <summary>
/// Compression type for Apache Arrow Flight IPC messages.
/// </summary>
public enum IPCCompressionType
{
    /// <summary>No compression (default).</summary>
    None = -1,

    /// <summary>LZ4 Frame compression.</summary>
    Lz4Frame = 0,

    /// <summary>Zstandard compression.</summary>
    Zstd = 1
}
