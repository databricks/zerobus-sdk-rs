namespace Databricks.Zerobus;

/// <summary>
/// Compression type for Apache Arrow Flight IPC messages.
/// Matches the C FFI values: -1 = None, 0 = LZ4_FRAME, 1 = ZSTD.
/// </summary>
public enum IPCCompressionType
{
    /// <summary>No compression.</summary>
    None = -1,

    /// <summary>LZ4 Frame compression.</summary>
    Lz4Frame = 0,

    /// <summary>Zstandard compression.</summary>
    Zstd = 1,
}
