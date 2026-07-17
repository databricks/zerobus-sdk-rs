namespace Databricks.Zerobus;

/// <summary>
/// Represents an encoded Arrow Flight batch with its IPC bytes and individual record lengths.
/// Returned by <see cref="ZerobusArrowStream.GetUnackedBatches"/>.
/// </summary>
public sealed class EncodedBatch
{
    /// <summary>
    /// The raw IPC-encoded batch bytes.
    /// </summary>
    public byte[] Data { get; }

    /// <summary>
    /// The lengths of individual records within the batch.
    /// </summary>
    public int[] Lengths { get; }

    /// <summary>
    /// Creates a new EncodedBatch.
    /// </summary>
    public EncodedBatch(byte[] data, int[] lengths)
    {
        Data = data ?? throw new ArgumentNullException(nameof(data));
        Lengths = lengths ?? throw new ArgumentNullException(nameof(lengths));
    }
}
