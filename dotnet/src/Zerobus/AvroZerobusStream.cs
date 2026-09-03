#if ZEROBUS_AVRO

namespace Databricks.Zerobus;

/// <summary>
/// A stream that accepts Avro binary payloads only (Beta).
/// </summary>
public sealed class AvroZerobusStream : TypedZerobusStream
{
    internal AvroZerobusStream(ZerobusStream innerStream)
        : base(innerStream)
    {
    }

    /// <inheritdoc cref="ZerobusStream.IngestRecord(byte[])"/>
    public long IngestRecord(byte[] payload)
    {
        return InnerStream.IngestRecord(payload);
    }

    /// <inheritdoc cref="ZerobusStream.IngestRecordAsync(byte[])"/>
    public Task<long> IngestRecordAsync(byte[] payload)
    {
        return InnerStream.IngestRecordAsync(payload);
    }

    /// <inheritdoc cref="ZerobusStream.IngestRecord(ReadOnlySpan{byte})"/>
    public long IngestRecord(ReadOnlySpan<byte> payload)
    {
        return InnerStream.IngestRecord(payload);
    }

    /// <inheritdoc cref="ZerobusStream.IngestRecords(byte[][])"/>
    public long IngestRecords(byte[][] records)
    {
        return InnerStream.IngestRecords(records);
    }

    /// <inheritdoc cref="ZerobusStream.IngestRecordsAsync(byte[][])"/>
    public Task<long> IngestRecordsAsync(byte[][] records)
    {
        return InnerStream.IngestRecordsAsync(records);
    }

    /// <summary>
    /// Retrieves all records that have not yet been acknowledged by the server.
    /// <para>
    /// <strong>Important:</strong> This should only be called after the stream has
    /// closed or failed. Calling it on an active stream will return an error.
    /// </para>
    /// </summary>
    /// <returns>
    /// An array of raw Avro record payloads as <see cref="ReadOnlyMemory{T}"/> of <see cref="byte"/>.
    /// </returns>
    /// <exception cref="ZerobusException">Thrown if retrieval fails.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    public ReadOnlyMemory<byte>[] GetUnackedRecords()
    {
        return InnerStream.GetUnackedRecords();
    }

    /// <summary>
    /// Asynchronously retrieves all records that have not yet been acknowledged by the server.
    /// <para>
    /// <strong>Important:</strong> This should only be called after the stream has
    /// closed or failed. Calling it on an active stream will return an error.
    /// </para>
    /// </summary>
    /// <returns>
    /// A task that resolves to an array of raw Avro record payloads as <see cref="ReadOnlyMemory{T}"/> of <see cref="byte"/>.
    /// </returns>
    /// <exception cref="ZerobusException">Thrown if retrieval fails.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    public Task<ReadOnlyMemory<byte>[]> GetUnackedRecordsAsync()
    {
        return InnerStream.GetUnackedRecordsAsync();
    }
}

#endif
