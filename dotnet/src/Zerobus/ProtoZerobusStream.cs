namespace Databricks.Zerobus;

/// <summary>
/// A stream that accepts protobuf payloads only.
/// </summary>
public sealed class ProtoZerobusStream : TypedZerobusStream
{
    internal ProtoZerobusStream(ZerobusStream innerStream)
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
}