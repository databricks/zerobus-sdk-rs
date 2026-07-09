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
}