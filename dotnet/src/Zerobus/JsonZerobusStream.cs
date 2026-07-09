namespace Databricks.Zerobus;

/// <summary>
/// A stream that accepts JSON payloads only.
/// </summary>
public sealed class JsonZerobusStream : TypedZerobusStream
{
    internal JsonZerobusStream(ZerobusStream innerStream)
        : base(innerStream)
    {
    }

    /// <inheritdoc cref="ZerobusStream.IngestRecord(string)"/>
    public long IngestRecord(string payload)
    {
        return InnerStream.IngestRecord(payload);
    }

    /// <inheritdoc cref="ZerobusStream.IngestRecords(string[])"/>
    public long IngestRecords(string[] records)
    {
        return InnerStream.IngestRecords(records);
    }
}