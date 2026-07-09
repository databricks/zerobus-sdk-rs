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

    /// <inheritdoc cref="ZerobusStream.IngestRecordAsync(string)"/>
    public Task<long> IngestRecordAsync(string payload)
    {
        return InnerStream.IngestRecordAsync(payload);
    }

    /// <inheritdoc cref="ZerobusStream.IngestRecords(string[])"/>
    public long IngestRecords(string[] records)
    {
        return InnerStream.IngestRecords(records);
    }

    /// <inheritdoc cref="ZerobusStream.IngestRecordsAsync(string[])"/>
    public Task<long> IngestRecordsAsync(string[] records)
    {
        return InnerStream.IngestRecordsAsync(records);
    }
}