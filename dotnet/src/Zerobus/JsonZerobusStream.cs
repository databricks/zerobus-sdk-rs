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

    /// <summary>
    /// Retrieves all records that have not yet been acknowledged by the server.
    /// <para>
    /// <strong>Important:</strong> This should only be called after the stream has
    /// closed or failed. Calling it on an active stream will return an error.
    /// </para>
    /// </summary>
    /// <returns>
    /// An array of JSON record payloads as strings, decoded from UTF-8.
    /// </returns>
    /// <exception cref="ZerobusException">Thrown if retrieval fails.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    public string[] GetUnackedRecords()
    {
        var records = InnerStream.GetUnackedRecords();
        return Array.ConvertAll(records, memory => System.Text.Encoding.UTF8.GetString(memory.Span));
    }

    /// <summary>
    /// Asynchronously retrieves all records that have not yet been acknowledged by the server.
    /// <para>
    /// <strong>Important:</strong> This should only be called after the stream has
    /// closed or failed. Calling it on an active stream will return an error.
    /// </para>
    /// </summary>
    /// <returns>
    /// A task that resolves to an array of JSON record payloads as strings, decoded from UTF-8.
    /// </returns>
    /// <exception cref="ZerobusException">Thrown if retrieval fails.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    public async Task<string[]> GetUnackedRecordsAsync()
    {
        var records = await InnerStream.GetUnackedRecordsAsync();
        return Array.ConvertAll(records, memory => System.Text.Encoding.UTF8.GetString(memory.Span));
    }
}