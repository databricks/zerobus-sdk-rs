namespace Databricks.Zerobus;

/// <summary>
/// Common lifecycle and acknowledgment APIs shared by type-safe stream wrappers.
/// </summary>
public abstract class TypedZerobusStream : IDisposable
{
    private readonly ZerobusStream _innerStream;

    internal TypedZerobusStream(ZerobusStream innerStream)
    {
        _innerStream = innerStream ?? throw new ArgumentNullException(nameof(innerStream));
    }

    internal ZerobusStream InnerStream => _innerStream;

    /// <inheritdoc cref="ZerobusStream.IsClosed"/>
    public bool IsClosed()
    {
        return _innerStream.IsClosed();
    }

    /// <inheritdoc cref="ZerobusStream.WaitForOffset"/>
    public void WaitForOffset(long offset)
    {
        _innerStream.WaitForOffset(offset);
    }

    /// <inheritdoc cref="ZerobusStream.WaitForOffsetAsync"/>
    public Task WaitForOffsetAsync(long offset)
    {
        return _innerStream.WaitForOffsetAsync(offset);
    }

    /// <inheritdoc cref="ZerobusStream.Flush"/>
    public void Flush()
    {
        _innerStream.Flush();
    }

    /// <inheritdoc cref="ZerobusStream.FlushAsync"/>
    public Task FlushAsync()
    {
        return _innerStream.FlushAsync();
    }

    /// <inheritdoc cref="ZerobusStream.GetUnackedRecords"/>
    public ReadOnlyMemory<byte>[] GetUnackedRecords()
    {
        return _innerStream.GetUnackedRecords();
    }

    /// <inheritdoc cref="ZerobusStream.GetUnackedRecordsAsync"/>
    public Task<ReadOnlyMemory<byte>[]> GetUnackedRecordsAsync()
    {
        return _innerStream.GetUnackedRecordsAsync();
    }

    /// <inheritdoc cref="ZerobusStream.Close"/>
    public void Close()
    {
        _innerStream.Close();
    }

    /// <inheritdoc cref="ZerobusStream.CloseAsync"/>
    public Task CloseAsync()
    {
        return _innerStream.CloseAsync();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _innerStream.Dispose();
    }
}