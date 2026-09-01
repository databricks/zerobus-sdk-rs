using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using Databricks.Zerobus.Native;

namespace Databricks.Zerobus;

/// <summary>
/// Represents an active JSON or Protocol Buffer stream for ingesting records.
/// Records can be ingested concurrently and will be acknowledged asynchronously.
/// </summary>
/// <remarks>
/// <para>
/// The stream is thread-safe — you may call <see cref="IngestRecord(string)"/> from
/// multiple threads concurrently, just like the Go SDK supports goroutines.
/// </para>
/// <para>
/// Ingestion is asynchronous: <see cref="IngestRecord(string)"/> returns as soon as the
/// record is queued; the SDK tracks its acknowledgment in the background.
/// </para>
/// <para>
/// <see cref="Close()"/> performs a graceful close (flush + close) but keeps the
/// native stream alive so callers can inspect <see cref="GetUnackedRecords"/> or
/// recover with <c>ZerobusSdk.RecreateStream(...)</c>. Call <see cref="Dispose()"/>
/// when you are completely finished with the stream to free native resources.
/// </para>
/// <para>
/// <c>ZerobusSdk.RecreateStream(stream)</c> consumes <c>stream</c> and returns a
/// replacement stream wrapper. The original wrapper is disposed during recreation.
/// <see cref="DisposeAsync()"/> performs the same graceful close as <see cref="Dispose()"/>
/// without blocking the calling thread, enabling <c>await using</c> for streams.
/// </para>
/// </remarks>
public sealed class ZerobusStream : IDisposable, IAsyncDisposable
{
    private IntPtr _ptr;
    private int _disposed;
    private readonly ReaderWriterLockSlim _lifetimeLock = new();
    private int _inflightAsyncOperations;
    private readonly ManualResetEventSlim _asyncOperationsDrained = new(initialState: true);

    internal ZerobusStream(IntPtr ptr)
    {
        _ptr = ptr;
    }

    /// <summary>
    /// Returns whether the stream has been closed
    /// </summary>
    /// <exception cref="ObjectDisposedException"></exception>
    public bool IsClosed()
    {
        return WithReadLock(NativeMethods.StreamIsClosed);
    }

    // ── Single-record ingestion ──────────────────────────────────────────

    /// <summary>
    /// Ingests a single record and returns the offset.
    /// This is the primary API for record ingestion.
    /// </summary>
    /// <remarks>
    /// Returns before server acknowledgment; use <see cref="Flush()"/> or
    /// <see cref="WaitForOffset(long)"/> when acknowledgment is required.
    /// </remarks>
    /// <param name="payload">
    /// The record payload. Pass a <see cref="string"/> for JSON records
    /// or a <c>byte[]</c> / <see cref="ReadOnlySpan{T}"/> of <see cref="byte"/>
    /// for Protocol Buffer records.
    /// </param>
    /// <returns>The logical offset assigned to this record.</returns>
    /// <exception cref="ZerobusException">Thrown if ingestion fails.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="payload"/> is null.</exception>
    /// <example>
    /// <code>
    /// // JSON stream
    /// for (int id = 1; id &lt;= 100; id++)
    /// {
    ///     jsonStream.IngestRecord($"{{\"id\": {id}, \"message\": \"Hello\"}}");
    /// }
    /// jsonStream.Flush();
    ///
    /// // Protobuf stream
    /// foreach (var message in messages)
    /// {
    ///     protoStream.IngestRecord(SerializeMyProto(message));
    /// }
    /// protoStream.Flush();
    /// </code>
    /// </example>
    public long IngestRecord(string payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        return WithReadLock(ptr => NativeInterop.StreamIngestJsonRecord(ptr, payload));
    }

    /// <inheritdoc cref="IngestRecord(string)"/>
    public Task<long> IngestRecordAsync(string payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        return WithReadLockAsync(ptr => NativeInterop.StreamIngestJsonRecordAsync(ptr, payload));
    }

    /// <inheritdoc cref="IngestRecord(string)"/>
    public long IngestRecord(byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        return WithReadLock(ptr => NativeInterop.StreamIngestProtoRecord(ptr, payload));
    }

    /// <inheritdoc cref="IngestRecord(byte[])"/>
    public Task<long> IngestRecordAsync(byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        return WithReadLockAsync(ptr => NativeInterop.StreamIngestProtoRecordAsync(ptr, payload));
    }

    /// <inheritdoc cref="IngestRecord(string)"/>
    public long IngestRecord(ReadOnlySpan<byte> payload)
    {
        using var handle = WithReadLock();
        return NativeInterop.StreamIngestProtoRecord(GetNativePointerForCall(), payload);
    }

    // ── Batch ingestion ──────────────────────────────────────────────────

    /// <summary>
    /// Ingests a batch of JSON records and returns its assigned offset.
    /// </summary>
    /// <remarks>
    /// The batch succeeds or fails as a unit. All records must be JSON strings. Prefer
    /// this API in hot paths to amortize the per-call interop overhead.
    /// </remarks>
    /// <param name="records">The JSON record strings to ingest.</param>
    /// <returns>The offset representing the entire batch, or -1 if the batch is empty.</returns>
    /// <exception cref="ZerobusException">Thrown if ingestion fails.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    /// <example>
    /// <code>
    /// string[] records =
    /// [
    ///     "{\"device\": \"sensor-001\", \"temp\": 20}",
    ///     "{\"device\": \"sensor-002\", \"temp\": 21}",
    /// ];
    /// stream.IngestRecords(records);
    /// stream.Flush();
    /// </code>
    /// </example>
    public long IngestRecords(string[] records)
    {
        ArgumentNullException.ThrowIfNull(records);
        return WithReadLock(ptr => NativeInterop.StreamIngestJsonRecords(ptr, records));
    }

    /// <inheritdoc cref="IngestRecords(string[])"/>
    public Task<long> IngestRecordsAsync(string[] records)
    {
        ArgumentNullException.ThrowIfNull(records);
        return WithReadLockAsync(ptr => NativeInterop.StreamIngestJsonRecordsAsync(ptr, records));
    }

    /// <summary>
    /// Ingests a batch of protobuf records and returns its assigned offset.
    /// </summary>
    /// <remarks>
    /// The batch succeeds or fails as a unit. All records must be serialized protobuf
    /// byte arrays. Prefer this API in hot paths to amortize the per-call interop overhead.
    /// </remarks>
    /// <param name="records">The protobuf record byte spans to ingest.</param>
    /// <returns>The offset representing the entire batch, or -1 if the batch is empty.</returns>
    /// <exception cref="ZerobusException">Thrown if ingestion fails.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    public long IngestRecords(byte[][] records)
    {
        ArgumentNullException.ThrowIfNull(records);
        return WithReadLock(ptr => NativeInterop.StreamIngestProtoRecords(ptr, records));
    }

    /// <inheritdoc cref="IngestRecords(byte[][])"/>
    public Task<long> IngestRecordsAsync(byte[][] records)
    {
        ArgumentNullException.ThrowIfNull(records);
        return WithReadLockAsync(ptr => NativeInterop.StreamIngestProtoRecordsAsync(ptr, records));
    }

    // ── Acknowledgment / flush ───────────────────────────────────────────

    /// <summary>
    /// Waits for the server to acknowledge all records through the specified offset.
    /// </summary>
    /// <remarks>
    /// Acknowledgments are ordered, so waiting for the last offset also confirms every
    /// earlier record. Use this for targeted waits; for a bulk run, <see cref="Flush()"/>
    /// is simpler. Avoid waiting after every record, which limits throughput to one
    /// record per round-trip.
    /// </remarks>
    /// <param name="offset">
    /// The offset to wait for, as returned by <see cref="IngestRecord(string)"/> or
    /// <see cref="IngestRecords(string[])"/>.
    /// </param>
    /// <exception cref="ZerobusException">Thrown if the wait fails.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    public void WaitForOffset(long offset)
    {
        WithReadLock(ptr => NativeInterop.StreamWaitForOffset(ptr, offset));
    }

    /// <inheritdoc cref="WaitForOffset(long)"/>
    public Task WaitForOffsetAsync(long offset)
    {
        return WithReadLockAsync(ptr => NativeInterop.StreamWaitForOffsetAsync(ptr, offset));
    }

    /// <summary>
    /// Waits for all records queued before the call to be acknowledged.
    /// </summary>
    /// <remarks>
    /// Records queued while the flush is in progress are not included.
    /// </remarks>
    /// <exception cref="ZerobusException">
    /// Thrown if the flush times out or a record fails with a non-retryable error.
    /// </exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    /// <example>
    /// <code>
    /// for (int i = 0; i &lt; 1000; i++)
    /// {
    ///     stream.IngestRecord(records[i]);
    /// }
    /// stream.Flush();
    /// </code>
    /// </example>
    public void Flush()
    {
        WithReadLock(NativeInterop.StreamFlush);
    }

    /// <inheritdoc cref="Flush"/>
    public Task FlushAsync()
    {
        return WithReadLockAsync(NativeInterop.StreamFlushAsync);
    }

    // ── Unacknowledged records ───────────────────────────────────────────

    /// <summary>
    /// Retrieves all records that have not yet been acknowledged by the server.
    /// <para>
    /// <strong>Important:</strong> This should only be called after the stream has
    /// closed or failed. Calling it on an active stream will return an error.
    /// </para>
    /// </summary>
    /// <returns>
    /// An array of raw record payloads as <see cref="ReadOnlyMemory{T}"/> of <see cref="byte"/>.
    /// JSON payloads are UTF-8 encoded; callers can decode as needed.
    /// </returns>
    /// <exception cref="ZerobusException">Thrown if retrieval fails.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    /// <example>
    /// <code>
    /// try
    /// {
    ///     stream.Flush();
    /// }
    /// catch (ZerobusException)
    /// {
    ///     // A flush timeout can leave the stream active. GetUnackedRecords
    ///     // requires a closed or failed stream.
    ///     try
    ///     {
    ///         var unacked = stream.GetUnackedRecords();
    ///         Console.WriteLine($"Failed to acknowledge {unacked.Length} records");
    ///     }
    ///     catch (ZerobusException retrieval)
    ///     {
    ///         Console.WriteLine($"Could not inspect unacked records: {retrieval.Message}");
    ///     }
    /// }
    /// </code>
    /// </example>
    public ReadOnlyMemory<byte>[] GetUnackedRecords()
    {
        return WithReadLock(NativeInterop.StreamGetUnackedRecords);
    }

    /// <inheritdoc cref="GetUnackedRecords"/>
    public Task<ReadOnlyMemory<byte>[]> GetUnackedRecordsAsync()
    {
        return WithReadLockAsync(NativeInterop.StreamGetUnackedRecordsAsync);
    }

    // ── Close / Dispose ──────────────────────────────────────────────────

    /// <summary>
    /// Gracefully closes the stream after flushing all pending records.
    /// After close, ingestion APIs are no longer usable, but the stream remains
    /// readable for recovery paths (for example <see cref="GetUnackedRecords"/>).
    /// </summary>
    /// <remarks>
    /// This method does not free native resources. Call <see cref="Dispose()"/>
    /// after you have finished any recovery operations.
    /// </remarks>
    /// <exception cref="ZerobusException">
    /// Thrown if flush or close fails.
    /// </exception>
    public void Close()
    {
        using var handle = WithWriteLock();
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        var ptr = GetNativePointerForCall();
        if (NativeMethods.StreamIsClosed(ptr)) return;

        NativeInterop.StreamClose(ptr);
    }

    /// <inheritdoc cref="Close"/>
    public Task CloseAsync()
    {
        return WithWriteLockAsync(
            ptr =>
            {
                if (NativeMethods.StreamIsClosed(ptr))
                    return Task.CompletedTask;
                return NativeInterop.StreamCloseAsync(ptr);
            });
    }

    /// <inheritdoc />
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0)
            return;

        IntPtr ptr = IntPtr.Zero;
        var shouldClose = false;
        Exception? closeError = null;

        using (WithWriteLock())
        {
            ptr = Interlocked.Exchange(ref _ptr, IntPtr.Zero);
            if (ptr == IntPtr.Zero)
            {
                return;
            }

            shouldClose = !NativeMethods.StreamIsClosed(ptr);
        }

        if (shouldClose)
        {
            try
            {
                await NativeInterop.StreamCloseAsync(ptr).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                closeError = ex;
            }
        }

        NativeMethods.StreamFree(ptr);
        GC.SuppressFinalize(this);

        if (closeError is not null)
            ExceptionDispatchInfo.Capture(closeError).Throw();
    }

    private void Dispose(bool disposing)
    {
        if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0) return;

        using var handle = WithWriteLock();
        var ptr = Interlocked.Exchange(ref _ptr, IntPtr.Zero);
        if (ptr == IntPtr.Zero)
        {
            return;
        }

        Exception? closeError = null;

        if (disposing && !NativeMethods.StreamIsClosed(ptr))
        {
            try
            {
                NativeInterop.StreamClose(ptr);
            }
            catch (Exception ex)
            {
                closeError = ex;
            }
        }

        NativeMethods.StreamFree(ptr);

        if (closeError is not null)
            ExceptionDispatchInfo.Capture(closeError).Throw();
    }

    /// <summary>
    /// Safety-net release of native memory for leaked instances.
    /// Only frees memory — does NOT call <see cref="NativeInterop.StreamClose"/>,
    /// which performs blocking gRPC I/O and may trigger managed callbacks on
    /// Rust-created threads that corrupt the .NET execution context
    /// (infinite recursion in <c>CultureInfo.CurrentUICulture</c>).
    /// </summary>
    ~ZerobusStream()
    {
        Dispose(false);
    }

    /// <summary>
    /// Returns the raw native pointer to the underlying C stream.
    /// Internal use only.
    /// </summary>
    internal IntPtr NativePointer
    {
        get
        {
            return WithReadLock(ptr => ptr);
        }
    }

    /// <summary>
    /// Transfers this stream's native pointer to a new wrapper around
    /// <paramref name="newPtr"/>, disposing this one. The provider (when one is
    /// used) is owned by the FFI, not the stream, so only the pointer moves.
    /// Internal use only.
    /// </summary>
    internal ZerobusStream Recreate(IntPtr newPtr)
    {
        var disposed = Interlocked.CompareExchange(ref _disposed, 1, 0);
        ObjectDisposedException.ThrowIf(disposed != 0, this);

        using var handle = WithWriteLock();

        var oldPtr = Interlocked.Exchange(ref _ptr, IntPtr.Zero);
        var newStream = new ZerobusStream(newPtr);

        // Free the old native stream (don't Close — it's already failed/closed)
        if (oldPtr != IntPtr.Zero)
            NativeMethods.StreamFree(oldPtr);

        return newStream;
    }

    private IntPtr GetNativePointerForCall()
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        var ptr = _ptr;
        ObjectDisposedException.ThrowIf(ptr == IntPtr.Zero, this);
        return ptr;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private T WithReadLock<T>(Func<IntPtr, T> call)
    {
        using var handle = WithReadLock();
        return call(GetNativePointerForCall());
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void WithReadLock(Action<IntPtr> call)
    {
        using var handle = WithReadLock();
        call(GetNativePointerForCall());
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private Task<T> WithReadLockAsync<T>(Func<IntPtr, Task<T>> call)
    {
        return WithAsyncLock(writeLock: false, call);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private Task WithReadLockAsync(Func<IntPtr, Task> call)
    {
        return WithAsyncLock(writeLock: false, call);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private Task WithWriteLockAsync(Func<IntPtr, Task> call)
    {
        return WithAsyncLock(writeLock: true, call);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private IDisposable WithReadLock()
    {
        _lifetimeLock.EnterReadLock();
        return new Disposable(() => _lifetimeLock.ExitReadLock());
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private IDisposable WithWriteLock()
    {
        _lifetimeLock.EnterWriteLock();
        _asyncOperationsDrained.Wait();
        return new Disposable(() => _lifetimeLock.ExitWriteLock());
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private Task<T> WithAsyncLock<T>(bool writeLock, Func<IntPtr, Task<T>> call)
    {
        var ptr = BeginAsyncOperation(writeLock);
        try
        {
            var task = call(ptr);
            return CompleteAsyncOperation(task);
        }
        catch
        {
            EndAsyncOperation();
            throw;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private Task WithAsyncLock(bool writeLock, Func<IntPtr, Task> call)
    {
        var ptr = BeginAsyncOperation(writeLock);
        try
        {
            var task = call(ptr);
            return CompleteAsyncOperation(task);
        }
        catch
        {
            EndAsyncOperation();
            throw;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private IntPtr BeginAsyncOperation(bool writeLock)
    {
        if (writeLock)
            _lifetimeLock.EnterWriteLock();
        else
            _lifetimeLock.EnterReadLock();

        try
        {
            var ptr = GetNativePointerForCall();
            if (Interlocked.Increment(ref _inflightAsyncOperations) == 1)
                _asyncOperationsDrained.Reset();
            return ptr;
        }
        finally
        {
            if (writeLock)
                _lifetimeLock.ExitWriteLock();
            else
                _lifetimeLock.ExitReadLock();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EndAsyncOperation()
    {
        if (Interlocked.Decrement(ref _inflightAsyncOperations) == 0)
            _asyncOperationsDrained.Set();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private async Task<T> CompleteAsyncOperation<T>(Task<T> task)
    {
        try
        {
            return await task.ConfigureAwait(false);
        }
        finally
        {
            EndAsyncOperation();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private async Task CompleteAsyncOperation(Task task)
    {
        try
        {
            await task.ConfigureAwait(false);
        }
        finally
        {
            EndAsyncOperation();
        }
    }

    private class Disposable(Action action) : IDisposable
    {
        public void Dispose() => action();
    }

}
