using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Databricks.Zerobus.Native;

namespace Databricks.Zerobus;

/// <summary>
/// Represents an active Arrow Flight ingestion stream for ingesting
/// IPC-encoded RecordBatches into a Unity Catalog Delta table.
/// </summary>
/// <remarks>
/// <para>
/// The stream is thread-safe — you may call IngestBatch from
/// multiple threads concurrently.
/// </para>
/// <para>
/// <see cref="Close()"/> performs a graceful close (flush + close) but keeps the
/// native stream alive so callers can inspect <see cref="GetUnackedBatches"/>.
/// Call <see cref="Dispose()"/> when you are completely finished to free native resources.
/// </para>
/// <para>
/// <b>Beta:</b> The Arrow Flight ingestion API is in beta and may change in future releases.
/// </para>
/// </remarks>
public sealed class ZerobusArrowStream : IDisposable, IAsyncDisposable
{
    private IntPtr _ptr;
    private int _disposed;
    private readonly ReaderWriterLockSlim _lifetimeLock = new();

    internal ZerobusArrowStream(IntPtr ptr)
    {
        _ptr = ptr;
    }

    /// <summary>
    /// Returns whether the underlying native stream has been closed.
    /// </summary>
    public bool IsClosed()
    {
        return WithReadLock(NativeMethods.ArrowStreamIsClosed);
    }

    // ── Batch ingestion ───────────────────────────────────────────────────

    /// <summary>
    /// Ingests a single Arrow IPC-encoded RecordBatch and returns its offset.
    /// </summary>
    /// <param name="ipcBytes">The IPC-serialized Arrow RecordBatch bytes.</param>
    /// <returns>The offset of the ingested batch.</returns>
    /// <exception cref="ZerobusException">Thrown if ingestion fails.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    /// <example>
    /// <code>
    /// byte[] batch = GetArrowRecordBatchIpcBytes();
    /// long offset = stream.IngestBatch(batch);
    /// </code>
    /// </example>
    public long IngestBatch(byte[] ipcBytes)
    {
        ArgumentNullException.ThrowIfNull(ipcBytes);
        return WithReadLock(ptr => NativeInterop.ArrowStreamIngestBatch(ptr, ipcBytes));
    }

    /// <inheritdoc cref="IngestBatch(byte[])"/>
    public long IngestBatch(ReadOnlySpan<byte> ipcBytes)
    {
        using var handle = WithReadLock();
        return NativeInterop.ArrowStreamIngestBatch(GetNativePointerForCall(), ipcBytes);
    }

    // ── Acknowledgment / flush ───────────────────────────────────────────

    /// <summary>
    /// Blocks until the server acknowledges the batch at the specified offset.
    /// </summary>
    /// <param name="offset">The offset to wait for.</param>
    /// <exception cref="ZerobusException">Thrown if the wait fails.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    public void WaitForOffset(long offset)
    {
        WithReadLock(ptr => NativeInterop.ArrowStreamWaitForOffset(ptr, offset));
    }

    /// <summary>
    /// Blocks until all pending batches have been acknowledged by the server.
    /// </summary>
    /// <exception cref="ZerobusException">Thrown if the flush fails.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    public void Flush()
    {
        WithReadLock(NativeInterop.ArrowStreamFlush);
    }

    // ── Unacknowledged batches ───────────────────────────────────────────

    /// <summary>
    /// Retrieves all Arrow batches that have not yet been acknowledged by the server.
    /// <para>
    /// <strong>Important:</strong> This should only be called after the stream has
    /// closed or failed. Calling it on an active stream will return an error.
    /// </para>
    /// </summary>
    /// <returns>
    /// An array of <see cref="ArrowBatchInfo"/> containing the unacknowledged batch data.
    /// </returns>
    /// <exception cref="ZerobusException">Thrown if retrieval fails.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    public ArrowBatchInfo[] GetUnackedBatches()
    {
        return WithReadLock(NativeInterop.ArrowStreamGetUnackedBatches);
    }

    // ── Close / Dispose ──────────────────────────────────────────────────

    /// <summary>
    /// Gracefully closes the stream after flushing all pending batches.
    /// After close, ingestion APIs are no longer usable.
    /// </summary>
    /// <remarks>
    /// This method does not free native resources. Call <see cref="Dispose()"/>
    /// after you have finished any recovery operations.
    /// </remarks>
    /// <exception cref="ZerobusException">Thrown if close fails.</exception>
    public void Close()
    {
        using var handle = WithWriteLock();
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        var ptr = GetNativePointerForCall();
        if (NativeMethods.ArrowStreamIsClosed(ptr)) return;

        NativeInterop.ArrowStreamClose(ptr);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0)
            return ValueTask.CompletedTask;

        IntPtr ptr;
        var shouldClose = false;

        using (WithWriteLock())
        {
            ptr = Interlocked.Exchange(ref _ptr, IntPtr.Zero);
            if (ptr == IntPtr.Zero) return ValueTask.CompletedTask;
            shouldClose = !NativeMethods.ArrowStreamIsClosed(ptr);
        }

        if (shouldClose)
        {
            NativeInterop.ArrowStreamClose(ptr);
        }

        NativeMethods.ArrowStreamFree(ptr);
        return ValueTask.CompletedTask;
    }

    private void Dispose(bool disposing)
    {
        if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0) return;

        using var handle = WithWriteLock();
        var ptr = Interlocked.Exchange(ref _ptr, IntPtr.Zero);
        if (ptr == IntPtr.Zero) return;

        if (disposing && !NativeMethods.ArrowStreamIsClosed(ptr))
        {
            NativeInterop.ArrowStreamClose(ptr);
        }

        NativeMethods.ArrowStreamFree(ptr);
    }

    /// <summary>
    /// Safety-net release of native memory for leaked instances.
    /// Only frees memory — does NOT call blocking gRPC close.
    /// </summary>
    ~ZerobusArrowStream()
    {
        Dispose(false);
    }

    internal IntPtr NativePointer
    {
        get { return WithReadLock(ptr => ptr); }
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
    private IDisposable WithReadLock()
    {
        _lifetimeLock.EnterReadLock();
        return new Disposable(() => _lifetimeLock.ExitReadLock());
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private IDisposable WithWriteLock()
    {
        _lifetimeLock.EnterWriteLock();
        return new Disposable(() => _lifetimeLock.ExitWriteLock());
    }

    private class Disposable(Action action) : IDisposable
    {
        public void Dispose() => action();
    }
}
