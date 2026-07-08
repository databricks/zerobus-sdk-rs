using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using Databricks.Zerobus.Native;

namespace Databricks.Zerobus;

/// <summary>
/// Represents an active bidirectional gRPC stream for ingesting records.
/// Records can be ingested concurrently and will be acknowledged asynchronously.
/// </summary>
/// <remarks>
/// <para>
/// The stream is thread-safe — you may call <see cref="IngestRecord(string)"/> from
/// multiple threads concurrently, just like the Go SDK supports goroutines.
/// </para>
/// <para>
/// <see cref="Close()"/> performs a graceful close (flush + close) but keeps the
/// native stream alive so callers can inspect <see cref="GetUnackedRecords"/> or
/// recover with <c>ZerobusSdk.RecreateStream(...)</c>. Call <see cref="Dispose()"/>
/// when you are completely finished with the stream to free native resources.
/// </para>
/// </remarks>
public sealed class ZerobusStream : IDisposable
{
    private IntPtr _ptr;
    private int _disposed;
    private readonly ReaderWriterLockSlim _lifetimeLock = new();

    // Prevent the GCHandle / delegate from being collected while the native code holds a reference.
    // Not readonly: GCHandle is not a readonly struct, so calling Free() on a readonly field
    // creates a defensive copy — the field is never actually mutated, causing double-free.
    private GCHandle _bridgeHandle;
    private readonly HeadersProviderCallback? _callbackRef;

    internal ZerobusStream(IntPtr ptr)
    {
        _ptr = ptr;
    }

    internal ZerobusStream(IntPtr ptr, GCHandle bridgeHandle, HeadersProviderCallback callbackRef)
    {
        _ptr = ptr;
        _bridgeHandle = bridgeHandle;
        _callbackRef = callbackRef;
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
    /// <param name="payload">
    /// The record payload. Pass a <see cref="string"/> for JSON records
    /// or a <c>byte[]</c> / <see cref="ReadOnlySpan{T}"/> of <see cref="byte"/>
    /// for Protocol Buffer records.
    /// </param>
    /// <returns>The offset of the ingested record.</returns>
    /// <exception cref="ZerobusException">Thrown if ingestion fails.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    /// <exception cref="ArgumentException">
    /// Thrown if the payload type is not <c>string</c> or <c>byte[]</c>.
    /// </exception>
    /// <example>
    /// <code>
    /// // JSON
    /// long offset = stream.IngestRecord("{\"id\": 1, \"message\": \"Hello\"}");
    ///
    /// // Protobuf
    /// byte[] protoBytes = SerializeMyProto(myMessage);
    /// long offset = stream.IngestRecord(protoBytes);
    /// </code>
    /// </example>
    public long IngestRecord(string payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        return WithReadLock(ptr => NativeInterop.StreamIngestJsonRecord(ptr, payload));
    }

    /// <inheritdoc cref="IngestRecord(string)"/>
    public long IngestRecord(byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        return WithReadLock(ptr => NativeInterop.StreamIngestProtoRecord(ptr, payload));
    }

    /// <inheritdoc cref="IngestRecord(string)"/>
    public long IngestRecord(ReadOnlySpan<byte> payload)
    {
        using var handle = WithReadLock();
        return NativeInterop.StreamIngestProtoRecord(GetNativePointerForCall(), payload);
    }

    // ── Batch ingestion ──────────────────────────────────────────────────

    /// <summary>
    /// Ingests a batch of JSON records and returns one offset for the entire batch.
    /// All records in the batch must be JSON strings.
    /// </summary>
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
    /// long batchOffset = stream.IngestRecords(records);
    /// </code>
    /// </example>
    public long IngestRecords(string[] records)
    {
        ArgumentNullException.ThrowIfNull(records);
        return WithReadLock(ptr => NativeInterop.StreamIngestJsonRecords(ptr, records));
    }

    /// <summary>
    /// Ingests a batch of protobuf records and returns one offset for the entire batch.
    /// All records in the batch must be serialised protobuf byte arrays.
    /// </summary>
    /// <param name="records">The protobuf record byte spans to ingest.</param>
    /// <returns>The offset representing the entire batch, or -1 if the batch is empty.</returns>
    /// <exception cref="ZerobusException">Thrown if ingestion fails.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    public long IngestRecords(byte[][] records)
    {
        ArgumentNullException.ThrowIfNull(records);
        return WithReadLock(ptr => NativeInterop.StreamIngestProtoRecords(ptr, records));
    }

    // ── Acknowledgment / flush ───────────────────────────────────────────

    /// <summary>
    /// Blocks until the server acknowledges the record at the specified offset.
    /// Use this with offsets returned from <see cref="IngestRecord(string)"/> to wait for
    /// specific records to be durably written without waiting for all pending records.
    /// </summary>
    /// <param name="offset">The offset to wait for.</param>
    /// <exception cref="ZerobusException">Thrown if the wait fails.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    /// <example>
    /// <code>
    /// long offset = stream.IngestRecord(data);
    /// // ... do other work ...
    /// stream.WaitForOffset(offset);
    /// </code>
    /// </example>
    public void WaitForOffset(long offset)
    {
        WithReadLock(ptr => NativeInterop.StreamWaitForOffset(ptr, offset));
    }

    /// <summary>
    /// Blocks until all pending records have been acknowledged by the server.
    /// This ensures durability guarantees before proceeding.
    /// </summary>
    /// <exception cref="ZerobusException">
    /// Thrown if the flush times out or a record fails with a non-retryable error.
    /// </exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    /// <example>
    /// <code>
    /// stream.Flush();
    /// Console.WriteLine("All records durably stored.");
    /// </code>
    /// </example>
    public void Flush()
    {
        WithReadLock(NativeInterop.StreamFlush);
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
    /// An array where each element is either a <see cref="string"/> (JSON)
    /// or a <c>byte[]</c> (protobuf).
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
    ///     var unacked = stream.GetUnackedRecords();
    ///     Console.WriteLine($"Failed to acknowledge {unacked.Length} records");
    /// }
    /// </code>
    /// </example>
    public object[] GetUnackedRecords()
    {
        return WithReadLock(NativeInterop.StreamGetUnackedRecords);
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

    /// <inheritdoc />
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    private void Dispose(bool disposing)
    {
        if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0) return;

        using var handle = WithWriteLock();
        var ptr = Interlocked.Exchange(ref _ptr, IntPtr.Zero);
        if (ptr == IntPtr.Zero)
        {
            FreeBridgeHandle();
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
        FreeBridgeHandle();

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

    private void FreeBridgeHandle()
    {
        if (_bridgeHandle.IsAllocated)
            _bridgeHandle.Free();
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
    /// Attempts to retrieve the bridge handle and callback reference for the stream.
    /// Internal use only.
    /// </summary>
    internal ZerobusStream Recreate(IntPtr newPtr)
    {
        var disposed = Interlocked.CompareExchange(ref _disposed, 1, 0);
        ObjectDisposedException.ThrowIf(disposed != 0, this);

        using var handle = WithWriteLock();

        var oldPtr = Interlocked.Exchange(ref _ptr, IntPtr.Zero);
        var newStream = _bridgeHandle.IsAllocated
            ? new ZerobusStream(newPtr, _bridgeHandle, _callbackRef!)
            : new ZerobusStream(newPtr);

        // Prevent accidental double-free of the shared handle from the old wrapper
        _bridgeHandle = default;

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
