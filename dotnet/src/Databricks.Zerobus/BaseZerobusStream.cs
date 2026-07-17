using Databricks.Zerobus.Native;
using System.Runtime.InteropServices;

namespace Databricks.Zerobus;

/// <summary>
/// Base class for Zerobus ingestion streams providing common native method
/// declarations, lifecycle management, and offset-based acknowledgment tracking.
/// </summary>
/// <remarks>
/// <para>Streams are thread-safe. Multiple threads can ingest records concurrently;
/// the SDK serializes access to the underlying native handle via a reader-writer lock.</para>
/// <para>Streams implement IDisposable and (on .NET 8+) IAsyncDisposable.
/// Always use <c>using</c> or <c>await using</c> statements to ensure native resources are freed.</para>
/// </remarks>
public abstract class BaseZerobusStream : IDisposable
#if NET8_0_OR_GREATER
    , IAsyncDisposable
#endif

{
    private readonly ReaderWriterLockSlim _lock = new(LockRecursionPolicy.NoRecursion);
    private ZerobusStreamHandle _handle;
    internal int _disposed;

    /// <summary>
    /// The fully qualified Unity Catalog table name.
    /// </summary>
    public string TableName { get; }

    /// <summary>
    /// The stream configuration options.
    /// </summary>
    public StreamConfigurationOptions Options { get; }

    /// <summary>
    /// Whether this is a JSON-mode stream (as opposed to protobuf mode).
    /// </summary>
    protected bool IsJsonMode { get; }

    /// <summary>
    /// Cached unacknowledged records available after stream closure.
    /// </summary>
    protected List<byte[]>? CachedUnackedRecords { get; private set; }

    /// <summary>
    /// Whether the stream has been closed or disposed.
    /// </summary>
    public bool IsClosed => _disposed != 0 || _handle.IsClosed || _handle.IsInvalid;

    internal IntPtr NativeHandle => _handle.DangerousGetHandle();

    /// <summary>
    /// Creates a new base stream with the given native handle and configuration.
    /// </summary>
    internal protected BaseZerobusStream(
        IntPtr nativeHandle,
        string tableName,
        StreamConfigurationOptions options,
        bool isJsonMode)
    {
        _handle = new ZerobusStreamHandle(nativeHandle);
        TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        Options = options ?? throw new ArgumentNullException(nameof(options));
        IsJsonMode = isJsonMode;
    }

    // ==================== Lifecycle (sync) ====================

    /// <summary>
    /// Waits until the record at the given offset has been durably acknowledged.
    /// </summary>
    public void WaitForOffset(long offset)
    {
        EnsureOpen();
        WithReadLock(() =>
        {
            CResult result;
            byte ok = NativeMethods.zerobus_stream_wait_for_offset(NativeHandle, offset, out result);
            if (ok == 0)
            {
                string msg = Marshal.PtrToStringAnsi(result.ErrorMessage) ?? "Wait for offset failed";
                SafeFreeErrorMessage(result.ErrorMessage);
                throw new ZerobusException(msg, isRetryable: result.IsRetryable);
            }
        });
    }

    /// <summary>
    /// Blocks until all currently queued records have been durably acknowledged.
    /// </summary>
    public void Flush()
    {
        EnsureOpen();
        WithReadLock(() =>
        {
            byte ok = NativeMethods.zerobus_stream_flush(NativeHandle);
            if (ok == 0)
                throw new ZerobusException("Flush failed. Check logs for details.", isRetryable: true);
        });
    }

    /// <summary>
    /// Gracefully closes the stream, flushing all pending records first.
    /// After calling this method, unacked records can be retrieved via
    /// <see cref="GetCachedUnackedRecords"/>.
    /// </summary>
    public void Close()
    {
        if (_disposed != 0 || _handle.IsClosed || _handle.IsInvalid)
            return;

        WithWriteLock(() =>
        {
            if (_disposed != 0 || _handle.IsClosed || _handle.IsInvalid)
                return;

            byte ok = NativeMethods.zerobus_stream_close(NativeHandle);
            if (ok == 1)
            {
                CacheUnackedData();
            }
        });

        DisposeNativeHandle();
    }

    // ==================== Lifecycle (async) ====================

    /// <inheritdoc cref="WaitForOffset"/>
    public Task WaitForOffsetAsync(long offset)
    {
        return Task.Run(() => WaitForOffset(offset));
    }

    /// <inheritdoc cref="Flush"/>
    public Task FlushAsync()
    {
        return Task.Run(() => Flush());
    }

    /// <inheritdoc cref="Close"/>
    public Task CloseAsync()
    {
        return Task.Run(() => Close());
    }

    // ==================== Unacked records ====================

    /// <summary>
    /// Returns any records that were not acknowledged when the stream was closed.
    /// Returns an empty list if called before the stream is closed.
    /// </summary>
    protected IReadOnlyList<byte[]> GetCachedUnackedRecords()
    {
        return CachedUnackedRecords ?? (IReadOnlyList<byte[]>)Array.Empty<byte[]>();
    }

    /// <summary>
    /// Retrieves unacknowledged records from the live native stream.
    /// </summary>
    protected List<byte[]> GetNativeUnackedRecords()
    {
        EnsureOpen();
        CRecordArray array = NativeMethods.zerobus_stream_get_unacked_records(NativeHandle);
        var result = new List<byte[]>((int)(ulong)array.Len);

        if (array.Records != IntPtr.Zero && array.Len != UIntPtr.Zero)
        {
            int recordSize = Marshal.SizeOf(typeof(CRecord));
            for (ulong i = 0; i < (ulong)array.Len; i++)
            {
                IntPtr recordPtr = array.Records + (int)(i * (ulong)recordSize);
                var record = (CRecord)Marshal.PtrToStructure(recordPtr, typeof(CRecord))!;
                if (record.Data != IntPtr.Zero && record.DataLen != UIntPtr.Zero)
                {
                    int len = (int)(ulong)record.DataLen;
                    var bytes = new byte[len];
                    Marshal.Copy(record.Data, bytes, 0, len);
                    result.Add(bytes);
                }
            }
        }

        NativeMethods.zerobus_free_record_array(array);
        return result;
    }

    /// <summary>
    /// Caches unacknowledged records before the native handle is destroyed.
    /// </summary>
    protected void CacheUnackedData()
    {
        try
        {
            CachedUnackedRecords = GetNativeUnackedRecords();
        }
        catch
        {
            CachedUnackedRecords = new List<byte[]>();
        }
    }

    // ==================== Guard / helpers ====================

    /// <summary>
    /// Throws if the stream is closed or disposed.
    /// </summary>
    protected void EnsureOpen()
    {
        if (IsClosed)
            throw new ZerobusException("Stream is closed or disposed.", isRetryable: false);
    }

    /// <summary>
    /// Executes an action under the read lock. Used for ingest, flush, wait operations.
    /// </summary>
    protected void WithReadLock(Action action)
    {
        _lock.EnterReadLock();
        try { action(); }
        finally { _lock.ExitReadLock(); }
    }

    /// <summary>
    /// Executes an action under the write lock. Used for close/dispose operations.
    /// </summary>
    protected void WithWriteLock(Action action)
    {
        _lock.EnterWriteLock();
        try { action(); }
        finally { _lock.ExitWriteLock(); }
    }

    /// <summary>
    /// Executes a function under the read lock, returning its result.
    /// </summary>
    protected T WithReadLock<T>(Func<T> func)
    {
        _lock.EnterReadLock();
        try { return func(); }
        finally { _lock.ExitReadLock(); }
    }

    /// <summary>
    /// Disposes native resources.
    /// </summary>
    protected void DisposeNativeHandle()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            _handle.Dispose();
            _lock.Dispose();
        }
    }

    // ==================== IDisposable / IAsyncDisposable ====================

    /// <summary>
    /// Releases all resources used by the stream.
    /// </summary>
    public void Dispose()
    {
        if (_disposed != 0) return;

        if (!_handle.IsClosed && !_handle.IsInvalid)
        {
            try { CacheUnackedData(); }
            catch { /* best effort */ }
        }

        DisposeNativeHandle();
    }

#if NET8_0_OR_GREATER
    /// <summary>
    /// Asynchronously releases all resources used by the stream.
    /// </summary>
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }
#endif

    private static void SafeFreeErrorMessage(IntPtr msg)
    {
        if (msg != IntPtr.Zero)
        {
            try { NativeMethods.zerobus_free_error_message(msg); }
            catch { /* best effort */ }
        }
    }
}
