using Databricks.Zerobus.Native;
using System.Runtime.InteropServices;

namespace Databricks.Zerobus;

/// <summary>
/// Stream for ingesting Apache Arrow RecordBatches into a Unity Catalog Delta table.
/// Uses the Arrow Flight protocol on the same gRPC connection as the standard streams.
/// </summary>
/// <remarks>
/// <b>Beta:</b> The Arrow Flight ingestion API is in beta and may change in future releases.
/// Best suited for naturally columnar or batched workloads.
/// </remarks>
public sealed class ZerobusArrowStream : IDisposable
{
    private ZerobusArrowStreamHandle _handle;
    private volatile int _disposed;

    /// <summary>
    /// The fully qualified Unity Catalog table name.
    /// </summary>
    public string TableName { get; }

    /// <summary>
    /// The Arrow stream configuration options.
    /// </summary>
    public ArrowStreamConfigurationOptions Options { get; }

    /// <summary>
    /// OAuth client ID used for stream authentication.
    /// </summary>
    public string ClientId { get; }

    /// <summary>
    /// Whether the stream has been closed or disposed.
    /// </summary>
    public bool IsClosed => _disposed != 0 || _handle.IsClosed || _handle.IsInvalid;

    internal IntPtr NativeHandle => _handle.DangerousGetHandle();

    internal ZerobusArrowStream(
        IntPtr nativeHandle,
        string tableName,
        ArrowStreamConfigurationOptions options,
        string clientId,
        string clientSecret)
    {
        _handle = new ZerobusArrowStreamHandle(nativeHandle);
        TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        Options = options ?? throw new ArgumentNullException(nameof(options));
        ClientId = clientId ?? throw new ArgumentNullException(nameof(clientId));
    }

    /// <summary>
    /// Ingests an Arrow IPC-encoded RecordBatch and returns its offset.
    /// </summary>
    /// <param name="ipcBytes">The IPC-serialized Arrow RecordBatch.</param>
    /// <returns>The offset of the ingested batch, or -1 on error.</returns>
    public long IngestBatch(byte[] ipcBytes)
    {
        if (ipcBytes == null) throw new ArgumentNullException(nameof(ipcBytes));
        EnsureOpen();

        GCHandle handle = GCHandle.Alloc(ipcBytes, GCHandleType.Pinned);
        try
        {
            CResult result;
            long offset = NativeMethods.zerobus_arrow_stream_ingest_batch(
                NativeHandle,
                handle.AddrOfPinnedObject(),
                (UIntPtr)ipcBytes.Length,
                out result);

            if (offset < 0)
            {
                string msg = Marshal.PtrToStringAnsi(result.ErrorMessage) ?? "Failed to ingest Arrow batch";
                SafeFreeErrorMessage(result.ErrorMessage);
                throw new ZerobusException(msg, isRetryable: result.IsRetryable);
            }

            return offset;
        }
        finally
        {
            handle.Free();
        }
    }

    /// <summary>
    /// Waits until the batch at the given offset has been durably acknowledged.
    /// </summary>
    public void WaitForOffset(long offset)
    {
        EnsureOpen();
        CResult result;
        byte ok = NativeMethods.zerobus_arrow_stream_wait_for_offset(NativeHandle, offset, out result);
        if (ok == 0)
        {
            string msg = Marshal.PtrToStringAnsi(result.ErrorMessage) ?? "Arrow wait_for_offset failed";
            SafeFreeErrorMessage(result.ErrorMessage);
            throw new ZerobusException(msg, isRetryable: result.IsRetryable);
        }
    }

    /// <summary>
    /// Blocks until all currently queued Arrow batches have been durably acknowledged.
    /// </summary>
    public void Flush()
    {
        EnsureOpen();
        byte ok = NativeMethods.zerobus_arrow_stream_flush(NativeHandle);
        if (ok == 0)
            throw new ZerobusException("Arrow stream flush failed.", isRetryable: true);
    }

    /// <summary>
    /// Gracefully closes the stream, flushing all pending batches first.
    /// </summary>
    public void Close()
    {
        if (_disposed != 0 || _handle.IsClosed || _handle.IsInvalid)
            return;

        NativeMethods.zerobus_arrow_stream_close(NativeHandle);
        DisposeHandle();
    }

    /// <summary>
    /// Returns unacknowledged Arrow batches. Only valid while the stream is open.
    /// </summary>
    public IReadOnlyList<EncodedBatch> GetUnackedBatches()
    {
        EnsureOpen();
        CArrowBatchArray array = NativeMethods.zerobus_arrow_stream_get_unacked_batches(NativeHandle);
        var result = new List<EncodedBatch>((int)(ulong)array.Count);

        if (array.Batches != IntPtr.Zero && array.Count != UIntPtr.Zero)
        {
            int ptrSize = IntPtr.Size;
            for (ulong i = 0; i < (ulong)array.Count; i++)
            {
                IntPtr batchPtr = Marshal.ReadIntPtr(array.Batches, (int)(i * (ulong)ptrSize));
                IntPtr lenPtr = array.Lengths == IntPtr.Zero
                    ? IntPtr.Zero
                    : Marshal.ReadIntPtr(array.Lengths, (int)(i * (ulong)ptrSize));

                int len = lenPtr != IntPtr.Zero ? (int)lenPtr : 0;
                // The batch data is a byte array; length isn't stored separately
                // in the C struct — we need to get it from the IPC metadata.
                // For now, the length pointer stores the byte count.
                var data = new byte[len];
                if (batchPtr != IntPtr.Zero && len > 0)
                {
                    Marshal.Copy(batchPtr, data, 0, len);
                }
                result.Add(new EncodedBatch(data, Array.Empty<int>()));
            }
        }

        NativeMethods.zerobus_arrow_free_batch_array(array);
        return result;
    }

    /// <summary>
    /// Disposes the stream, freeing native resources.
    /// </summary>
    public void Dispose()
    {
        if (_disposed != 0) return;

        if (!_handle.IsClosed && !_handle.IsInvalid)
        {
            try { NativeMethods.zerobus_arrow_stream_close(NativeHandle); }
            catch { /* best effort */ }
        }

        DisposeHandle();
    }

    private void DisposeHandle()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            _handle.Dispose();
        }
    }

    private void EnsureOpen()
    {
        if (IsClosed)
            throw new ZerobusException("Arrow stream is closed or disposed.", isRetryable: false);
    }

    private static void SafeFreeErrorMessage(IntPtr msg)
    {
        if (msg != IntPtr.Zero)
        {
            try { NativeMethods.zerobus_free_error_message(msg); }
            catch { /* best effort */ }
        }
    }
}
