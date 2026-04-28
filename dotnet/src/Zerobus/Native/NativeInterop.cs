// High-level safe wrappers around P/Invoke calls.
// Handles marshalling, error conversion, and memory management.
// This is the .NET equivalent of the unexported ffi* functions in ffi.go.

using System.Runtime.InteropServices;
using System.Text;

namespace Databricks.Zerobus.Native;

/// <summary>
/// Provides safe, managed wrappers around the raw P/Invoke layer.
/// All methods convert <see cref="CResult"/> errors into <see cref="ZerobusException"/>.
/// </summary>
internal static class NativeInterop
{
    private const int StackAllocThresholdBytes = 4096;

    /// <summary>
    /// Converts a <see cref="CResult"/> to a <see cref="ZerobusException"/> (or null on success).
    /// Frees the native error message string.
    /// </summary>
    internal static ZerobusException? ToException(ref CResult result)
    {
        if (result.Success)
            return null;

        string message;
        if (result.ErrorMessage != IntPtr.Zero)
        {
            message = Marshal.PtrToStringUTF8(result.ErrorMessage) ?? "unknown error";
            NativeMethods.FreeErrorMessage(result.ErrorMessage);
            result.ErrorMessage = IntPtr.Zero;
        }
        else
        {
            message = "unknown error";
        }

        return new ZerobusException(message, result.IsRetryable);
    }

    /// <summary>
    /// Throws if the <see cref="CResult"/> indicates failure.
    /// </summary>
    internal static void ThrowIfFailed(ref CResult result)
    {
        var ex = ToException(ref result);
        if (ex is not null)
            throw ex;
    }

    /// <summary>
    /// Creates a new SDK instance.
    /// </summary>
    public static IntPtr SdkNew(string zerobusEndpoint, string unityCatalogUrl)
    {
        var result = new CResult();
        var ptr = NativeMethods.SdkNew(zerobusEndpoint, unityCatalogUrl, ref result);

        if (ptr == IntPtr.Zero)
        {
            var ex = ToException(ref result);
            throw ex ?? new ZerobusException("Failed to create SDK instance", isRetryable: false);
        }

        return ptr;
    }

    /// <summary>
    /// Creates a stream with OAuth credentials.
    /// </summary>
    public static unsafe IntPtr SdkCreateStream(
        IntPtr sdkPtr,
        string tableName,
        ReadOnlySpan<byte> descriptorProto,
        string clientId,
        string clientSecret,
        ref CStreamConfigurationOptions options)
    {
        var result = new CResult();
        IntPtr ptr;

        fixed (byte* descPtr = descriptorProto)
        {
            ptr = NativeMethods.SdkCreateStream(
                sdkPtr,
                tableName,
                descPtr,
                (nuint)descriptorProto.Length,
                clientId,
                clientSecret,
                ref options,
                ref result);
        }

        if (ptr == IntPtr.Zero)
        {
            var ex = ToException(ref result);
            throw ex ?? new ZerobusException("Failed to create stream", isRetryable: false);
        }

        return ptr;
    }

    /// <summary>
    /// Creates a stream with a custom headers provider callback.
    /// </summary>
    public static unsafe IntPtr SdkCreateStreamWithHeadersProvider(
        IntPtr sdkPtr,
        string tableName,
        ReadOnlySpan<byte> descriptorProto,
        HeadersProviderCallback callback,
        IntPtr userData,
        ref CStreamConfigurationOptions options)
    {
        var result = new CResult();
        IntPtr ptr;

        fixed (byte* descPtr = descriptorProto)
        {
            ptr = NativeMethods.SdkCreateStreamWithHeadersProvider(
                sdkPtr,
                tableName,
                descPtr,
                (nuint)descriptorProto.Length,
                callback,
                userData,
                ref options,
                ref result);
        }

        if (ptr == IntPtr.Zero)
        {
            var ex = ToException(ref result);
            throw ex ?? new ZerobusException("Failed to create stream with headers provider", isRetryable: false);
        }

        return ptr;
    }

    /// <summary>
    /// Recreates a stream from an existing stream.
    /// </summary>
    public static IntPtr SdkRecreateStream(IntPtr sdkPtr, IntPtr streamPtr)
    {
        var result = new CResult();
        var ptr = NativeMethods.SdkRecreateStream(sdkPtr, streamPtr, ref result);

        if (ptr == IntPtr.Zero)
        {
            var ex = ToException(ref result);
            throw ex ?? new ZerobusException("Failed to recreate stream", isRetryable: false);
        }

        return ptr;
    }

    /// <summary>
    /// Ingests a single protobuf record and returns the offset.
    /// </summary>
    public static unsafe long StreamIngestProtoRecord(IntPtr streamPtr, ReadOnlySpan<byte> data)
    {
        if (data.IsEmpty)
            throw new ZerobusException("empty data", isRetryable: false);

        var result = new CResult();
        long offset;

        fixed (byte* dataPtr = data)
        {
            offset = NativeMethods.StreamIngestProtoRecord(
                streamPtr,
                dataPtr,
                (nuint)data.Length,
                ref result);
        }

        if (offset < 0)
        {
            ThrowIfFailed(ref result);
            throw new ZerobusException("Ingest failed with unknown error", isRetryable: false);
        }

        return offset;
    }

    /// <summary>
    /// Ingests a single JSON record and returns the offset.
    /// </summary>
    public static long StreamIngestJsonRecord(IntPtr streamPtr, string jsonData)
    {
        var result = new CResult();
        var offset = NativeMethods.StreamIngestJsonRecord(streamPtr, jsonData, ref result);

        if (offset < 0)
        {
            ThrowIfFailed(ref result);
            throw new ZerobusException("Ingest failed with unknown error", isRetryable: false);
        }

        return offset;
    }

    /// <summary>
    /// Ingests a batch of protobuf records and returns the last offset.
    /// </summary>
    public static unsafe long StreamIngestProtoRecords(IntPtr streamPtr, byte[][] records)
    {
        if (records.Length == 0)
            return -1;

        var result = new CResult();
        var numRecords = (nuint)records.Length;

        // Pin all record buffers and collect pointers.
        // Use stackalloc for small batches, heap array for large ones.
        var handles = new GCHandle[records.Length];
        var ptrs = records.Length * sizeof(nint) <= StackAllocThresholdBytes
            ? stackalloc IntPtr[records.Length]
            : new IntPtr[records.Length];
        var lens = records.Length * sizeof(nuint) <= StackAllocThresholdBytes
            ? stackalloc nuint[records.Length]
            : new nuint[records.Length];

        try
        {
            for (var i = 0; i < records.Length; i++)
            {
                handles[i] = GCHandle.Alloc(records[i], GCHandleType.Pinned);
                ptrs[i] = handles[i].AddrOfPinnedObject();
                lens[i] = (nuint)records[i].Length;
            }

            fixed (IntPtr* pointers = ptrs)
            fixed (nuint* lengths = lens)
            {
                var offset = NativeMethods.StreamIngestProtoRecords(
                    streamPtr,
                    (byte**)pointers,
                    lengths,
                    numRecords,
                    ref result);

                if (offset == -2) return -1; // empty batch
                if (offset < 0)
                {
                    ThrowIfFailed(ref result);
                    throw new ZerobusException("Batch ingest failed with unknown error", isRetryable: false);
                }

                return offset;
            }
        }
        finally
        {
            for (var i = 0; i < handles.Length; i++)
            {
                if (handles[i].IsAllocated)
                    handles[i].Free();
            }
        }
    }

    /// <summary>
    /// Ingests a batch of JSON records and returns the last offset.
    /// </summary>
    public static unsafe long StreamIngestJsonRecords(IntPtr streamPtr, string[] records)
    {
        if (records.Length == 0)
            return -1;

        var result = new CResult();
        var numRecords = (nuint)records.Length;

        // Encode each string as null-terminated UTF-8 and pin.
        // Use stackalloc for small batches, heap array for large ones.
        var handles = new GCHandle[records.Length];
        var ptrs = records.Length * sizeof(nint) <= StackAllocThresholdBytes
            ? stackalloc IntPtr[records.Length]
            : new IntPtr[records.Length];

        try
        {
            for (var i = 0; i < records.Length; i++)
            {
                // Encode with null terminator
                var byteCount = Encoding.UTF8.GetByteCount(records[i]);
                var utf8 = new byte[byteCount + 1];
                Encoding.UTF8.GetBytes(records[i], utf8);
                handles[i] = GCHandle.Alloc(utf8, GCHandleType.Pinned);
                ptrs[i] = handles[i].AddrOfPinnedObject();
            }

            fixed (IntPtr* pointers = ptrs)
            {
                var offset = NativeMethods.StreamIngestJsonRecords(
                    streamPtr,
                    (byte**)pointers,
                    numRecords,
                    ref result);

                if (offset == -2) return -1; // empty batch
                if (offset < 0)
                {
                    ThrowIfFailed(ref result);
                    throw new ZerobusException("Batch ingest failed with unknown error", isRetryable: false);
                }

                return offset;
            }
        }
        finally
        {
            for (var i = 0; i < handles.Length; i++)
            {
                if (handles[i].IsAllocated)
                    handles[i].Free();
            }
        }
    }

    /// <summary>
    /// Waits for a specific offset to be acknowledged.
    /// </summary>
    public static void StreamWaitForOffset(IntPtr streamPtr, long offset)
    {
        var result = new CResult();
        var success = NativeMethods.StreamWaitForOffset(streamPtr, offset, ref result);

        if (!success)
            ThrowIfFailed(ref result);
    }

    /// <summary>
    /// Flushes all pending records.
    /// </summary>
    public static void StreamFlush(IntPtr streamPtr)
    {
        var result = new CResult();
        var success = NativeMethods.StreamFlush(streamPtr, ref result);

        if (!success)
            ThrowIfFailed(ref result);
    }

    /// <summary>
    /// Retrieves all unacknowledged records from a closed/failed stream.
    /// </summary>
    public static object[] StreamGetUnackedRecords(IntPtr streamPtr)
    {
        var result = new CResult();
        var cArray = NativeMethods.StreamGetUnackedRecords(streamPtr, ref result);

        if (cArray.Records == IntPtr.Zero)
        {
            if ((int)cArray.Len == 0)
            {
                var ex = ToException(ref result);
                if (ex is not null) throw ex;
                return [];
            }

            ThrowIfFailed(ref result);
            return [];
        }

        if ((int)cArray.Len == 0)
            return [];

        var records = new object[(int)cArray.Len];
        var recordSize = Marshal.SizeOf<CRecord>();

        for (var i = 0; i < (int)cArray.Len; i++)
        {
            var cRecord = Marshal.PtrToStructure<CRecord>(cArray.Records + i * recordSize);
            var data = new byte[(int)cRecord.DataLen];
            Marshal.Copy(cRecord.Data, data, 0, data.Length);

            records[i] = cRecord.IsJson ? Encoding.UTF8.GetString(data) : data;
        }

        NativeMethods.FreeRecordArray(cArray);
        return records;
    }

    /// <summary>
    /// Closes the stream gracefully.
    /// </summary>
    public static void StreamClose(IntPtr streamPtr)
    {
        var result = new CResult();
        var success = NativeMethods.StreamClose(streamPtr, ref result);

        if (!success)
            ThrowIfFailed(ref result);
    }

    /// <summary>
    /// Converts managed <see cref="StreamConfigurationOptions"/> to the native struct,
    /// applying defaults for unset values.
    /// </summary>
    public static CStreamConfigurationOptions ConvertConfig(StreamConfigurationOptions? options)
    {
        if (options is null)
            return NativeMethods.GetDefaultConfig();

        var defaults = StreamConfigurationOptions.Default;

        return new CStreamConfigurationOptions
        {
            MaxInflightRequests = (nuint)(options.MaxInflightRequests > 0
                ? options.MaxInflightRequests
                : defaults.MaxInflightRequests),
            Recovery = options.Recovery,
            RecoveryTimeoutMs = options.RecoveryTimeoutMs > 0
                ? options.RecoveryTimeoutMs
                : defaults.RecoveryTimeoutMs,
            RecoveryBackoffMs = options.RecoveryBackoffMs > 0
                ? options.RecoveryBackoffMs
                : defaults.RecoveryBackoffMs,
            RecoveryRetries = options.RecoveryRetries > 0
                ? options.RecoveryRetries
                : defaults.RecoveryRetries,
            ServerLackOfAckTimeoutMs = options.ServerLackOfAckTimeoutMs > 0
                ? options.ServerLackOfAckTimeoutMs
                : defaults.ServerLackOfAckTimeoutMs,
            FlushTimeoutMs = options.FlushTimeoutMs > 0
                ? options.FlushTimeoutMs
                : defaults.FlushTimeoutMs,
            RecordType = (int)(options.RecordType != RecordType.Unspecified
                ? options.RecordType
                : defaults.RecordType),
            StreamPausedMaxWaitTimeMs = options.StreamPausedMaxWaitTimeMs ?? 0,
            HasStreamPausedMaxWaitTimeMs = options.StreamPausedMaxWaitTimeMs.HasValue,
            CallbackMaxWaitTimeMs = 0,
            HasCallbackMaxWaitTimeMs = false,
            AckOnAck = IntPtr.Zero,
            AckOnError = IntPtr.Zero,
            AckUserData = IntPtr.Zero,
        };
    }
}
