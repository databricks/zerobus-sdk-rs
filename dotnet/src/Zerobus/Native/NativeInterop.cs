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

    private static unsafe void ApplyResult(TaskCompletionSource tcs, CResult* result)
    {
        if (result->Success)
            tcs.TrySetResult();
        else
            tcs.TrySetException(ToException(result));
    }

    private static unsafe void ApplyResult<T>(TaskCompletionSource<T> tcs, CResult* result, T successValue)
    {
        if (result->Success)
            tcs.TrySetResult(successValue);
        else
            tcs.TrySetException(ToException(result));
    }

    private static ReadOnlyMemory<byte>[] ConvertRecordArrayAndFree(CRecordArray cArray)
    {
        if ((int)cArray.Len == 0 || cArray.Records == IntPtr.Zero)
            return [];

        try
        {
            var records = new ReadOnlyMemory<byte>[(int)cArray.Len];
            var recordSize = Marshal.SizeOf<CRecord>();

            for (var i = 0; i < (int)cArray.Len; i++)
            {
                var cRecord = Marshal.PtrToStructure<CRecord>(cArray.Records + i * recordSize);
                var data = new byte[(int)cRecord.DataLen];
                Marshal.Copy(cRecord.Data, data, 0, data.Length);
                records[i] = data;
            }

            return records;
        }
        finally
        {
            NativeMethods.FreeRecordArray(cArray);
        }
    }

    /// <summary>
    /// Converts a transient <see cref="CResult"/> pointer (valid only for the duration of
    /// a native callback) to a <see cref="ZerobusException"/>.
    /// Unlike <see cref="ToException(ref CResult)"/>, this overload does <b>not</b> free
    /// the error message — Rust owns and frees it after the callback returns.
    /// </summary>
    private static unsafe ZerobusException ToException(CResult* result)
    {
        var message = result->ErrorMessage != IntPtr.Zero
            ? Marshal.PtrToStringUTF8(result->ErrorMessage) ?? "unknown error"
            : "unknown error";
        return new ZerobusException(message, result->IsRetryable);
    }

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
    /// Creates a stream with OAuth credentials asynchronously.
    /// Returns immediately; the returned <see cref="Task{IntPtr}"/> completes on the Tokio
    /// thread when stream creation succeeds or fails.
    /// </summary>
    /// <remarks>
    /// <paramref name="descriptorProto"/> is copied by Rust before this method returns, so
    /// the span does not need to remain valid after the call.
    /// </remarks>
    public static unsafe Task<IntPtr> SdkCreateStreamAsync(
        IntPtr sdkPtr,
        string tableName,
        ReadOnlySpan<byte> descriptorProto,
        string clientId,
        string clientSecret,
        ref CStreamConfigurationOptions options)
    {
        var tcs = new TaskCompletionSource<IntPtr>(TaskCreationOptions.RunContinuationsAsynchronously);

        CreateStreamAsyncCallback callbackDelegate = (stream, result, _) =>
        {
            ApplyResult(tcs, (CResult*)result, stream);
        };

        var handle = GCHandle.Alloc(callbackDelegate);

        fixed (byte* descPtr = descriptorProto)
        {
            var result = new CResult();
            if (!NativeMethods.SdkCreateStreamAsync(
                    sdkPtr,
                    tableName,
                    descPtr,
                    (nuint)descriptorProto.Length,
                    clientId,
                    clientSecret,
                    ref options,
                    callbackDelegate,
                    IntPtr.Zero,
                    ref result))
            {
                var ex = ToException(ref result)
                         ?? new ZerobusException("Failed to schedule async stream creation", isRetryable: false);
                tcs.TrySetException(ex);
            }
        }

        _ = tcs.Task.ContinueWith(
            _ =>
            {
                if (handle.IsAllocated)
                    handle.Free();
            },
            TaskContinuationOptions.ExecuteSynchronously);

        return tcs.Task;
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
        HeadersProviderFreeCallback freeUserData,
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
                freeUserData,
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
    /// Creates a stream with a custom headers provider callback asynchronously.
    /// </summary>
    public static unsafe Task<IntPtr> SdkCreateStreamWithHeadersProviderAsync(
        IntPtr sdkPtr,
        string tableName,
        byte[] descriptorProto,
        HeadersProviderCallback headersCallback,
        IntPtr userData,
        CStreamConfigurationOptions options)
    {
        var tcs = new TaskCompletionSource<IntPtr>(TaskCreationOptions.RunContinuationsAsynchronously);

        CreateStreamAsyncCallback completionCallback = (stream, result, _) =>
        {
            ApplyResult(tcs, (CResult*)result, stream);
        };

        var callbackHandle = GCHandle.Alloc(completionCallback);

        fixed (byte* descPtr = descriptorProto)
        {
            var result = new CResult();
            if (!NativeMethods.SdkCreateStreamWithHeadersProviderAsync(
                    sdkPtr,
                    tableName,
                    descPtr,
                    (nuint)descriptorProto.Length,
                    headersCallback,
                    userData,
                    ref options,
                    completionCallback,
                    IntPtr.Zero,
                    ref result))
            {
                var ex = ToException(ref result)
                         ?? new ZerobusException("Failed to schedule async stream creation with headers provider", isRetryable: false);
                tcs.TrySetException(ex);
            }
        }

        _ = tcs.Task.ContinueWith(
            _ =>
            {
                if (callbackHandle.IsAllocated)
                    callbackHandle.Free();
            },
            TaskContinuationOptions.ExecuteSynchronously);

        return tcs.Task;
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
    /// Recreates a stream from an existing stream asynchronously.
    /// </summary>
    public static Task<IntPtr> SdkRecreateStreamAsync(
        IntPtr sdkPtr,
        IntPtr streamPtr)
    {
        var tcs = new TaskCompletionSource<IntPtr>(TaskCreationOptions.RunContinuationsAsynchronously);

        CreateStreamAsyncCallback callbackDelegate = (stream, result, _) =>
        {
            unsafe { ApplyResult(tcs, (CResult*)result, stream); }
        };

        var handle = GCHandle.Alloc(callbackDelegate);
        var scheduleResult = new CResult();
        if (!NativeMethods.SdkRecreateStreamAsync(
                sdkPtr,
                streamPtr,
                callbackDelegate,
                IntPtr.Zero,
                ref scheduleResult))
        {
            var ex = ToException(ref scheduleResult)
                     ?? new ZerobusException("Failed to schedule async stream recreation", isRetryable: false);
            tcs.TrySetException(ex);
        }

        _ = tcs.Task.ContinueWith(
            _ =>
            {
                if (handle.IsAllocated)
                    handle.Free();
            },
            TaskContinuationOptions.ExecuteSynchronously);

        return tcs.Task;
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
    /// Ingests a single protobuf record asynchronously and returns the offset.
    /// </summary>
    public static Task<long> StreamIngestProtoRecordAsync(
        IntPtr streamPtr,
        byte[] data)
    {
        if (data.Length == 0)
            return Task.FromException<long>(new ZerobusException("empty data", isRetryable: false));

        var tcs = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);

        OffsetAsyncCallback callback = (offset, result, _) =>
        {
            unsafe { ApplyResult(tcs, (CResult*)result, offset); }
        };

        var handle = GCHandle.Alloc(callback);

        unsafe
        {
            fixed (byte* dataPtr = data)
            {
                var scheduleResult = new CResult();
                if (!NativeMethods.StreamIngestProtoRecordAsync(
                        streamPtr,
                        dataPtr,
                        (nuint)data.Length,
                        callback,
                        IntPtr.Zero,
                        ref scheduleResult))
                {
                    var ex = ToException(ref scheduleResult)
                             ?? new ZerobusException("Failed to schedule async ingest", isRetryable: false);
                    tcs.TrySetException(ex);
                }
            }
        }

        _ = tcs.Task.ContinueWith(
            _ =>
            {
                if (handle.IsAllocated)
                    handle.Free();
            },
            TaskContinuationOptions.ExecuteSynchronously);

        return tcs.Task;
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
    /// Ingests a single JSON record asynchronously and returns the offset.
    /// </summary>
    public static Task<long> StreamIngestJsonRecordAsync(
        IntPtr streamPtr,
        string jsonData)
    {
        var tcs = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);

        OffsetAsyncCallback callback = (offset, result, _) =>
        {
            unsafe { ApplyResult(tcs, (CResult*)result, offset); }
        };

        var handle = GCHandle.Alloc(callback);

        var scheduleResult = new CResult();
        if (!NativeMethods.StreamIngestJsonRecordAsync(
                streamPtr,
                jsonData,
                callback,
                IntPtr.Zero,
                ref scheduleResult))
        {
            var ex = ToException(ref scheduleResult)
                     ?? new ZerobusException("Failed to schedule async ingest", isRetryable: false);
            tcs.TrySetException(ex);
        }

        _ = tcs.Task.ContinueWith(
            _ =>
            {
                if (handle.IsAllocated)
                    handle.Free();
            },
            TaskContinuationOptions.ExecuteSynchronously);

        return tcs.Task;
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
        var ptrs = records.Length * IntPtr.Size <= StackAllocThresholdBytes
            ? stackalloc IntPtr[records.Length]
            : new IntPtr[records.Length];
        var lens = records.Length * IntPtr.Size <= StackAllocThresholdBytes
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
    /// Ingests a batch of protobuf records asynchronously and returns the last offset.
    /// </summary>
    public static Task<long> StreamIngestProtoRecordsAsync(
        IntPtr streamPtr,
        byte[][] records)
    {
        if (records.Length == 0)
            return Task.FromResult(-1L);

        var tcs = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var numRecords = (nuint)records.Length;
        var handles = new GCHandle[records.Length];

        var callback = new OffsetAsyncCallback((offset, result, _) =>
        {
            unsafe { ApplyResult(tcs, (CResult*)result, offset == -2 ? -1 : offset); }
        });
        var callbackHandle = GCHandle.Alloc(callback);

        var ptrs = records.Length * IntPtr.Size <= StackAllocThresholdBytes
            ? stackalloc IntPtr[records.Length]
            : new IntPtr[records.Length];
        var lens = records.Length * IntPtr.Size <= StackAllocThresholdBytes
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

            unsafe
            {
                fixed (IntPtr* pointers = ptrs)
                fixed (nuint* lengths = lens)
                {
                    var scheduleResult = new CResult();
                    if (!NativeMethods.StreamIngestProtoRecordsAsync(
                            streamPtr,
                            (byte**)pointers,
                            lengths,
                            numRecords,
                            callback,
                            IntPtr.Zero,
                            ref scheduleResult))
                    {
                        var ex = ToException(ref scheduleResult)
                                 ?? new ZerobusException("Failed to schedule async batch ingest", isRetryable: false);
                        tcs.TrySetException(ex);
                    }
                }
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

        _ = tcs.Task.ContinueWith(
            _ =>
            {
                if (callbackHandle.IsAllocated)
                    callbackHandle.Free();
            },
            TaskContinuationOptions.ExecuteSynchronously);

        return tcs.Task;
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
        var ptrs = records.Length * IntPtr.Size <= StackAllocThresholdBytes
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
    /// Ingests a batch of JSON records asynchronously and returns the last offset.
    /// </summary>
    public static Task<long> StreamIngestJsonRecordsAsync(
        IntPtr streamPtr,
        string[] records)
    {
        if (records.Length == 0)
            return Task.FromResult(-1L);

        var tcs = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var numRecords = (nuint)records.Length;
        var handles = new GCHandle[records.Length];

        var callback = new OffsetAsyncCallback((offset, result, _) =>
        {
            unsafe { ApplyResult(tcs, (CResult*)result, offset == -2 ? -1 : offset); }
        });
        var callbackHandle = GCHandle.Alloc(callback);

        var ptrs = records.Length * IntPtr.Size <= StackAllocThresholdBytes
            ? stackalloc IntPtr[records.Length]
            : new IntPtr[records.Length];

        try
        {
            for (var i = 0; i < records.Length; i++)
            {
                var byteCount = Encoding.UTF8.GetByteCount(records[i]);
                var utf8 = new byte[byteCount + 1];
                Encoding.UTF8.GetBytes(records[i], utf8);
                handles[i] = GCHandle.Alloc(utf8, GCHandleType.Pinned);
                ptrs[i] = handles[i].AddrOfPinnedObject();
            }

            unsafe
            {
                fixed (IntPtr* pointers = ptrs)
                {
                    var scheduleResult = new CResult();
                    if (!NativeMethods.StreamIngestJsonRecordsAsync(
                            streamPtr,
                            (byte**)pointers,
                            numRecords,
                            callback,
                            IntPtr.Zero,
                            ref scheduleResult))
                    {
                        var ex = ToException(ref scheduleResult)
                                 ?? new ZerobusException("Failed to schedule async batch ingest", isRetryable: false);
                        tcs.TrySetException(ex);
                    }
                }
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

        _ = tcs.Task.ContinueWith(
            _ =>
            {
                if (callbackHandle.IsAllocated)
                    callbackHandle.Free();
            },
            TaskContinuationOptions.ExecuteSynchronously);

        return tcs.Task;
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
    /// Waits for a specific offset to be acknowledged asynchronously.
    /// </summary>
    public static Task StreamWaitForOffsetAsync(
        IntPtr streamPtr,
        long offset)
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        BoolAsyncCallback callback = (_, result, _) =>
        {
            unsafe { ApplyResult(tcs, (CResult*)result); }
        };

        var handle = GCHandle.Alloc(callback);

        var scheduleResult = new CResult();
        if (!NativeMethods.StreamWaitForOffsetAsync(
                streamPtr,
                offset,
                callback,
                IntPtr.Zero,
                ref scheduleResult))
        {
            var ex = ToException(ref scheduleResult)
                     ?? new ZerobusException("Failed to schedule async wait_for_offset", isRetryable: false);
            tcs.TrySetException(ex);
        }

        _ = tcs.Task.ContinueWith(
            _ =>
            {
                if (handle.IsAllocated)
                    handle.Free();
            },
            TaskContinuationOptions.ExecuteSynchronously);

        return tcs.Task;
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
    /// Flushes all pending records asynchronously.
    /// </summary>
    public static Task StreamFlushAsync(IntPtr streamPtr)
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        BoolAsyncCallback callback = (_, result, _) =>
        {
            unsafe { ApplyResult(tcs, (CResult*)result); }
        };

        var handle = GCHandle.Alloc(callback);

        var scheduleResult = new CResult();
        if (!NativeMethods.StreamFlushAsync(
                streamPtr,
                callback,
                IntPtr.Zero,
                ref scheduleResult))
        {
            var ex = ToException(ref scheduleResult)
                     ?? new ZerobusException("Failed to schedule async flush", isRetryable: false);
            tcs.TrySetException(ex);
        }

        _ = tcs.Task.ContinueWith(
            _ =>
            {
                if (handle.IsAllocated)
                    handle.Free();
            },
            TaskContinuationOptions.ExecuteSynchronously);

        return tcs.Task;
    }

    /// <summary>
    /// Retrieves all unacknowledged records from a closed/failed stream.
    /// </summary>
    public static ReadOnlyMemory<byte>[] StreamGetUnackedRecords(IntPtr streamPtr)
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

        try
        {
            var records = new ReadOnlyMemory<byte>[(int)cArray.Len];
            var recordSize = Marshal.SizeOf<CRecord>();

            for (var i = 0; i < (int)cArray.Len; i++)
            {
                var cRecord = Marshal.PtrToStructure<CRecord>(cArray.Records + i * recordSize);
                var data = new byte[(int)cRecord.DataLen];
                Marshal.Copy(cRecord.Data, data, 0, data.Length);

                records[i] = data;
            }

            return records;
        }
        finally
        {
            NativeMethods.FreeRecordArray(cArray);
        }
    }

    /// <summary>
    /// Retrieves all unacknowledged records from a closed/failed stream asynchronously.
    /// </summary>
    public static Task<ReadOnlyMemory<byte>[]> StreamGetUnackedRecordsAsync(
        IntPtr streamPtr)
    {
        var tcs = new TaskCompletionSource<ReadOnlyMemory<byte>[]>(TaskCreationOptions.RunContinuationsAsynchronously);

        RecordArrayAsyncCallback callback = (records, result, _) =>
        {
            unsafe
            {
                var cResult = (CResult*)result;
                if (!cResult->Success)
                {
                    tcs.TrySetException(ToException(cResult));
                    return;
                }
            }

            tcs.TrySetResult(ConvertRecordArrayAndFree(records));
        };

        var handle = GCHandle.Alloc(callback);

        var scheduleResult = new CResult();
        if (!NativeMethods.StreamGetUnackedRecordsAsync(
                streamPtr,
                callback,
                IntPtr.Zero,
                ref scheduleResult))
        {
            var ex = ToException(ref scheduleResult)
                     ?? new ZerobusException("Failed to schedule async get_unacked_records", isRetryable: false);
            tcs.TrySetException(ex);
        }

        _ = tcs.Task.ContinueWith(
            _ =>
            {
                if (handle.IsAllocated)
                    handle.Free();
            },
            TaskContinuationOptions.ExecuteSynchronously);

        return tcs.Task;
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
    /// Closes the stream gracefully asynchronously.
    /// </summary>
    public static Task StreamCloseAsync(IntPtr streamPtr)
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        BoolAsyncCallback callback = (_, result, _) =>
        {
            unsafe { ApplyResult(tcs, (CResult*)result); }
        };

        var handle = GCHandle.Alloc(callback);

        var scheduleResult = new CResult();
        if (!NativeMethods.StreamCloseAsync(
                streamPtr,
                callback,
                IntPtr.Zero,
                ref scheduleResult))
        {
            var ex = ToException(ref scheduleResult)
                     ?? new ZerobusException("Failed to schedule async close", isRetryable: false);
            tcs.TrySetException(ex);
        }

        _ = tcs.Task.ContinueWith(
            _ =>
            {
                if (handle.IsAllocated)
                    handle.Free();
            },
            TaskContinuationOptions.ExecuteSynchronously);

        return tcs.Task;
    }

    /// <summary>
    /// Converts managed <see cref="StreamConfigurationOptions"/> to the native struct,
    /// applying defaults for unset values.
    /// </summary>
    public static CStreamConfigurationOptions ConvertConfig(StreamConfigurationOptions? options)
    {
        if (options is null)
            return NativeMethods.GetDefaultConfig();

        var config = NativeMethods.GetDefaultConfig();

        config.MaxInflightRequests = (nuint)(options.MaxInflightRequests ?? config.MaxInflightRequests);
        config.Recovery = options.Recovery;
        config.RecoveryTimeoutMs = options.RecoveryTimeoutMs ?? config.RecoveryTimeoutMs;
        config.RecoveryBackoffMs = options.RecoveryBackoffMs ?? config.RecoveryBackoffMs;
        config.RecoveryRetries = options.RecoveryRetries ?? config.RecoveryRetries;
        config.ServerLackOfAckTimeoutMs = options.ServerLackOfAckTimeoutMs ?? config.ServerLackOfAckTimeoutMs;
        config.FlushTimeoutMs = options.FlushTimeoutMs ?? config.FlushTimeoutMs;
        if (options.RecordType != RecordType.Unspecified)
            config.RecordType = (int)options.RecordType;

        if (options.StreamPausedMaxWaitTimeMs.HasValue)
        {
            config.StreamPausedMaxWaitTimeMs = options.StreamPausedMaxWaitTimeMs.Value;
            config.HasStreamPausedMaxWaitTimeMs = true;
        }

        return config;
    }

    /// <summary>
    /// Allocates a new native SDK builder. Must be terminated with
    /// <see cref="SdkBuilderBuild"/> or <see cref="NativeMethods.SdkBuilderFree"/>.
    /// </summary>
    public static IntPtr SdkBuilderNew()
    {
        var ptr = NativeMethods.SdkBuilderNew();
        if (ptr == IntPtr.Zero)
            throw new ZerobusException("Failed to allocate SDK builder", isRetryable: false);
        return ptr;
    }

    /// <summary>
    /// Consumes the builder and returns a native SDK pointer.
    /// The builder pointer must not be used after this call.
    /// </summary>
    public static IntPtr SdkBuilderBuild(IntPtr builder)
    {
        var result = new CResult();
        var ptr = NativeMethods.SdkBuilderBuild(builder, ref result);

        if (ptr == IntPtr.Zero)
        {
            var ex = ToException(ref result);
            throw ex ?? new ZerobusException("Failed to build SDK", isRetryable: false);
        }

        return ptr;
    }
}
