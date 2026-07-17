using System.Runtime.InteropServices;

namespace Databricks.Zerobus.Native;

/// <summary>
/// C interop structs that mirror the Zerobus C FFI (zerobus.h).
/// Layout must match exactly — changing field types or order breaks native ABI.
/// </summary>

[StructLayout(LayoutKind.Sequential)]
internal struct CHeader
{
    public IntPtr Key;   // char*
    public IntPtr Value; // char*
}

[StructLayout(LayoutKind.Sequential)]
internal struct CHeaders
{
    public IntPtr Headers;      // CHeader*
    public nuint Count;
    public IntPtr ErrorMessage; // char*
}

[StructLayout(LayoutKind.Sequential)]
internal struct CResult
{
    [MarshalAs(UnmanagedType.U1)]
    public bool Success;
    public IntPtr ErrorMessage; // char*
    [MarshalAs(UnmanagedType.U1)]
    public bool IsRetryable;
}

[StructLayout(LayoutKind.Sequential)]
internal struct CRecord
{
    [MarshalAs(UnmanagedType.U1)]
    public bool IsJson;
    public IntPtr Data;    // uint8_t*
    public nuint DataLen;
}

[StructLayout(LayoutKind.Sequential)]
internal struct CRecordArray
{
    public IntPtr Records; // CRecord*
    public nuint Len;
}

[StructLayout(LayoutKind.Sequential)]
internal struct CArrowBatchArray
{
    public IntPtr Batches;  // uint8_t**
    public IntPtr Lengths;  // uintptr_t*
    public nuint Count;
}

[StructLayout(LayoutKind.Sequential)]
internal struct CStreamConfigurationOptions
{
    public nuint MaxInflightRequests;          // uintptr_t
    [MarshalAs(UnmanagedType.U1)]
    public bool Recovery;
    public ulong RecoveryTimeoutMs;            // uint64_t
    public ulong RecoveryBackoffMs;            // uint64_t
    public uint RecoveryRetries;               // uint32_t
    public ulong ServerLackOfAckTimeoutMs;     // uint64_t
    public ulong FlushTimeoutMs;               // uint64_t
    public int RecordType;
    public ulong StreamPausedMaxWaitTimeMs;    // uint64_t
    [MarshalAs(UnmanagedType.U1)]
    public bool HasStreamPausedMaxWaitTimeMs;
    public ulong CallbackMaxWaitTimeMs;        // uint64_t
    [MarshalAs(UnmanagedType.U1)]
    public bool HasCallbackMaxWaitTimeMs;
    public IntPtr AckOnAck;                    // void (*)(int64_t, void*)
    public IntPtr AckOnError;                  // void (*)(int64_t, const char*, void*)
    public IntPtr AckUserData;                 // void*
}

[StructLayout(LayoutKind.Sequential)]
internal struct CArrowStreamConfigurationOptions
{
    public nuint MaxInflightBatches;           // uintptr_t
    [MarshalAs(UnmanagedType.U1)]
    public bool Recovery;
    public ulong RecoveryTimeoutMs;            // uint64_t
    public ulong RecoveryBackoffMs;            // uint64_t
    public uint RecoveryRetries;               // uint32_t
    public ulong ServerLackOfAckTimeoutMs;     // uint64_t
    public ulong FlushTimeoutMs;               // uint64_t
    public ulong ConnectionTimeoutMs;          // uint64_t
    public int IpcCompression;                 // -1=None, 0=LZ4_FRAME, 1=ZSTD
    public ulong StreamPausedMaxWaitTimeMs;    // uint64_t
}
