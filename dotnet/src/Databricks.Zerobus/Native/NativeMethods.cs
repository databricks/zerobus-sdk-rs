using System.Runtime.InteropServices;

namespace Databricks.Zerobus.Native;

/// <summary>
/// C FFI function declarations for the Zerobus native library.
/// All functions use Cdecl calling convention and map to zerobus_ffi.{dll,so,dylib}.
/// </summary>
internal static partial class NativeMethods
{
    // The library name is resolved at runtime by NativeLibraryResolver.
    // This constant is used as a fallback and for DllImport decoration.
    private const string LibraryName = "zerobus_ffi";

    // ==================== SDK Builder API ====================

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr zerobus_sdk_builder_new();

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void zerobus_sdk_builder_endpoint(IntPtr builder, IntPtr endpoint);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void zerobus_sdk_builder_unity_catalog_url(IntPtr builder, IntPtr url);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void zerobus_sdk_builder_sdk_identifier(IntPtr builder, IntPtr identifier);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void zerobus_sdk_builder_application_name(IntPtr builder, IntPtr name);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void zerobus_sdk_builder_disable_tls(IntPtr builder);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr zerobus_sdk_builder_build(IntPtr builder);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void zerobus_sdk_builder_free(IntPtr builder);

    // ==================== Legacy SDK API (ABI back-compat) ====================

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr zerobus_sdk_new(
        [MarshalAs(UnmanagedType.LPStr)] string endpoint,
        [MarshalAs(UnmanagedType.LPStr)] string ucUrl,
        out CResult result);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void zerobus_sdk_free(IntPtr sdk);

    // ==================== Stream Creation (gRPC JSON/Proto) ====================

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr zerobus_sdk_create_stream(
        IntPtr sdk,
        [MarshalAs(UnmanagedType.LPStr)] string tableName,
        IntPtr descriptorProtoBytes,
        UIntPtr descriptorProtoLen,
        [MarshalAs(UnmanagedType.LPStr)] string clientId,
        [MarshalAs(UnmanagedType.LPStr)] string clientSecret,
        ref CStreamConfigurationOptions options,
        out CResult result);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr zerobus_sdk_create_stream_with_headers_provider(
        IntPtr sdk,
        [MarshalAs(UnmanagedType.LPStr)] string tableName,
        IntPtr descriptorProtoBytes,
        UIntPtr descriptorProtoLen,
        HeadersProviderNativeCallback headersProvider,
        IntPtr userData,
        ref CStreamConfigurationOptions options,
        out CResult result);

    // ==================== Stream Lifecycle (Generic) ====================

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void zerobus_stream_free(IntPtr stream);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern byte zerobus_stream_flush(IntPtr stream);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern byte zerobus_stream_close(IntPtr stream);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern byte zerobus_stream_is_closed(IntPtr stream);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern byte zerobus_stream_wait_for_offset(
        IntPtr stream,
        long offset,
        out CResult result);

    // ==================== Record Ingestion (Generic Stream) ====================

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long zerobus_stream_ingest_proto_record(
        IntPtr stream,
        IntPtr data,
        UIntPtr dataLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long zerobus_stream_ingest_json_record(
        IntPtr stream,
        IntPtr data,
        UIntPtr dataLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long zerobus_stream_ingest_proto_records(
        IntPtr stream,
        [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 2)]
        IntPtr[] records,
        IntPtr[] lengths,
        UIntPtr count);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long zerobus_stream_ingest_json_records(
        IntPtr stream,
        [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 2)]
        IntPtr[] records,
        IntPtr[] lengths,
        UIntPtr count);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void zerobus_stream_ingest_proto_record_nowait(
        IntPtr stream,
        IntPtr data,
        UIntPtr dataLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void zerobus_stream_ingest_json_record_nowait(
        IntPtr stream,
        IntPtr data,
        UIntPtr dataLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void zerobus_stream_ingest_proto_records_nowait(
        IntPtr stream,
        [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 2)]
        IntPtr[] records,
        IntPtr[] lengths,
        UIntPtr count);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void zerobus_stream_ingest_json_records_nowait(
        IntPtr stream,
        [MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 2)]
        IntPtr[] records,
        IntPtr[] lengths,
        UIntPtr count);

    // ==================== Unacked Records ====================

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern CRecordArray zerobus_stream_get_unacked_records(IntPtr stream);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void zerobus_free_record_array(CRecordArray array);

    // ==================== Arrow Stream API ====================

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr zerobus_sdk_create_arrow_stream(
        IntPtr sdk,
        [MarshalAs(UnmanagedType.LPStr)] string tableName,
        IntPtr schemaIpcBytes,
        UIntPtr schemaIpcLen,
        [MarshalAs(UnmanagedType.LPStr)] string clientId,
        [MarshalAs(UnmanagedType.LPStr)] string clientSecret,
        ref CArrowStreamConfigurationOptions options,
        out CResult result);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr zerobus_sdk_create_arrow_stream_with_headers_provider(
        IntPtr sdk,
        [MarshalAs(UnmanagedType.LPStr)] string tableName,
        IntPtr schemaIpcBytes,
        UIntPtr schemaIpcLen,
        HeadersProviderNativeCallback headersProvider,
        IntPtr userData,
        ref CArrowStreamConfigurationOptions options,
        out CResult result);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void zerobus_arrow_stream_free(IntPtr stream);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long zerobus_arrow_stream_ingest_batch(
        IntPtr stream,
        IntPtr ipcBytes,
        UIntPtr ipcLen,
        out CResult result);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern byte zerobus_arrow_stream_wait_for_offset(
        IntPtr stream,
        long offset,
        out CResult result);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern byte zerobus_arrow_stream_flush(IntPtr stream);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern byte zerobus_arrow_stream_close(IntPtr stream);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern CArrowBatchArray zerobus_arrow_stream_get_unacked_batches(IntPtr stream);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void zerobus_arrow_free_batch_array(CArrowBatchArray array);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern byte zerobus_arrow_stream_is_closed(IntPtr stream);

    // ==================== Configuration Defaults ====================

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern CStreamConfigurationOptions zerobus_get_default_config();

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern CArrowStreamConfigurationOptions zerobus_arrow_get_default_config();

    // ==================== Protobuf Schema API ====================

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr zerobus_proto_schema_from_uc_json(
        [MarshalAs(UnmanagedType.LPStr)] string ucTableJson,
        out CResult result);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr zerobus_proto_schema_descriptor_bytes(
        IntPtr schema,
        out UIntPtr outLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern byte zerobus_proto_schema_encode_json(
        IntPtr schema,
        [MarshalAs(UnmanagedType.LPStr)] string json,
        out IntPtr outData,
        out UIntPtr outLen);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void zerobus_free_proto_bytes(IntPtr data, UIntPtr len);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void zerobus_proto_schema_free(IntPtr schema);

    // ==================== Helper/Allocator Functions ====================

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr zerobus_alloc_header_array(UIntPtr count);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr zerobus_alloc_cstring(IntPtr data, UIntPtr len);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void zerobus_free_headers(CHeaders headers);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void zerobus_free_error_message(IntPtr errorMessage);

    // ==================== Delegate Types ====================

    /// <summary>
    /// Native callback for custom headers provider.
    /// Returns a CHeaders struct by value.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate CHeaders HeadersProviderNativeCallback(IntPtr userData);
}
