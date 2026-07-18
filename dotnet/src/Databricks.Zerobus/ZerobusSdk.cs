using Databricks.Zerobus.Native;
using System.Runtime.InteropServices;

namespace Databricks.Zerobus;

/// <summary>
/// Main entry point for the Zerobus Ingest SDK. Manages the connection to the
/// Databricks Zerobus gRPC service and creates ingestion streams.
/// </summary>
/// <remarks>
/// <para>The SDK implements <see cref="IDisposable"/>. Always use <c>using</c> statements
/// or explicitly call <see cref="Dispose()"/> to free native resources.</para>
/// <para>The SDK is <b>not</b> thread-safe. Do not share SDK instances across threads
/// without external synchronization.</para>
/// <para>Use <see cref="CreateBuilder(string, string)"/> to configure and create SDK instances
/// with custom settings, or use the constructor for quick-start scenarios.</para>
///
/// <code>
/// using var sdk = ZerobusSdk.CreateBuilder(
///     "https://workspace.databricks.com",
///     "https://workspace.databricks.com/api/2.1/unity-catalog")
///     .Build();
///
/// await using var stream = await sdk.StreamBuilder()
///     .Table("my_catalog.my_schema.my_table")
///     .OAuth(clientId, clientSecret)
///     .Json()
///     .BuildAsync();
///
/// stream.IngestRecord("{\"id\": 1, \"name\": \"test\"}");
/// stream.Flush();
/// </code>
/// </remarks>
public sealed class ZerobusSdk : IDisposable
{
    private ZerobusSdkHandle _handle;
    private volatile int _disposed;

    /// <summary>
    /// The gRPC server endpoint URL.
    /// </summary>
    public string ServerEndpoint { get; }

    /// <summary>
    /// The Unity Catalog API endpoint URL.
    /// </summary>
    public string UnityCatalogEndpoint { get; }

    // Static initializer ensures native library is loaded once per process
    static ZerobusSdk()
    {
        NativeLibraryResolver.EnsureLoaded();
    }

    private ZerobusSdk(IntPtr nativeHandle, string serverEndpoint, string unityCatalogEndpoint)
    {
        _handle = new ZerobusSdkHandle(nativeHandle);
        ServerEndpoint = serverEndpoint ?? throw new ArgumentNullException(nameof(serverEndpoint));
        UnityCatalogEndpoint = unityCatalogEndpoint ?? throw new ArgumentNullException(nameof(unityCatalogEndpoint));
    }

    /// <summary>
    /// Creates a new SDK instance with the specified endpoints.
    /// For advanced configuration, use <see cref="CreateBuilder(string, string)"/> instead.
    /// </summary>
    /// <param name="serverEndpoint">The gRPC server endpoint URL.</param>
    /// <param name="unityCatalogEndpoint">The Unity Catalog API endpoint URL.</param>
    public ZerobusSdk(string serverEndpoint, string unityCatalogEndpoint)
        : this(CreateSdkNative(serverEndpoint, unityCatalogEndpoint), serverEndpoint, unityCatalogEndpoint)
    {
    }

    /// <summary>
    /// Creates a new SDK instance with an application name appended to the user-agent header.
    /// </summary>
    public ZerobusSdk(string serverEndpoint, string unityCatalogEndpoint, string applicationName)
        : this(CreateSdkNative(serverEndpoint, unityCatalogEndpoint, applicationName),
               serverEndpoint, unityCatalogEndpoint)
    {
    }

    /// <summary>
    /// Creates a new SDK builder for advanced configuration.
    /// </summary>
    /// <param name="serverEndpoint">The gRPC server endpoint URL.</param>
    /// <param name="unityCatalogEndpoint">The Unity Catalog API endpoint URL.</param>
    public static SdkBuilder CreateBuilder(string serverEndpoint, string unityCatalogEndpoint)
    {
        return new SdkBuilder(serverEndpoint, unityCatalogEndpoint);
    }

    private static IntPtr CreateSdkNative(string endpoint, string ucUrl, string? appName = null)
    {
        IntPtr builder = NativeMethods.zerobus_sdk_builder_new();
        if (builder == IntPtr.Zero)
            throw new ZerobusException("Failed to create SDK builder.", isRetryable: false);

        try
        {
            var endpointPtr = Marshal.StringToHGlobalAnsi(endpoint);
            var ucUrlPtr = Marshal.StringToHGlobalAnsi(ucUrl);
            try
            {
                NativeMethods.zerobus_sdk_builder_endpoint(builder, endpointPtr);
                NativeMethods.zerobus_sdk_builder_unity_catalog_url(builder, ucUrlPtr);

                if (!string.IsNullOrEmpty(appName))
                {
                    var appNamePtr = Marshal.StringToHGlobalAnsi(appName);
                    try
                    {
                        NativeMethods.zerobus_sdk_builder_application_name(builder, appNamePtr);
                    }
                    finally
                    {
                        Marshal.FreeHGlobal(appNamePtr);
                    }
                }
            }
            finally
            {
                Marshal.FreeHGlobal(endpointPtr);
                Marshal.FreeHGlobal(ucUrlPtr);
            }

            IntPtr sdk = NativeMethods.zerobus_sdk_builder_build(builder);
            // Builder is consumed/freed by build. If build fails, builder is freed but
            // the returned pointer is null.
            if (sdk == IntPtr.Zero)
            {
                throw new ZerobusException("Failed to build SDK. Check endpoint URLs and connectivity.",
                    isRetryable: true);
            }

            return sdk;
        }
        catch
        {
            // Builder may still be alive if build wasn't called; free it
            try { NativeMethods.zerobus_sdk_builder_free(builder); }
            catch { /* best effort */ }
            throw;
        }
    }

    /// <summary>
    /// Creates a new stream builder bound to this SDK instance.
    /// This is the recommended way to create streams.
    /// </summary>
    public StreamBuilder StreamBuilder() => new(this);

    /// <summary>
    /// Creates a protobuf ingestion stream. Internal use by StreamBuilder.
    /// </summary>
    internal async Task<ZerobusProtoStream<T>> CreateProtoStreamAsync<T>(
        string tableName,
        byte[] descriptorProtoBytes,
        string clientId,
        string clientSecret,
        StreamConfigurationOptions options) where T : Google.Protobuf.IMessage<T>
    {
        EnsureOpen();

        // The native stream creation is synchronous under the hood (gRPC connection
        // establishment is handled internally), but we wrap it in Task.Run for
        // non-blocking behavior.
        return await Task.Run(() => CreateProtoStreamInternal<T>(
            tableName, descriptorProtoBytes, clientId, clientSecret, options))
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Creates a JSON ingestion stream. Internal use by StreamBuilder.
    /// </summary>
    internal async Task<ZerobusJsonStream> CreateJsonStreamAsync(
        string tableName,
        string clientId,
        string clientSecret,
        StreamConfigurationOptions options)
    {
        EnsureOpen();
        return await Task.Run(() => CreateJsonStreamInternal(
            tableName, clientId, clientSecret, options))
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Creates an Arrow Flight ingestion stream. Internal use by StreamBuilder.
    /// </summary>
    internal async Task<ZerobusArrowStream> CreateArrowStreamAsync(
        string tableName,
        byte[] schemaIpcBytes,
        string clientId,
        string clientSecret,
        ArrowStreamConfigurationOptions options)
    {
        EnsureOpen();
        return await Task.Run(() => CreateArrowStreamInternal(
            tableName, schemaIpcBytes, clientId, clientSecret, options))
            .ConfigureAwait(false);
    }

    private ZerobusProtoStream<T> CreateProtoStreamInternal<T>(
        string tableName,
        byte[] descriptorProtoBytes,
        string clientId,
        string clientSecret,
        StreamConfigurationOptions options) where T : Google.Protobuf.IMessage<T>
    {
        NativeLibraryResolver.EnsureLoaded();

        var cOpts = options.ToNative();
        cOpts.RecordType = 1; // RecordType::Proto per Rust FFI mapping
        CResult result;
        GCHandle descHandle = default;
        IntPtr descPtr = IntPtr.Zero;
        if (descriptorProtoBytes != null && descriptorProtoBytes.Length > 0)
        {
            descHandle = GCHandle.Alloc(descriptorProtoBytes, GCHandleType.Pinned);
            descPtr = descHandle.AddrOfPinnedObject();
        }

        try
        {
            IntPtr stream = NativeMethods.zerobus_sdk_create_stream(
                _handle.DangerousGetHandle(),
                tableName,
                descPtr,
                (UIntPtr)(descriptorProtoBytes?.Length ?? 0),
                clientId,
                clientSecret,
                ref cOpts,
                out result);

            if (stream == IntPtr.Zero)
            {
                string msg = Marshal.PtrToStringAnsi(result.ErrorMessage) ?? "Failed to create proto stream";
                SafeFreeErrorMessage(result.ErrorMessage);
                throw new ZerobusException(msg, isRetryable: result.IsRetryable);
            }

            return new ZerobusProtoStream<T>(stream, tableName, options, descriptorProtoBytes!, clientId, clientSecret);
        }
        finally
        {
            if (descHandle.IsAllocated) descHandle.Free();
        }
    }

    private ZerobusJsonStream CreateJsonStreamInternal(
        string tableName,
        string clientId,
        string clientSecret,
        StreamConfigurationOptions options)
    {
        NativeLibraryResolver.EnsureLoaded();

        var cOpts = options.ToNative();
        cOpts.RecordType = 2; // RecordType::Json per Rust FFI mapping
        CResult result;

        IntPtr stream = NativeMethods.zerobus_sdk_create_stream(
            _handle.DangerousGetHandle(),
            tableName,
            IntPtr.Zero,   // null descriptor for JSON mode
            UIntPtr.Zero,
            clientId,
            clientSecret,
            ref cOpts,
            out result);

        if (stream == IntPtr.Zero)
        {
            string msg = Marshal.PtrToStringAnsi(result.ErrorMessage) ?? "Failed to create JSON stream";
            SafeFreeErrorMessage(result.ErrorMessage);
            throw new ZerobusException(msg, isRetryable: result.IsRetryable);
        }

        return new ZerobusJsonStream(stream, tableName, options, clientId, clientSecret);
    }

    private ZerobusArrowStream CreateArrowStreamInternal(
        string tableName,
        byte[] schemaIpcBytes,
        string clientId,
        string clientSecret,
        ArrowStreamConfigurationOptions options)
    {
        NativeLibraryResolver.EnsureLoaded();

        var cOpts = options.ToNative();
        CResult result;

        GCHandle schemaHandle = GCHandle.Alloc(schemaIpcBytes, GCHandleType.Pinned);
        try
        {
            IntPtr stream = NativeMethods.zerobus_sdk_create_arrow_stream(
                _handle.DangerousGetHandle(),
                tableName,
                schemaHandle.AddrOfPinnedObject(),
                (UIntPtr)schemaIpcBytes.Length,
                clientId,
                clientSecret,
                ref cOpts,
                out result);

            if (stream == IntPtr.Zero)
            {
                string msg = Marshal.PtrToStringAnsi(result.ErrorMessage) ?? "Failed to create Arrow stream";
                SafeFreeErrorMessage(result.ErrorMessage);
                throw new ZerobusException(msg, isRetryable: result.IsRetryable);
            }

            return new ZerobusArrowStream(stream, tableName, options, clientId, clientSecret);
        }
        finally
        {
            schemaHandle.Free();
        }
    }

    /// <summary>
    /// Recreates a stream for recovery purposes. Uses the stream's stored
    /// configuration to create a new native stream.
    /// </summary>
    internal async Task<ZerobusProtoStream<T>> RecreateStreamAsync<T>(
        ZerobusProtoStream<T> existing) where T : Google.Protobuf.IMessage<T>
    {
        if (existing == null) throw new ArgumentNullException(nameof(existing));
        return await CreateProtoStreamAsync<T>(
            existing.TableName,
            existing.DescriptorProtoBytes,
            existing.ClientId,
            existing.ClientSecret,
            existing.Options).ConfigureAwait(false);
    }

    /// <summary>
    /// Recreates a JSON stream for recovery purposes.
    /// </summary>
    internal async Task<ZerobusJsonStream> RecreateStreamAsync(ZerobusJsonStream existing)
    {
        if (existing == null) throw new ArgumentNullException(nameof(existing));
        return await CreateJsonStreamAsync(
            existing.TableName,
            existing.ClientId,
            existing.ClientSecret,
            existing.Options).ConfigureAwait(false);
    }

    /// <summary>
    /// Recreates an Arrow stream for recovery purposes.
    /// </summary>
    internal async Task<ZerobusArrowStream> RecreateStreamAsync(ZerobusArrowStream existing)
    {
        if (existing == null) throw new ArgumentNullException(nameof(existing));
        // Arrow schema IPC bytes aren't stored — caller provides them
        throw new NotSupportedException(
            "Arrow stream recreation requires the original schema IPC bytes. " +
            "Create a new stream via StreamBuilder().Arrow(schema).BuildAsync().");
    }

    /// <summary>
    /// Disposes the SDK, closing all streams and freeing native resources.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            _handle.Dispose();
        }
    }

    private void EnsureOpen()
    {
        if (_disposed != 0 || _handle.IsClosed || _handle.IsInvalid)
            throw new ZerobusException("SDK is closed or disposed.", isRetryable: false);
    }

    private static void SafeFreeErrorMessage(IntPtr msg)
    {
        if (msg != IntPtr.Zero)
        {
            try { NativeMethods.zerobus_free_error_message(msg); }
            catch { /* best effort */ }
        }
    }

    /// <summary>
    /// Builder for advanced SDK configuration.
    /// </summary>
    public sealed class SdkBuilder
    {
        private readonly string _endpoint;
        private readonly string _ucUrl;
        private string? _sdkIdentifier;
        private string? _applicationName;
        private bool _disableTls;

        internal SdkBuilder(string endpoint, string ucUrl)
        {
            _endpoint = endpoint ?? throw new ArgumentNullException(nameof(endpoint));
            _ucUrl = ucUrl ?? throw new ArgumentNullException(nameof(ucUrl));
        }

        /// <summary>
        /// Overrides the SDK identifier in the user-agent header.
        /// </summary>
        public SdkBuilder SdkIdentifier(string identifier)
        {
            _sdkIdentifier = identifier;
            return this;
        }

        /// <summary>
        /// Appends an application name to the user-agent header.
        /// </summary>
        public SdkBuilder ApplicationName(string name)
        {
            _applicationName = name;
            return this;
        }

        /// <summary>
        /// Disables TLS verification (for development/testing only).
        /// </summary>
        public SdkBuilder DisableTls()
        {
            _disableTls = true;
            return this;
        }

        /// <summary>
        /// Builds the SDK instance.
        /// </summary>
        public ZerobusSdk Build()
        {
            IntPtr builder = NativeMethods.zerobus_sdk_builder_new();
            if (builder == IntPtr.Zero)
                throw new ZerobusException("Failed to create SDK builder.", isRetryable: false);

            try
            {
                SetStringParam(builder, _endpoint, NativeMethods.zerobus_sdk_builder_endpoint);
                SetStringParam(builder, _ucUrl, NativeMethods.zerobus_sdk_builder_unity_catalog_url);

                if (!string.IsNullOrEmpty(_sdkIdentifier))
                    SetStringParam(builder, _sdkIdentifier!, NativeMethods.zerobus_sdk_builder_sdk_identifier);

                if (!string.IsNullOrEmpty(_applicationName))
                    SetStringParam(builder, _applicationName!, NativeMethods.zerobus_sdk_builder_application_name);

                if (_disableTls)
                    NativeMethods.zerobus_sdk_builder_disable_tls(builder);

                IntPtr sdk = NativeMethods.zerobus_sdk_builder_build(builder);
                if (sdk == IntPtr.Zero)
                    throw new ZerobusException("Failed to build SDK.", isRetryable: true);

                return new ZerobusSdk(sdk, _endpoint, _ucUrl);
            }
            catch
            {
                try { NativeMethods.zerobus_sdk_builder_free(builder); }
                catch { /* best effort */ }
                throw;
            }
        }

        private static void SetStringParam(IntPtr builder, string value, Action<IntPtr, IntPtr> setter)
        {
            IntPtr ptr = Marshal.StringToHGlobalAnsi(value);
            try { setter(builder, ptr); }
            finally { Marshal.FreeHGlobal(ptr); }
        }
    }
}

/// <summary>
/// Extension methods for converting managed options to native structs.
/// </summary>
internal static class OptionsExtensions
{
    internal static CStreamConfigurationOptions ToNative(this StreamConfigurationOptions opts)
    {
        return new CStreamConfigurationOptions
        {
            MaxInflightRequests = (nuint)opts.MaxInflightRecords,
            Recovery = opts.Recovery,
            RecoveryTimeoutMs = (ulong)opts.RecoveryTimeoutMs,
            RecoveryBackoffMs = (ulong)opts.RecoveryBackoffMs,
            RecoveryRetries = (uint)opts.RecoveryRetries,
            ServerLackOfAckTimeoutMs = (ulong)opts.ServerLackOfAckTimeoutMs,
            FlushTimeoutMs = (ulong)opts.FlushTimeoutMs,
            RecordType = 0,
            StreamPausedMaxWaitTimeMs = opts.StreamPausedMaxWaitTimeMs.HasValue ? (ulong)opts.StreamPausedMaxWaitTimeMs.Value : 0UL,
            HasStreamPausedMaxWaitTimeMs = opts.StreamPausedMaxWaitTimeMs.HasValue,
            CallbackMaxWaitTimeMs = opts.CallbackMaxWaitTimeMs.HasValue ? (ulong)opts.CallbackMaxWaitTimeMs.Value : 0UL,
            HasCallbackMaxWaitTimeMs = opts.CallbackMaxWaitTimeMs.HasValue,
            AckOnAck = IntPtr.Zero,
            AckOnError = IntPtr.Zero,
            AckUserData = IntPtr.Zero,
        };
    }

    internal static CArrowStreamConfigurationOptions ToNative(this ArrowStreamConfigurationOptions opts)
    {
        return new CArrowStreamConfigurationOptions
        {
            MaxInflightBatches = (nuint)opts.MaxInflightBatches,
            Recovery = opts.Recovery,
            RecoveryTimeoutMs = (ulong)opts.RecoveryTimeoutMs,
            RecoveryBackoffMs = (ulong)opts.RecoveryBackoffMs,
            RecoveryRetries = (uint)opts.RecoveryRetries,
            ServerLackOfAckTimeoutMs = (ulong)opts.ServerLackOfAckTimeoutMs,
            FlushTimeoutMs = (ulong)opts.FlushTimeoutMs,
            ConnectionTimeoutMs = (ulong)opts.ConnectionTimeoutMs,
            IpcCompression = (int)opts.IpcCompression,
            StreamPausedMaxWaitTimeMs = (ulong)opts.StreamPausedMaxWaitTimeMs,
        };
    }
}
