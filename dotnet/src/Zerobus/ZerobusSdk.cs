using System.Runtime.InteropServices;
using Databricks.Zerobus.Native;

namespace Databricks.Zerobus;

/// <summary>
/// The main entry point for interacting with the Zerobus ingestion service.
/// Manages the connection to the Zerobus endpoint and Unity Catalog.
/// </summary>
/// <remarks>
/// <para>
/// This class wraps a native Rust SDK via P/Invoke and manages the lifecycle
/// of the underlying unmanaged resource. Always dispose when finished.
/// </para>
/// <para>
/// The SDK is thread-safe — you may create multiple streams from one instance.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// using var sdk = new ZerobusSdk(
///     "https://your-shard.zerobus.databricks.com",
///     "https://your-workspace.databricks.com");
///
/// var options = StreamConfigurationOptions.Default with
/// {
///     RecordType = RecordType.Json,
/// };
///
/// using var stream = sdk.CreateStream(
///     new TableProperties("catalog.schema.table"),
///     clientId,
///     clientSecret,
///     options);
/// </code>
/// </example>
public sealed class ZerobusSdk : IDisposable
{
    private IntPtr _ptr;
    private bool _disposed;

    /// <summary>
    /// Creates a new SDK instance.
    /// </summary>
    /// <param name="zerobusEndpoint">
    /// The gRPC endpoint for the Zerobus service
    /// (e.g. <c>https://zerobus.databricks.com</c>).
    /// </param>
    /// <param name="unityCatalogUrl">
    /// The Unity Catalog URL for OAuth token acquisition
    /// (e.g. <c>https://workspace.databricks.com</c>).
    /// </param>
    /// <exception cref="ZerobusException">
    /// Thrown if the SDK cannot be initialised (invalid URLs, etc.).
    /// </exception>
    public ZerobusSdk(string zerobusEndpoint, string unityCatalogUrl)
    {
        ArgumentNullException.ThrowIfNull(zerobusEndpoint);
        ArgumentNullException.ThrowIfNull(unityCatalogUrl);

        _ptr = NativeInterop.SdkNew(zerobusEndpoint, unityCatalogUrl);
    }

    /// <summary>
    /// Creates a new bidirectional gRPC stream for ingesting records into a Databricks table.
    /// Uses OAuth 2.0 client credentials flow for authentication.
    /// </summary>
    /// <param name="tableProperties">Table properties including name and optional protobuf descriptor.</param>
    /// <param name="clientId">OAuth 2.0 client ID.</param>
    /// <param name="clientSecret">OAuth 2.0 client secret.</param>
    /// <param name="options">
    /// Stream configuration options. Pass <c>null</c> or omit to use defaults.
    /// </param>
    /// <returns>A new <see cref="ZerobusStream"/> ready for record ingestion.</returns>
    /// <exception cref="ZerobusException">
    /// Thrown if the stream cannot be created (auth failure, invalid table, etc.).
    /// </exception>
    /// <exception cref="ObjectDisposedException">Thrown if the SDK has been disposed.</exception>
    /// <example>
    /// <code>
    /// using var stream = sdk.CreateStream(
    ///     new TableProperties("catalog.schema.table"),
    ///     clientId,
    ///     clientSecret);
    /// </code>
    /// </example>
    public ZerobusStream CreateStream(
        TableProperties tableProperties,
        string clientId,
        string clientSecret,
        StreamConfigurationOptions? options = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(tableProperties);
        ArgumentNullException.ThrowIfNull(clientId);
        ArgumentNullException.ThrowIfNull(clientSecret);

        var nativeOpts = NativeInterop.ConvertConfig(options);

        var streamPtr = NativeInterop.SdkCreateStream(
            _ptr,
            tableProperties.TableName,
            tableProperties.DescriptorProto ?? [],
            clientId,
            clientSecret,
            ref nativeOpts);

        return new ZerobusStream(streamPtr);
    }

    /// <summary>
    /// Creates a new bidirectional gRPC stream using a custom headers provider.
    /// Use this for custom authentication logic (managed identity, vaults, etc.).
    /// </summary>
    /// <param name="tableProperties">Table properties including name and optional protobuf descriptor.</param>
    /// <param name="headersProvider">Custom implementation of <see cref="IHeadersProvider"/>.</param>
    /// <param name="options">
    /// Stream configuration options. Pass <c>null</c> or omit to use defaults.
    /// </param>
    /// <returns>A new <see cref="ZerobusStream"/> ready for record ingestion.</returns>
    /// <exception cref="ZerobusException">
    /// Thrown if the stream cannot be created (headers provider error, network issues, etc.).
    /// </exception>
    /// <exception cref="ObjectDisposedException">Thrown if the SDK has been disposed.</exception>
    /// <example>
    /// <code>
    /// var provider = new CustomHeadersProvider();
    /// using var stream = sdk.CreateStreamWithHeadersProvider(
    ///     new TableProperties("catalog.schema.table"),
    ///     provider);
    /// </code>
    /// </example>
    public ZerobusStream CreateStreamWithHeadersProvider(
        TableProperties tableProperties,
        IHeadersProvider headersProvider,
        StreamConfigurationOptions? options = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(tableProperties);
        ArgumentNullException.ThrowIfNull(headersProvider);

        var nativeOpts = NativeInterop.ConvertConfig(options);

        // Create the callback bridge that the native code will invoke.
        var bridge = new HeadersProviderBridge(headersProvider);
        var callback = new HeadersProviderCallback(bridge.NativeCallback);

        // GCHandle keeps the bridge + callback alive for the lifetime of the stream.
        var handle = GCHandle.Alloc(bridge);

        IntPtr streamPtr;
        try
        {
            streamPtr = NativeInterop.SdkCreateStreamWithHeadersProvider(
                _ptr,
                tableProperties.TableName,
                tableProperties.DescriptorProto ?? [],
                callback,
                GCHandle.ToIntPtr(handle),
                ref nativeOpts);
        }
        catch
        {
            handle.Free();
            throw;
        }

        return new ZerobusStream(streamPtr, handle, callback);
    }

    /// <summary>
    /// Recreates a new bidirectional gRPC stream from an existing stream.
    /// This is used for recovery scenarios where a stream needs to be re-established
    /// using the existing stream's configuration and state.
    /// </summary>
    /// <param name="stream">The existing stream to recreate from.</param>
    /// <returns>A new <see cref="ZerobusStream"/> ready for record ingestion.</returns>
    /// <exception cref="ZerobusException">Thrown if the stream cannot be recreated.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the SDK has been disposed.</exception>
    public ZerobusStream RecreateStream(ZerobusStream stream)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(stream);

        var ptr = NativeInterop.SdkRecreateStream(_ptr, stream.NativePointer);
        return stream.Recreate(ptr);
    }

    private void Free()
    {
        var ptr = Interlocked.Exchange(ref _ptr, IntPtr.Zero);
        if (ptr != IntPtr.Zero)
        {
            NativeMethods.SdkFree(ptr);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        Free();
        GC.SuppressFinalize(this);
    }

    /// <summary>Safety-net release of native memory for leaked instances.</summary>
    ~ZerobusSdk() => Free();
}
