using System.Reflection;
using Databricks.Zerobus.Native;

namespace Databricks.Zerobus;

/// <summary>
/// A builder for creating <see cref="ZerobusSdk"/> instances with optional configuration.
/// </summary>
/// <remarks>
/// <para>
/// Obtain an instance via <see cref="ZerobusSdk.CreateBuilder()"/> and call the
/// fluent setter methods before calling <see cref="Build"/>.
/// </para>
/// <para>
/// The builder is single-use: calling <see cref="Build"/> or <see cref="Dispose"/>
/// consumes it. Any further method calls after either will throw
/// <see cref="ObjectDisposedException"/>.
/// </para>
/// </remarks>
/// <example>
/// OAuth credentials:
/// <code>
/// using var sdk = ZerobusSdk.CreateBuilder()
///     .Endpoint("https://zerobus.databricks.com")
///     .UnityCatalogUrl("https://workspace.databricks.com")
///     .ApplicationName("my-service")
///     .Build();
/// </code>
///
/// Custom headers provider (Unity Catalog URL not required):
/// <code>
/// using var sdk = ZerobusSdk.CreateBuilder()
///     .Endpoint("https://zerobus.databricks.com")
///     .ApplicationName("my-service")
///     .Build();
/// </code>
/// </example>
public sealed class ZerobusSdkBuilder : IDisposable
{
    private string? _endpoint;
    private string? _unityCatalogUrl;
    private string? _sdkIdentifier;
    private string? _applicationName;
    private bool _disableTls;
    private int _consumed;  // 0 = live, 1 = consumed/disposed

    // Sent when the caller doesn't set SdkIdentifier(); without it the Rust core's own
    // identifier goes on the wire and .NET traffic is attributed to Rust.
    private static readonly string DefaultSdkIdentifier = $"zerobus-sdk-dotnet/{SdkVersion()}";

    // The informational version carries <Version> from the csproj, unlike AssemblyVersion,
    // which callers may pin separately. SourceLink appends "+<commit>" to it.
    private static string SdkVersion()
    {
        var informational = typeof(ZerobusSdkBuilder).Assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;

        return string.IsNullOrEmpty(informational)
            ? "0.0.0"
            : informational.Split('+')[0];
    }

    internal ZerobusSdkBuilder() { }

    /// <summary>
    /// Sets the Zerobus gRPC endpoint URL (required).
    /// </summary>
    /// <param name="endpoint">The gRPC endpoint (e.g. <c>https://zerobus.databricks.com</c>).</param>
    /// <returns>This builder, for chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="endpoint"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the builder has been consumed or disposed.</exception>
    public ZerobusSdkBuilder Endpoint(string endpoint)
    {
        ThrowIfConsumed();
        ArgumentNullException.ThrowIfNull(endpoint);
        _endpoint = endpoint;
        return this;
    }

    /// <summary>
    /// Sets the Unity Catalog URL used for OAuth token acquisition.
    /// Required when using OAuth credentials; optional with a custom headers provider.
    /// </summary>
    /// <param name="unityCatalogUrl">The Unity Catalog URL (e.g. <c>https://workspace.databricks.com</c>).</param>
    /// <returns>This builder, for chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="unityCatalogUrl"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the builder has been consumed or disposed.</exception>
    public ZerobusSdkBuilder UnityCatalogUrl(string unityCatalogUrl)
    {
        ThrowIfConsumed();
        ArgumentNullException.ThrowIfNull(unityCatalogUrl);
        _unityCatalogUrl = unityCatalogUrl;
        return this;
    }

    /// <summary>
    /// Overrides the SDK identifier portion of the <c>User-Agent</c> header.
    /// Wrapper SDKs use this to identify themselves; end-user code should prefer
    /// <see cref="ApplicationName"/> instead.
    /// </summary>
    /// <remarks>
    /// Without this call the SDK sends <c>zerobus-sdk-dotnet/&lt;version&gt;</c>. A blank
    /// or whitespace-only value falls back to that same default.
    /// </remarks>
    /// <param name="sdkIdentifier">The SDK identifier string.</param>
    /// <returns>This builder, for chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="sdkIdentifier"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the builder has been consumed or disposed.</exception>
    public ZerobusSdkBuilder SdkIdentifier(string sdkIdentifier)
    {
        ThrowIfConsumed();
        ArgumentNullException.ThrowIfNull(sdkIdentifier);
        _sdkIdentifier = sdkIdentifier;
        return this;
    }

    /// <summary>
    /// Appends an application name to the <c>User-Agent</c> header.
    /// Useful for identifying which application is sending data in server-side logs.
    /// </summary>
    /// <param name="applicationName">The application name.</param>
    /// <returns>This builder, for chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="applicationName"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the builder has been consumed or disposed.</exception>
    public ZerobusSdkBuilder ApplicationName(string applicationName)
    {
        ThrowIfConsumed();
        ArgumentNullException.ThrowIfNull(applicationName);
        _applicationName = applicationName;
        return this;
    }

    /// <summary>
    /// Disables TLS for the gRPC connection. TLS is enabled by default.
    /// Only use this for local development or testing.
    /// </summary>
    /// <returns>This builder, for chaining.</returns>
    /// <exception cref="ObjectDisposedException">Thrown if the builder has been consumed or disposed.</exception>
    public ZerobusSdkBuilder DisableTls()
    {
        ThrowIfConsumed();
        _disableTls = true;
        return this;
    }

    /// <summary>
    /// Builds and returns a <see cref="ZerobusSdk"/> instance.
    /// Consumes the builder — any further calls will throw <see cref="ObjectDisposedException"/>.
    /// </summary>
    /// <returns>A fully initialised <see cref="ZerobusSdk"/>.</returns>
    /// <exception cref="ZerobusException">Thrown if the SDK cannot be initialised.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the builder has already been consumed or disposed.</exception>
    public ZerobusSdk Build()
    {
        ThrowIfConsumed();

        if (Interlocked.CompareExchange(ref _consumed, 1, 0) != 0)
            throw new ObjectDisposedException(nameof(ZerobusSdkBuilder));

        var builderPtr = NativeInterop.SdkBuilderNew();

        try
        {
            if (_endpoint is not null)
                NativeMethods.SdkBuilderEndpoint(builderPtr, _endpoint);
            if (_unityCatalogUrl is not null)
                NativeMethods.SdkBuilderUnityCatalogUrl(builderPtr, _unityCatalogUrl);
            // The core substitutes its own identifier only for a literal empty override, not
            // for whitespace, so a blank value would otherwise reach the wire verbatim.
            NativeMethods.SdkBuilderSdkIdentifier(
                builderPtr,
                string.IsNullOrWhiteSpace(_sdkIdentifier) ? DefaultSdkIdentifier : _sdkIdentifier);
            if (_applicationName is not null)
                NativeMethods.SdkBuilderApplicationName(builderPtr, _applicationName);
            if (_disableTls)
                NativeMethods.SdkBuilderDisableTls(builderPtr);
        }
        catch
        {
            NativeMethods.SdkBuilderFree(builderPtr);
            throw;
        }

        // SdkBuilderBuild consumes the native pointer on both success and failure.
        var sdkPtr = NativeInterop.SdkBuilderBuild(builderPtr);
        return new ZerobusSdk(sdkPtr);
    }

    private void ThrowIfConsumed()
    {
        ObjectDisposedException.ThrowIf(_consumed != 0, this);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        Interlocked.Exchange(ref _consumed, 1);
    }
}
