using Databricks.Zerobus.Native;

namespace Databricks.Zerobus;

/// <summary>
/// Wraps a Zerobus protocol buffer schema generated from a Unity Catalog table.
/// Provides methods to get compiled descriptor bytes and to encode JSON records
/// into protobuf format matching the table schema.
/// </summary>
/// <remarks>
/// <para>
/// Use <see cref="FromUnityCatalogJson"/> to create a schema from the JSON response
/// of the Databricks Unity Catalog API's get-table endpoint.
/// </para>
/// <para>
/// The schema handle is a native resource and must be disposed when no longer needed.
/// Use <c>using</c> statements to guarantee cleanup.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// using var schema = ProtoSchema.FromUnityCatalogJson(ucTableJson);
/// byte[] descriptor = schema.GetDescriptorBytes();
/// byte[] protoBytes = schema.EncodeJson("{\"id\": 1, \"name\": \"test\"}");
/// </code>
/// </example>
public sealed class ProtoSchema : IDisposable
{
    private IntPtr _ptr;
    private int _disposed;

    private ProtoSchema(IntPtr ptr)
    {
        _ptr = ptr;
    }

    /// <summary>
    /// Creates a proto schema from a Unity Catalog table JSON representation.
    /// The JSON should be the response body from the Databricks Unity Catalog API's
    /// <c>GET /api/2.1/unity-catalog/tables/&lt;catalog&gt;.&lt;schema&gt;.&lt;table&gt;</c> endpoint.
    /// </summary>
    /// <param name="ucTableJson">JSON representation of the Unity Catalog table.</param>
    /// <returns>A new <see cref="ProtoSchema"/> instance.</returns>
    /// <exception cref="ArgumentException">Thrown if <paramref name="ucTableJson"/> is empty.</exception>
    /// <exception cref="ZerobusException">Thrown if schema generation fails.</exception>
    public static ProtoSchema FromUnityCatalogJson(string ucTableJson)
    {
        if (string.IsNullOrWhiteSpace(ucTableJson))
            throw new ArgumentException("Unity Catalog table JSON must not be empty", nameof(ucTableJson));

        var ptr = NativeInterop.ProtoSchemaFromUcJson(ucTableJson);
        return new ProtoSchema(ptr);
    }

    /// <summary>
    /// Returns the compiled protocol buffer descriptor bytes for this schema.
    /// These bytes can be passed to <c>ZerobusSdk.CreateProtoStream</c> as the
    /// <c>descriptorProto</c> parameter.
    /// </summary>
    /// <returns>A byte array containing the serialized FileDescriptorProto.</returns>
    /// <exception cref="ObjectDisposedException">Thrown if this schema has been disposed.</exception>
    /// <exception cref="ZerobusException">Thrown if descriptor retrieval fails.</exception>
    public byte[] GetDescriptorBytes()
    {
        EnsureNotDisposed();
        return NativeInterop.ProtoSchemaDescriptorBytes(_ptr);
    }

    /// <summary>
    /// Encodes a JSON record string into protocol buffer bytes using this schema.
    /// </summary>
    /// <param name="json">The JSON record to encode. Must match the table schema.</param>
    /// <returns>The encoded protobuf bytes.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="json"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if this schema has been disposed.</exception>
    /// <exception cref="ZerobusException">Thrown if encoding fails due to schema mismatch.</exception>
    public byte[] EncodeJson(string json)
    {
        ArgumentNullException.ThrowIfNull(json);
        EnsureNotDisposed();
        return NativeInterop.ProtoSchemaEncodeJson(_ptr, json);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0) return;

        var ptr = Interlocked.Exchange(ref _ptr, IntPtr.Zero);
        if (ptr != IntPtr.Zero)
        {
            NativeMethods.ProtoSchemaFree(ptr);
        }

        GC.SuppressFinalize(this);
    }

    /// <summary>Safety-net release of native memory for leaked instances.</summary>
    ~ProtoSchema()
    {
        var ptr = Interlocked.Exchange(ref _ptr, IntPtr.Zero);
        if (ptr != IntPtr.Zero)
        {
            NativeMethods.ProtoSchemaFree(ptr);
        }
    }

    private void EnsureNotDisposed()
    {
        if (_disposed != 0 || _ptr == IntPtr.Zero)
            throw new ObjectDisposedException(nameof(ProtoSchema));
    }
}
