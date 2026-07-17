using Databricks.Zerobus.Native;
using System.Runtime.InteropServices;

namespace Databricks.Zerobus;

/// <summary>
/// Wraps a Zerobus protocol buffer schema generated from a Unity Catalog table.
/// Provides methods to get the descriptor bytes and encode JSON records to protobuf.
/// Must be disposed after all operations complete.
/// </summary>
public sealed class ProtoSchema : IDisposable
{
    private ZerobusProtoSchemaHandle _handle;
    private volatile int _disposed;

    private ProtoSchema(ZerobusProtoSchemaHandle handle)
    {
        _handle = handle;
    }

    /// <summary>
    /// Generates a proto schema from a Unity Catalog table JSON representation.
    /// The JSON should be the response from the Databricks Unity Catalog API's get-table endpoint.
    /// </summary>
    /// <param name="ucTableJson">JSON representation of the Unity Catalog table.</param>
    /// <returns>A new ProtoSchema instance.</returns>
    /// <exception cref="ZerobusException">Thrown if schema generation fails.</exception>
    public static ProtoSchema FromUnityCatalogJson(string ucTableJson)
    {
        if (string.IsNullOrWhiteSpace(ucTableJson))
            throw new ArgumentException("Unity Catalog table JSON must not be empty", nameof(ucTableJson));

        NativeLibraryResolver.EnsureLoaded();

        var result = new CResult();
        IntPtr raw = NativeMethods.zerobus_proto_schema_from_uc_json(ucTableJson, out result);

        if (!result.Success || raw == IntPtr.Zero)
        {
            string msg = Marshal.PtrToStringAnsi(result.ErrorMessage) ?? "Unknown error generating proto schema";
            SafeFreeErrorMessageIfNeeded(result.ErrorMessage);
            throw new ZerobusException(msg, isRetryable: result.IsRetryable);
        }

        return new ProtoSchema(new ZerobusProtoSchemaHandle(raw));
    }

    /// <summary>
    /// Returns the compiled protocol buffer descriptor bytes for this schema.
    /// The returned bytes are valid until this schema is disposed.
    /// </summary>
    public byte[] GetDescriptorBytes()
    {
        EnsureNotDisposed();

        UIntPtr outLen;
        IntPtr raw = NativeMethods.zerobus_proto_schema_descriptor_bytes(_handle.DangerousGetHandle(), out outLen);
        if (raw == IntPtr.Zero)
            throw new ZerobusException("Failed to get descriptor bytes from proto schema", isRetryable: false);

        int len = (int)(ulong)outLen;
        var bytes = new byte[len];
        Marshal.Copy(raw, bytes, 0, len);
        return bytes;
    }

    /// <summary>
    /// Encodes a JSON record string into protocol buffer bytes using this schema.
    /// The caller must free the returned bytes.
    /// </summary>
    /// <param name="json">The JSON record to encode.</param>
    /// <returns>The encoded protobuf bytes.</returns>
    public byte[] EncodeJson(string json)
    {
        if (json == null) throw new ArgumentNullException(nameof(json));
        EnsureNotDisposed();

        IntPtr outData;
        UIntPtr outLen;
        byte success = NativeMethods.zerobus_proto_schema_encode_json(
            _handle.DangerousGetHandle(), json, out outData, out outLen);

        if (success == 0 || outData == IntPtr.Zero)
            throw new ZerobusException("Failed to encode JSON to protobuf bytes", isRetryable: false);

        int len = (int)(ulong)outLen;
        var bytes = new byte[len];
        Marshal.Copy(outData, bytes, 0, len);

        NativeMethods.zerobus_free_proto_bytes(outData, outLen);

        return bytes;
    }

    /// <summary>
    /// Disposes the schema, freeing native resources.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            _handle.Dispose();
        }
    }

    private void EnsureNotDisposed()
    {
        if (_disposed != 0 || _handle.IsClosed || _handle.IsInvalid)
            throw new ObjectDisposedException(nameof(ProtoSchema));
    }

    private static void SafeFreeErrorMessageIfNeeded(IntPtr msg)
    {
        if (msg != IntPtr.Zero)
        {
            try { NativeMethods.zerobus_free_error_message(msg); }
            catch { /* best effort */ }
        }
    }
}
