using System.Runtime.InteropServices;
using System.Collections.Concurrent;

namespace Databricks.Zerobus.Native;

/// <summary>
/// Managed-to-native callback bridge for custom headers providers.
/// Keeps delegates alive via GCHandle and routes to a C# IHeadersProvider.
/// </summary>
internal sealed class HeadersProviderBridge : IDisposable
{
    private readonly HeadersProviderDelegate _provider;
    private readonly NativeMethods.HeadersProviderNativeCallback _nativeCallback;
    private GCHandle _handle;
    private volatile int _disposed;

    /// <summary>
    /// Creates a bridge from a managed headers provider.
    /// </summary>
    public HeadersProviderBridge(HeadersProviderDelegate provider)
    {
        _provider = provider ?? throw new ArgumentNullException(nameof(provider));
        _nativeCallback = OnGetHeaders;
        _handle = GCHandle.Alloc(this); // Keep this bridge alive for the native callback
    }

    /// <summary>
    /// Native callback delegate pointer. Pass this to native SDK functions
    /// that accept a HeadersProviderCallback.
    /// </summary>
    public IntPtr NativeCallbackPtr =>
        Marshal.GetFunctionPointerForDelegate(_nativeCallback);

    /// <summary>
    /// User data pointer. Pass this alongside the callback pointer.
    /// </summary>
    public IntPtr UserDataPtr =>
        GCHandle.ToIntPtr(_handle);

    /// <summary>
    /// The native callback implementation. Called from Rust across FFI.
    /// </summary>
    private CHeaders OnGetHeaders(IntPtr userData)
    {
        try
        {
            var headers = _provider();
            return BuildNativeHeaders(headers);
        }
        catch (Exception ex)
        {
            return new CHeaders
            {
                Headers = IntPtr.Zero,
                Count = 0,
                ErrorMessage = Marshal.StringToHGlobalAnsi(ex.Message)
            };
        }
    }

    private static unsafe CHeaders BuildNativeHeaders(IReadOnlyDictionary<string, string> headers)
    {
        if (headers == null || headers.Count == 0)
            return new CHeaders { Headers = IntPtr.Zero, Count = 0, ErrorMessage = IntPtr.Zero };

        nuint count = (nuint)headers.Count;
        IntPtr array = NativeMethods.zerobus_alloc_header_array(count);
        if (array == IntPtr.Zero)
            return new CHeaders
            {
                Headers = IntPtr.Zero,
                Count = 0,
                ErrorMessage = Marshal.StringToHGlobalAnsi("Failed to allocate header array")
            };

        int headerSize = Marshal.SizeOf(typeof(CHeader));
        int i = 0;
        foreach (var kv in headers)
        {
            IntPtr headerPtr = array + (i * headerSize);

            IntPtr keyPtr = AllocUtf8(kv.Key);
            IntPtr valuePtr = AllocUtf8(kv.Value);

            var header = new CHeader { Key = keyPtr, Value = valuePtr };
            Marshal.StructureToPtr(header, headerPtr, false);
            i++;
        }

        return new CHeaders { Headers = array, Count = count, ErrorMessage = IntPtr.Zero };
    }

    private static IntPtr AllocUtf8(string s)
    {
        byte[] bytes = System.Text.Encoding.UTF8.GetBytes(s);
        IntPtr ptr = Marshal.AllocHGlobal(bytes.Length + 1);
        Marshal.Copy(bytes, 0, ptr, bytes.Length);
        Marshal.WriteByte(ptr, bytes.Length, 0); // null terminator
        return ptr;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            if (_handle.IsAllocated)
                _handle.Free();
        }
    }
}
