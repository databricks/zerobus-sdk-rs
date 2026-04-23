// Bridges the managed IHeadersProvider interface to the native callback expected by Rust.
// This is the .NET equivalent of the goGetHeaders / cHeadersCallback pattern in ffi.go.

using System.Runtime.InteropServices;
using System.Text;

namespace Databricks.Zerobus.Native;

/// <summary>
/// Bridges a managed <see cref="IHeadersProvider"/> to the native
/// <see cref="HeadersProviderCallback"/> function pointer.
/// </summary>
internal sealed class HeadersProviderBridge
{
    private readonly IHeadersProvider _provider;

    public HeadersProviderBridge(IHeadersProvider provider)
    {
        _provider = provider;
    }

    /// <summary>
    /// The native callback that can be passed as a function pointer to Rust.
    /// Called from native code on the Rust runtime thread.
    /// </summary>
    public unsafe CHeaders NativeCallback(IntPtr userData)
    {
        var result = new CHeaders();

        try
        {
            var headers = _provider.GetHeaders();

            if (headers is null || headers.Count == 0)
            {
                result.Headers = IntPtr.Zero;
                result.Count = 0;
                result.ErrorMessage = IntPtr.Zero;
                return result;
            }

            // Allocate an array of CHeader structs in unmanaged memory.
            var headerSize = Marshal.SizeOf<CHeader>();
            var arrayPtr = (nint)NativeMemory.Alloc((nuint)(headerSize * headers.Count));

            int idx = 0;
            foreach (var (key, value) in headers)
            {
                var headerPtr = arrayPtr + idx * headerSize;
                var cHeader = new CHeader
                {
                    Key = AllocUtf8String(key),
                    Value = AllocUtf8String(value),
                };
                Marshal.StructureToPtr(cHeader, headerPtr, false);
                idx++;
            }

            result.Headers = arrayPtr;
            result.Count = (nuint)headers.Count;
            result.ErrorMessage = IntPtr.Zero;
        }
        catch (Exception ex)
        {
            result.Headers = IntPtr.Zero;
            result.Count = 0;
            result.ErrorMessage = AllocUtf8String(ex.Message);
        }

        return result;
    }

    private static unsafe IntPtr AllocUtf8String(string s)
    {
        var byteCount = Encoding.UTF8.GetByteCount(s);
        var ptr = (byte*) NativeMemory.Alloc((nuint)(byteCount + 1));
        Encoding.UTF8.GetBytes(s, new Span<byte>(ptr, byteCount));
        ptr[byteCount] = 0;
        return (IntPtr)ptr;
    }
}
