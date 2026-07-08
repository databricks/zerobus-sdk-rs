// Bridges the managed IHeadersProvider interface to the native callback expected by Rust.
// This is the .NET equivalent of the goGetHeaders / cHeadersCallback pattern in ffi.go.

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
        IntPtr arrayPtr = IntPtr.Zero;
        nuint arrayCount = 0;

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

            // Allocate via Rust FFI so allocation/free always happen on the same heap.
            arrayCount = (nuint)headers.Count;
            arrayPtr = NativeMethods.AllocHeaderArray(arrayCount);
            if (arrayPtr == IntPtr.Zero)
            {
                throw new OutOfMemoryException("Failed to allocate headers array");
            }

            var headerArray = (CHeader*)arrayPtr;
            int idx = 0;
            foreach (var (key, value) in headers)
            {
                var cKey = AllocUtf8String(key);
                if (cKey == IntPtr.Zero)
                {
                    throw new OutOfMemoryException("Failed to allocate header key string");
                }

                var cValue = AllocUtf8String(value);
                if (cValue == IntPtr.Zero)
                {
                    throw new OutOfMemoryException("Failed to allocate header value string");
                }

                headerArray[idx] = new CHeader
                {
                    Key = cKey,
                    Value = cValue,
                };
                idx++;
            }

            result.Headers = arrayPtr;
            result.Count = arrayCount;
            result.ErrorMessage = IntPtr.Zero;
            arrayPtr = IntPtr.Zero;
            arrayCount = 0;
        }
        catch (Exception ex)
        {
            if (arrayPtr != IntPtr.Zero)
            {
                NativeMethods.FreeHeaders(new CHeaders
                {
                    Headers = arrayPtr,
                    Count = arrayCount,
                    ErrorMessage = IntPtr.Zero,
                });
            }

            result.Headers = IntPtr.Zero;
            result.Count = 0;
            result.ErrorMessage = AllocUtf8String(ex.Message);
        }

        return result;
    }

    private static unsafe IntPtr AllocUtf8String(string s)
    {
        var byteCount = Encoding.UTF8.GetByteCount(s);
        if (byteCount == 0)
        {
            return NativeMethods.AllocCString(null, 0);
        }

        var utf8 = Encoding.UTF8.GetBytes(s);
        fixed (byte* ptr = utf8)
        {
            return NativeMethods.AllocCString(ptr, (nuint)utf8.Length);
        }
    }
}
