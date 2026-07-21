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

    // Delegate instances are held here so their native thunks stay alive exactly
    // as long as this bridge does. On the ownership-transfer (sync) path the
    // bridge is rooted only by the GCHandle handed to the FFI, so rooting the
    // delegates through the bridge keeps both callbacks valid until the FFI
    // releases that handle via NativeFree.
    public HeadersProviderCallback Callback { get; }
    public HeadersProviderFreeCallback FreeCallback { get; }

    public HeadersProviderBridge(IHeadersProvider provider)
    {
        _provider = provider;
        Callback = NativeCallback;
        FreeCallback = NativeFree;
    }

    /// <summary>
    /// Native destroy callback matching <see cref="HeadersProviderFreeCallback"/>.
    /// The FFI owns <paramref name="userData"/> (a <see cref="GCHandle"/> to the
    /// bridge) and invokes this exactly once — after any in-flight
    /// <see cref="NativeCallback"/> has returned — so freeing the handle here
    /// cannot race a live callback. This is what closes the recovery-vs-teardown
    /// use-after-free; it is the .NET equivalent of goFreeHeadersProvider /
    /// zerobus_cpp_headers_free. Must not throw across the native boundary.
    /// </summary>
    public static void NativeFree(IntPtr userData)
    {
        if (userData == IntPtr.Zero)
        {
            return;
        }

        try
        {
            var handle = GCHandle.FromIntPtr(userData);
            if (handle.IsAllocated)
            {
                handle.Free();
            }
        }
        catch
        {
            // Never let an exception unwind across the native boundary.
        }
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
        int filledCount = 0;

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

                headerArray[filledCount] = new CHeader
                {
                    Key = cKey,
                    Value = cValue,
                };
                filledCount++;
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
                // Only free the headers that were actually filled, not the entire allocated count.
                // This prevents attempting to free uninitialized memory if an allocation failed mid-loop.
                NativeMethods.FreeHeaders(new CHeaders
                {
                    Headers = arrayPtr,
                    Count = (nuint)filledCount,
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
