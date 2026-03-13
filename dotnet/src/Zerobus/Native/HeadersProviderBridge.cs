// Bridges the managed IHeadersProvider interface to the native callback expected by Rust.
// This is the .NET equivalent of the goGetHeaders / cHeadersCallback pattern in ffi.go.

using System.Runtime.InteropServices;

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
    public CHeaders NativeCallback(IntPtr userData)
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
            var arrayPtr = Marshal.AllocHGlobal(headerSize * headers.Count);

            int idx = 0;
            foreach (var (key, value) in headers)
            {
                var headerPtr = arrayPtr + idx * headerSize;
                var cHeader = new CHeader
                {
                    Key = Marshal.StringToCoTaskMemUTF8(key),
                    Value = Marshal.StringToCoTaskMemUTF8(value),
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
            result.ErrorMessage = Marshal.StringToCoTaskMemUTF8(ex.Message);
        }

        return result;
    }
}
