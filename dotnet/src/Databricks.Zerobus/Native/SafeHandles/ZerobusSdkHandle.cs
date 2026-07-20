using Microsoft.Win32.SafeHandles;

namespace Databricks.Zerobus.Native;

/// <summary>
/// SafeHandle for a CZerobusSdk opaque pointer.
/// </summary>
internal sealed class ZerobusSdkHandle : SafeHandleZeroOrMinusOneIsInvalid
{
    public ZerobusSdkHandle() : base(true) { }
    public ZerobusSdkHandle(IntPtr handle) : base(true) { SetHandle(handle); }

    protected override bool ReleaseHandle()
    {
        if (!IsInvalid && !IsClosed)
        {
            NativeMethods.zerobus_sdk_free(handle);
        }
        return true;
    }
}
