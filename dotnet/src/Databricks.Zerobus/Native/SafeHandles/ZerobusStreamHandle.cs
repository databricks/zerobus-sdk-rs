using Microsoft.Win32.SafeHandles;

namespace Databricks.Zerobus.Native;

/// <summary>
/// SafeHandle for a CZerobusStream opaque pointer.
/// </summary>
internal sealed class ZerobusStreamHandle : SafeHandleZeroOrMinusOneIsInvalid
{
    public ZerobusStreamHandle() : base(true) { }
    public ZerobusStreamHandle(IntPtr handle) : base(true) { SetHandle(handle); }

    protected override bool ReleaseHandle()
    {
        if (!IsInvalid && !IsClosed)
        {
            NativeMethods.zerobus_stream_free(handle);
        }
        return true;
    }
}
