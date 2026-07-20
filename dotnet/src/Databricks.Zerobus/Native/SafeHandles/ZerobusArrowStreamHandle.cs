using Microsoft.Win32.SafeHandles;

namespace Databricks.Zerobus.Native;

/// <summary>
/// SafeHandle for a CArrowStream opaque pointer.
/// </summary>
internal sealed class ZerobusArrowStreamHandle : SafeHandleZeroOrMinusOneIsInvalid
{
    public ZerobusArrowStreamHandle() : base(true) { }
    public ZerobusArrowStreamHandle(IntPtr handle) : base(true) { SetHandle(handle); }

    protected override bool ReleaseHandle()
    {
        if (!IsInvalid && !IsClosed)
        {
            NativeMethods.zerobus_arrow_stream_free(handle);
        }
        return true;
    }
}
