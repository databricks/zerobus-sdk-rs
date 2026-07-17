using Microsoft.Win32.SafeHandles;

namespace Databricks.Zerobus.Native;

/// <summary>
/// SafeHandle for a CZerobusProtoSchema opaque pointer.
/// </summary>
internal sealed class ZerobusProtoSchemaHandle : SafeHandleZeroOrMinusOneIsInvalid
{
    public ZerobusProtoSchemaHandle() : base(true) { }
    public ZerobusProtoSchemaHandle(IntPtr handle) : base(true) { SetHandle(handle); }

    protected override bool ReleaseHandle()
    {
        if (!IsInvalid && !IsClosed)
        {
            NativeMethods.zerobus_proto_schema_free(handle);
        }
        return true;
    }
}
