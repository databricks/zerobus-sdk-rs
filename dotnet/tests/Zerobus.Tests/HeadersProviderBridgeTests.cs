using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Databricks.Zerobus;
using Databricks.Zerobus.Native;
using NUnit.Framework;

namespace Databricks.Zerobus.Tests;

// Verifies the .NET side of the headers-provider ownership transfer: the free
// callback (NativeFree) releases the GCHandle it is handed, and tolerates null.
// This is the mechanism that closes the recovery-vs-teardown use-after-free —
// the FFI now owns the provider handle and releases it via NativeFree after any
// in-flight GetHeaders returns, instead of the stream freeing it on dispose.
// The live race itself is driven from the Rust core (the supervisor holds the
// provider Arc across the callback). Pure-managed, so no native lib is needed.
[TestFixture]
public class HeadersProviderBridgeTests
{
    private sealed class MapProvider : IHeadersProvider
    {
        public IDictionary<string, string> GetHeaders() =>
            new Dictionary<string, string>();
    }

    [Test]
    public void NativeFree_ReleasesHandle()
    {
        // The GCHandle is the only strong root keeping the bridge alive, mirroring
        // ownership transfer to the FFI. After NativeFree releases that root the
        // bridge must become collectable.
        //
        // Note: we cannot assert on `handle.IsAllocated` here — GCHandle is a
        // struct wrapping an IntPtr, so NativeFree frees the runtime handle via
        // its own reconstructed copy (GCHandle.FromIntPtr) and cannot zero this
        // local's field. A WeakReference observes the actual release instead.
        var (userData, weak) = AllocBridgeHandle();

        HeadersProviderBridge.NativeFree(userData);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        Assert.That(weak.IsAlive, Is.False,
            "NativeFree must free the GCHandle so the bridge can be collected");
    }

    // Separate non-inlined method so the bridge has no lingering local/JIT root
    // on the caller's frame once it returns.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (IntPtr userData, WeakReference weak) AllocBridgeHandle()
    {
        var bridge = new HeadersProviderBridge(new MapProvider());
        var handle = GCHandle.Alloc(bridge);
        return (GCHandle.ToIntPtr(handle), new WeakReference(bridge));
    }

    [Test]
    public void NativeFree_NullUserData_IsNoOp()
    {
        // Freeing IntPtr.Zero must not throw (mirrors delete nullptr / nil guard).
        Assert.DoesNotThrow(() => HeadersProviderBridge.NativeFree(IntPtr.Zero));
    }
}
