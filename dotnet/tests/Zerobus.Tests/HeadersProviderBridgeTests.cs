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
        var bridge = new HeadersProviderBridge(new MapProvider());
        var handle = GCHandle.Alloc(bridge);
        var userData = GCHandle.ToIntPtr(handle);

        // The FFI's destroy callback releases the handle.
        HeadersProviderBridge.NativeFree(userData);

        Assert.That(handle.IsAllocated, Is.False,
            "NativeFree must free the GCHandle it is handed");
    }

    [Test]
    public void NativeFree_NullUserData_IsNoOp()
    {
        // Freeing IntPtr.Zero must not throw (mirrors delete nullptr / nil guard).
        Assert.DoesNotThrow(() => HeadersProviderBridge.NativeFree(IntPtr.Zero));
    }
}
