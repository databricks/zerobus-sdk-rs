using NUnit.Framework;

namespace Databricks.Zerobus.IntegrationTests;

/// <summary>
/// Integration tests for Arrow Flight ingestion streams.
/// Tests that require the native library auto-skip when it's not available.
/// </summary>
[TestFixture]
public class ArrowIntegrationTests : IntegrationTestBase
{
    private static readonly bool NativeAvailable = IsNativeAvailable();

    private static bool IsNativeAvailable()
    {
        try
        {
            // Try to load the native library for a quick check.
            // If it's not available, all native-dependent tests will be skipped.
            System.Runtime.InteropServices.NativeLibrary.TryLoad(
                "zerobus_ffi",
                typeof(ZerobusSdk).Assembly,
                null,
                out _);
            return true;
        }
        catch
        {
            return false;
        }
    }

    // ──── Argument validation (always runs) ────────────────────────────────

    [Test]
    public async Task CreateArrowStream_NullTableName_ThrowsArgumentNullException()
    {
        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        Assert.That(
            () => sdk.CreateArrowStream(null!, [0x01], "id", "secret"),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task CreateArrowStream_NullSchema_ThrowsArgumentNullException()
    {
        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        Assert.That(
            () => sdk.CreateArrowStream("table", null!, "id", "secret"),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task CreateArrowStream_NullClientId_ThrowsArgumentNullException()
    {
        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        Assert.That(
            () => sdk.CreateArrowStream("table", [0x01], null!, "secret"),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task CreateArrowStreamAsync_NullTableName_ThrowsArgumentNullException()
    {
        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        Assert.That(
            async () => await sdk.CreateArrowStreamAsync(null!, [0x01], "id", "secret"),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task CreateArrowStreamWithHeadersProvider_NullProvider_ThrowsArgumentNullException()
    {
        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        Assert.That(
            () => sdk.CreateArrowStreamWithHeadersProvider("table", [0x01], null!, null),
            Throws.ArgumentNullException);
    }

    // ──── StreamBuilder integration ─────────────────────────────────────────

    [Test]
    public async Task StreamBuilder_Json_BuildsWithMockServer()
    {
        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        var stream = sdk.StreamBuilder()
            .Table(TestTableName)
            .OAuth("client", "secret")
            .MaxInflightRequests(100)
            .Recovery(false)
            .Json()
            .Build();

        Assert.That(stream, Is.Not.Null);

        stream.Dispose();
    }

    [Test]
    public async Task StreamBuilder_Json_BuildAsync()
    {
        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        var stream = await sdk.StreamBuilder()
            .Table(TestTableName)
            .OAuth("client", "secret")
            .MaxInflightRequests(100)
            .Json()
            .BuildAsync();

        Assert.That(stream, Is.Not.Null);

        await stream.DisposeAsync();
    }

    [Test]
    public async Task StreamBuilder_Proto_BuildsWithDescriptor()
    {
        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        var stream = sdk.StreamBuilder()
            .Table(TestTableName)
            .OAuth("client", "secret")
            .CompiledProto(TestDescriptor.CreateTestDescriptorProto())
            .Build();

        Assert.That(stream, Is.Not.Null);

        stream.Dispose();
    }

    // ──── Arrow stream lifecycle (requires native lib) ──────────────────────

    [Test]
    public async Task ArrowStream_CreateAndDispose_DoesNotLeak()
    {
        if (!NativeAvailable)
            Assert.Ignore("Native library (zerobus_ffi) not available.");

        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        // Minimal Arrow schema IPC bytes (0xFFFFFFFFFFFFFFFF 0x0000000000000000)
        var schemaBytes = new byte[] {
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        };

        ZerobusArrowStream? stream = null;

        Assert.That(() =>
        {
            stream = sdk.CreateArrowStream(TestTableName, schemaBytes, "client", "secret");
            Assert.That(stream, Is.Not.Null);
        }, Throws.Nothing);

        Assert.That(() => stream!.Dispose(), Throws.Nothing);
    }

    [Test]
    public async Task ArrowStream_Close_AndDispose_NoError()
    {
        if (!NativeAvailable)
            Assert.Ignore("Native library (zerobus_ffi) not available.");

        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        var schemaBytes = new byte[16];

        using var stream = sdk.CreateArrowStream(TestTableName, schemaBytes, "client", "secret");

        Assert.That(stream.IsClosed(), Is.False);

        Assert.That(() => stream.Close(), Throws.Nothing);
        Assert.That(stream.IsClosed(), Is.True);
    }

    [Test]
    public async Task ArrowStream_IngestBatchAfterClose_Throws()
    {
        if (!NativeAvailable)
            Assert.Ignore("Native library (zerobus_ffi) not available.");

        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        var schemaBytes = new byte[16];

        using var stream = sdk.CreateArrowStream(TestTableName, schemaBytes, "client", "secret");
        stream.Close();

        Assert.That(() => stream.IngestBatch([0x01, 0x02]),
            Throws.InstanceOf<ZerobusException>());
    }

    // ──── Async Arrow ops ───────────────────────────────────────────────────

    [Test]
    public async Task ArrowStream_IngestBatchAsync_Completes()
    {
        if (!NativeAvailable)
            Assert.Ignore("Native library (zerobus_ffi) not available.");

        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        var schemaBytes = new byte[16];
        using var stream = sdk.CreateArrowStream(TestTableName, schemaBytes, "client", "secret");

        var offset = await stream.IngestBatchAsync([0x01, 0x02, 0x03]);
        Assert.That(offset, Is.GreaterThanOrEqualTo(0));
    }

    [Test]
    public async Task ArrowStream_WaitForOffsetAsync_Completes()
    {
        if (!NativeAvailable)
            Assert.Ignore("Native library (zerobus_ffi) not available.");

        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        var schemaBytes = new byte[16];
        using var stream = sdk.CreateArrowStream(TestTableName, schemaBytes, "client", "secret");

        var offset = stream.IngestBatch([0x01]);
        Assert.That(() => stream.WaitForOffsetAsync(offset), Throws.Nothing);
    }

    [Test]
    public async Task ArrowStream_FlushAsync_Completes()
    {
        if (!NativeAvailable)
            Assert.Ignore("Native library (zerobus_ffi) not available.");

        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        var schemaBytes = new byte[16];
        using var stream = sdk.CreateArrowStream(TestTableName, schemaBytes, "client", "secret");

        stream.IngestBatch([0x01]);
        Assert.That(() => stream.FlushAsync(), Throws.Nothing);
    }

    [Test]
    public async Task ArrowStream_CloseAsync_Completes()
    {
        if (!NativeAvailable)
            Assert.Ignore("Native library (zerobus_ffi) not available.");

        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        var schemaBytes = new byte[16];
        using var stream = sdk.CreateArrowStream(TestTableName, schemaBytes, "client", "secret");

        await stream.CloseAsync();
        Assert.That(stream.IsClosed(), Is.True);
    }

    [Test]
    public async Task ArrowStream_MultipleBatches_FlushThenClose()
    {
        if (!NativeAvailable)
            Assert.Ignore("Native library (zerobus_ffi) not available.");

        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        var schemaBytes = new byte[16];
        using var stream = sdk.CreateArrowStream(TestTableName, schemaBytes, "client", "secret");

        var offsets = new long[5];
        for (var i = 0; i < 5; i++)
        {
            offsets[i] = stream.IngestBatch([(byte)i, (byte)(i + 1)]);
            Assert.That(offsets[i], Is.GreaterThanOrEqualTo(0));
        }

        stream.Flush();
        stream.Close();
    }

    // ──── StreamBuilder Arrow ───────────────────────────────────────────────

    [Test]
    public async Task StreamBuilder_Arrow_BuildsWithSchema()
    {
        if (!NativeAvailable)
            Assert.Ignore("Native library (zerobus_ffi) not available.");

        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        var schemaBytes = new byte[16];

        using var stream = sdk.StreamBuilder()
            .Table(TestTableName)
            .OAuth("client", "secret")
            .MaxInflightRequests(100)
            .Recovery(false)
            .Arrow(schemaBytes)
            .MaxInflightBatches(500)
            .IpcCompression(IPCCompressionType.Lz4Frame)
            .Build();

        Assert.That(stream, Is.Not.Null);
        Assert.That(stream.IsClosed(), Is.False);
    }

    [Test]
    public async Task StreamBuilder_Arrow_BuildAsync()
    {
        if (!NativeAvailable)
            Assert.Ignore("Native library (zerobus_ffi) not available.");

        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        var schemaBytes = new byte[16];

        var stream = await sdk.StreamBuilder()
            .Table(TestTableName)
            .OAuth("client", "secret")
            .Arrow(schemaBytes)
            .ConnectionTimeoutMs(60_000)
            .BuildAsync();

        Assert.That(stream, Is.Not.Null);

        await stream.DisposeAsync();
    }
}
