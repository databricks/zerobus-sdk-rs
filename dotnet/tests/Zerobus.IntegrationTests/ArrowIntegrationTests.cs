using NUnit.Framework;

namespace Databricks.Zerobus.IntegrationTests;

/// <summary>
/// Integration tests for Arrow Flight ingestion streams.
/// Tests that require the native library auto-skip when it's not available.
/// Arrow lifecycle tests use the <see cref="MockFlightServer"/> DoPut endpoint.
/// </summary>
[TestFixture]
public class ArrowIntegrationTests : IntegrationTestBase
{
    private static readonly bool NativeAvailable = IsNativeAvailable();

    /// <summary>
    /// Returns null when Arrow Flight tests can run, or a reason string when they cannot.
    /// Arrow lifecycle tests now use the mock Flight server (which speaks DoPut),
    /// so they only need the native library to be loadable.
    /// </summary>
    private static readonly string? ArrowUnavailableReason = DetectArrowAvailability();

    private static bool IsNativeAvailable()
    {
        try
        {
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

    private static string? DetectArrowAvailability()
    {
        if (!IsNativeAvailable())
            return "Native library (zerobus_ffi) not available.";

        // The MockFlightServer in MockServerFixture provides the Arrow Flight
        // DoPut endpoint, so lifecycle tests can run against the local mock.
        return null;
    }

    /// <summary>
    /// Creates a minimal valid Arrow IPC schema message (zero fields).
    /// Uses Apache.Arrow to generate properly formatted bytes.
    /// </summary>
    private static byte[] CreateValidSchemaIpcBytes()
    {
        var schema = new Apache.Arrow.Schema(
            Enumerable.Empty<Apache.Arrow.Field>(),
            null);

        using var stream = new MemoryStream();

        // ArrowStreamWriter writes the schema when the first batch is written.
        // Write a single empty batch to force schema serialization.
        using (var writer = new Apache.Arrow.Ipc.ArrowStreamWriter(stream, schema, leaveOpen: true))
        {
            var emptyBatch = new Apache.Arrow.RecordBatch(
                schema,
                Array.Empty<Apache.Arrow.IArrowArray>(),
                0);
            writer.WriteRecordBatch(emptyBatch);
        }

        // Return the full Arrow IPC stream bytes.
        // The Rust FFI expects the complete stream format including schema.
        return stream.ToArray();
    }

    /// <summary>
    /// Creates a valid Arrow IPC-encoded RecordBatch matching the empty schema.
    /// The Rust SDK's ingest_ipc_batch requires valid Arrow IPC stream bytes.
    /// </summary>
    private static byte[] CreateValidBatchIpcBytes(int rowCount = 1)
    {
        var schema = new Apache.Arrow.Schema(
            Enumerable.Empty<Apache.Arrow.Field>(),
            null);

        using var stream = new MemoryStream();

        // Arrow IPC stream format: schema + RecordBatch.
        using (var writer = new Apache.Arrow.Ipc.ArrowStreamWriter(stream, schema, leaveOpen: true))
        {
            var batch = new Apache.Arrow.RecordBatch(
                schema,
                Array.Empty<Apache.Arrow.IArrowArray>(),
                rowCount);
            writer.WriteRecordBatch(batch);
        }

        return stream.ToArray();
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

    // ──── StreamBuilder + factory integration ──────────────────────────────

    [Test]
    public async Task StreamBuilder_Json_BuildsWithMockServer()
    {
        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        // Use HeadersProvider to bypass OAuth against mock-uc.com
        var stream = sdk.CreateJsonStreamWithHeadersProvider(
            TestTableName,
            new TestHeadersProvider(),
            StreamConfigurationOptions.Default with { Recovery = false });

        Assert.That(stream, Is.Not.Null);
        stream.Dispose();
    }

    [Test]
    public async Task StreamBuilder_Json_BuildAsync()
    {
        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        var stream = await sdk.CreateJsonStreamWithHeadersProviderAsync(
            TestTableName,
            new TestHeadersProvider(),
            StreamConfigurationOptions.Default with { Recovery = false });

        Assert.That(stream, Is.Not.Null);
        await stream.DisposeAsync();
    }

    [Test]
    public async Task StreamBuilder_Proto_BuildsWithDescriptor()
    {
        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        var stream = sdk.CreateProtoStreamWithHeadersProvider(
            TestTableName,
            TestDescriptor.CreateTestDescriptorProto(),
            new TestHeadersProvider(),
            StreamConfigurationOptions.Default with { Recovery = false });

        Assert.That(stream, Is.Not.Null);
        stream.Dispose();
    }

    // ──── Arrow stream lifecycle (uses mock Flight server) ──────────────────

    [Test]
    public async Task ArrowStream_CreateAndDispose_DoesNotLeak()
    {
        if (ArrowUnavailableReason != null)
            Assert.Ignore(ArrowUnavailableReason);

        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        var schemaBytes = CreateValidSchemaIpcBytes();

        ZerobusArrowStream? stream = null;

        Assert.That(() =>
        {
            stream = sdk.CreateArrowStreamWithHeadersProvider(
                TestTableName, schemaBytes, new TestHeadersProvider());
            Assert.That(stream, Is.Not.Null);
        }, Throws.Nothing);

        Assert.That(() => stream!.Dispose(), Throws.Nothing);
    }

    [Test]
    public async Task ArrowStream_Close_AndDispose_NoError()
    {
        if (ArrowUnavailableReason != null)
            Assert.Ignore(ArrowUnavailableReason);

        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        var schemaBytes = CreateValidSchemaIpcBytes();

        using var stream = sdk.CreateArrowStreamWithHeadersProvider(
            TestTableName, schemaBytes, new TestHeadersProvider());

        Assert.That(stream.IsClosed(), Is.False);

        Assert.That(() => stream.Close(), Throws.Nothing);
        Assert.That(stream.IsClosed(), Is.True);
    }

    [Test]
    public async Task ArrowStream_IngestBatchAfterClose_Throws()
    {
        if (ArrowUnavailableReason != null)
            Assert.Ignore(ArrowUnavailableReason);

        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        var schemaBytes = CreateValidSchemaIpcBytes();

        using var stream = sdk.CreateArrowStreamWithHeadersProvider(
            TestTableName, schemaBytes, new TestHeadersProvider());
        stream.Close();

        Assert.That(() => stream.IngestBatch([0x01, 0x02]),
            Throws.InstanceOf<ZerobusException>());
    }

    // ──── Async Arrow ops ───────────────────────────────────────────────────

    [Test]
    public async Task ArrowStream_IngestBatchAsync_Completes()
    {
        if (ArrowUnavailableReason != null)
            Assert.Ignore(ArrowUnavailableReason);

        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        var schemaBytes = CreateValidSchemaIpcBytes();
        using var stream = sdk.CreateArrowStreamWithHeadersProvider(
            TestTableName, schemaBytes, new TestHeadersProvider());

        var batchBytes = CreateValidBatchIpcBytes(3);
        var offset = await stream.IngestBatchAsync(batchBytes);
        Assert.That(offset, Is.GreaterThanOrEqualTo(0));
        stream.Flush(); // Ensure the batch is delivered and acked before asserting.
        Assert.That(fixture.ArrowFlightServer.BatchesReceived, Is.GreaterThanOrEqualTo(1));
    }

    [Test]
    public async Task ArrowStream_WaitForOffsetAsync_Completes()
    {
        if (ArrowUnavailableReason != null)
            Assert.Ignore(ArrowUnavailableReason);

        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        var schemaBytes = CreateValidSchemaIpcBytes();
        using var stream = sdk.CreateArrowStreamWithHeadersProvider(
            TestTableName, schemaBytes, new TestHeadersProvider());

        var batchBytes1 = CreateValidBatchIpcBytes(1);
        var offset = stream.IngestBatch(batchBytes1);
        Assert.That(offset, Is.GreaterThanOrEqualTo(0));

        // The mock server acks every batch, so WaitForOffset should return quickly.
        Assert.That(() => stream.WaitForOffset(offset), Throws.Nothing);
        Assert.That(fixture.ArrowFlightServer.BatchesReceived, Is.GreaterThanOrEqualTo(1));
    }

    [Test]
    public async Task ArrowStream_FlushAsync_Completes()
    {
        if (ArrowUnavailableReason != null)
            Assert.Ignore(ArrowUnavailableReason);

        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        var schemaBytes = CreateValidSchemaIpcBytes();
        using var stream = sdk.CreateArrowStreamWithHeadersProvider(
            TestTableName, schemaBytes, new TestHeadersProvider());

        var batchForFlush = CreateValidBatchIpcBytes(1);
        stream.IngestBatch(batchForFlush);
        Assert.That(() => stream.Flush(), Throws.Nothing);
        Assert.That(fixture.ArrowFlightServer.BatchesReceived, Is.GreaterThanOrEqualTo(1));
    }

    [Test]
    public async Task ArrowStream_CloseAsync_Completes()
    {
        if (ArrowUnavailableReason != null)
            Assert.Ignore(ArrowUnavailableReason);

        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        var schemaBytes = CreateValidSchemaIpcBytes();
        using var stream = sdk.CreateArrowStreamWithHeadersProvider(
            TestTableName, schemaBytes, new TestHeadersProvider());

        // Ingest at least one batch before closing to exercise the ack path.
        var batchForClose = CreateValidBatchIpcBytes(1);
        stream.IngestBatch(batchForClose);

        await stream.CloseAsync();
        Assert.That(stream.IsClosed(), Is.True);
        Assert.That(fixture.ArrowFlightServer.BatchesReceived, Is.GreaterThanOrEqualTo(1));
    }

    [Test]
    public async Task ArrowStream_MultipleBatches_FlushThenClose()
    {
        if (ArrowUnavailableReason != null)
            Assert.Ignore(ArrowUnavailableReason);

        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        var schemaBytes = CreateValidSchemaIpcBytes();
        using var stream = sdk.CreateArrowStreamWithHeadersProvider(
            TestTableName, schemaBytes, new TestHeadersProvider());

        var offsets = new long[5];
        for (var i = 0; i < 5; i++)
        {
            var batchIpc = CreateValidBatchIpcBytes(i + 1);
            offsets[i] = stream.IngestBatch(batchIpc);
            Assert.That(offsets[i], Is.GreaterThanOrEqualTo(0));
        }

        stream.Flush();
        stream.Close();

        // The mock Flight server should have received 5 data batches.
        Assert.That(fixture.ArrowFlightServer.BatchesReceived, Is.EqualTo(5));
        Assert.That(fixture.ArrowFlightServer.MaxOffsetSeen, Is.GreaterThanOrEqualTo(4));
    }

    // ──── StreamBuilder Arrow ───────────────────────────────────────────────

    [Test]
    public async Task StreamBuilder_Arrow_BuildsWithSchema()
    {
        if (ArrowUnavailableReason != null)
            Assert.Ignore(ArrowUnavailableReason);

        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        var schemaBytes = CreateValidSchemaIpcBytes();

        using var stream = sdk.CreateArrowStreamWithHeadersProvider(
            TestTableName, schemaBytes, new TestHeadersProvider());

        Assert.That(stream, Is.Not.Null);
        Assert.That(stream.IsClosed(), Is.False);
    }

    [Test]
    public async Task StreamBuilder_Arrow_BuildAsync()
    {
        if (ArrowUnavailableReason != null)
            Assert.Ignore(ArrowUnavailableReason);

        await using var fixture = await MockServerFixture.StartAsync();
        using var sdk = CreateDefaultSdk(fixture);

        var schemaBytes = CreateValidSchemaIpcBytes();

        var stream = await sdk.CreateArrowStreamWithHeadersProviderAsync(
            TestTableName, schemaBytes, new TestHeadersProvider());

        Assert.That(stream, Is.Not.Null);

        await stream.DisposeAsync();
    }
}
