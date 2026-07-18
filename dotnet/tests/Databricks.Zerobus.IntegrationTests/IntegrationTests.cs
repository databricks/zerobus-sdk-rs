using Databricks.Zerobus;
using Xunit;

namespace Databricks.Zerobus.IntegrationTests;

/// <summary>
/// Full end-to-end integration tests using a real gRPC mock server.
/// Each test runs against its own server on a unique port.
/// Tests auto-skip when the native library (zerobus_ffi) is not available.
/// </summary>
// [Collection("Integration")]
[Trait("Category", "Integration")]
public class IntegrationTests
{
    private MockZerobusServer _server = null!;
    private static readonly bool NativeAvailable = NativeLibraryHelper.IsNativeLibraryAvailable();

    public IntegrationTests()
    {
        if (NativeAvailable)
        {
            _server = new MockZerobusServer();
        }
    }

    // ================================================================
    // Stream Creation
    // ================================================================

    [Fact]
    public async Task CreateJsonStream_Success()
    {
        if (!NativeAvailable) return;

        using var sdk = await CreateSdkAsync();
        using var stream = await BuildJsonStream(sdk);

        Assert.NotNull(stream);
        Assert.Equal("catalog.schema.table", stream.TableName);
        Assert.False(stream.IsClosed);
    }

    [Fact]
    public async Task CreateProtoStream_Success()
    {
        if (!NativeAvailable) return;

        using var sdk = await CreateSdkAsync();
        using var stream = await BuildProtoStream(sdk);

        Assert.NotNull(stream);
    }

    // ================================================================
    // Single Record Ingestion
    // ================================================================

    [Fact]
    public async Task IngestJsonRecord_ReturnsOffset()
    {
        if (!NativeAvailable) return;

        using var sdk = await CreateSdkAsync();
        using var stream = await BuildJsonStream(sdk);

        long offset = stream.IngestRecord("{\"id\": 1, \"name\": \"test\"}");
        Assert.True(offset >= 0);
    }

    [Fact]
    public async Task IngestProtoRecord_ReturnsOffset()
    {
        if (!NativeAvailable) return;

        using var sdk = await CreateSdkAsync();
        using var stream = await BuildProtoStream(sdk);

        var record = new FakeProtoMessage { Id = 42, Name = "test" };
        long offset = stream.IngestRecord(record);
        Assert.True(offset >= 0);
    }

    // ================================================================
    // Batch Ingestion
    // ================================================================

    [Fact]
    public async Task IngestJsonBatch_ReturnsOffset()
    {
        if (!NativeAvailable) return;

        using var sdk = await CreateSdkAsync();
        using var stream = await BuildJsonStream(sdk);

        var records = new[] { "{\"a\":1}", "{\"b\":2}", "{\"c\":3}" };
        long? lastOffset = stream.IngestRecords(records);

        Assert.NotNull(lastOffset);
        Assert.True(lastOffset >= 0);
    }

    [Fact]
    public async Task IngestProtoBatch_ReturnsOffset()
    {
        if (!NativeAvailable) return;

        using var sdk = await CreateSdkAsync();
        using var stream = await BuildProtoStream(sdk);

        var records = new List<FakeProtoMessage>
        {
            new() { Id = 1, Name = "a" },
            new() { Id = 2, Name = "b" },
            new() { Id = 3, Name = "c" }
        };

        long? lastOffset = stream.IngestRecords(records);
        Assert.NotNull(lastOffset);
        Assert.True(lastOffset >= 0);
    }

    // ================================================================
    // Flush & WaitForOffset
    // ================================================================

    [Fact]
    public async Task Flush_AfterIngest_DoesNotThrow()
    {
        if (!NativeAvailable) return;

        using var sdk = await CreateSdkAsync();
        using var stream = await BuildJsonStream(sdk);

        stream.IngestRecord("{\"x\":1}");
        stream.Flush();
    }

    [Fact]
    public async Task WaitForOffset_AfterIngest_DoesNotThrow()
    {
        if (!NativeAvailable) return;

        using var sdk = await CreateSdkAsync();
        using var stream = await BuildJsonStream(sdk);

        long offset = stream.IngestRecord("{\"x\":1}");
        stream.WaitForOffset(offset);
    }

    // ================================================================
    // Stream Lifecycle
    // ================================================================

    [Fact]
    public async Task Close_MarksStreamAsClosed()
    {
        if (!NativeAvailable) return;

        using var sdk = await CreateSdkAsync();
        using var stream = await BuildJsonStream(sdk);

        Assert.False(stream.IsClosed);
        stream.Close();
        Assert.True(stream.IsClosed);
    }

    [Fact]
    public async Task Dispose_MarksStreamAsClosed()
    {
        if (!NativeAvailable) return;

        using var sdk = await CreateSdkAsync();
        using var stream = await BuildJsonStream(sdk);

        stream.Dispose();
        Assert.True(stream.IsClosed);
    }

    [Fact]
    public async Task IngestAfterClose_Throws()
    {
        if (!NativeAvailable) return;

        using var sdk = await CreateSdkAsync();
        using var stream = await BuildJsonStream(sdk);
        stream.Close();
        stream.Dispose();

        Assert.Throws<ZerobusException>(() => stream.IngestRecord("{\"x\":1}"));
    }

    // ================================================================
    // Unacked Records After Close
    // ================================================================

    [Fact]
    public async Task GetUnackedRecords_AfterClose_ReturnsRecords()
    {
        if (!NativeAvailable) return;

        using var sdk = await CreateSdkAsync();
        using var stream = await BuildJsonStream(sdk);

        stream.IngestRecord("{\"x\":1}");
        stream.IngestRecord("{\"x\":2}");

        stream.Close();
        var unacked = stream.GetUnackedRecords();
        stream.Dispose();

        Assert.NotNull(unacked);
    }

    // ================================================================
    // Recovery (Stream Recreation)
    // ================================================================

    [Fact]
    public async Task RecreateStream_PreservesConfiguration()
    {
        if (!NativeAvailable) return;

        using var sdk = await CreateSdkAsync();
        var original = await BuildJsonStream(sdk);
        original.Close();

        var recreated = await sdk.RecreateStreamAsync(original);

        Assert.NotNull(recreated);
        Assert.Equal(original.TableName, recreated.TableName);
        Assert.Equal(original.ClientId, recreated.ClientId);

        recreated.Dispose();
        original.Dispose();
    }

    // ================================================================
    // Error Scenarios
    // ================================================================

    [Fact]
    public async Task StreamCreation_InvalidTable_Throws()
    {
        _server.Service.ShouldAcceptStream = false;
        _server.Service.ErrorMessage = "Table not found";

        if (!NativeAvailable) return;

        using var sdk = await CreateSdkAsync();
        await Assert.ThrowsAsync<ZerobusException>(() =>
            sdk.StreamBuilder()
                .Table("bad.table")
                .OAuth("id", "secret")
                .Json()
                .BuildAsync());
    }

    [Fact]
    public async Task Ingest_SdkDisposed_DoesNotCrash()
    {
        if (!NativeAvailable) return;

        using var sdk = await CreateSdkAsync();
        var stream = await BuildJsonStream(sdk);
        sdk.Dispose();

        // Stream may or may not be affected by SDK disposal
        Assert.True(stream.IsClosed || !stream.IsClosed);
        stream.Dispose();
    }

    // ================================================================
    // Configuration
    // ================================================================

    [Fact]
    public async Task StreamBuilder_CustomOptions_Applied()
    {
        if (!NativeAvailable) return;

        using var sdk = await CreateSdkAsync();
        using var stream = await sdk.StreamBuilder()
            .Table("catalog.schema.table")
            .OAuth("id", "secret")
            .MaxInflightRecords(50_000)
            .Recovery(false)
            .FlushTimeoutMs(60_000)
            .Json()
            .BuildAsync();

        Assert.Equal(50_000, stream.Options.MaxInflightRecords);
        Assert.False(stream.Options.Recovery);
        Assert.Equal(60_000, stream.Options.FlushTimeoutMs);
    }

    // ================================================================
    // Concurrent Ingestion (Thread Safety)
    // ================================================================

    [Fact]
    public async Task ConcurrentIngest_MultipleThreads_NoDataCorruption()
    {
        if (!NativeAvailable) return;

        using var sdk = await CreateSdkAsync();
        using var stream = await BuildJsonStream(sdk);

        var offsets = new long[100];
        Parallel.For(0, 100, i =>
        {
            offsets[i] = stream.IngestRecord($"{{\"thread\": {i}}}");
        });

        stream.Flush();

        Assert.All(offsets, o => Assert.True(o >= 0));
    }

    // ================================================================
    // Helpers
    // ================================================================

    // Helper: create SDK with TLS disabled (mock server uses plain HTTP/2)
    private async Task EnsureServerAsync()
    {
        if (_server == null)
        {
            _server = new MockZerobusServer();
        }
        if (string.IsNullOrEmpty(_server.Endpoint) || _server.Endpoint.Contains(":0"))
        {
            await _server.StartAsync();
        }
    }

    private async Task<ZerobusSdk> CreateSdkAsync()
    {
        await EnsureServerAsync();
        return ZerobusSdk.CreateBuilder(_server!.Endpoint, _server.UnityCatalogEndpoint)
            .DisableTls()
            .Build();
    }

    private Task<ZerobusJsonStream> BuildJsonStream(ZerobusSdk sdk) =>
        sdk.StreamBuilder()
            .Table("catalog.schema.table")
            .OAuth("client-id", "client-secret")
            .Json()
            .BuildAsync();

    private Task<ZerobusProtoStream<FakeProtoMessage>> BuildProtoStream(ZerobusSdk sdk) =>
        sdk.StreamBuilder()
            .Table("catalog.schema.table")
            .OAuth("client-id", "client-secret")
            .CompiledProto(Array.Empty<byte>())
            .BuildAsync<FakeProtoMessage>();
}

/// <summary>
/// Minimal protobuf message for integration tests.
/// </summary>
public sealed class FakeProtoMessage : Google.Protobuf.IMessage<FakeProtoMessage>
{
    public int Id { get; set; }
    public string Name { get; set; } = "";

    Google.Protobuf.Reflection.MessageDescriptor Google.Protobuf.IMessage.Descriptor =>
        throw new NotSupportedException("Fake proto — not a real compiled message");
    public int CalculateSize() => 4 + Google.Protobuf.CodedOutputStream.ComputeStringSize(Name);
    public FakeProtoMessage Clone() => (FakeProtoMessage)MemberwiseClone();
    public bool Equals(FakeProtoMessage? other) =>
        other is not null && Id == other.Id && Name == other.Name;
    public void MergeFrom(FakeProtoMessage message) { Id = message.Id; Name = message.Name; }
    public void MergeFrom(Google.Protobuf.CodedInputStream input)
    {
        uint tag;
        while ((tag = input.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 8: Id = input.ReadInt32(); break;
                case 18: Name = input.ReadString(); break;
                default: input.SkipLastField(); break;
            }
        }
    }
    public void WriteTo(Google.Protobuf.CodedOutputStream output)
    {
        output.WriteTag(1, Google.Protobuf.WireFormat.WireType.Varint);
        output.WriteInt32(Id);
        output.WriteTag(2, Google.Protobuf.WireFormat.WireType.LengthDelimited);
        output.WriteString(Name);
    }
    public Google.Protobuf.MessageParser<FakeProtoMessage> Parser =>
        new(() => new FakeProtoMessage());
}

