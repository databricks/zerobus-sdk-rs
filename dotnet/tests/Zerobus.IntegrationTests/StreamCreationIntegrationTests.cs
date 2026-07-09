using System.Diagnostics;
using Grpc.Core;
using NUnit.Framework;

namespace Databricks.Zerobus.IntegrationTests;

[TestFixture]
[Parallelizable(ParallelScope.Children)]
public class StreamCreationIntegrationTests : IntegrationTestBase
{
    [Test]
    public async Task CreateStream_NullTableProperties_ThrowsArgumentNullException()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        using var sdk = CreateDefaultSdk(fixture);

        Assert.Throws<ArgumentNullException>(() =>
        {
            sdk.CreateStream(null!, "id", "secret");
        });
    }

    [Test]
    public async Task CreateStreamAsync_NullTableProperties_ThrowsArgumentNullException()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        using var sdk = CreateDefaultSdk(fixture);

        Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await sdk.CreateStreamAsync(null!, "id", "secret");
        });
    }

    [Test]
    public async Task CreateStream_NullClientId_ThrowsArgumentNullException()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        using var sdk = CreateDefaultSdk(fixture);

        Assert.Throws<ArgumentNullException>(() =>
        {
            sdk.CreateStream(new TableProperties("test_table"), null!, "secret");
        });
    }

    [Test]
    public async Task CreateStreamAsync_NullClientId_ThrowsArgumentNullException()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        using var sdk = CreateDefaultSdk(fixture);

        Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await sdk.CreateStreamAsync(new TableProperties("test_table"), null!, "secret");
        });
    }

    [Test]
    public async Task CreateStream_NullClientSecret_ThrowsArgumentNullException()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        using var sdk = CreateDefaultSdk(fixture);

        Assert.Throws<ArgumentNullException>(() =>
        {
            sdk.CreateStream(new TableProperties("test_table"), "id", null!);
        });
    }

    [Test]
    public async Task CreateStreamAsync_NullClientSecret_ThrowsArgumentNullException()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        using var sdk = CreateDefaultSdk(fixture);

        Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await sdk.CreateStreamAsync(new TableProperties("test_table"), "id", null!);
        });
    }

    [Test]
    public async Task SuccessfulStreamCreation()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();
        var options = CreateDefaultOptions();

        using var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        Assert.That(stream, Is.Not.Null);
    }

    [Test]
    public async Task SuccessfulStreamCreation_AsyncApi()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_async_success"),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();
        var options = CreateDefaultOptions();

        using var stream = await sdk.CreateStreamWithHeadersProviderAsync(tableProps, new TestHeadersProvider(), options);

        Assert.That(stream, Is.Not.Null);
    }

    [Test]
    public async Task CreateJsonStreamWithHeadersProvider_ReturnsTypedJsonStream()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_json"),
            MockResponses.RecordAckResponse(0),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var options = CreateDefaultOptions();

        using var stream = sdk.CreateJsonStreamWithHeadersProvider(
            TestTableName,
            new TestHeadersProvider(),
            options);

        var offset = stream.IngestRecord("{\"message\":\"json\"}");
        stream.WaitForOffset(offset);

        Assert.That(offset, Is.EqualTo(0));
    }

    [Test]
    public async Task CreateJsonStreamWithHeadersProviderAsync_ReturnsTypedJsonStream()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_json_async"),
            MockResponses.RecordAckResponse(0),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var options = CreateDefaultOptions();

        using var stream = await sdk.CreateJsonStreamWithHeadersProviderAsync(
            TestTableName,
            new TestHeadersProvider(),
            options);

        var offset = await stream.IngestRecordAsync("{\"message\":\"json\"}");
        await stream.WaitForOffsetAsync(offset);

        Assert.That(offset, Is.EqualTo(0));
    }

    [Test]
    public async Task CreateProtoStreamWithHeadersProvider_ReturnsTypedProtoStream()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_proto"),
            MockResponses.RecordAckResponse(0),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var options = CreateDefaultOptions();

        using var stream = sdk.CreateProtoStreamWithHeadersProvider(
            TestTableName,
            TestDescriptor.CreateTestDescriptorProto(),
            new TestHeadersProvider(),
            options);

        var offset = stream.IngestRecord("proto-payload"u8.ToArray());
        stream.WaitForOffset(offset);

        Assert.That(offset, Is.EqualTo(0));
    }

    [Test]
    public async Task CreateProtoStreamWithHeadersProviderAsync_ReturnsTypedProtoStream()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_proto_async"),
            MockResponses.RecordAckResponse(0),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var options = CreateDefaultOptions();

        using var stream = await sdk.CreateProtoStreamWithHeadersProviderAsync(
            TestTableName,
            TestDescriptor.CreateTestDescriptorProto(),
            new TestHeadersProvider(),
            options);

        var offset = await stream.IngestRecordAsync("proto-payload"u8.ToArray());
        await stream.WaitForOffsetAsync(offset);

        Assert.That(offset, Is.EqualTo(0));
    }

    [Test]
    public async Task CreateStream_JsonRecordTypeWithDescriptor_ThrowsArgumentException()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        using var sdk = CreateDefaultSdk(fixture);
        var options = CreateDefaultOptions() with { RecordType = RecordType.Json };
        var tableProperties = CreateTableProperties();

        var ex = Assert.Throws<ArgumentException>(() =>
        {
            sdk.CreateStreamWithHeadersProvider(tableProperties, new TestHeadersProvider(), options);
        });

        Assert.That(ex!.Message, Does.Contain("JSON streams cannot specify DescriptorProto"));
    }

    [Test]
    public async Task CreateStream_ProtoRecordTypeWithoutDescriptor_ThrowsArgumentException()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        using var sdk = CreateDefaultSdk(fixture);
        var options = CreateDefaultOptions() with { RecordType = RecordType.Proto };

        var ex = Assert.Throws<ArgumentException>(() =>
        {
            sdk.CreateStreamWithHeadersProvider(
                new TableProperties(TestTableName),
                new TestHeadersProvider(),
                options);
        });

        Assert.That(ex!.Message, Does.Contain("Proto streams require a non-empty DescriptorProto"));
    }

    [Test]
    public async Task TimeoutedStreamCreation()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1", delayMs: 300),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            RecoveryTimeoutMs = 100,
            Recovery = false,
        };

        Assert.Throws<ZerobusException>(() =>
        {
            sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);
        });

        await Task.Delay(100);
    }

    [Test]
    public async Task TimeoutedStreamCreation_AsyncApi()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_timeout_async", delayMs: 300),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            RecoveryTimeoutMs = 100,
            Recovery = false,
        };

        Assert.ThrowsAsync<ZerobusException>(async () =>
        {
            await sdk.CreateStreamWithHeadersProviderAsync(tableProps, new TestHeadersProvider(), options);
        });
    }

    [Test]
    public async Task NonRetriableErrorDuringStreamCreation()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.ErrorResponse(StatusCode.Unauthenticated, "Non-retriable error"),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = true,
        };

        Assert.Throws<ZerobusException>(() =>
        {
            sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);
        });
    }

    [Test]
    public async Task NonRetriableErrorDuringStreamCreation_AsyncApi()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.ErrorResponse(StatusCode.Unauthenticated, "Non-retriable error"),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = true,
        };

        Assert.ThrowsAsync<ZerobusException>(async () =>
        {
            await sdk.CreateStreamWithHeadersProviderAsync(tableProps, new TestHeadersProvider(), options);
        });
    }

    [Test]
    public async Task RetriableErrorWithoutRecoveryDuringStreamCreation()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.ErrorResponse(StatusCode.Unavailable, "Retriable error"),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
            RecoveryTimeoutMs = 100,
            RecoveryBackoffMs = 100,
        };

        var sw = Stopwatch.StartNew();

        var ex = Assert.Throws<ZerobusException>(() =>
        {
            sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);
        });

        Assert.That(ex, Is.Not.Null);
        Assert.That(ex!.IsRetryable, Is.True);

        sw.Stop();

        Assert.That(sw.ElapsedMilliseconds, Is.LessThan(1000),
            $"Expected reasonable failure time, but took {sw.ElapsedMilliseconds}ms");
    }

    [Test]
    public async Task RetriableErrorWithoutRecoveryDuringStreamCreation_AsyncApi()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.ErrorResponse(StatusCode.Unavailable, "Retriable error"),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
            RecoveryTimeoutMs = 100,
            RecoveryBackoffMs = 100,
        };

        var ex = Assert.ThrowsAsync<ZerobusException>(async () =>
        {
            await sdk.CreateStreamWithHeadersProviderAsync(tableProps, new TestHeadersProvider(), options);
        });

        Assert.That(ex, Is.Not.Null);
        Assert.That(ex!.IsRetryable, Is.True);
    }

    [Test]
    public async Task Builder_CreatesWorkingSdk()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_builder"),
        ]);

        using var sdk = ZerobusSdk.CreateBuilder()
            .Endpoint(fixture.ServerUrl)
            .ApplicationName("integration-test")
            .DisableTls()
            .Build();

        var tableProps = CreateTableProperties();

        using var stream = sdk.CreateStreamWithHeadersProvider(
            tableProps,
            new TestHeadersProvider(),
            StreamConfigurationOptions.Default with { Recovery = false });

        Assert.That(stream, Is.Not.Null);
    }
}
