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
