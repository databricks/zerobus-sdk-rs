using NUnit.Framework;

namespace Databricks.Zerobus.IntegrationTests;

[TestFixture]
[Parallelizable(ParallelScope.Children)]
public class SdkIdentifierIntegrationTests : IntegrationTestBase
{
    [Test]
    public async Task DefaultSdkIdentifier_SentInUserAgent()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_sdk_identifier"),
            MockResponses.RecordAckResponse(0),
        ]);

        using var sdk = CreateDefaultSdk(fixture);

        using var stream = sdk.CreateStreamWithHeadersProvider(
            CreateTableProperties(), new TestHeadersProvider(), CreateDefaultOptions());

        stream.IngestRecord("test record data"u8.ToArray());
        stream.Flush();

        var observedHeaders = fixture.MockServer.GetLastRequestHeaders(TestTableName);
        Assert.That(observedHeaders.TryGetValue("user-agent", out var userAgent), Is.True);
        Assert.That(userAgent, Does.StartWith("zerobus-sdk-dotnet/"));
    }

    [Test]
    public async Task ExplicitSdkIdentifier_TakesPrecedenceOverDefault()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_sdk_identifier_override"),
            MockResponses.RecordAckResponse(0),
        ]);

        using var sdk = ZerobusSdk.CreateBuilder()
            .Endpoint(fixture.ServerUrl)
            .UnityCatalogUrl("https://mock-uc.com")
            .SdkIdentifier("custom-wrapper/9.9.9")
            .DisableTls()
            .Build();

        using var stream = sdk.CreateStreamWithHeadersProvider(
            CreateTableProperties(), new TestHeadersProvider(), CreateDefaultOptions());

        stream.IngestRecord("test record data"u8.ToArray());
        stream.Flush();

        var observedHeaders = fixture.MockServer.GetLastRequestHeaders(TestTableName);
        Assert.That(observedHeaders.TryGetValue("user-agent", out var userAgent), Is.True);
        Assert.That(userAgent, Does.StartWith("custom-wrapper/9.9.9"));
    }

    [Test]
    public async Task BlankSdkIdentifier_FallsBackToDefault()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_sdk_identifier_blank"),
            MockResponses.RecordAckResponse(0),
        ]);

        using var sdk = ZerobusSdk.CreateBuilder()
            .Endpoint(fixture.ServerUrl)
            .UnityCatalogUrl("https://mock-uc.com")
            .SdkIdentifier("   ")
            .DisableTls()
            .Build();

        using var stream = sdk.CreateStreamWithHeadersProvider(
            CreateTableProperties(), new TestHeadersProvider(), CreateDefaultOptions());

        stream.IngestRecord("test record data"u8.ToArray());
        stream.Flush();

        // The core only substitutes its default for a literal empty override, not whitespace.
        var observedHeaders = fixture.MockServer.GetLastRequestHeaders(TestTableName);
        Assert.That(observedHeaders.TryGetValue("user-agent", out var userAgent), Is.True);
        Assert.That(userAgent, Does.StartWith("zerobus-sdk-dotnet/"));
    }

    [Test]
    public async Task ApplicationName_AppendedToDefaultSdkIdentifier()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_sdk_identifier_app_name"),
            MockResponses.RecordAckResponse(0),
        ]);

        using var sdk = ZerobusSdk.CreateBuilder()
            .Endpoint(fixture.ServerUrl)
            .UnityCatalogUrl("https://mock-uc.com")
            .ApplicationName("integration-test")
            .DisableTls()
            .Build();

        using var stream = sdk.CreateStreamWithHeadersProvider(
            CreateTableProperties(), new TestHeadersProvider(), CreateDefaultOptions());

        stream.IngestRecord("test record data"u8.ToArray());
        stream.Flush();

        // The core sends "<identifier> <application name>" and tonic appends its own token.
        var observedHeaders = fixture.MockServer.GetLastRequestHeaders(TestTableName);
        Assert.That(observedHeaders.TryGetValue("user-agent", out var userAgent), Is.True);
        Assert.That(userAgent, Does.StartWith("zerobus-sdk-dotnet/"));
        Assert.That(userAgent, Does.Contain(" integration-test"));
    }
}
