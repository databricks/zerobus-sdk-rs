using NUnit.Framework;

namespace Databricks.Zerobus.IntegrationTests;

[TestFixture]
[Parallelizable(ParallelScope.Children)]
public class HeadersProviderIntegrationTests : IntegrationTestBase
{
    [Test]
    public async Task CreateStreamWithHeadersProvider_InvokesCallbackAndSendsHeaders()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_headers_provider"),
        ]);

        var provider = new CountingHeadersProvider(new Dictionary<string, string>
        {
            ["authorization"] = "Bearer callback_token",
            ["x-databricks-zerobus-table-name"] = TestTableName,
            ["x-test-callback-header"] = "headers-bridge-path",
        });

        using var sdk = CreateDefaultSdk(fixture);

        var tableProps = CreateTableProperties();

        using var stream = sdk.CreateStreamWithHeadersProvider(
            tableProps,
            provider,
            StreamConfigurationOptions.Default with { Recovery = false });

        Assert.That(provider.CallCount, Is.GreaterThan(0));

        var observedHeaders = fixture.MockServer.GetLastRequestHeaders(TestTableName);
        Assert.That(observedHeaders.TryGetValue("x-test-callback-header", out var callbackHeader), Is.True);
        Assert.That(callbackHeader, Is.EqualTo("headers-bridge-path"));
        Assert.That(observedHeaders.TryGetValue("authorization", out var authHeader), Is.True);
        Assert.That(authHeader, Is.EqualTo("Bearer callback_token"));
    }

    [Test]
    public async Task CreateStreamWithHeadersProviderAsync_InvokesCallbackAndSendsHeaders()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_headers_provider_async"),
        ]);

        var provider = new CountingHeadersProvider(new Dictionary<string, string>
        {
            ["authorization"] = "Bearer callback_token",
            ["x-databricks-zerobus-table-name"] = TestTableName,
            ["x-test-callback-header"] = "headers-bridge-path",
        });

        using var sdk = CreateDefaultSdk(fixture);

        var tableProps = CreateTableProperties();

        await using var stream = await sdk.CreateStreamWithHeadersProviderAsync(
            tableProps,
            provider,
            StreamConfigurationOptions.Default with { Recovery = false });

        Assert.That(provider.CallCount, Is.GreaterThan(0));

        var observedHeaders = fixture.MockServer.GetLastRequestHeaders(TestTableName);
        Assert.That(observedHeaders.TryGetValue("x-test-callback-header", out var callbackHeader), Is.True);
        Assert.That(callbackHeader, Is.EqualTo("headers-bridge-path"));
        Assert.That(observedHeaders.TryGetValue("authorization", out var authHeader), Is.True);
        Assert.That(authHeader, Is.EqualTo("Bearer callback_token"));
    }
}
