using System.Net;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Databricks.Zerobus.IntegrationTests;

/// <summary>
/// Simple headers provider for testing.
/// Returns a fixed Bearer token and table name header.
/// </summary>
public sealed class TestHeadersProvider : IHeadersProvider
{
    public IDictionary<string, string> GetHeaders()
    {
        return new Dictionary<string, string>
        {
            ["authorization"] = "Bearer test_token",
            ["x-databricks-zerobus-table-name"] = "test_table",
        };
    }
}

/// <summary>
/// Helper methods for creating mock responses and test data.
/// </summary>
public static class MockResponses
{
    public static MockResponse CreateStreamResponse(string streamId, long delayMs = 0) => new()
    {
        Type = MockResponseType.CreateStream,
        StreamId = streamId,
        DelayMs = delayMs,
    };

    public static MockResponse RecordAckResponse(long offset, long delayMs = 0) => new()
    {
        Type = MockResponseType.RecordAck,
        AckUpToOffset = offset,
        DelayMs = delayMs,
    };

    public static MockResponse ErrorResponse(StatusCode code, string message, long delayMs = 0) => new()
    {
        Type = MockResponseType.Error,
        GrpcStatusCode = code,
        GrpcMessage = message,
        DelayMs = delayMs,
    };

    public static MockResponse CloseStreamSignalResponse(long durationSeconds, long delayMs = 0) => new()
    {
        Type = MockResponseType.CloseStreamSignal,
        DurationSeconds = durationSeconds,
        DelayMs = delayMs,
    };
}

/// <summary>
/// Creates a simple test protobuf descriptor.
/// This is a pre-serialized FileDescriptorSet for:
///   message TestMessage { int64 id = 1; string message = 2; }
/// Matches the Go CreateTestDescriptorProto().
/// </summary>
public static class TestDescriptor
{
    public static byte[] CreateTestDescriptorProto() =>
    [
        0x0a, 0x45, 0x0a, 0x0b, 0x74, 0x65, 0x73, 0x74, 0x2e, 0x70, 0x72, 0x6f,
        0x74, 0x6f, 0x22, 0x36, 0x0a, 0x0b, 0x54, 0x65, 0x73, 0x74, 0x4d, 0x65,
        0x73, 0x73, 0x61, 0x67, 0x65, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18,
        0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x02, 0x69, 0x64, 0x12, 0x17, 0x0a,
        0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x18, 0x02, 0x20, 0x01,
        0x28, 0x09, 0x52, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x62,
        0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
    ];
}

/// <summary>
/// Manages the lifecycle of the mock gRPC server for integration tests.
/// Starts an ASP.NET Core Kestrel server with the gRPC mock service.
/// </summary>
public sealed class MockServerFixture : IAsyncDisposable
{
    public MockZerobusServer MockServer { get; }
    public string ServerUrl { get; }

    private readonly WebApplication _app;

    private MockServerFixture(MockZerobusServer mockServer, string serverUrl, WebApplication app)
    {
        MockServer = mockServer;
        ServerUrl = serverUrl;
        _app = app;
    }

    /// <summary>
    /// Starts a new mock gRPC server on a random available port.
    /// </summary>
    public static async Task<MockServerFixture> StartAsync()
    {
        var mockServer = new MockZerobusServer();

        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders(); // Suppress noisy ASP.NET Core logs in test output.
        builder.Services.AddGrpc(options =>
        {
            options.Interceptors.Add<ExceptionInterceptor>();
        });
        builder.Services.AddSingleton(mockServer);
        builder.WebHost.ConfigureKestrel(options =>
        {
            // Listen on 127.0.0.1 with a random port and HTTP/2 (required for gRPC).
            options.Listen(IPAddress.Loopback, 0, listenOptions =>
            {
                listenOptions.Protocols = HttpProtocols.Http2;
            });
        });

        var app = builder.Build();
        app.MapGrpcService<MockZerobusServer>();

        await app.StartAsync();

        // Give the server a moment to be fully ready for connections.
        await Task.Delay(100);

        // Get the actual bound address from the server features.
        var addresses = app.Urls;
        var serverUrl = addresses.FirstOrDefault()
            ?? throw new InvalidOperationException("No server URL was bound.");

        return new MockServerFixture(mockServer, serverUrl, app);
    }

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
    }
}

/// <summary>
/// gRPC server interceptor that catches unhandled exceptions and converts them
/// to proper <see cref="RpcException"/> responses. Without this, ASP.NET Core
/// wraps unhandled exceptions as StatusCode.Unknown "Exception was thrown by handler."
/// which confuses the Rust SDK's error classification.
/// </summary>
internal sealed class ExceptionInterceptor : Interceptor
{
    public override async Task DuplexStreamingServerHandler<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        DuplexStreamingServerMethod<TRequest, TResponse> continuation)
    {
        try
        {
            await continuation(requestStream, responseStream, context);
        }
        catch (RpcException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw new RpcException(new Status(StatusCode.Cancelled, "Client disconnected"));
        }
        catch (IOException)
        {
            throw new RpcException(new Status(StatusCode.Cancelled, "Client disconnected"));
        }
        catch (InvalidOperationException)
        {
            throw new RpcException(new Status(StatusCode.Cancelled, "Client disconnected"));
        }
    }
}
