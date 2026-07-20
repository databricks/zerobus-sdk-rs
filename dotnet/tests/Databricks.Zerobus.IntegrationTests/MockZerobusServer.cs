using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Databricks.Zerobus.IntegrationTests;

/// <summary>
/// Manages a real gRPC server that simulates the Zerobus ingestion service.
/// Each test gets its own server on a unique port — fully parallelizable.
/// </summary>
public sealed class MockZerobusServer : IAsyncDisposable
{
    private IHost? _host;
    private int _port;

    /// <summary>
    /// The mock service implementation. Configure behavior via its properties
    /// before starting the server.
    /// </summary>
    public MockZerobusService Service { get; } = new();

    /// <summary>
    /// The gRPC endpoint for this server (http://localhost:{port}).
    /// </summary>
    public string Endpoint => $"http://localhost:{_port}";

    /// <summary>
    /// The simulated Unity Catalog endpoint.
    /// </summary>
    public string UnityCatalogEndpoint => $"http://localhost:{_port}/api/2.1/unity-catalog";

    /// <summary>
    /// Starts the gRPC server on a random available port.
    /// </summary>
    public async Task StartAsync()
    {
        _port = AllocatePort();

        _host = Host.CreateDefaultBuilder()
            .ConfigureWebHostDefaults(webBuilder =>
            {
                webBuilder.UseUrls($"http://localhost:{_port}");
                webBuilder.ConfigureKestrel(opts =>
                {
                    opts.ListenLocalhost(_port, o => o.Protocols = HttpProtocols.Http2);
                });
                webBuilder.ConfigureServices(services =>
                {
                    services.AddGrpc();
                });
                webBuilder.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(endpoints =>
                    {
                        endpoints.MapGrpcService<MockZerobusService>();
                    });
                });
            })
            .Build();

        await _host.StartAsync();
    }

    /// <summary>
    /// Stops the server gracefully.
    /// </summary>
    public async Task StopAsync()
    {
        if (_host != null)
        {
            await _host.StopAsync();
            _host.Dispose();
            _host = null;
        }
    }

    /// <summary>
    /// Resets all state and tracking for a new test scenario.
    /// </summary>
    public void Reset()
    {
        Service.ShouldAcceptStream = true;
        Service.ShouldAckRecords = true;
        Service.AckDelayMs = 0;
        Service.FailAfterNRecords = null;
        Service.ErrorMessage = null;
        Service.SimulateDisconnect = false;
        Service.IngestedRecords.Clear();
        Service.AcknowledgedOffsets.Clear();
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
    }

    private static int AllocatePort()
    {
        using var socket = new System.Net.Sockets.Socket(
            System.Net.Sockets.AddressFamily.InterNetwork,
            System.Net.Sockets.SocketType.Stream,
            System.Net.Sockets.ProtocolType.Tcp);
        socket.Bind(new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 0));
        return ((System.Net.IPEndPoint)socket.LocalEndPoint!).Port;
    }
}
