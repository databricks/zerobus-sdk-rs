using Grpc.Core;
using Grpc.Net.Client;
using Xunit;

namespace Databricks.Zerobus.IntegrationTests;

/// <summary>
/// Tests the mock gRPC server standalone — no Zerobus SDK dependency.
/// Proves the mock infrastructure works before plugging in native libs.
/// </summary>
public class MockServerSelfTest : IAsyncLifetime
{
    private readonly MockZerobusServer _server = new();

    public async Task InitializeAsync() => await _server.StartAsync();
    public Task DisposeAsync() => _server.DisposeAsync().AsTask();

    [Fact]
    public async Task ServerStartsAndAcceptsConnection()
    {
        using var channel = GrpcChannel.ForAddress(_server.Endpoint);
        var client = new Zerobus.ZerobusClient(channel);

        var call = client.EphemeralStream();

        // Send create stream request
        await call.RequestStream.WriteAsync(new EphemeralStreamRequest
        {
            CreateStream = new CreateIngestStreamRequest
            {
                TableName = "test.table",
                RecordType = RecordType.Json
            }
        });
        await call.RequestStream.CompleteAsync();

        // Read response
        var responses = new List<EphemeralStreamResponse>();
        await foreach (var resp in call.ResponseStream.ReadAllAsync())
        {
            responses.Add(resp);
        }

        Assert.NotEmpty(responses);
        Assert.NotNull(responses[0].CreateStreamResponse?.StreamId);
    }

    [Fact]
    public async Task IngestJsonRecord_ReceivesAck()
    {
        using var channel = GrpcChannel.ForAddress(_server.Endpoint);
        var client = new Zerobus.ZerobusClient(channel);
        var call = client.EphemeralStream();

        // Create stream
        await call.RequestStream.WriteAsync(new EphemeralStreamRequest
        {
            CreateStream = new CreateIngestStreamRequest
            {
                TableName = "test.table",
                RecordType = RecordType.Json
            }
        });

        // Ingest a record
        await call.RequestStream.WriteAsync(new EphemeralStreamRequest
        {
            IngestRecord = new IngestRecordRequest
            {
                OffsetId = 42,
                JsonRecord = "{\"key\":\"value\"}"
            }
        });

        await call.RequestStream.CompleteAsync();

        var responses = new List<EphemeralStreamResponse>();
        await foreach (var resp in call.ResponseStream.ReadAllAsync())
        {
            responses.Add(resp);
        }

        // Should have: create_stream_response + ingest_record_response (ack)
        Assert.True(responses.Count >= 2,
            $"Expected >=2 responses, got {responses.Count}");
        Assert.NotNull(responses[0].CreateStreamResponse?.StreamId);
        Assert.Equal(42, responses[1].IngestRecordResponse?.DurabilityAckUpToOffset);
    }

    [Fact]
    public async Task MockTracksIngestedRecords()
    {
        _server.Service.ShouldAckRecords = true;

        using var channel = GrpcChannel.ForAddress(_server.Endpoint);
        var client = new Zerobus.ZerobusClient(channel);
        var call = client.EphemeralStream();

        await call.RequestStream.WriteAsync(new EphemeralStreamRequest
        {
            CreateStream = new CreateIngestStreamRequest
            {
                TableName = "test.table",
                RecordType = RecordType.Proto
            }
        });

        for (long i = 1; i <= 3; i++)
        {
            await call.RequestStream.WriteAsync(new EphemeralStreamRequest
            {
                IngestRecord = new IngestRecordRequest
                {
                    OffsetId = i,
                    ProtoEncodedRecord = Google.Protobuf.ByteString.CopyFrom(new byte[] { (byte)i })
                }
            });
        }

        await call.RequestStream.CompleteAsync();

        // Drain all responses. With ShouldAckRecords=true, we get ack for each record.
        var acks = new List<long>();
        await foreach (var resp in call.ResponseStream.ReadAllAsync())
        {
            if (resp.IngestRecordResponse != null)
                acks.Add(resp.IngestRecordResponse.DurabilityAckUpToOffset);
        }

        // We should have received acks for the ingested records
        Assert.NotEmpty(acks);
        Assert.Equal(3, acks.Count);
        Assert.Equal(1, acks[0]);
        Assert.Equal(3, acks[2]);
    }

    [Fact]
    public async Task BatchIngestion_ReceivesSingleAck()
    {
        _server.Service.ShouldAckRecords = true;

        using var channel = GrpcChannel.ForAddress(_server.Endpoint);
        var client = new Zerobus.ZerobusClient(channel);
        var call = client.EphemeralStream();

        await call.RequestStream.WriteAsync(new EphemeralStreamRequest
        {
            CreateStream = new CreateIngestStreamRequest
            {
                TableName = "test.table",
                RecordType = RecordType.Json
            }
        });

        await call.RequestStream.WriteAsync(new EphemeralStreamRequest
        {
            IngestRecordBatch = new IngestRecordBatchRequest
            {
                OffsetId = 100,
                JsonBatch = new JsonRecordBatch
                {
                    Records = { "{\"a\":1}", "{\"b\":2}", "{\"c\":3}" }
                }
            }
        });

        await call.RequestStream.CompleteAsync();

        var responses = new List<EphemeralStreamResponse>();
        await foreach (var resp in call.ResponseStream.ReadAllAsync())
            responses.Add(resp);

        // Should receive: create_stream_response + batch ack
        Assert.True(responses.Count >= 2);
        Assert.Equal(100, responses[1].IngestRecordResponse?.DurabilityAckUpToOffset);
    }
}
