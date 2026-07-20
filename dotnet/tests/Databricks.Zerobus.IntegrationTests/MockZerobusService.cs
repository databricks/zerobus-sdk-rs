using Databricks.Zerobus;
using Grpc.Core;

namespace Databricks.Zerobus.IntegrationTests;

/// <summary>
/// Real gRPC service implementation that simulates the Zerobus ingestion server.
/// Implements EphemeralStream — a bidirectional streaming RPC.
/// </summary>
public class MockZerobusService : Zerobus.ZerobusBase
{
    // Injectable behavior for test scenarios
    public bool ShouldAcceptStream { get; set; } = true;
    public bool ShouldAckRecords { get; set; } = true;
    public int AckDelayMs { get; set; }
    public int? FailAfterNRecords { get; set; }
    public string? ErrorMessage { get; set; }
    public bool SimulateDisconnect { get; set; }

    // Tracking for assertions
    public List<IngestedRecord> IngestedRecords { get; } = new();
    public List<long> AcknowledgedOffsets { get; } = new();
    public string? LastTableName { get; private set; }
    public byte[]? LastDescriptorProto { get; private set; }
    public RecordType? LastRecordType { get; private set; }
    private int _streamCount;
    public int StreamCount => _streamCount;

    public override async Task EphemeralStream(
        IAsyncStreamReader<EphemeralStreamRequest> requestStream,
        IServerStreamWriter<EphemeralStreamResponse> responseStream,
        ServerCallContext context)
    {
        Interlocked.Increment(ref _streamCount);

        long lastAckedOffset = -1;
        bool streamCreated = false;

        try
        {
            await foreach (var request in requestStream.ReadAllAsync(context.CancellationToken))
            {
                switch (request.PayloadCase)
                {
                    case EphemeralStreamRequest.PayloadOneofCase.CreateStream:
                        await HandleCreateStream(request.CreateStream, responseStream, context);
                        streamCreated = true;
                        break;

                    case EphemeralStreamRequest.PayloadOneofCase.IngestRecord:
                        if (!streamCreated) throw new InvalidOperationException("Ingest before stream creation");
                        lastAckedOffset = HandleIngestRecord(request.IngestRecord);
                        if (ShouldAckRecords)
                            await SendAck(responseStream, lastAckedOffset, context);
                        break;

                    case EphemeralStreamRequest.PayloadOneofCase.IngestRecordBatch:
                        if (!streamCreated) throw new InvalidOperationException("Ingest batch before stream creation");
                        lastAckedOffset = HandleIngestBatch(request.IngestRecordBatch);
                        if (ShouldAckRecords)
                            await SendAck(responseStream, lastAckedOffset, context);
                        break;

                    case EphemeralStreamRequest.PayloadOneofCase.None:
                        break;
                }

                if (SimulateDisconnect)
                {
                    throw new IOException("Simulated network disconnect");
                }

                if (FailAfterNRecords.HasValue && IngestedRecords.Count >= FailAfterNRecords.Value)
                {
                    throw new RpcException(new Status(StatusCode.Internal,
                        ErrorMessage ?? "Simulated server error"));
                }
            }
        }
        catch (IOException)
        {
            // Simulate disconnect — don't send close signal
            throw;
        }
    }

    private async Task HandleCreateStream(
        CreateIngestStreamRequest request,
        IServerStreamWriter<EphemeralStreamResponse> responseStream,
        ServerCallContext context)
    {
        LastTableName = request.TableName;
        LastDescriptorProto = request.DescriptorProto?.ToByteArray();
        LastRecordType = request.RecordType;

        if (!ShouldAcceptStream)
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied,
                ErrorMessage ?? "Stream rejected by mock"));
        }

        var response = new EphemeralStreamResponse
        {
            CreateStreamResponse = new CreateIngestStreamResponse
            {
                StreamId = Guid.NewGuid().ToString()
            }
        };

        await responseStream.WriteAsync(response);
    }

    private long HandleIngestRecord(IngestRecordRequest request)
    {
        var record = new IngestedRecord
        {
            OffsetId = request.OffsetId,
            IsJson = request.RecordCase == IngestRecordRequest.RecordOneofCase.JsonRecord,
            Data = request.RecordCase switch
            {
                IngestRecordRequest.RecordOneofCase.ProtoEncodedRecord =>
                    request.ProtoEncodedRecord.ToByteArray(),
                IngestRecordRequest.RecordOneofCase.JsonRecord =>
                    System.Text.Encoding.UTF8.GetBytes(request.JsonRecord),
                _ => Array.Empty<byte>()
            },
            Timestamp = DateTimeOffset.UtcNow
        };

        IngestedRecords.Add(record);

        // Simulate processing delay
        if (AckDelayMs > 0)
            Thread.Sleep(AckDelayMs);

        return request.OffsetId;
    }

    private long HandleIngestBatch(IngestRecordBatchRequest request)
    {
        AcknowledgedOffsets.Add(request.OffsetId);

        switch (request.BatchCase)
        {
            case IngestRecordBatchRequest.BatchOneofCase.ProtoEncodedBatch:
                foreach (var bytes in request.ProtoEncodedBatch.Records)
                {
                    IngestedRecords.Add(new IngestedRecord
                    {
                        OffsetId = request.OffsetId,
                        IsJson = false,
                        Data = bytes.ToByteArray(),
                        Timestamp = DateTimeOffset.UtcNow
                    });
                }
                break;

            case IngestRecordBatchRequest.BatchOneofCase.JsonBatch:
                foreach (var json in request.JsonBatch.Records)
                {
                    IngestedRecords.Add(new IngestedRecord
                    {
                        OffsetId = request.OffsetId,
                        IsJson = true,
                        Data = System.Text.Encoding.UTF8.GetBytes(json),
                        Timestamp = DateTimeOffset.UtcNow
                    });
                }
                break;
        }

        return request.OffsetId;
    }

    private async Task SendAck(
        IServerStreamWriter<EphemeralStreamResponse> writer,
        long offset,
        ServerCallContext context)
    {
        AcknowledgedOffsets.Add(offset);

        var response = new EphemeralStreamResponse
        {
            IngestRecordResponse = new IngestRecordResponse
            {
                DurabilityAckUpToOffset = offset
            }
        };
        await writer.WriteAsync(response);
    }
}

/// <summary>
/// Tracked record from mock gRPC ingestion.
/// </summary>
public sealed class IngestedRecord
{
    public long OffsetId { get; set; }
    public bool IsJson { get; set; }
    public byte[] Data { get; set; } = Array.Empty<byte>();
    public DateTimeOffset Timestamp { get; set; } = DateTimeOffset.UtcNow;
}
