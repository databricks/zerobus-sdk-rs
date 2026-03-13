using Grpc.Core;
using Google.Protobuf.WellKnownTypes;
using Databricks.Zerobus.IntegrationTests.Protos;

namespace Databricks.Zerobus.IntegrationTests;

/// <summary>
/// The type of a mock response that can be injected into the mock server.
/// </summary>
public enum MockResponseType
{
    CreateStream,
    RecordAck,
    CloseStreamSignal,
    Error,
}

/// <summary>
/// Represents a response that can be injected into the mock server.
/// </summary>
public sealed class MockResponse
{
    public MockResponseType Type { get; init; }
    public string? StreamId { get; init; }
    public long DelayMs { get; init; }
    public long AckUpToOffset { get; init; }
    public long DurationSeconds { get; init; }
    public StatusCode? GrpcStatusCode { get; init; }
    public string? GrpcMessage { get; init; }
}

/// <summary>
/// Mock gRPC server that implements the Zerobus EphemeralStream RPC for integration testing.
/// Faithfully ports the Go mock_server.go implementation.
/// </summary>
public sealed class MockZerobusServer : Protos.Zerobus.ZerobusBase
{
    private readonly object _responsesLock = new();
    private readonly object _counterLock = new();
    private readonly object _offsetLock = new();
    private readonly object _writeCountLock = new();
    private readonly object _responseIndicesLock = new();

    private Dictionary<string, List<MockResponse>> _responses = new();
    private Dictionary<string, int> _responseIndices = new();
    private int _streamCounter;
    private long _maxOffsetSent = -1;
    private ulong _writeCount;

    /// <summary>
    /// Configures responses for a specific table.
    /// </summary>
    public void InjectResponses(string tableName, List<MockResponse> responses)
    {
        lock (_responsesLock)
        {
            _responses[tableName] = new List<MockResponse>(responses);
        }

        lock (_responseIndicesLock)
        {
            _responseIndices[tableName] = 0;
        }
    }

    /// <summary>
    /// Returns the maximum offset sent by clients.
    /// </summary>
    public long GetMaxOffsetSent()
    {
        lock (_offsetLock)
        {
            return _maxOffsetSent;
        }
    }

    /// <summary>
    /// Returns the number of writes received.
    /// </summary>
    public ulong GetWriteCount()
    {
        lock (_writeCountLock)
        {
            return _writeCount;
        }
    }

    /// <summary>
    /// Resets all server state.
    /// </summary>
    public void Reset()
    {
        lock (_responsesLock) { _responses = new Dictionary<string, List<MockResponse>>(); }
        lock (_responseIndicesLock) { _responseIndices = new Dictionary<string, int>(); }
        lock (_offsetLock) { _maxOffsetSent = -1; }
        lock (_writeCountLock) { _writeCount = 0; }
        lock (_counterLock) { _streamCounter = 0; }
    }

    /// <summary>
    /// Implements the bidirectional streaming EphemeralStream RPC.
    /// </summary>
    public override async Task EphemeralStream(
        IAsyncStreamReader<EphemeralStreamRequest> requestStream,
        IServerStreamWriter<EphemeralStreamResponse> responseStream,
        ServerCallContext context)
    {
        // Send initial headers/metadata to signal readiness.
        // This is CRITICAL: Without this, the Rust SDK will wait indefinitely.
        await context.WriteResponseHeadersAsync(new Metadata());

        // Read the first message (CreateStream request).
        if (!await requestStream.MoveNext(context.CancellationToken))
        {
            return;
        }

        var firstRequest = requestStream.Current;
        var createReq = firstRequest.CreateStream;
        if (createReq is null)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "first message must be CreateStream"));
        }

        var tableName = createReq.TableName ?? "";

        // Get stream ID.
        string streamId;
        lock (_counterLock)
        {
            _streamCounter++;
            streamId = $"test_stream_{_streamCounter}";
        }

        // Get configured responses for this table.
        List<MockResponse> streamResponses;
        lock (_responsesLock)
        {
            streamResponses = _responses.TryGetValue(tableName, out var responses)
                ? new List<MockResponse>(responses)
                : [];
        }

        // Get current response index.
        int responseIndex;
        lock (_responseIndicesLock)
        {
            _responseIndices.TryGetValue(tableName, out responseIndex);
        }

        // Search for the next CreateStream response starting from responseIndex.
        var createStreamFound = false;
        for (var idx = responseIndex; idx < streamResponses.Count; idx++)
        {
            var mockResp = streamResponses[idx];
            if (mockResp.Type == MockResponseType.CreateStream)
            {
                responseIndex = idx;
                createStreamFound = true;

                if (mockResp.DelayMs > 0)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(mockResp.DelayMs));
                }

                var customId = mockResp.StreamId ?? streamId;

                await responseStream.WriteAsync(new EphemeralStreamResponse
                {
                    CreateStreamResponse = new CreateIngestStreamResponse
                    {
                        StreamId = customId,
                    },
                });

                responseIndex++;
                lock (_responseIndicesLock)
                {
                    _responseIndices[tableName] = responseIndex;
                }

                break;
            }
            else if (mockResp.Type == MockResponseType.Error)
            {
                if (mockResp.DelayMs > 0)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(mockResp.DelayMs));
                }

                lock (_responseIndicesLock)
                {
                    _responseIndices[tableName] = idx + 1;
                }

                throw new RpcException(new Status(
                    mockResp.GrpcStatusCode ?? StatusCode.Internal,
                    mockResp.GrpcMessage ?? "Unknown error"));
            }
        }

        // If no CreateStream response was configured, send default.
        if (!createStreamFound)
        {
            await responseStream.WriteAsync(new EphemeralStreamResponse
            {
                CreateStreamResponse = new CreateIngestStreamResponse
                {
                    StreamId = streamId,
                },
            });
        }

        // Process subsequent requests.
        while (await requestStream.MoveNext(context.CancellationToken))
        {
            var request = requestStream.Current;

            if (request.IngestRecord is { } ingestRecord)
            {
                responseIndex = await HandleIngestRecord(responseStream, ingestRecord, streamResponses, responseIndex, tableName);
            }
            else if (request.IngestRecordBatch is { } ingestBatch)
            {
                responseIndex = await HandleIngestRecordBatch(responseStream, ingestBatch, streamResponses, responseIndex, tableName);
            }
        }
    }

    private async Task<int> HandleIngestRecord(
        IServerStreamWriter<EphemeralStreamResponse> responseStream,
        IngestRecordRequest req,
        List<MockResponse> streamResponses,
        int responseIndex,
        string tableName)
    {
        // Update max offset.
        if (req.HasOffsetId)
        {
            lock (_offsetLock)
            {
                if (req.OffsetId > _maxOffsetSent)
                {
                    _maxOffsetSent = req.OffsetId;
                }
            }
        }

        // Increment write count.
        lock (_writeCountLock)
        {
            _writeCount++;
        }

        // Process mock response.
        if (responseIndex < streamResponses.Count)
        {
            var (shouldContinue, indexIncrement) = await HandleMockResponse(
                responseStream, streamResponses[responseIndex], req.HasOffsetId ? req.OffsetId : null, tableName);
            responseIndex += indexIncrement;

            lock (_responseIndicesLock)
            {
                _responseIndices[tableName] = responseIndex;
            }

            if (!shouldContinue)
            {
                throw new RpcException(new Status(StatusCode.Internal, "mock response indicated stop"));
            }
        }

        return responseIndex;
    }

    private async Task<int> HandleIngestRecordBatch(
        IServerStreamWriter<EphemeralStreamResponse> responseStream,
        IngestRecordBatchRequest req,
        List<MockResponse> streamResponses,
        int responseIndex,
        string tableName)
    {
        // Count records in the batch.
        var recordCount = 0;
        if (req.ProtoEncodedBatch is { } protoBatch)
        {
            recordCount = protoBatch.Records.Count;
        }
        else if (req.JsonBatch is { } jsonBatch)
        {
            recordCount = jsonBatch.Records.Count;
        }

        // Update max offset.
        if (req.HasOffsetId)
        {
            lock (_offsetLock)
            {
                if (req.OffsetId > _maxOffsetSent)
                {
                    _maxOffsetSent = req.OffsetId;
                }
            }
        }

        // Increment write count by number of records.
        lock (_writeCountLock)
        {
            _writeCount += (ulong)recordCount;
        }

        // Process mock response.
        if (responseIndex < streamResponses.Count)
        {
            var (shouldContinue, indexIncrement) = await HandleMockResponse(
                responseStream, streamResponses[responseIndex], req.HasOffsetId ? req.OffsetId : null, tableName);
            responseIndex += indexIncrement;

            lock (_responseIndicesLock)
            {
                _responseIndices[tableName] = responseIndex;
            }

            if (!shouldContinue)
            {
                throw new RpcException(new Status(StatusCode.Internal, "mock response indicated stop"));
            }
        }

        return responseIndex;
    }

    private async Task<(bool ShouldContinue, int IndexIncrement)> HandleMockResponse(
        IServerStreamWriter<EphemeralStreamResponse> responseStream,
        MockResponse mockResp,
        long? offset,
        string tableName)
    {
        switch (mockResp.Type)
        {
            case MockResponseType.RecordAck:
                if (offset.HasValue && offset.Value == mockResp.AckUpToOffset)
                {
                    if (mockResp.DelayMs > 0)
                    {
                        await Task.Delay(TimeSpan.FromMilliseconds(mockResp.DelayMs));
                    }

                    await responseStream.WriteAsync(new EphemeralStreamResponse
                    {
                        IngestRecordResponse = new IngestRecordResponse
                        {
                            DurabilityAckUpToOffset = mockResp.AckUpToOffset,
                        },
                    });

                    return (true, 1);
                }
                return (true, 0);

            case MockResponseType.CloseStreamSignal:
                if (mockResp.DelayMs > 0)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(mockResp.DelayMs));
                }

                await responseStream.WriteAsync(new EphemeralStreamResponse
                {
                    CloseStreamSignal = new CloseStreamSignal
                    {
                        Duration = Duration.FromTimeSpan(TimeSpan.FromSeconds(mockResp.DurationSeconds)),
                    },
                });
                return (true, 1);

            case MockResponseType.Error:
                if (mockResp.DelayMs > 0)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(mockResp.DelayMs));
                }
                return (false, 1);

            case MockResponseType.CreateStream:
                return (true, 1);

            default:
                return (true, 0);
        }
    }
}
