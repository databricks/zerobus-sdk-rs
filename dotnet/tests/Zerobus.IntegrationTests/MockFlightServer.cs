using System.Text.Json;
using Apache.Arrow.Flight;
using Apache.Arrow.Flight.Server;
using Google.Protobuf;
using Grpc.Core;

namespace Databricks.Zerobus.IntegrationTests;

/// <summary>
/// Minimal Arrow Flight DoPut mock server for testing Arrow stream lifecycle.
///
/// Uses the Apache.Arrow.Flight FlightServer base class. The internal
/// FlightServerImplementation adapter handles protobuf serialization of
/// FlightData/PutResult messages and delegates to our DoPut override.
///
/// Protocol (matches Rust SDK arrow_stream.rs):
///   1. Client sends schema FlightData via DoPut request stream.
///   2. Server sends ready signal: PutResult { app_metadata = {"ack_up_to_offset":-1,"ack_up_to_records":0} }
///   3. Client sends data FlightData messages with app_metadata = {"offset_id": N}.
///   4. Server sends ack for each data batch: PutResult { app_metadata = {"ack_up_to_offset":N,"ack_up_to_records":M} }
/// </summary>
public sealed class MockFlightServer : FlightServer
{
    /// <summary>
    /// Static counter incremented on every DoPut call — used to verify the
    /// internal FlightServerImplementation adapter is routing requests.
    /// </summary>
    public static int DoPutCallCount;

    private readonly object _lock = new();

    private int _batchesReceived;
    private long _maxOffsetSeen = -1;
    private long _totalRecordsAcked;

    public int BatchesReceived
    {
        get { lock (_lock) { return _batchesReceived; } }
    }

    public long MaxOffsetSeen
    {
        get { lock (_lock) { return _maxOffsetSeen; } }
    }

    public long TotalRecordsAcked
    {
        get { lock (_lock) { return _totalRecordsAcked; } }
    }

    public void ArrowReset()
    {
        lock (_lock)
        {
            _batchesReceived = 0;
            _maxOffsetSeen = -1;
            _totalRecordsAcked = 0;
        }
    }

    /// <summary>
    /// Implements the Arrow Flight DoPut RPC.
    /// </summary>
    public override async Task DoPut(
        FlightServerRecordBatchStreamReader requestStream,
        IAsyncStreamWriter<FlightPutResult> responseStream,
        ServerCallContext context)
    {
        Interlocked.Increment(ref DoPutCallCount);

        try
        {
            // 1. Send stream-ready signal.
            // IMPORTANT: Do NOT pass context.CancellationToken to WriteAsync —
            // this gRPC implementation does not support it and throws NotSupportedException.
            var readyMeta = JsonSerializer.Serialize(
                new ArrowFlightAckMeta(AckUpToOffset: -1, AckUpToRecords: 0),
                ArrowFlightJson.Options);

            await responseStream.WriteAsync(
                new FlightPutResult(ByteString.CopyFromUtf8(readyMeta)));

            var cumulativeRecords = 0UL;

            // 2. Process incoming data FlightData messages.
            while (await requestStream.MoveNext(context.CancellationToken))
            {
                long offsetId = 0L;

                // ApplicationMetadata is IReadOnlyList<ByteString> — one entry
                // per data message. The schema message (consumed internally)
                // does not contribute to this list.
                var meta = requestStream.ApplicationMetadata;
                if (meta.Count > 0)
                {
                    var rawMeta = meta[meta.Count - 1];
                    try
                    {
                        var json = rawMeta.ToStringUtf8();
                        var batchMeta = JsonSerializer.Deserialize<ArrowFlightBatchMeta>(
                            json, ArrowFlightJson.Options);
                        if (batchMeta != null)
                        {
                            offsetId = batchMeta.OffsetId;
                        }
                    }
                    catch (JsonException)
                    {
                        // Malformed metadata — still ack to avoid hanging the Rust client.
                    }
                }

                // Extract row count and dispose native Arrow memory.
                var batch = requestStream.Current;
                ulong rowCount = 0;

                if (batch != null)
                {
                    try
                    {
                        rowCount = (ulong)batch.Length;
                    }
                    finally
                    {
                        batch.Dispose();
                    }
                }

                if (rowCount == 0)
                {
                    rowCount = 1;
                }

                cumulativeRecords += rowCount;

                lock (_lock)
                {
                    _batchesReceived++;
                    if (offsetId > _maxOffsetSeen)
                        _maxOffsetSeen = offsetId;
                    _totalRecordsAcked += (long)rowCount;
                }

                // 3. Send acknowledgement for the processed batch.
                var ackMeta = JsonSerializer.Serialize(
                    new ArrowFlightAckMeta(
                        AckUpToOffset: offsetId,
                        AckUpToRecords: cumulativeRecords),
                    ArrowFlightJson.Options);

                await responseStream.WriteAsync(
                    new FlightPutResult(ByteString.CopyFromUtf8(ackMeta)));
            }
        }
        catch (IOException)
        {
            // Client disconnected before sending data — normal termination
            // (e.g., stream was disposed without ingesting any batches).
        }
        catch (InvalidOperationException)
        {
            // Can't read after request is complete — normal during shutdown.
        }
    }
}

// ─── Metadata DTOs ────────────────────────────────────────────────────────────
// These match the Rust SDK's FlightBatchMetadata / FlightAckMetadata structs.

internal sealed record ArrowFlightBatchMeta(long OffsetId);

internal sealed record ArrowFlightAckMeta(long AckUpToOffset, ulong AckUpToRecords);

/// <summary>
/// Pre-configured JSON options with snake_case naming to match the Rust SDK.
/// </summary>
internal static class ArrowFlightJson
{
    public static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
    };
}

[System.Text.Json.Serialization.JsonSerializable(typeof(ArrowFlightBatchMeta))]
[System.Text.Json.Serialization.JsonSerializable(typeof(ArrowFlightAckMeta))]
internal partial class JsonContext : System.Text.Json.Serialization.JsonSerializerContext
{
}
