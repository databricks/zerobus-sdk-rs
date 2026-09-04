using Databricks.Zerobus.Protocol;
using Google.Protobuf;
using Grpc.Net.Client;
using NUnit.Framework;

namespace Databricks.Zerobus.IntegrationTests;

[TestFixture]
[Parallelizable(ParallelScope.Children)]
public class MockZerobusServerIntegrationTests
{
    [Test]
    public async Task AvroBatchUpdatesWriteCount()
    {
        await using var fixture = await MockServerFixture.StartAsync();
        using var channel = GrpcChannel.ForAddress(fixture.ServerUrl);
        var client = new Databricks.Zerobus.Protocol.Zerobus.ZerobusClient(channel);
        using var call = client.EphemeralStream(deadline: DateTime.UtcNow.AddSeconds(10));

        await call.RequestStream.WriteAsync(new EphemeralStreamRequest
        {
            CreateStream = new CreateIngestStreamRequest
            {
                TableName = "catalog.schema.table",
                RecordType = Databricks.Zerobus.Protocol.RecordType.Avro,
                AvroSchemaJson = """{"type":"record","name":"TestRecord","fields":[]}""",
            },
        });
        Assert.That(await call.ResponseStream.MoveNext(CancellationToken.None), Is.True);

        await call.RequestStream.WriteAsync(new EphemeralStreamRequest
        {
            IngestRecordBatch = new IngestRecordBatchRequest
            {
                AvroBatch = new AvroRecordBatch
                {
                    Records = { ByteString.Empty, ByteString.Empty, ByteString.Empty },
                },
            },
        });

        await call.RequestStream.CompleteAsync();
        while (await call.ResponseStream.MoveNext(CancellationToken.None))
        {
        }

        Assert.That(fixture.MockServer.GetWriteCount(), Is.EqualTo(3));
    }
}
