using Databricks.Zerobus;

namespace Databricks.Zerobus.Examples.ProtoIngestion;

public static class BatchIngestionExample
{
    public static void Run()
    // ===================================================================
    // Batch Protocol Buffers Ingestion Example
    // ===================================================================
    {
        Console.WriteLine("Zerobus Protocol Buffers Batch Ingestion Example");
        Console.WriteLine("================================================");

        // Replace with real values:
        // const string serverEndpoint = "https://YOUR_WORKSPACE.databricks.com";
        // const string unityCatalogUrl = "https://YOUR_WORKSPACE.databricks.com/api/2.1/unity-catalog";
        // const string tableName = "my_catalog.my_schema.my_table";
        // const string clientId = "YOUR_SERVICE_PRINCIPAL_CLIENT_ID";
        // const string clientSecret = "YOUR_SERVICE_PRINCIPAL_CLIENT_SECRET";

        // byte[] descriptorBytes = Array.Empty<byte>();
        // using var sdk = new ZerobusSdk(serverEndpoint, unityCatalogUrl);
        //
        // await using var stream = await sdk.StreamBuilder()
        //     .Table(tableName)
        //     .OAuth(clientId, clientSecret)
        //     .CompiledProto(descriptorBytes)
        //     .BuildAsync<MyProtoMessage>();
        //
        // var records = new List<byte[]>();
        // for (int i = 0; i < 1000; i++)
        // {
        //     var msg = new MyProtoMessage { Id = i, Name = $"Item {i}" };
        //     records.Add(msg.ToByteArray());
        // }
        //
        // var lastOffset = stream.IngestRecords(records);
        // stream.Flush();

        Console.WriteLine("See code comments for protobuf batch ingestion setup.");
        Console.WriteLine("Done.");
    }
}
