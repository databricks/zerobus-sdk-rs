using Databricks.Zerobus;

namespace Databricks.Zerobus.Examples.JsonIngestion;

public static class BatchIngestionExample
{
    public static async Task Run()
    // ===================================================================
    // Batch JSON Ingestion Example
    // ===================================================================
    {
        const string serverEndpoint = "https://YOUR_WORKSPACE.databricks.com";
        const string unityCatalogUrl = "https://YOUR_WORKSPACE.databricks.com/api/2.1/unity-catalog";
        const string tableName = "my_catalog.my_schema.my_table";
        const string clientId = "YOUR_SERVICE_PRINCIPAL_CLIENT_ID";
        const string clientSecret = "YOUR_SERVICE_PRINCIPAL_CLIENT_SECRET";

        Console.WriteLine("Zerobus JSON Batch Ingestion Example");
        Console.WriteLine("=====================================");

        using var sdk = new ZerobusSdk(serverEndpoint, unityCatalogUrl);

        using var stream = await sdk.StreamBuilder()
            .Table(tableName)
            .OAuth(clientId, clientSecret)
            .MaxInflightRecords(500_000)
            .Recovery(true)
            .Json()
            .BuildAsync();

        Console.WriteLine($"Stream created for table: {stream.TableName}");

        var records = new List<string>();
        for (int i = 1; i <= 1000; i++)
        {
            records.Add($"{{\"id\": {i}, \"name\": \"Batch Item {i}\", \"value\": {i * 1.5}}}");
        }

        var lastOffset = stream.IngestRecords(records);
        Console.WriteLine($"Ingested {records.Count} records. Last offset: {lastOffset}");

        stream.Flush();
        Console.WriteLine("All records flushed and durably acknowledged.");
        Console.WriteLine("Done.");
    }
}
