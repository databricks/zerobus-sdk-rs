using Databricks.Zerobus;

// Get configuration from environment.
var zerobusEndpoint = Environment.GetEnvironmentVariable("ZEROBUS_SERVER_ENDPOINT")
    ?? throw new InvalidOperationException("ZEROBUS_SERVER_ENDPOINT not set");
var unityCatalogUrl = Environment.GetEnvironmentVariable("DATABRICKS_WORKSPACE_URL")
    ?? throw new InvalidOperationException("DATABRICKS_WORKSPACE_URL not set");
var clientId = Environment.GetEnvironmentVariable("DATABRICKS_CLIENT_ID")
    ?? throw new InvalidOperationException("DATABRICKS_CLIENT_ID not set");
var clientSecret = Environment.GetEnvironmentVariable("DATABRICKS_CLIENT_SECRET")
    ?? throw new InvalidOperationException("DATABRICKS_CLIENT_SECRET not set");
var tableName = Environment.GetEnvironmentVariable("ZEROBUS_TABLE_NAME")
    ?? throw new InvalidOperationException("ZEROBUS_TABLE_NAME not set");

// Create SDK instance.
using var sdk = new ZerobusSdk(zerobusEndpoint, unityCatalogUrl);

// Configure stream options (optional).
var options = StreamConfigurationOptions.Default with
{
    MaxInflightRequests = 50_000,
    RecordType = RecordType.Json,
};

// Create stream.
using var stream = sdk.CreateStream(
    new TableProperties(tableName),
    clientId,
    clientSecret,
    options);

Console.WriteLine("Ingesting records...");
var offsets = new List<long>();

for (int i = 0; i < 5; i++)
{
    // Change this JSON to match the schema of your table.
    var jsonRecord = """
        {
            "device_name": "sensor-001",
            "temp": 20,
            "humidity": 60
        }
        """;

    try
    {
        long offset = stream.IngestRecord(jsonRecord);
        Console.WriteLine($"Ingested record {i} at offset {offset}");
        offsets.Add(offset);
    }
    catch (ZerobusException ex) when (ex.IsRetryable)
    {
        Console.WriteLine($"Failed to ingest record {i} (retryable): {ex.RawMessage}");
    }
    catch (ZerobusException ex)
    {
        Console.WriteLine($"Failed to ingest record {i}: {ex.RawMessage}");
    }
}

// Wait for specific offsets to be acknowledged.
Console.WriteLine("Waiting for acknowledgments...");
foreach (var offset in offsets)
{
    stream.WaitForOffset(offset);
    Console.WriteLine($"Record at offset {offset} acknowledged");
}

Console.WriteLine("All records successfully ingested and acknowledged!");
