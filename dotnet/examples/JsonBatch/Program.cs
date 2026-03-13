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

Console.WriteLine("Ingesting batch of records...");

string[] batchRecords =
[
    """{"device_name": "sensor-001", "temp": 20, "humidity": 60}""",
    """{"device_name": "sensor-002", "temp": 21, "humidity": 61}""",
    """{"device_name": "sensor-003", "temp": 22, "humidity": 62}""",
    """{"device_name": "sensor-004", "temp": 23, "humidity": 63}""",
    """{"device_name": "sensor-005", "temp": 24, "humidity": 64}""",
];

long lastOffset = stream.IngestRecords(batchRecords);
Console.WriteLine($"Batch of {batchRecords.Length} records ingested, last offset: {lastOffset}");

// Wait for the last offset to ensure the entire batch is acknowledged.
stream.WaitForOffset(lastOffset);
Console.WriteLine("Batch acknowledged!");

Console.WriteLine("All operations completed successfully!");
