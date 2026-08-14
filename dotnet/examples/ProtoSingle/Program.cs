using Databricks.Zerobus;
using Databricks.Zerobus.Examples;
using Google.Protobuf;

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

// Use the descriptor for the exact generated message type sent on this stream.
byte[] descriptorProto = MyMessage.Descriptor.ToProto().ToByteArray();

// Create SDK instance.
using var sdk = ZerobusSdk.CreateBuilder()
    .Endpoint(zerobusEndpoint)
    .UnityCatalogUrl(unityCatalogUrl)
    .Build();

// Configure stream options for protobuf.
var options = StreamConfigurationOptions.Default with
{
    MaxInflightRequests = 50_000,
};

// Create protobuf stream.
using var stream = sdk.CreateProtoStream(
    tableName,
    descriptorProto,
    clientId,
    clientSecret,
    options);

Console.WriteLine("Ingesting protobuf records...");

for (int i = 0; i < 5; i++)
{
    var message = new MyMessage
    {
        DeviceName = $"sensor-{i}",
        Temp = 20 + i,
        Humidity = 60 + i,
    };
    byte[] protoBytes = message.ToByteArray();

    long offset = stream.IngestRecord(protoBytes);
    Console.WriteLine($"Ingested protobuf record {i} at offset {offset}");
}

// Flush all pending records.
stream.Flush();
Console.WriteLine("All protobuf records flushed and acknowledged!");
