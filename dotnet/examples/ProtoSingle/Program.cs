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

// In a real application, you would load the DescriptorProto from your compiled .proto file.
// For example, using Google.Protobuf.Reflection:
//   var descriptor = MyMessage.Descriptor.File.SerializedData.ToByteArray();
byte[] descriptorProto = []; // Replace with your actual descriptor bytes.

// Create SDK instance.
using var sdk = new ZerobusSdk(zerobusEndpoint, unityCatalogUrl);

// Configure stream options for protobuf.
var options = StreamConfigurationOptions.Default with
{
    MaxInflightRequests = 50_000,
    RecordType = RecordType.Proto,
};

// Create stream with protobuf descriptor.
using var stream = sdk.CreateStream(
    new TableProperties(tableName, descriptorProto),
    clientId,
    clientSecret,
    options);

Console.WriteLine("Ingesting protobuf records...");

for (int i = 0; i < 5; i++)
{
    // In a real application, serialize your protobuf message:
    //   byte[] protoBytes = myMessage.ToByteArray();
    byte[] protoBytes = [0x08, 0x01]; // Placeholder — replace with real protobuf data.

    long offset = stream.IngestRecord(protoBytes);
    Console.WriteLine($"Ingested protobuf record {i} at offset {offset}");
}

// Flush all pending records.
stream.Flush();
Console.WriteLine("All protobuf records flushed and acknowledged!");
