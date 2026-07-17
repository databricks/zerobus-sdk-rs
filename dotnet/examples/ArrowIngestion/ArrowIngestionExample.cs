using Databricks.Zerobus;

// ===================================================================
// Arrow Flight Ingestion Example (Beta)
// ===================================================================
// This example demonstrates ingesting Apache Arrow RecordBatches
// using the Arrow Flight protocol via the Zerobus SDK.
//
// Since the SDK accepts raw Arrow IPC bytes, you can use any
// Arrow library (Apache.Arrow, ADBC, etc.) to produce them.
// ===================================================================

const string serverEndpoint = "https://YOUR_WORKSPACE.databricks.com";
const string unityCatalogUrl = "https://YOUR_WORKSPACE.databricks.com/api/2.1/unity-catalog";

Console.WriteLine("Zerobus Arrow Flight Ingestion Example (Beta)");
Console.WriteLine("=============================================");

// Step 1: Serialize your Arrow schema to IPC format bytes
// Using Apache.Arrow:
//
// var schema = new Schema.Builder()
//     .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
//     .Field(f => f.Name("name").DataType(StringType.Default).Nullable(true))
//     .Build();
//
// byte[] schemaIpcBytes;
// using (var ms = new MemoryStream())
// {
//     var writer = new Apache.Arrow.Ipc.ArrowStreamWriter(ms, schema);
//     writer.WriteStart();
//     writer.WriteEnd();
//     schemaIpcBytes = ms.ToArray();
// }

byte[] schemaIpcBytes = Array.Empty<byte>(); // Replace with real schema IPC bytes

using var sdk = new ZerobusSdk(serverEndpoint, unityCatalogUrl);

// await using var stream = await sdk.StreamBuilder()
//     .Table(tableName)
//     .OAuth(clientId, clientSecret)
//     .Arrow(schemaIpcBytes)
//     .MaxInflightBatches(10_000)
//     .IpcCompression(IPCCompressionType.Zstd)
//     .BuildAsync();
//
// // Serialize Arrow RecordBatch to IPC bytes
// byte[] batchIpcBytes = SerializeRecordBatch(recordBatch);
//
// long offset = stream.IngestBatch(batchIpcBytes);
// stream.WaitForOffset(offset);

Console.WriteLine("See code comments for Arrow ingestion setup.");
Console.WriteLine("Done.");
