using Databricks.Zerobus;
using Google.Protobuf;

// ===================================================================
// Single Record Protocol Buffers Ingestion Example
// ===================================================================
// This example demonstrates ingesting protobuf records using the
// Zerobus SDK. You need:
// 1. A compiled .proto schema generated from your Unity Catalog table
//    (use the generate-proto tool or ProtoSchema.FromUnityCatalogJson)
// 2. The compiled descriptor proto bytes
// 3. Your protobuf message type (here using a manual example)
// ===================================================================

const string serverEndpoint = "https://YOUR_WORKSPACE.databricks.com";
const string unityCatalogUrl = "https://YOUR_WORKSPACE.databricks.com/api/2.1/unity-catalog";

Console.WriteLine("Zerobus Protocol Buffers Single Record Ingestion Example");
Console.WriteLine("=======================================================");

// Step 1: Generate proto schema from Unity Catalog
// This would normally be done via the Unity Catalog API response JSON
// string ucTableJson = await FetchTableFromUnityCatalog(tableName);
// var protoSchema = ProtoSchema.FromUnityCatalogJson(ucTableJson);
// byte[] descriptorBytes = protoSchema.GetDescriptorBytes();
// protoSchema.Dispose();

// For this example, use pre-existing descriptor bytes
byte[] descriptorBytes = Array.Empty<byte>(); // Replace with real descriptor bytes

using var sdk = new ZerobusSdk(serverEndpoint, unityCatalogUrl);

// Step 2: Create a protobuf stream. The generic type T must be your protobuf message type.
// Replace 'MyProtoMessage' with your actual generated message class.
//
// await using var stream = await sdk.StreamBuilder()
//     .Table(tableName)
//     .OAuth(clientId, clientSecret)
//     .CompiledProto(descriptorBytes)
//     .BuildAsync<MyProtoMessage>();
//
// var record = new MyProtoMessage { Id = 1, Name = "Test", Price = 29.99 };
// var offset = stream.IngestRecord(record);

Console.WriteLine("See code comments for protobuf ingestion setup.");
Console.WriteLine("Done.");
