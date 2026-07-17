using Databricks.Zerobus;

// ===================================================================
// Single Record JSON Ingestion Example
// ===================================================================
// This example demonstrates ingesting JSON records one at a time
// into a Databricks Delta table using the Zerobus SDK.
//
// Prerequisites:
// - A Databricks workspace with Unity Catalog
// - A Delta table created with USING DELTA
// - A service principal with OAuth credentials and table permissions
// - The Zerobus native library (zerobus_ffi.dll/.so/.dylib)
//   available in your runtimes folder or system path
// ===================================================================

const string serverEndpoint = "https://YOUR_WORKSPACE.databricks.com";
const string unityCatalogUrl = "https://YOUR_WORKSPACE.databricks.com/api/2.1/unity-catalog";
const string tableName = "my_catalog.my_schema.my_table";
const string clientId = "YOUR_SERVICE_PRINCIPAL_CLIENT_ID";
const string clientSecret = "YOUR_SERVICE_PRINCIPAL_CLIENT_SECRET";

Console.WriteLine("Zerobus JSON Single Record Ingestion Example");
Console.WriteLine("=============================================");

using var sdk = new ZerobusSdk(serverEndpoint, unityCatalogUrl);

using var stream = await sdk.StreamBuilder()
    .Table(tableName)
    .OAuth(clientId, clientSecret)
    .MaxInflightRecords(100_000)
    .Json()
    .BuildAsync();

Console.WriteLine($"Stream created for table: {stream.TableName}");

// Ingest single records
var offset1 = stream.IngestRecord("{\"id\": 1, \"name\": \"Product A\", \"price\": 29.99}");
Console.WriteLine($"Ingested record at offset: {offset1}");

var offset2 = stream.IngestRecord("{\"id\": 2, \"name\": \"Product B\", \"price\": 49.99}");
Console.WriteLine($"Ingested record at offset: {offset2}");

// Wait for the last offset to ensure all records are durably stored
stream.WaitForOffset(offset2);
Console.WriteLine("All records durably acknowledged.");

// Or, flush all pending records
// stream.Flush();

Console.WriteLine("Done.");
