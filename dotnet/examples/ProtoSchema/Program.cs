using System.Net.Http.Headers;
using Databricks.Zerobus;

// ─── Configuration ──────────────────────────────────────────────────────────

var zerobusEndpoint = Environment.GetEnvironmentVariable("ZEROBUS_SERVER_ENDPOINT")
    ?? throw new InvalidOperationException("ZEROBUS_SERVER_ENDPOINT not set");
var workspaceUrl = Environment.GetEnvironmentVariable("DATABRICKS_WORKSPACE_URL")
    ?? throw new InvalidOperationException("DATABRICKS_WORKSPACE_URL not set");
var clientId = Environment.GetEnvironmentVariable("DATABRICKS_CLIENT_ID")
    ?? throw new InvalidOperationException("DATABRICKS_CLIENT_ID not set");
var clientSecret = Environment.GetEnvironmentVariable("DATABRICKS_CLIENT_SECRET")
    ?? throw new InvalidOperationException("DATABRICKS_CLIENT_SECRET not set");
var tableName = Environment.GetEnvironmentVariable("ZEROBUS_TABLE_NAME")
    ?? throw new InvalidOperationException("ZEROBUS_TABLE_NAME not set");

// ─── Step 1: Get Unity Catalog table metadata ───────────────────────────────
//
// Option A — Fetch via Databricks REST API (production path):
//   Calls GET /api/2.1/unity-catalog/tables/<catalog>.<schema>.<table>
//   Authenticated with a Databricks personal access token (PAT).
//
// Option B — Load inline JSON (development / CI fallback):
//   Useful when you don't have API access from the current environment.
//   The JSON must have "columns" with name/type_name/nullable fields.

string ucTableJson;

var databricksToken = Environment.GetEnvironmentVariable("DATABRICKS_TOKEN");

if (!string.IsNullOrEmpty(databricksToken))
{
    // ─── Option A: Fetch from Databricks Unity Catalog REST API ──────────

    Console.WriteLine($"Fetching table metadata from Unity Catalog: {tableName}");

    using var http = new HttpClient();
    http.DefaultRequestHeaders.Authorization =
        new AuthenticationHeaderValue("Bearer", databricksToken);

    var ucUrl = $"{workspaceUrl.TrimEnd('/')}/api/2.1/unity-catalog/tables/{tableName}";

    var response = await http.GetAsync(ucUrl);
    response.EnsureSuccessStatusCode();

    ucTableJson = await response.Content.ReadAsStringAsync();
    Console.WriteLine($"  Received {ucTableJson.Length} bytes from Unity Catalog API");
}
else
{
    // ─── Option B: Inline JSON (no API call needed) ──────────────────────

    Console.WriteLine("No DATABRICKS_TOKEN set — using inline table metadata.");

    ucTableJson = """
    {
      "name": "orders",
      "catalog_name": "main",
      "schema_name": "default",
      "table_type": "MANAGED",
      "columns": [
        {"name": "id",         "type_name": "BIGINT",    "nullable": false},
        {"name": "customer",   "type_name": "STRING",    "nullable": true},
        {"name": "amount",     "type_name": "DECIMAL",   "nullable": true},
        {"name": "created_at", "type_name": "TIMESTAMP", "nullable": false}
      ]
    }
    """;
}

// ─── Step 2: Build schema and descriptor from UC JSON ───────────────────────

Console.WriteLine("Generating proto schema from Unity Catalog JSON...");

using var schema = ProtoSchema.FromUnityCatalogJson(ucTableJson);

byte[] descriptor = schema.GetDescriptorBytes();
Console.WriteLine($"  Descriptor: {descriptor.Length} bytes");

// ─── Step 3: Create SDK and protobuf stream ─────────────────────────────────

using var sdk = ZerobusSdk.CreateBuilder()
    .Endpoint(zerobusEndpoint)
    .UnityCatalogUrl(workspaceUrl)
    .Build();

using var stream = sdk.StreamBuilder()
    .Table(tableName)
    .OAuth(clientId, clientSecret)
    .CompiledProto(descriptor)
    .Build();

Console.WriteLine($"Stream created for table: {tableName}");

// ─── Step 4: Encode JSON → Protobuf and ingest ──────────────────────────────
//
// ProtoSchema.EncodeJson() converts plain JSON into protobuf bytes matching
// the table schema — no .proto compilation or message classes needed.
// Values follow protobuf JSON mapping:
//   - TIMESTAMP/DATE: integers (micros/days since epoch), not strings
//   - DECIMAL: string (e.g. "123.45") to preserve precision
//   - BINARY: base64-encoded string

Console.WriteLine("Encoding and ingesting records...");

string[] records =
[
    """{"id": 1, "customer": "Acme Inc.", "amount": "150.00", "created_at": 1716508800000000}""",
    """{"id": 2, "customer": "Globex Corp.", "amount": "275.50", "created_at": 1716595200000000}""",
    """{"id": 3, "customer": "Initech",      "amount": "42.99", "created_at": 1716681600000000}""",
];

for (int i = 0; i < records.Length; i++)
{
    byte[] protoBytes = schema.EncodeJson(records[i]);
    long offset = stream.IngestRecord(protoBytes);
    Console.WriteLine($"  Record {i}: offset={offset}, proto_size={protoBytes.Length} bytes");
}

// ─── Step 5: Flush ──────────────────────────────────────────────────────────

stream.Flush();
Console.WriteLine($"All {records.Length} records flushed and acknowledged!");

// ─── When NOT to use ProtoSchema ────────────────────────────────────────────
//
// If you already compile .proto files in your project, skip ProtoSchema:
//
//   byte[] descriptor = MyMessage.Descriptor.File.SerializedData.ToByteArray();
//   using var stream = sdk.CreateProtoStream(tableName, descriptor, clientId, clientSecret);
//
// ProtoSchema is best when:
//   1. Your schema lives in Unity Catalog (no local .proto files)
//   2. You want to validate/encode JSON against a UC schema at runtime
//   3. You need descriptor bytes without adding protobuf compilation to CI
