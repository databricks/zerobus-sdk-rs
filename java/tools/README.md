# Generate Proto Tool

A standalone tool for generating Protocol Buffer (proto2) definition files from Unity Catalog table schemas.

## Overview

The `GenerateProto` tool fetches table schema information from Unity Catalog and automatically generates a corresponding `.proto` file with proper type mappings. This is useful when you need to create Protocol Buffer message definitions that match your Delta table schemas for use with the Zerobus SDK.

The tool is **packaged within the Zerobus SDK JAR**, so users can run it directly after downloading the SDK without needing to clone the repository.

## Features

- Fetches table schema directly from Unity Catalog
- Supports all standard Delta data types
- Generates proto2 format files
- Handles complex types (arrays and maps)
- Uses OAuth 2.0 client credentials authentication
- No external dependencies beyond Java standard library
- Packaged in SDK JAR for easy distribution

## Requirements

- Java 8 or higher
- Zerobus SDK JAR (built with `mvn package -Dzerobus.skipNativeLibCheck=true`)
- OAuth client ID and client secret with access to Unity Catalog
- Access to a Unity Catalog endpoint

## Usage

### Method 1: Using the Helper Script (Recommended for Development)

If you have the SDK source repository:

```bash
# First, build the SDK JAR
mvn package -Dzerobus.skipNativeLibCheck=true

# Then run the tool
./tools/generate_proto.sh \
  --uc-endpoint "https://your-workspace.cloud.databricks.com" \
  --client-id "your-client-id" \
  --client-secret "your-client-secret" \
  --table "catalog.schema.table_name" \
  --output "output.proto" \
  --proto-msg "TableMessage"
```

### Method 2: Running Directly from the SDK JAR (Recommended for Users)

If you have downloaded the SDK JAR without the source code:

```bash
# Using the shaded JAR (includes all dependencies)
java -cp databricks-zerobus-ingest-sdk-0.1.0-jar-with-dependencies.jar \
  com.databricks.zerobus.tools.GenerateProto \
  --uc-endpoint "https://your-workspace.cloud.databricks.com" \
  --client-id "your-client-id" \
  --client-secret "your-client-secret" \
  --table "catalog.schema.table_name" \
  --output "output.proto" \
  --proto-msg "TableMessage"
```

Or, if the JAR has a Main-Class manifest entry (which it does):

```bash
# Even simpler - just use -jar flag
java -jar databricks-zerobus-ingest-sdk-0.1.0-jar-with-dependencies.jar \
  --uc-endpoint "https://your-workspace.cloud.databricks.com" \
  --client-id "your-client-id" \
  --client-secret "your-client-secret" \
  --table "catalog.schema.table_name" \
  --output "output.proto" \
  --proto-msg "TableMessage"
```

## Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `--uc-endpoint` | Yes | Unity Catalog endpoint URL (e.g., `https://your-workspace.cloud.databricks.com`) |
| `--client-id` | Yes | OAuth client ID for authentication |
| `--client-secret` | Yes | OAuth client secret for authentication |
| `--table` | Yes | Full table name in format `catalog.schema.table_name` |
| `--output` | Yes | Output path for the generated proto file (e.g., `output.proto`) |
| `--proto-msg` | No | Name of the protobuf message (defaults to the table name) |

## Type Mappings

The tool automatically maps Delta/Unity Catalog types to Protocol Buffer types:

| Delta Type | Proto2 Type |
|------------|-------------|
| `INT`, `SHORT`, `SMALLINT` | `int32` |
| `LONG`, `BIGINT` | `int64` |
| `STRING`, `VARCHAR(n)` | `string` |
| `FLOAT` | `float` |
| `DOUBLE` | `double` |
| `BOOLEAN` | `bool` |
| `BINARY` | `bytes` |
| `DATE` | `int32` |
| `TIMESTAMP` | `int64` |
| `ARRAY<type>` | `repeated type` |
| `MAP<key_type, value_type>` | `map<key_type, value_type>` |

## Examples

### Basic Usage

Generate a proto file for a simple table:

**From the SDK JAR:**
```bash
java -jar databricks-zerobus-ingest-sdk-0.1.0-jar-with-dependencies.jar \
  --uc-endpoint "https://myworkspace.cloud.databricks.com" \
  --client-id "abc123" \
  --client-secret "secret123" \
  --table "my_catalog.my_schema.users" \
  --output "users.proto"
```

**Or, if you have the source repository:**
```bash
./tools/generate_proto.sh \
  --uc-endpoint "https://myworkspace.cloud.databricks.com" \
  --client-id "abc123" \
  --client-secret "secret123" \
  --table "my_catalog.my_schema.users" \
  --output "users.proto"
```

This might generate:

```protobuf
syntax = "proto2";

message users {
    required int32 user_id = 1;
    required string username = 2;
    optional string email = 3;
    required int64 created_at = 4;
}
```

### Custom Message Name

Specify a custom message name:

```bash
java -jar databricks-zerobus-ingest-sdk-0.1.0-jar-with-dependencies.jar \
  --uc-endpoint "https://myworkspace.cloud.databricks.com" \
  --client-id "abc123" \
  --client-secret "secret123" \
  --table "my_catalog.my_schema.events" \
  --output "events.proto" \
  --proto-msg "EventRecord"
```

### Complex Types

The tool handles complex types like arrays and maps:

```bash
java -jar databricks-zerobus-ingest-sdk-0.1.0-jar-with-dependencies.jar \
  --uc-endpoint "https://myworkspace.cloud.databricks.com" \
  --client-id "abc123" \
  --client-secret "secret123" \
  --table "my_catalog.my_schema.products" \
  --output "products.proto"
```

If the table has columns like:
- `tags ARRAY<STRING>`
- `attributes MAP<STRING, STRING>`

The generated proto will include:
```protobuf
syntax = "proto2";

message products {
    required int32 product_id = 1;
    required string name = 2;
    repeated string tags = 3;
    map<string, string> attributes = 4;
}
```

## Authentication

The tool uses OAuth 2.0 client credentials flow to authenticate with Unity Catalog. Unlike the SDK's token generation (which includes resource and authorization details for specific table privileges), this tool uses basic authentication with minimal scope to fetch table metadata.

The authentication flow:
1. Exchanges client ID and secret for an OAuth token
2. Uses the token to fetch table schema from Unity Catalog API
3. Token is used only for metadata retrieval (read-only operation)

## Integration with Zerobus SDK

After generating the `.proto` file:

1. Place it in your project's proto directory (e.g., `src/main/proto/`)
2. Compile it using the protobuf compiler:
   ```bash
   protoc --java_out=src/main/java your_proto_file.proto
   ```
3. Use the generated Java classes with the Zerobus SDK:
   ```java
   TableProperties<YourMessage> tableProperties =
       new TableProperties<>("catalog.schema.table", YourMessage.getDefaultInstance());

   ZerobusStream<YourMessage> stream = sdk.createStream(
       tableProperties, clientId, clientSecret).join();
   ```

## Troubleshooting

### Authentication Errors

If you receive authentication errors:
- Verify your client ID and secret are correct
- Ensure your OAuth client has access to Unity Catalog
- Check that the endpoint URL is correct

### Table Not Found

If the table cannot be found:
- Verify the table name format is `catalog.schema.table`
- Ensure the table exists in Unity Catalog
- Check that your OAuth client has permission to read the table metadata

### Unsupported Type Errors

If you encounter unsupported type errors:
- Check if your table uses custom or complex types not listed in the type mappings
- Consider simplifying the column type or manually editing the generated proto file

## Distribution

The tool is distributed as part of the Zerobus SDK JAR. When you download or build the SDK, the `GenerateProto` tool is automatically included in the shaded JAR file (`databricks-zerobus-ingest-sdk-*-jar-with-dependencies.jar`).

Users can run the tool directly from the JAR without needing access to the source code:

```bash
# Download the SDK JAR (or build it with mvn package -Dzerobus.skipNativeLibCheck=true)
# Then simply run:
java -jar databricks-zerobus-ingest-sdk-0.1.0-jar-with-dependencies.jar \
  --uc-endpoint "..." \
  --client-id "..." \
  --client-secret "..." \
  --table "..." \
  --output "output.proto"
```

## Files

- `src/main/java/com/databricks/zerobus/tools/GenerateProto.java` - Main tool implementation (packaged in SDK JAR)
- `tools/generate_proto.sh` - Helper script for running from source repository
- `tools/README.md` - This documentation file

## License

This tool is part of the Databricks Zerobus SDK for Java.
