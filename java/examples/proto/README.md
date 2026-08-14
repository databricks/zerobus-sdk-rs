# Protocol Buffer Examples

This directory contains examples for ingesting data using `ZerobusProtoStream`.

## Overview

`ZerobusProtoStream` provides a type-safe API for Protocol Buffer ingestion:
- **Method-level generics** - Flexible typing per method call
- **Two input formats** - Message objects or pre-encoded bytes
- **Batch support** - High-throughput batch ingestion
- **Compact encoding** - Smaller payload than JSON

## Building and Running

```bash
cd examples
SDK_JAR=$(ls ../target/zerobus-ingest-sdk-*-jar-with-dependencies.jar | head -n 1)

# Generate AirQualityProto.java from the proto schema (not checked in)
protoc --java_out=proto proto/air_quality.proto

# Compile
javac -d . -cp "$SDK_JAR" \
  proto/com/databricks/zerobus/examples/proto/AirQualityProto.java \
  proto/SingleRecordExample.java \
  proto/BatchIngestionExample.java

# Set environment variables
export ZEROBUS_SERVER_ENDPOINT="https://<workspace-id>.zerobus.<region>.cloud.databricks.com"
export DATABRICKS_WORKSPACE_URL="https://<workspace>.cloud.databricks.com"
export ZEROBUS_TABLE_NAME="catalog.schema.table"
export DATABRICKS_CLIENT_ID="your-client-id"
export DATABRICKS_CLIENT_SECRET="your-client-secret"

# Run single record example
java -cp ".:$SDK_JAR" \
  com.databricks.zerobus.examples.proto.SingleRecordExample

# Run batch example
java -cp ".:$SDK_JAR" \
  com.databricks.zerobus.examples.proto.BatchIngestionExample
```

## API Overview

### Creating a Proto Stream

```java
try (ZerobusProtoStream stream = sdk.streamBuilder()
        .table(tableName)
        .oauth(clientId, clientSecret)
        .compiledProto(AirQuality.getDescriptor().toProto())
        .build()
        .join()) {
    // ingest...
}
```

### Single Record Ingestion

```java
// Method 1: Message directly (auto-encodes)
AirQuality record = AirQuality.newBuilder()
    .setDeviceName("sensor-1")
    .setTemp(25)
    .setHumidity(65)
    .build();
stream.ingestRecordOffset(record);

// Method 2: Pre-encoded bytes
byte[] encodedBytes = record.toByteArray();
stream.ingestRecordOffset(encodedBytes);

stream.flush();
```

### Batch Ingestion

```java
// Method 1: List of messages
List<AirQuality> messages = Arrays.asList(record1, record2, record3);
stream.ingestRecordsOffset(messages);

// Method 2: List of pre-encoded bytes
List<byte[]> encodedRecords = Arrays.asList(bytes1, bytes2, bytes3);
stream.ingestRecordsOffset(encodedRecords);

stream.flush();
```

### Getting Unacknowledged Records

```java
// As raw bytes
List<byte[]> unackedBytes = stream.getUnackedRecords();

// As parsed messages
List<AirQuality> unackedMessages = stream.getUnackedRecords(AirQuality.parser());
```

## Examples

### SingleRecordExample

Demonstrates both single-record ingestion methods plus recreateStream:
1. **Message directly** - 10 records, then flush
2. **Pre-encoded bytes** - 10 records, then flush
3. **RecreateStream demo** - 3 records

**Total: 23 records**

### BatchIngestionExample

Demonstrates both batch ingestion methods plus recreateStream:
1. **Batch of messages** - 11 batches × 10 records = 110 records
2. **Batch of bytes** - 11 batches × 10 records = 110 records
3. **RecreateStream demo** - 5 records

**Total: 225 records**

## Schema

The examples use this Protocol Buffer schema:

```protobuf
syntax = "proto2";

package com.databricks.zerobus.examples.proto;

option java_package = "com.databricks.zerobus.examples.proto";
option java_outer_classname = "AirQualityProto";

message AirQuality {
    optional string device_name = 1;
    optional int32 temp = 2;
    optional int64 humidity = 3;
}
```

## Type Mappings

| Delta Type | Proto2 Type |
|-----------|-------------|
| STRING | string |
| INT | int32 |
| BIGINT | int64 |
| FLOAT | float |
| DOUBLE | double |
| BOOLEAN | bool |
| BINARY | bytes |

## Generating Proto from Table Schema

Use the SDK's built-in tool to generate proto from Unity Catalog:

```bash
java -jar zerobus-ingest-sdk-*-jar-with-dependencies.jar \
  --uc-endpoint "https://workspace.cloud.databricks.com" \
  --client-id "your-client-id" \
  --client-secret "your-client-secret" \
  --table "catalog.schema.table" \
  --output "my_record.proto"
```

## Performance Tips

1. **Use batch ingestion** for higher throughput
2. **Pre-encode bytes** when you already have serialized data
3. **Reuse builders** with `clear()` instead of creating new ones
4. **Increase `maxInflightRecords`** for better pipelining
