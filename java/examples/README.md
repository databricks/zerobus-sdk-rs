# Zerobus SDK Examples

This directory contains example applications demonstrating different usage patterns of the Zerobus Ingest SDK for Java.

## Overview

The examples are organized by stream type and demonstrate both single-record and batch ingestion patterns.

**Features demonstrated:**
- `ZerobusProtoStream` - Protocol Buffer ingestion with method-level generics
- `ZerobusJsonStream` - JSON ingestion with flexible serialization
- `ZerobusArrowStream` (Beta) - Arrow Flight columnar ingestion
- `ZerobusStream` (deprecated) - Legacy Future-based API for backward compatibility

## Directory Structure

```
examples/
├── README.md              (this file)
├── proto/                 (Protocol Buffer examples - ZerobusProtoStream)
│   ├── README.md
│   ├── AirQualityProto.java  (generated proto)
│   ├── SingleRecordExample.java
│   └── BatchIngestionExample.java
├── json/                  (JSON examples - ZerobusJsonStream)
│   ├── README.md
│   ├── SingleRecordExample.java
│   └── BatchIngestionExample.java
├── arrow/                 (Arrow Flight examples - ZerobusArrowStream, Beta)
│   ├── README.md
│   └── ArrowIngestionExample.java
└── legacy/                (Legacy examples - ZerobusStream)
    └── LegacyStreamExample.java
```

## Examples Overview

| Example | Stream Class | Description |
|---------|--------------|-------------|
| `proto/SingleRecordExample` | `ZerobusProtoStream` | Single record ingestion (Message + byte[]) |
| `proto/BatchIngestionExample` | `ZerobusProtoStream` | Batch ingestion |
| `json/SingleRecordExample` | `ZerobusJsonStream` | Single record ingestion (Object + String) |
| `json/BatchIngestionExample` | `ZerobusJsonStream` | Batch ingestion |
| `json/StreamBuilderExample` | `ZerobusJsonStream` | Stream creation via the recommended `streamBuilder()` fluent API |
| `arrow/ArrowIngestionExample` | `ZerobusArrowStream` | Three streams demonstrating each IPC compression codec (NONE, LZ4_FRAME, ZSTD); 10 batches per stream, waitForOffset + flush + close (Beta) |
| `legacy/LegacyStreamExample` | `ZerobusStream` | Legacy Future-based API |

Each example demonstrates: single ingestion + wait, batch ingestion + wait for last, and recreateStream.

## Stream Classes

### ZerobusProtoStream (Recommended for Protocol Buffers)

```java
ZerobusProtoStream stream = sdk.streamBuilder()
    .table(tableName)
    .oauth(clientId, clientSecret)
    .compiledProto(MyProto.getDescriptor().toProto())
    .build()
    .join();

// Method-level generics - flexible typing
stream.ingestRecordOffset(myProtoMessage);        // Message
stream.ingestRecordOffset(preEncodedBytes);       // byte[]
stream.ingestRecordsOffset(listOfMessages);       // batch
stream.ingestRecordsOffset(listOfByteArrays);     // batch
```

### ZerobusJsonStream (Recommended for JSON)

```java
ZerobusJsonStream stream = sdk.streamBuilder()
    .table(tableName)
    .oauth(clientId, clientSecret)
    .json()
    .build()
    .join();

// Method-level generics - flexible typing
stream.ingestRecordOffset(object, gson::toJson);  // Object + serializer
stream.ingestRecordOffset(jsonString);            // String
stream.ingestRecordsOffset(objects, gson::toJson);// batch
stream.ingestRecordsOffset(jsonStrings);          // batch
```

### ZerobusArrowStream (Beta - Arrow Flight)

```java
Schema schema = new Schema(Arrays.asList(
    Field.nullable("device_name", ArrowType.LargeUtf8.INSTANCE),
    Field.nullable("temp", new ArrowType.Int(32, true))
));

ZerobusArrowStream stream = sdk.createArrowStream(
    tableName, schema, clientId, clientSecret
).join();

// Columnar batch ingestion
Optional<Long> offset = stream.ingestBatch(vectorSchemaRoot);
offset.ifPresent(stream::waitForOffset);
```

### ZerobusStream (Deprecated)

```java
@SuppressWarnings("deprecation")
ZerobusStream<MyProto> stream = sdk.createStream(
    tableProperties, clientId, clientSecret
).join();

// Class-level generics - fixed type, Future-based
stream.ingestRecord(myProtoMessage).join();  // CompletableFuture<Void>
```

## Prerequisites

### 1. Create a Delta Table

```sql
CREATE TABLE <catalog>.default.air_quality (
    device_name STRING,
    temp INT,
    humidity BIGINT
) USING DELTA;
```

### 2. Set Up Service Principal

Create a service principal with `SELECT` and `MODIFY` permissions on the table.

### 3. Set Environment Variables

```bash
export ZEROBUS_SERVER_ENDPOINT="https://<workspace-id>.zerobus.<region>.cloud.databricks.com"
export DATABRICKS_WORKSPACE_URL="https://<workspace>.cloud.databricks.com"
export ZEROBUS_TABLE_NAME="<catalog>.<schema>.<table>"
export DATABRICKS_CLIENT_ID="your-client-id"
export DATABRICKS_CLIENT_SECRET="your-client-secret"
```

### 4. Build the SDK

```bash
cd ..  # Go to SDK root
mvn package -DskipTests
```

## Running Examples

### Protocol Buffer Examples

```bash
cd examples

# Compile examples
javac -d . -cp "../target/classes:$(cd .. && mvn dependency:build-classpath -q -DincludeScope=runtime -Dmdep.outputFile=/dev/stdout)" \
  proto/com/databricks/zerobus/examples/proto/AirQualityProto.java \
  proto/SingleRecordExample.java \
  proto/BatchIngestionExample.java

# Run single record example
java -cp ".:../target/classes:$(cd .. && mvn dependency:build-classpath -q -DincludeScope=runtime -Dmdep.outputFile=/dev/stdout)" \
  com.databricks.zerobus.examples.proto.SingleRecordExample

# Run batch example
java -cp ".:../target/classes:$(cd .. && mvn dependency:build-classpath -q -DincludeScope=runtime -Dmdep.outputFile=/dev/stdout)" \
  com.databricks.zerobus.examples.proto.BatchIngestionExample
```

### JSON Examples

```bash
cd examples

# Compile examples
javac -d . -cp "../target/classes:$(cd .. && mvn dependency:build-classpath -q -DincludeScope=runtime -Dmdep.outputFile=/dev/stdout)" \
  json/SingleRecordExample.java \
  json/BatchIngestionExample.java

# Run single record example
java -cp ".:../target/classes:$(cd .. && mvn dependency:build-classpath -q -DincludeScope=runtime -Dmdep.outputFile=/dev/stdout)" \
  com.databricks.zerobus.examples.json.SingleRecordExample

# Run batch example
java -cp ".:../target/classes:$(cd .. && mvn dependency:build-classpath -q -DincludeScope=runtime -Dmdep.outputFile=/dev/stdout)" \
  com.databricks.zerobus.examples.json.BatchIngestionExample
```

### Legacy Examples

```bash
cd examples

# Compile (requires proto for AirQuality)
javac -d . -cp "../target/classes:$(cd .. && mvn dependency:build-classpath -q -DincludeScope=runtime -Dmdep.outputFile=/dev/stdout)" \
  proto/com/databricks/zerobus/examples/proto/AirQualityProto.java \
  legacy/LegacyStreamExample.java

# Run legacy example
java -cp ".:../target/classes:$(cd .. && mvn dependency:build-classpath -q -DincludeScope=runtime -Dmdep.outputFile=/dev/stdout)" \
  com.databricks.zerobus.examples.legacy.LegacyStreamExample
```

## Choosing the Right Stream

| Use Case | Stream Class | Why |
|----------|--------------|-----|
| Protocol Buffers (new code) | `ZerobusProtoStream` | Method-level generics, batch support |
| JSON (new code) | `ZerobusJsonStream` | Clean API, no proto dependency |
| Large columnar datasets | `ZerobusArrowStream` | Arrow Flight, high throughput (Beta) |
| Existing code with `ZerobusStream` | `ZerobusStream` | Backward compatible, migrate later |

## API Comparison

| Feature | ZerobusProtoStream | ZerobusJsonStream | ZerobusArrowStream | ZerobusStream |
|---------|-------------------|-------------------|--------------------|---------------|
| Input | `Message` / `byte[]` | `Object` / `String` | `VectorSchemaRoot` | `Message` |
| Return Type | `long` offset | `long` offset | `Optional<Long>` | `CompletableFuture` |
| Batch Support | Yes | Yes | Yes (columnar) | No |
| Extra Deps | protobuf-java | None | arrow-vector, arrow-memory-netty | protobuf-java |
| Status | **Recommended** | **Recommended** | **Beta** | Deprecated |

## Additional Resources

- [SDK Documentation](../README.md)
- [Changelog](../CHANGELOG.md)
- [Protocol Buffers Guide](https://developers.google.com/protocol-buffers)
