# Zerobus SDK Examples

This directory contains example applications demonstrating different usage patterns of the Zerobus Ingest SDK for Java.

## Overview

The examples are organized by stream type and demonstrate both single-record and batch ingestion patterns.

**Features demonstrated:**
- `ZerobusProtoStream` - Protocol Buffer ingestion with method-level generics
- `ZerobusJsonStream` - JSON ingestion with flexible serialization
- `ZerobusArrowStream` - Arrow Flight columnar ingestion
- `ZerobusStream` (deprecated) - Legacy Future-based API for backward compatibility

## Directory Structure

```
examples/
├── README.md              (this file)
├── proto/                 (Protocol Buffer examples - ZerobusProtoStream)
│   ├── README.md
│   ├── air_quality.proto     (compile with protoc --java_out=proto)
│   ├── SingleRecordExample.java
│   └── BatchIngestionExample.java
├── json/                  (JSON examples - ZerobusJsonStream)
│   ├── README.md
│   ├── SingleRecordExample.java
│   └── BatchIngestionExample.java
├── arrow/                 (Arrow Flight examples - ZerobusArrowStream)
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
| `arrow/ArrowIngestionExample` | `ZerobusArrowStream` | Three streams demonstrating each IPC compression codec (NONE, LZ4_FRAME, ZSTD); 10 batches per stream, then flush + close |
| `legacy/LegacyStreamExample` | `ZerobusStream` | Legacy Future-based API |

Each example demonstrates: queue then flush, batch ingestion, and recreateStream.

## Stream Classes

### ZerobusProtoStream (Recommended for Protocol Buffers)

```java
try (ZerobusProtoStream stream = sdk.streamBuilder()
        .table(tableName)
        .oauth(clientId, clientSecret)
        .compiledProto(MyProto.getDescriptor().toProto())
        .build()
        .join()) {
    stream.ingestRecordOffset(myProtoMessage);        // Message
    stream.ingestRecordOffset(preEncodedBytes);       // byte[]
    stream.ingestRecordsOffset(listOfMessages);       // batch
    stream.ingestRecordsOffset(listOfByteArrays);     // batch
    stream.flush();
}
```

### ZerobusJsonStream (Recommended for JSON)

```java
try (ZerobusJsonStream stream = sdk.streamBuilder()
        .table(tableName)
        .oauth(clientId, clientSecret)
        .json()
        .build()
        .join()) {
    stream.ingestRecordOffset(object, gson::toJson);  // Object + serializer
    stream.ingestRecordOffset(jsonString);            // String
    stream.ingestRecordsOffset(objects, gson::toJson);// batch
    stream.ingestRecordsOffset(jsonStrings);          // batch
    stream.flush();
}
```

### ZerobusArrowStream (Arrow Flight)

```java
Schema schema = new Schema(Arrays.asList(
    Field.nullable("device_name", ArrowType.LargeUtf8.INSTANCE),
    Field.nullable("temp", new ArrowType.Int(32, true))
));

try (ZerobusArrowStream stream = sdk.streamBuilder()
        .table(tableName)
        .oauth(clientId, clientSecret)
        .arrow(schema)
        .build()
        .join()) {
    stream.ingestBatch(vectorSchemaRoot);
    stream.flush();
}
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

The example `java` commands below load JNI from the packaged fat JAR, not from
`target/classes`. `-Dzerobus.skipNativeLibCheck=true` compiles Java sources only
and those commands will fail at native load. Either install the published
artifact from Maven Central, or stage JNI libraries under
`src/main/resources/native/` and package without the skip flag:

```bash
cd ..  # Go to SDK root
# Stage JNI under src/main/resources/native/, then:
mvn package -DskipTests
```

Set `SDK_JAR` to that packaged artifact before compiling or running:

```bash
SDK_JAR=$(ls ../target/zerobus-ingest-sdk-*-jar-with-dependencies.jar | head -n 1)
```

## Running Examples

### Protocol Buffer Examples

```bash
cd examples
SDK_JAR=$(ls ../target/zerobus-ingest-sdk-*-jar-with-dependencies.jar | head -n 1)

# Generate AirQualityProto.java from the proto schema (not checked in)
protoc --java_out=proto proto/air_quality.proto

# Compile examples
javac -d . -cp "$SDK_JAR" \
  proto/com/databricks/zerobus/examples/proto/AirQualityProto.java \
  proto/SingleRecordExample.java \
  proto/BatchIngestionExample.java

# Run single record example
java -cp ".:$SDK_JAR" \
  com.databricks.zerobus.examples.proto.SingleRecordExample

# Run batch example
java -cp ".:$SDK_JAR" \
  com.databricks.zerobus.examples.proto.BatchIngestionExample
```

### JSON Examples

```bash
cd examples
SDK_JAR=$(ls ../target/zerobus-ingest-sdk-*-jar-with-dependencies.jar | head -n 1)

# Compile examples
javac -d . -cp "$SDK_JAR" \
  json/SingleRecordExample.java \
  json/BatchIngestionExample.java

# Run single record example
java -cp ".:$SDK_JAR" \
  com.databricks.zerobus.examples.json.SingleRecordExample

# Run batch example
java -cp ".:$SDK_JAR" \
  com.databricks.zerobus.examples.json.BatchIngestionExample
```

### Legacy Examples

```bash
cd examples
SDK_JAR=$(ls ../target/zerobus-ingest-sdk-*-jar-with-dependencies.jar | head -n 1)

# Generate AirQualityProto.java if you have not already (not checked in)
protoc --java_out=proto proto/air_quality.proto

# Compile
javac -d . -cp "$SDK_JAR" \
  proto/com/databricks/zerobus/examples/proto/AirQualityProto.java \
  legacy/LegacyStreamExample.java

# Run legacy example
java -cp ".:$SDK_JAR" \
  com.databricks.zerobus.examples.legacy.LegacyStreamExample
```

## Choosing the Right Stream

| Use Case | Stream Class | Why |
|----------|--------------|-----|
| Protocol Buffers (new code) | `ZerobusProtoStream` | Method-level generics, batch support |
| JSON (new code) | `ZerobusJsonStream` | Clean API, no proto dependency |
| Large columnar datasets | `ZerobusArrowStream` | Arrow Flight, high throughput |
| Existing code with `ZerobusStream` | `ZerobusStream` | Backward compatible, migrate later |

## API Comparison

| Feature | ZerobusProtoStream | ZerobusJsonStream | ZerobusArrowStream | ZerobusStream |
|---------|-------------------|-------------------|--------------------|---------------|
| Input | `Message` / `byte[]` | `Object` / `String` | `VectorSchemaRoot` | `Message` |
| Return Type | `long` offset | `long` offset | `Optional<Long>` | `CompletableFuture` |
| Batch Support | Yes | Yes | Yes (columnar) | No |
| Extra Deps | protobuf-java | None | arrow-vector, arrow-memory-netty | protobuf-java |
| Status | **Recommended** | **Recommended** | **Recommended** | Deprecated |

## Additional Resources

- [SDK Documentation](../README.md)
- [Changelog](../CHANGELOG.md)
- [Protocol Buffers Guide](https://developers.google.com/protocol-buffers)
