# Legacy Examples

This directory contains examples using the deprecated `ZerobusStream<T>` class.

## Overview

`ZerobusStream<T>` is the original stream class with:
- **Class-level generics** - Fixed type at stream creation
- **Future-based API** - Returns `CompletableFuture<Void>`
- **No batch support** - Single record ingestion only

**Status: Deprecated** - Use `ZerobusProtoStream` or `ZerobusJsonStream` instead.

## Building and Running

```bash
cd examples

# Generate AirQualityProto.java from the proto schema (not checked in)
protoc --java_out=proto proto/air_quality.proto

# Compile
javac -d . -cp "../target/classes:$(cd .. && mvn dependency:build-classpath -q -DincludeScope=runtime -Dmdep.outputFile=/dev/stdout)" \
  proto/com/databricks/zerobus/examples/proto/AirQualityProto.java \
  legacy/LegacyStreamExample.java

# Set environment variables
export ZEROBUS_SERVER_ENDPOINT="https://<workspace-id>.zerobus.<region>.cloud.databricks.com"
export DATABRICKS_WORKSPACE_URL="https://<workspace>.cloud.databricks.com"
export ZEROBUS_TABLE_NAME="catalog.schema.table"
export DATABRICKS_CLIENT_ID="your-client-id"
export DATABRICKS_CLIENT_SECRET="your-client-secret"

# Run
java -cp ".:../target/classes:$(cd .. && mvn dependency:build-classpath -q -DincludeScope=runtime -Dmdep.outputFile=/dev/stdout)" \
  com.databricks.zerobus.examples.legacy.LegacyStreamExample
```

## API Overview

### Creating a Legacy Stream

```java
TableProperties<AirQuality> tableProperties = new TableProperties<>(
    tableName,
    AirQuality.getDefaultInstance()
);

@SuppressWarnings("deprecation")
ZerobusStream<AirQuality> stream = sdk.createStream(
    tableProperties,
    clientId,
    clientSecret
).join();
```

### Ingesting Records

```java
// Future-based API - blocks until acknowledged
stream.ingestRecord(record).join();
```

## Migration Guide

### Before (ZerobusStream)

```java
ZerobusStream<AirQuality> stream = sdk.createStream(
    tableProperties, clientId, clientSecret
).join();

// Blocks on each record
stream.ingestRecord(record).join();
```

### After (ZerobusProtoStream)

```java
try (ZerobusProtoStream stream = sdk.streamBuilder()
        .table(tableName)
        .oauth(clientId, clientSecret)
        .compiledProto(AirQuality.getDescriptor().toProto())
        .build()
        .join()) {
    for (AirQuality record : records) {
        stream.ingestRecordOffset(record);
    }
    stream.flush();

    Optional<Long> batchOffset = stream.ingestRecordsOffset(records);
    if (batchOffset.isPresent()) {
        stream.waitForOffset(batchOffset.get());
    }
}
```

## Why Migrate?

| Feature | ZerobusStream | ZerobusProtoStream |
|---------|---------------|-------------------|
| Generics | Class-level (fixed) | Method-level (flexible) |
| Return Type | `CompletableFuture` | `long` offset |
| Batch Support | No | Yes |
| Overhead | Higher (Future allocation) | Lower (primitive return) |
| Status | Deprecated | **Recommended** |

## LegacyStreamExample

Demonstrates the Future-based API plus recreateStream:
- Creates stream with `TableProperties`
- Ingests 11 records using `ingestRecord().join()` (1 + 10)
- Demonstrates `getUnackedRecords()` (returns empty due to type erasure)
- Demonstrates `recreateStream()` with 3 additional records

**Total: 14 records**
