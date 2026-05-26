# Arrow Flight Examples (Beta)

This directory contains examples for ingesting data using `ZerobusArrowStream` via the Arrow Flight protocol.

> **Beta**: Arrow Flight ingestion is in Beta. The API is stabilising but may still change before reaching GA.

## Overview

`ZerobusArrowStream` provides high-performance columnar data ingestion:
- **Apache Arrow native types** - Accepts `VectorSchemaRoot` directly
- **Zero-overhead queuing** - `ingestBatch()` returns immediately with an offset
- **Automatic IPC serialization** - VectorSchemaRoot is serialized to Arrow IPC internally
- **Optional IPC compression** - LZ4 or ZSTD compression on the wire
- **Recovery support** - Unacknowledged batches can be retrieved and re-ingested

## Dependencies

Arrow Flight requires additional dependencies (not bundled with the SDK):

```xml
<dependency>
    <groupId>org.apache.arrow</groupId>
    <artifactId>arrow-vector</artifactId>
    <version>17.0.0</version>
</dependency>
<dependency>
    <groupId>org.apache.arrow</groupId>
    <artifactId>arrow-memory-netty</artifactId>
    <version>17.0.0</version>
</dependency>
```

## JVM module flags (JDK 9+)

Apache Arrow Java's `arrow-memory-netty` allocator needs reflective access to `java.nio.Buffer` and fails on startup without it:

```
java.lang.RuntimeException: Failed to initialize MemoryUtil. You must start Java with
  `--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED`
```

Pass both opens to your application JVM whenever you use `ZerobusArrowStream`:

```
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.nio=org.apache.arrow.memory.core
```

(Two opens because Arrow resolves the importing module as `org.apache.arrow.memory.core` under JPMS and `ALL-UNNAMED` on the classic classpath — covering both works in either setup.)

## Building and Running

```bash
cd examples

# Compile (requires Arrow JARs on classpath)
ARROW_CP=$(echo ../target/arrow-deps/*.jar | tr ' ' ':')
javac -d . -cp "../target/classes:$ARROW_CP" \
  arrow/ArrowIngestionExample.java

# Set environment variables
export ZEROBUS_SERVER_ENDPOINT="https://<workspace-id>.zerobus.<region>.cloud.databricks.com"
export DATABRICKS_WORKSPACE_URL="https://<workspace>.cloud.databricks.com"
export ZEROBUS_TABLE_NAME="catalog.schema.table"
export DATABRICKS_CLIENT_ID="your-client-id"
export DATABRICKS_CLIENT_SECRET="your-client-secret"

# Run
java --add-opens=java.base/java.nio=ALL-UNNAMED \
     --add-opens=java.base/java.nio=org.apache.arrow.memory.core \
     -cp ".:../target/zerobus-ingest-sdk-*-jar-with-dependencies.jar:$ARROW_CP" \
     com.databricks.zerobus.examples.arrow.ArrowIngestionExample
```

## Examples

### ArrowIngestionExample

Opens three Arrow Flight streams against the same table, one per IPC compression codec (`NONE`, `LZ4_FRAME`, `ZSTD`). For each stream, ingests 10 batches × 10 rows, waits for the last batch's offset to be acknowledged, flushes pending batches, then closes the stream.

## API Overview

### Creating an Arrow Stream

```java
Schema schema = new Schema(Arrays.asList(
    Field.nullable("device_name", ArrowType.LargeUtf8.INSTANCE),
    Field.nullable("temp", new ArrowType.Int(32, true)),
    Field.nullable("humidity", new ArrowType.Int(64, true))
));

ZerobusArrowStream stream = sdk.createArrowStream(
    tableName, schema, clientId, clientSecret
).join();
```

### Ingesting Batches

```java
try (VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
    // Populate the batch...
    batch.setRowCount(rowCount);

    Optional<Long> offset = stream.ingestBatch(batch);
    offset.ifPresent(stream::waitForOffset);
}
```

### Custom Options

```java
ArrowStreamConfigurationOptions options = ArrowStreamConfigurationOptions.builder()
    .setMaxInflightBatches(2000)
    .setFlushTimeoutMs(600000)
    .setRecovery(true)
    .setRecoveryRetries(5)
    .setIpcCompression(IPCCompressionType.ZSTD)
    .setStreamPausedMaxWaitTimeMs(5000)
    .build();

ZerobusArrowStream stream = sdk.createArrowStream(
    tableName, schema, clientId, clientSecret, options
).join();
```

### Recovering Unacknowledged Batches

```java
stream.close();

List<byte[]> unacked = stream.getUnackedBatches();
if (!unacked.isEmpty()) {
    ZerobusArrowStream recovered = sdk.recreateArrowStream(stream).join();
    // recreateArrowStream re-ingests the unacked batches and flushes
    recovered.close();
}
```

## When to Use Arrow

| Use Case | Recommended |
|----------|-------------|
| Large columnar datasets | Arrow |
| Data already in Arrow format (Spark, Pandas) | Arrow |
| Maximum throughput needed | Arrow |
| Simple key-value records | JSON |
| Strongly-typed schemas with protobuf | Proto |
