# Arrow Flight Examples (Beta)

This directory contains examples for ingesting data using `ZerobusArrowStream` via the Arrow Flight protocol.

> **Beta**: Arrow Flight ingestion is in Beta. The API is stabilising but may still change before reaching GA.

## Overview

`ZerobusArrowStream` provides high-performance columnar data ingestion:
- **Apache Arrow native types** - Accepts `VectorSchemaRoot` directly
- **Zero-overhead queuing** - `ingestBatch()` returns immediately with an offset
- **Automatic IPC serialization** - VectorSchemaRoot is serialized to Arrow IPC internally
- **Optional IPC compression** - LZ4 or ZSTD compression on the wire
- **Graceful close** - On server-signaled close the stream drains in-flight acks within a bounded wait, then recovers
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

## Building and Running

```bash
cd examples

# Compile (requires Arrow JARs on classpath)
ARROW_CP=$(echo ../target/arrow-deps/*.jar | tr ' ' ':')
javac -d . -cp "../target/classes:$ARROW_CP" \
  arrow/SingleBatchExample.java \
  arrow/BatchIngestionExample.java

# Set environment variables
export ZEROBUS_SERVER_ENDPOINT="https://<workspace-id>.zerobus.<region>.cloud.databricks.com"
export DATABRICKS_WORKSPACE_URL="https://<workspace>.cloud.databricks.com"
export ZEROBUS_TABLE_NAME="catalog.schema.table"
export DATABRICKS_CLIENT_ID="your-client-id"
export DATABRICKS_CLIENT_SECRET="your-client-secret"

# Run single batch example
java -cp ".:../target/zerobus-ingest-sdk-*-jar-with-dependencies.jar:$ARROW_CP" \
  com.databricks.zerobus.examples.arrow.SingleBatchExample

# Run batch ingestion example
java -cp ".:../target/zerobus-ingest-sdk-*-jar-with-dependencies.jar:$ARROW_CP" \
  com.databricks.zerobus.examples.arrow.BatchIngestionExample
```

## Examples

### SingleBatchExample

Minimal end-to-end Arrow Flight usage: build a schema, create the stream, populate a single `VectorSchemaRoot`, ingest it, wait for the ack, and close.

### BatchIngestionExample

Realistic loop-driven usage covering everything you need for production:

- Ingesting many batches with periodic `waitForOffset` checkpoints
- Custom `ArrowStreamConfigurationOptions` (max inflight, IPC compression, recovery, graceful close wait)
- Recovery via `getUnackedBatches()` + `recreateArrowStream()` after a stream close

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
