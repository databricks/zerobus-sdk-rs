# Arrow Flight Examples (Experimental)

This directory contains examples for ingesting data using `ZerobusArrowStream` via the Arrow Flight protocol.

**Note**: Arrow Flight is not yet supported by default from the Zerobus server side.

## Overview

`ZerobusArrowStream` provides high-performance columnar data ingestion:
- **Apache Arrow native types** - Accepts `VectorSchemaRoot` directly
- **Zero-overhead queuing** - `ingestBatch()` returns immediately with an offset
- **Automatic IPC serialization** - VectorSchemaRoot is serialized to Arrow IPC internally
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
  arrow/ArrowIngestionExample.java

# Set environment variables
export ZEROBUS_SERVER_ENDPOINT="https://<workspace-id>.zerobus.<region>.cloud.databricks.com"
export DATABRICKS_WORKSPACE_URL="https://<workspace>.cloud.databricks.com"
export ZEROBUS_TABLE_NAME="catalog.schema.table"
export DATABRICKS_CLIENT_ID="your-client-id"
export DATABRICKS_CLIENT_SECRET="your-client-secret"

# Run
java -cp ".:../target/zerobus-ingest-sdk-*-jar-with-dependencies.jar:$ARROW_CP" \
  com.databricks.zerobus.examples.arrow.ArrowIngestionExample
```

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
    offset.ifPresent(o -> stream.waitForOffset(o));
}
```

### Custom Options

```java
ArrowStreamConfigurationOptions options = ArrowStreamConfigurationOptions.builder()
    .setMaxInflightBatches(2000)
    .setFlushTimeoutMs(600000)
    .setRecovery(true)
    .setRecoveryRetries(5)
    .build();

ZerobusArrowStream stream = sdk.createArrowStream(
    tableName, schema, clientId, clientSecret, options
).join();
```

### Getting Unacknowledged Batches

```java
// After close, retrieve unacked batches as IPC byte arrays
List<byte[]> unacked = stream.getUnackedBatches();
```

### Recreating a Stream

```java
stream.close();
ZerobusArrowStream newStream = sdk.recreateArrowStream(stream).join();
```

## Examples

### ArrowIngestionExample

Demonstrates the full Arrow Flight lifecycle:
1. **Single batch** - 5 rows ingested and acknowledged
2. **Multiple batches** - 3 batches × 10 rows = 30 rows flushed
3. **Custom options** - Stream with custom configuration
4. **Unacked batches** - Retrieve after close
5. **Recreate stream** - 1 row on recreated stream

## When to Use Arrow

| Use Case | Recommended |
|----------|-------------|
| Large columnar datasets | Arrow |
| Data already in Arrow format (Spark, Pandas) | Arrow |
| Maximum throughput needed | Arrow |
| Simple key-value records | JSON |
| Strongly-typed schemas with protobuf | Proto |
