# JSON Examples

This directory contains examples for ingesting data using `ZerobusJsonStream`.

## Overview

`ZerobusJsonStream` provides a clean API for JSON ingestion:
- **No Protocol Buffer dependency** - Pure JSON, no schema compilation
- **Method-level generics** - Flexible typing per method call
- **Two input formats** - Objects with serializer or raw JSON strings
- **Batch support** - High-throughput batch ingestion

## Building and Running

```bash
cd examples

# Compile
javac -d . -cp "../target/classes:$(cd .. && mvn dependency:build-classpath -q -DincludeScope=runtime -Dmdep.outputFile=/dev/stdout)" \
  json/SingleRecordExample.java \
  json/BatchIngestionExample.java

# Set environment variables
export ZEROBUS_SERVER_ENDPOINT="https://<workspace-id>.zerobus.<region>.cloud.databricks.com"
export DATABRICKS_WORKSPACE_URL="https://<workspace>.cloud.databricks.com"
export ZEROBUS_TABLE_NAME="catalog.schema.table"
export DATABRICKS_CLIENT_ID="your-client-id"
export DATABRICKS_CLIENT_SECRET="your-client-secret"

# Run single record example
java -cp ".:../target/classes:$(cd .. && mvn dependency:build-classpath -q -DincludeScope=runtime -Dmdep.outputFile=/dev/stdout)" \
  com.databricks.zerobus.examples.json.SingleRecordExample

# Run batch example
java -cp ".:../target/classes:$(cd .. && mvn dependency:build-classpath -q -DincludeScope=runtime -Dmdep.outputFile=/dev/stdout)" \
  com.databricks.zerobus.examples.json.BatchIngestionExample
```

## API Overview

### Creating a JSON Stream

```java
try (ZerobusJsonStream stream = sdk.streamBuilder()
        .table(tableName)
        .oauth(clientId, clientSecret)
        .json()
        .build()
        .join()) {
    // ingest...
}
```

For custom authentication, provide the authorization and table name headers:

```java
HeadersProvider provider = () -> {
    Map<String, String> headers = new HashMap<>();
    headers.put("authorization", "Bearer " + System.getenv("DATABRICKS_TOKEN"));
    headers.put("x-databricks-zerobus-table-name", tableName);
    return headers;
};

try (ZerobusJsonStream stream = sdk.streamBuilder()
        .table(tableName)
        .headersProvider(provider)
        .json()
        .build()
        .join()) {
    // ingest...
}
```

### Single Record Ingestion

```java
// Method 1: Raw JSON string
long offset = stream.ingestRecordOffset("{\"device_name\": \"sensor-1\", \"temp\": 25}");

// Method 2: Object with serializer (Gson, Jackson, or custom)
Map<String, Object> data = new HashMap<>();
data.put("device_name", "sensor-1");
data.put("temp", 25);
offset = stream.ingestRecordOffset(data, gson::toJson);

stream.waitForOffset(offset);
```

### Batch Ingestion

```java
// Method 1: List of JSON strings
List<String> jsonBatch = Arrays.asList(
    "{\"device_name\": \"s1\", \"temp\": 20}",
    "{\"device_name\": \"s2\", \"temp\": 21}"
);
Optional<Long> offset = stream.ingestRecordsOffset(jsonBatch);

// Method 2: List of objects with serializer
List<Map<String, Object>> objectBatch = ...;
offset = stream.ingestRecordsOffset(objectBatch, gson::toJson);

if (offset.isPresent()) {
    stream.waitForOffset(offset.get());
}
```

### Getting Unacknowledged Records

```java
// As JSON strings
List<String> unackedJson = stream.getUnackedRecords();

// As deserialized objects
List<MyData> unackedObjects = stream.getUnackedRecords(json -> gson.fromJson(json, MyData.class));
```

## Examples

### SingleRecordExample

Demonstrates both single-record ingestion methods plus recreateStream:
1. **Object with serializer** - 11 records (1 + 10)
2. **String directly** - 11 records (1 + 10)
3. **RecreateStream demo** - 3 records

**Total: 25 records**

### BatchIngestionExample

Demonstrates both batch ingestion methods plus recreateStream:
1. **Batch of objects** - 11 batches × 10 records = 110 records
2. **Batch of strings** - 11 batches × 10 records = 110 records
3. **RecreateStream demo** - 5 records

**Total: 225 records**

## Using JSON Libraries

### With Gson

```java
import com.google.gson.Gson;

Gson gson = new Gson();

// Single record
stream.ingestRecordOffset(myObject, gson::toJson);

// Batch
stream.ingestRecordsOffset(objectList, gson::toJson);

// Deserialize unacked
stream.getUnackedRecords(json -> gson.fromJson(json, MyClass.class));
```

### With Jackson

```java
import com.fasterxml.jackson.databind.ObjectMapper;

ObjectMapper mapper = new ObjectMapper();

stream.ingestRecordOffset(myObject, obj -> {
    try {
        return mapper.writeValueAsString(obj);
    } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
    }
});
```

## When to Use JSON

| Use Case | Recommended |
|----------|-------------|
| Rapid prototyping | SON |
| Data already in JSON format | JSON |
| Schema changes frequently | JSON |
| Production with stable schema | Proto |
| Maximum performance needed | Proto |
| Type safety required | Proto |
