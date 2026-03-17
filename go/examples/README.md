# Zerobus Go SDK Examples

This directory contains examples demonstrating how to use the Zerobus Go SDK to ingest data into Databricks Delta tables.

## Available Examples

Examples are organized by data format and ingestion pattern:

| Example | Format | Method | Location |
|---------|--------|--------|----------|
| JSON Single | JSON | Single-record | `examples/json/single/main.go` |
| JSON Batch | JSON | Batch | `examples/json/batch/main.go` |
| Proto Single | Protocol Buffers | Single-record | `examples/proto/single/main.go` |
| Proto Batch | Protocol Buffers | Batch | `examples/proto/batch/main.go` |
| Arrow Flight | Arrow IPC | Batch (RecordBatch) | `examples/arrow/main.go` |

## Prerequisites

### 1. Create a Delta Table

```sql
CREATE TABLE catalog.schema.air_quality (
  device_name STRING,
  temp INT,
  humidity BIGINT
);
```

### 2. Set Up Service Principal

Create a service principal with `SELECT` and `MODIFY` permissions on the table. See the [monorepo prerequisites](https://github.com/databricks/zerobus-sdk/blob/main/README.md#prerequisites) for detailed setup instructions.

### 3. Set Environment Variables

```bash
export ZEROBUS_SERVER_ENDPOINT="https://workspace-id.zerobus.region.cloud.databricks.com"
export DATABRICKS_WORKSPACE_URL="https://your-workspace.cloud.databricks.com"
export DATABRICKS_CLIENT_ID="your-client-id"
export DATABRICKS_CLIENT_SECRET="your-client-secret"
export ZEROBUS_TABLE_NAME="catalog.schema.air_quality"
```

## Running JSON Examples

JSON examples require no schema generation.

```bash
# Single record
cd examples/json/single
go run main.go

# Batch
cd examples/json/batch
go run main.go
```

## Running Protocol Buffers Examples

### Step 1: Install Protocol Buffer Compiler

```bash
# Ubuntu/Debian
sudo apt-get install protobuf-compiler

# macOS
brew install protobuf

# Install Go plugin
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
```

### Step 2: Generate Go Code (if needed)

The proto code is already generated, but if you need to regenerate:

```bash
cd examples/proto
./generate_proto.sh
```

### Step 3: Run Examples

```bash
# Single record
cd examples/proto/single
go run main.go

# Batch
cd examples/proto/batch
go run main.go
```

## Choosing a Format

| Feature | JSON | Protocol Buffers |
|---------|------|------------------|
| Setup | No schema generation needed | Requires `protoc` and code generation |
| Type Safety | Runtime validation only | Compile-time type checking |
| Performance | Text-based encoding | Efficient binary encoding |
| Best For | Prototyping, simple use cases | Production, high-throughput |

## Code Pattern Comparison

### Single Record Ingestion

**JSON:**
```go
jsonRecord := `{"device_name": "sensor-001", "temp": 20, "humidity": 60}`
offset, err := stream.IngestRecordOffset(jsonRecord)
```

**Protocol Buffers:**
```go
message := &pb.AirQuality{
    DeviceName: proto.String("sensor-001"),
    Temp:       proto.Int32(20),
    Humidity:   proto.Int64(60),
}
data, _ := proto.Marshal(message)
offset, err := stream.IngestRecordOffset(data)
```

### Batch Ingestion

**JSON:**
```go
records := []interface{}{
    `{"device_name": "sensor-001", "temp": 20, "humidity": 60}`,
    `{"device_name": "sensor-002", "temp": 21, "humidity": 61}`,
}
batchOffset, err := stream.IngestRecordsOffset(records)
```

**Protocol Buffers:**
```go
var records []interface{}
for i := 0; i < 5; i++ {
    message := &pb.AirQuality{...}
    data, _ := proto.Marshal(message)
    records = append(records, data)
}
batchOffset, err := stream.IngestRecordsOffset(records)
```

## Running Arrow Flight Examples (Experimental)

> **Experimental/Unsupported**: Arrow Flight ingestion is experimental and not yet supported for production use. The API may change in future releases.

The Arrow Flight example demonstrates ingestion of Apache Arrow RecordBatches. The schema defined in the example must match the target Delta table's column names and types.

### Prerequisites

In addition to the general prerequisites above, install the Arrow Go library:

```bash
go get github.com/apache/arrow-go/v18/arrow
go get github.com/apache/arrow-go/v18/arrow/array
go get github.com/apache/arrow-go/v18/arrow/ipc
go get github.com/apache/arrow-go/v18/arrow/memory
```

```bash
cd examples/arrow
go run main.go
```

## Additional Resources

- [SDK Documentation](../README.md)
- [API Reference](../README.md#api-reference)
- [Protocol Buffers Documentation](https://protobuf.dev/)
