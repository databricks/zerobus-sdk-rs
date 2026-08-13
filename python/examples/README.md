# Zerobus SDK Examples

This directory contains runnable synchronous and asynchronous examples for Protobuf,
JSON, and Arrow Flight ingestion with the Zerobus Ingest SDK for Python.

For complete SDK documentation including installation, API reference, and configuration details, see the [main README](../README.md).

## Running the Examples

### 1. Clone or Check Out the Repository

```bash
git clone https://github.com/databricks/zerobus-sdk.git
cd zerobus-sdk/python
```

### 2. Install Dependencies

```bash
pip install -e ".[arrow]"
```

The examples use a pre-generated protobuf file (`record_pb2.py`) based on the included `record.proto` schema.

### 3. Configure Credentials

Set the following environment variables:

```bash
export DATABRICKS_CLIENT_ID="your-service-principal-application-id"
export DATABRICKS_CLIENT_SECRET="your-service-principal-secret"
# For AWS:
export ZEROBUS_SERVER_ENDPOINT="https://workspace-id.zerobus.region.cloud.databricks.com"
export DATABRICKS_WORKSPACE_URL="https://your-workspace.cloud.databricks.com"
# For Azure:
# export ZEROBUS_SERVER_ENDPOINT="https://workspace-id.zerobus.region.azuredatabricks.net"
# export DATABRICKS_WORKSPACE_URL="https://your-workspace.azuredatabricks.net"
export ZEROBUS_TABLE_NAME="catalog.schema.table"
```

### 4. Run an Example

```bash
# Synchronous examples (blocking I/O)
python examples/sync_example_proto.py     # Protobuf
python examples/sync_example_json.py      # JSON
python examples/sync_example_arrow.py     # Arrow Flight

# Asynchronous examples (non-blocking I/O)
python examples/async_example_proto.py    # Protobuf
python examples/async_example_json.py     # JSON
python examples/async_example_arrow.py    # Arrow Flight
```

## Examples Overview

All examples demonstrate the recommended offset APIs:

1. `ingest_record_offset()` - Single record with offset tracking
2. `ingest_records_offset()` - Batch ingestion with offset tracking (preferred bulk path)

Queue records or batches, then call `flush()` once to confirm durability. The nowait APIs
are not shown because they spawn detached tasks and are not safely synchronized with
`flush()`.

### Serialization Formats

The row-oriented examples cover two serialization formats. The Arrow Flight
examples use `pyarrow.RecordBatch` data instead.

#### Protocol Buffers
**Files:** `sync_example_proto.py`, `async_example_proto.py`

More efficient over the wire. You can pass either:
- **Message object** (SDK serializes to bytes)
- **Pre-serialized bytes** (client controls serialization)

```python
# Create protobuf record
record = record_pb2.AirQuality(device_name="sensor-1", temp=25, humidity=60)
table_properties = TableProperties(TABLE_NAME, record_pb2.AirQuality.DESCRIPTOR)

# Recommended: ingest_record_offset() then flush() once
offset = stream.ingest_record_offset(record)
stream.flush()

# Preferred bulk path: ingest_records_offset() then flush() once
# batch_offset = stream.ingest_records_offset([record])
# stream.flush()

# Option 2: Pass pre-serialized bytes (client controls serialization)
# offset = stream.ingest_record_offset(record.SerializeToString())
```

#### JSON
**Files:** `sync_example_json.py`, `async_example_json.py`

Good for getting started. No protobuf schema required. You can pass either:
- **dict** (SDK serializes to JSON)
- **Pre-serialized JSON string** (client controls serialization)

```python
# Create JSON record
record_dict = {"device_name": "sensor-1", "temp": 25, "humidity": 60}
table_properties = TableProperties(TABLE_NAME)

# Recommended: ingest_record_offset() then flush() once
offset = stream.ingest_record_offset(record_dict)
stream.flush()

# Preferred bulk path: ingest_records_offset() then flush() once
# batch_offset = stream.ingest_records_offset([record_dict])
# stream.flush()

# Option 2: Pass pre-serialized JSON string (client controls serialization)
# offset = stream.ingest_record_offset(json.dumps(record_dict))
```

### Synchronous vs Asynchronous APIs

All record type modes are available in both synchronous and asynchronous variants:

#### Synchronous API (`zerobus.sdk.sync`)
Suitable for:
- Simple scripts and applications
- Code that doesn't use asyncio
- Straightforward blocking I/O patterns

**Key characteristics:**
- Uses standard Python synchronous functions
- Blocking API calls
- Works in any Python environment

#### Asynchronous API (`zerobus.sdk.aio`)
Suitable for:
- Applications already using asyncio
- Async web frameworks (FastAPI, aiohttp, etc.)
- Event-driven architectures
- Integration with other async operations

**Key characteristics:**
- Uses Python's `async`/`await` syntax
- Non-blocking API calls
- Requires an asyncio event loop

## Quick Reference

### API Comparison: Sync vs Async

Both APIs provide the same functionality and performance. The key differences are:

| Aspect | Synchronous (`sync`) | Asynchronous (`aio`) |
|--------|---------------------|----------------------|
| Import | `from zerobus.sdk.sync import ZerobusSdk` | `from zerobus.sdk.aio import ZerobusSdk` |
| Stream creation | `stream = sdk.create_stream(...)` | `stream = await sdk.create_stream(...)` |
| Record ingestion (with offset) | `offset = stream.ingest_record_offset(record)` | `offset = await stream.ingest_record_offset(record)` |
| Batch ingestion (with offset) | `offset = stream.ingest_records_offset(records)` | `offset = await stream.ingest_records_offset(records)` |
| Flush | `stream.flush()` | `await stream.flush()` |
| Close | `stream.close()` | `await stream.close()` |
| Execution context | Standard Python | Requires asyncio event loop |
| Use case | General Python applications | Asyncio-based applications |

**Performance:** Both APIs offer equivalent throughput and durability. Choose based on your application's architecture, not performance needs.

Recommended methods:

- `ingest_records_offset()` - Preferred bulk path: queue a batch, then `flush()` once
- `ingest_record_offset()` - Single records: ingest in a loop, then `flush()` once

Deprecated:

- `ingest_record()` - Use `ingest_record_offset()` instead

The nowait APIs spawn detached tasks and are not safely synchronized with `flush()`.

### Serialization Format Comparison

| Format | Record Input | Configuration |
|--------|-------------|---------------|
| **Protobuf** | `Message` object or `bytes` | `TableProperties(table_name, descriptor_proto=descriptor)` |
| **JSON** | `dict` or `str` (JSON string) | `TableProperties(table_name)` |

## Authentication

All examples use OAuth 2.0 authentication with `create_stream()`. The SDK automatically handles secure TLS connections.

For advanced configurations with custom headers, see the commented examples of `CustomHeadersProvider` in each example file.

## Using Your Own Schema

### For Protobuf Schemas

To use your own protobuf schema:

1. Modify `record.proto` or create a new proto file
2. Generate Python code:
   ```bash
   python -m grpc_tools.protoc --python_out=. --proto_path=. your_schema.proto
   ```
3. Update the example code to import and use your generated protobuf classes

### For JSON Mode

To use your own JSON structure:

1. Define your JSON structure in code:
   ```python
   json_record = json.dumps({"field1": "value1", "field2": 123})
   ```
2. Construct `TableProperties` without a Protobuf descriptor to select JSON
3. Ensure your JSON structure matches the schema of your Databricks table

Note: The SDK sends JSON strings directly without client-side schema validation.

## Additional Resources

- [Main README](../README.md) - Complete SDK documentation
- [API Reference](../README.md#api-reference) - Detailed API documentation
