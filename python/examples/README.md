# Zerobus SDK Examples

This directory contains runnable example applications demonstrating both synchronous and asynchronous usage of the Zerobus Ingest SDK for Python, with examples for both both record type modes: **protobuf** and **JSON**.

For complete SDK documentation including installation, API reference, and configuration details, see the [main README](../README.md).

## Running the Examples

### 1. Clone or Check Out the Repository

```bash
git clone https://github.com/databricks/zerobus-sdk.git
cd zerobus-sdk/python
```

### 2. Install Dependencies

```bash
pip install -e .
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
python examples/sync_example_arrow.py     # Arrow Flight (Beta)

# Asynchronous examples (non-blocking I/O)
python examples/async_example_proto.py    # Protobuf
python examples/async_example_json.py     # JSON
python examples/async_example_arrow.py    # Arrow Flight (Beta)
```

## Examples Overview

All examples demonstrate multiple ingestion methods:

1. **`ingest_record_offset()`** - Single record with offset tracking
2. **`ingest_records_offset()`** - Batch ingestion with offset tracking
3. **`ingest_record_nowait()`** - Fire-and-forget single record
4. **`ingest_records_nowait()`** - Fire-and-forget batch (highest throughput)

Each example includes detailed comments explaining when to use each method and their performance characteristics.

### Serialization Formats

The SDK supports two serialization formats:

#### Protocol Buffers
**Files:** `sync_example_proto.py`, `async_example_proto.py`

More efficient over the wire. You can pass either:
- **Message object** (SDK serializes to bytes)
- **Pre-serialized bytes** (client controls serialization)

```python
from zerobus import Format, OAuth, ZerobusSdk
import record_pb2

sdk = ZerobusSdk(host=..., unity_catalog_url=...)
stream = sdk.create_stream(
    table=TABLE_NAME,
    auth=OAuth(client_id, client_secret),
    record_format=Format.proto(record_pb2.AirQuality.DESCRIPTOR),
)

record = record_pb2.AirQuality(device_name="sensor-1", temp=25, humidity=60)
offset = stream.ingest_record_offset(record)

# Or fire-and-forget for maximum throughput
stream.ingest_record_nowait(record)

# Or pass pre-serialized bytes (client controls serialization):
# offset = stream.ingest_record_offset(record.SerializeToString())
```

#### JSON
**Files:** `sync_example_json.py`, `async_example_json.py`

Good for getting started. No protobuf schema required. You can pass either:
- **dict** (SDK serializes to JSON)
- **Pre-serialized JSON string** (client controls serialization)

```python
from zerobus import Format, OAuth, ZerobusSdk

sdk = ZerobusSdk(host=..., unity_catalog_url=...)
stream = sdk.create_stream(
    table=TABLE_NAME,
    auth=OAuth(client_id, client_secret),
    record_format=Format.JSON,
)

record_dict = {"device_name": "sensor-1", "temp": 25, "humidity": 60}
offset = stream.ingest_record_offset(record_dict)

# Or fire-and-forget for maximum throughput
stream.ingest_record_nowait(record_dict)

# Or pass a pre-serialized JSON string:
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
| Import | `from zerobus import ZerobusSdk` | `from zerobus.aio import ZerobusSdk` |
| Stream creation | `stream = sdk.create_stream(...)` | `stream = await sdk.create_stream(...)` |
| Record ingestion (with offset) | `offset = stream.ingest_record_offset(record)` | `offset = await stream.ingest_record_offset(record)` |
| Record ingestion (fire-and-forget) | `stream.ingest_record_nowait(record)` | `stream.ingest_record_nowait(record)` |
| Flush | `stream.flush()` | `await stream.flush()` |
| Close | `stream.close()` | `await stream.close()` |
| Execution context | Standard Python | Requires asyncio event loop |
| Use case | General Python applications | Asyncio-based applications |

**Performance:** Both APIs offer equivalent throughput and durability. Choose based on your application's architecture, not performance needs.

**Recommended Methods:**

**Single Record Ingestion:**
- `ingest_record_offset()` - Returns offset immediately, use when you need to track offsets
- `ingest_record_nowait()` - Fire-and-forget, best for maximum throughput

**Batch Ingestion:**
- `ingest_records_offset()` - Batch multiple records, returns final offset
- `ingest_records_nowait()` - Fire-and-forget batch ingestion, most efficient for bulk data

### Serialization Format Comparison

| Format | Record Input | `create_stream` argument |
|--------|-------------|--------------------------|
| **Protobuf** | `Message` object or `bytes` | `record_format=Format.proto(MyMessage.DESCRIPTOR)` |
| **JSON** | `dict` or `str` (JSON string) | `record_format=Format.JSON` |
| **Arrow Flight** (Beta) | `pyarrow.RecordBatch` or `pyarrow.Table` | use `create_arrow_stream(table=..., schema=..., auth=...)` |

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
2. Pass `record_format=Format.JSON` to `sdk.create_stream(...)`
3. Ensure your JSON structure matches the schema of your Databricks table

Note: The SDK sends JSON strings directly without client-side schema validation.

### Arrow Flight (Beta)

**Files:** `sync_example_arrow.py`, `async_example_arrow.py`

> Arrow Flight ingestion is in **Beta** in v2.0.0. The API is stabilising but
> may still change before reaching GA. Use
> `pip install "databricks-zerobus-ingest-sdk[arrow]"` to install the
> `pyarrow` dependency.

The SDK uses a **zero-copy** path (forwarding Arrow IPC bytes straight to
Arrow Flight) when `ipc_compression=IPCCompression.NONE` (the default).
Enabling `IPCCompression.LZ4_FRAME` or `IPCCompression.ZSTD` disables zero-copy.

## Additional Resources

- [Main README](../README.md) - Complete SDK documentation
- [API Reference](../README.md#api-reference) - Detailed API documentation
