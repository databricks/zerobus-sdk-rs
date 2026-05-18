# Zerobus Python SDK

[![PyPI - Downloads](https://img.shields.io/pypi/dw/databricks-zerobus-ingest-sdk)](https://pypistats.org/packages/databricks-zerobus-ingest-sdk)
[![PyPI - License](https://img.shields.io/pypi/l/databricks-zerobus-ingest-sdk)](https://github.com/databricks/zerobus-sdk/blob/main/LICENSE)
![PyPI](https://img.shields.io/pypi/v/databricks-zerobus-ingest-sdk)

A high-performance Python client for streaming data ingestion into Databricks Delta tables using the Zerobus service.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
  - [JSON (Simplest)](#option-1-json-simplest)
  - [Protocol Buffers](#option-2-protocol-buffers)
  - [Arrow Flight (Beta)](#option-3-arrow-flight-beta)
- [Configuration](#configuration)
- [Error Handling](#error-handling)
- [Handling Stream Failures](#handling-stream-failures)
- [Performance Tips](#performance-tips)
- [API Reference](#api-reference)
- [Debugging](#debugging)
- [Building from Source](#building-from-source)
- [Migrating from v1.x](#migrating-from-v1x)
- [Community and Contributing](#community-and-contributing)
- [License](#license)

## Overview

The Zerobus Python SDK is a thin wrapper around the [Zerobus Rust SDK](../rust/), built using PyO3 bindings. It delivers native performance with a Python-friendly API supporting both synchronous and asynchronous usage.

**What is Zerobus?** See the [project overview](https://github.com/databricks/zerobus-sdk/blob/main/README.md#what-is-zerobus) for details on the Zerobus service.

**Prerequisites** (workspace setup, table creation, service principal): See the [top-level README](https://github.com/databricks/zerobus-sdk/blob/main/README.md#prerequisites).

### Architecture

```
┌─────────────────────────────────────────┐
│         Python Application Code         │
└─────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────┐
│       Python SDK (Thin Wrapper)         │
│    • Sync and async APIs                │
│    • Python types & error handling      │
└─────────────────────────────────────────┘
                    │
                    ▼ (PyO3 bindings)
┌─────────────────────────────────────────┐
│         Rust Core Implementation        │
│    • gRPC communication                 │
│    • OAuth 2.0 authentication           │
│    • Stream management & recovery       │
└─────────────────────────────────────────┘
```

## Features

- **Rust-backed performance** - Native Rust implementation via PyO3 bindings for maximum throughput
- **Sync and Async support** - Both synchronous and asynchronous Python APIs
- **Automatic recovery** - Built-in retry and reconnection for transient failures
- **Multiple serialization formats** - JSON (simple) and Protocol Buffers (type-safe)
- **OAuth 2.0 authentication** - Secure authentication with client credentials, automatically refreshed
- **Acknowledgment callbacks** - Receive notifications when records are acknowledged or encounter errors
- **Flexible configuration** - Fine-tune timeouts, retries, and recovery behavior

## Installation

### From PyPI (Recommended)

```bash
pip install databricks-zerobus-ingest-sdk
```

Pre-built wheels are available for:
- **Linux**: x86_64, aarch64 (manylinux)
- **macOS**: x86_64, arm64
- **Windows**: x86_64

### Python Version

Requires **Python 3.9 or higher**.

### Dependencies

- `protobuf` >= 4.25.0, < 7.0 (for Protocol Buffer schema handling)
- `requests` >= 2.28.1, < 3 (only for the `generate_proto` utility tool)
- `pyarrow` >= 14.0.0, < 22.0 (optional, via the `[arrow]` extra; required for Arrow Flight)

All core ingestion functionality (gRPC, OAuth, stream management) is handled by the native Rust implementation.

## Quick Start

### Choose Your Serialization Format

1. **JSON** - Simple, no schema compilation needed. Good for getting started.
2. **Protocol Buffers** - Strongly-typed schemas, more efficient over the wire.

### Option 1: JSON (Simplest)

**Synchronous:**

```python
from zerobus import Format, OAuth, ZerobusSdk

server_endpoint = "https://1234567890123456.zerobus.us-west-2.cloud.databricks.com"
workspace_url = "https://dbc-a1b2c3d4-e5f6.cloud.databricks.com"

sdk = ZerobusSdk(host=server_endpoint, unity_catalog_url=workspace_url)
stream = sdk.create_stream(
    table="main.default.air_quality",
    auth=OAuth(client_id, client_secret),
    record_format=Format.JSON,
)

try:
    for i in range(100):
        offset = stream.ingest_record_offset({
            "device_name": f"sensor-{i % 10}",
            "temp": 20 + (i % 15),
            "humidity": 50 + (i % 40)
        })
    stream.flush()
finally:
    stream.close()
```

**Asynchronous:**

```python
import asyncio
from zerobus import Format, OAuth
from zerobus.aio import ZerobusSdk

async def main():
    sdk = ZerobusSdk(host=server_endpoint, unity_catalog_url=workspace_url)
    stream = await sdk.create_stream(
        table="main.default.air_quality",
        auth=OAuth(client_id, client_secret),
        record_format=Format.JSON,
    )

    try:
        for i in range(100):
            offset = await stream.ingest_record_offset({
                "device_name": f"sensor-{i % 10}",
                "temp": 20 + (i % 15),
                "humidity": 50 + (i % 40)
            })
        await stream.flush()
    finally:
        await stream.close()

asyncio.run(main())
```

### Option 2: Protocol Buffers

First, define a protobuf schema. Use `proto2` syntax with `optional` fields to match Delta table columns:

```protobuf
// record.proto
syntax = "proto2";
message AirQuality {
    optional string device_name = 1;
    optional int32 temp = 2;
    optional int64 humidity = 3;
}
```

See the [Delta → Protobuf type mappings](https://github.com/databricks/zerobus-sdk/blob/main/README.md#delta--protobuf-type-mappings) in the top-level README.

**Compile the schema** to generate a Python module:

```bash
pip install "grpcio-tools>=1.60.0,<2.0"
python -m grpc_tools.protoc --python_out=. --proto_path=. record.proto
# Generates record_pb2.py
```

**Load the descriptor** from the generated module and pass it to `Format.proto(...)`:

```python
import record_pb2
from zerobus import Format

format_spec = Format.proto(record_pb2.AirQuality.DESCRIPTOR)
```

Alternatively, generate the schema automatically from an existing Unity Catalog table:

```bash
python -m zerobus.tools.generate_proto \
    --uc-endpoint "https://dbc-a1b2c3d4-e5f6.cloud.databricks.com" \
    --client-id "your-client-id" \
    --client-secret "your-client-secret" \
    --table "main.default.air_quality" \
    --output "record.proto" \
    --proto-msg "AirQuality"

# Then compile the generated file the same way:
python -m grpc_tools.protoc --python_out=. --proto_path=. record.proto
```

**Synchronous:**

```python
import record_pb2
from zerobus import Format, OAuth, ZerobusSdk

sdk = ZerobusSdk(host=server_endpoint, unity_catalog_url=workspace_url)
stream = sdk.create_stream(
    table="main.default.air_quality",
    auth=OAuth(client_id, client_secret),
    record_format=Format.proto(record_pb2.AirQuality.DESCRIPTOR),
)

try:
    for i in range(100):
        record = record_pb2.AirQuality(
            device_name=f"sensor-{i % 10}",
            temp=20 + (i % 15),
            humidity=50 + (i % 40)
        )
        stream.ingest_record_nowait(record)
    stream.flush()
finally:
    stream.close()
```

**Asynchronous:**

```python
import asyncio
import record_pb2
from zerobus import Format, OAuth
from zerobus.aio import ZerobusSdk

async def main():
    sdk = ZerobusSdk(host=server_endpoint, unity_catalog_url=workspace_url)
    stream = await sdk.create_stream(
        table="main.default.air_quality",
        auth=OAuth(client_id, client_secret),
        record_format=Format.proto(record_pb2.AirQuality.DESCRIPTOR),
    )

    try:
        for i in range(100):
            record = record_pb2.AirQuality(
                device_name=f"sensor-{i % 10}",
                temp=20 + (i % 15),
                humidity=50 + (i % 40)
            )
            stream.ingest_record_nowait(record)
        await stream.flush()
    finally:
        await stream.close()

asyncio.run(main())
```

### Option 3: Arrow Flight (Beta)

> **Beta**: Arrow Flight ingestion is in **Beta** in v2.0.0. The API is
> stabilising but may still change before reaching GA.

Arrow ingestion is gated behind the `arrow` extra:

```bash
pip install "databricks-zerobus-ingest-sdk[arrow]"
```

```python
import pyarrow as pa
from zerobus import IPCCompression, OAuth, ZerobusSdk
from zerobus import ArrowStreamConfigurationOptions

schema = pa.schema([
    ("device_name", pa.large_utf8()),
    ("temp", pa.int32()),
    ("humidity", pa.int32()),
])

sdk = ZerobusSdk(host=server_endpoint, unity_catalog_url=workspace_url)
stream = sdk.create_arrow_stream(
    table="main.default.air_quality",
    schema=schema,
    auth=OAuth(client_id, client_secret),
    # Leave compression at NONE to take the zero-copy ingest path.
    options=ArrowStreamConfigurationOptions(ipc_compression=IPCCompression.NONE),
)

batch = pa.record_batch({
    "device_name": ["s1", "s2"],
    "temp": [22, 23],
    "humidity": [60, 61],
}, schema=schema)
offset = stream.ingest_batch(batch)
stream.flush()
stream.close()
```

When `ipc_compression=IPCCompression.NONE` (the default), the SDK forwards the
Arrow IPC bytes straight to Arrow Flight without parsing and re-serialising —
this is the recommended path. Enabling LZ4 / ZSTD compression disables the
zero-copy path.

See the [`examples/`](examples/) directory for complete runnable examples.

## Configuration

Configure stream behavior by passing `StreamConfigurationOptions` to
`create_stream()`. The `record_type` field is set automatically from the
`format` argument and does not need to be specified.

```python
from zerobus import AckCallback, OAuth, StreamConfigurationOptions, Format, ZerobusSdk

class MyCallback(AckCallback):
    def on_ack(self, offset: int):
        print(f"Acknowledged offset: {offset}")

options = StreamConfigurationOptions(
    max_inflight_records=10000,
    recovery=True,
    ack_callback=MyCallback(),
)

stream = sdk.create_stream(
    table="main.default.air_quality",
    auth=OAuth(client_id, client_secret),
    record_format=Format.JSON,
    options=options,
)
```

### Available Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `max_inflight_records` | `int` | `50000` | Maximum number of unacknowledged records |
| `recovery` | `bool` | `True` | Enable automatic stream recovery |
| `recovery_timeout_ms` | `int` | `15000` | Timeout for recovery operations (ms) |
| `recovery_backoff_ms` | `int` | `2000` | Delay between recovery attempts (ms) |
| `recovery_retries` | `int` | `3` | Maximum number of recovery attempts |
| `flush_timeout_ms` | `int` | `300000` | Timeout for flush operations (ms) |
| `server_lack_of_ack_timeout_ms` | `int` | `60000` | Server acknowledgment timeout (ms) |
| `stream_paused_max_wait_time_ms` | `Optional[int]` | `None` | Max wait during graceful stream close. `None` = full server duration, `0` = immediate, `x` = min(x, server_duration) |
| `callback_max_wait_time_ms` | `Optional[int]` | `5000` | Max wait for callbacks after `close()`. `None` = wait forever |
| `ack_callback` | `AckCallback` | `None` | Callback invoked on record acknowledgment or error |

## Error Handling

The SDK raises two types of exceptions:

- `ZerobusException` - Retriable errors (network issues, temporary server errors)
- `NonRetriableException` - Non-retriable errors (invalid credentials, missing table)

```python
from zerobus import ZerobusException, NonRetriableException

try:
    stream.ingest_record_offset(record)
except NonRetriableException as e:
    print(f"Fatal error: {e}")
    raise
except ZerobusException as e:
    print(f"Retriable error: {e}")
```

## Handling Stream Failures

The SDK automatically handles retries for transient errors. Use `get_unacked_records()` only when a stream has **permanently failed** (non-retriable error or max retries exceeded):

```python
from zerobus import Format, NonRetriableException, OAuth

try:
    for i in range(10000):
        stream.ingest_record_offset(record)
    stream.flush()
except NonRetriableException as e:
    unacked = list(stream.get_unacked_records())  # Iterator[bytes]
    print(f"Stream failed: {e}. {len(unacked)} records unacknowledged.")

    # Retry with a new stream
    new_stream = sdk.create_stream(
        table="main.default.air_quality",
        auth=OAuth(client_id, client_secret),
        record_format=Format.JSON,
    )
    for record_bytes in unacked:
        new_stream.ingest_record_offset(record_bytes)
    new_stream.flush()
    new_stream.close()
```

Use `get_unacked_batches()` for batch-level retry:

```python
unacked_batches = stream.get_unacked_batches()  # Returns List[List[bytes]]
for batch in unacked_batches:
    new_stream.ingest_records_offset(batch)
```

**Decoding unacked records:**
- **JSON mode**: `json.loads(record_bytes.decode('utf-8'))`
- **Protobuf mode**: `YourMessage.FromString(record_bytes)`

## Performance Tips

| Method | Throughput | Use case |
|--------|------------|----------|
| `ingest_record_nowait()` | **Highest** | Fire-and-forget: no offset returned; maximum throughput when you do not need per-record ack tracking in the hot path |
| `ingest_record_offset()` | Medium | Recommended for most apps: returns an offset after queueing; call `wait_for_offset()` when you need durability confirmation |

For Arrow Flight ingestion, the SDK uses a **zero-copy** ingest path when
`ipc_compression` is left at `IPCCompression.NONE` — Arrow IPC bytes are
forwarded straight to Arrow Flight without parse/re-encode. Enabling compression
disables the zero-copy path.

## API Reference

### `ZerobusSdk`

Main entry point. Sync: `from zerobus import ZerobusSdk` / Async: `from zerobus.aio import ZerobusSdk`

```python
sdk = ZerobusSdk(
    host: str,
    unity_catalog_url: str,
    *,
    application_name: Optional[str] = None,
)
```

`application_name` is appended to the HTTP `user-agent` for server-side
telemetry; the SDK prefix (`zerobus-sdk-py/<version>`) is always emitted.

```python
# Sync
stream = sdk.create_stream(
    table=...,
    auth=OAuth(client_id, client_secret),     # or Headers(my_provider)
    record_format=Format.JSON,                       # or Format.proto(descriptor)
    options=StreamConfigurationOptions(...),  # optional
)
# Async — same kwargs, awaited
stream = await sdk.create_stream(...)

# Arrow (Beta)
arrow_stream = sdk.create_arrow_stream(
    table=...,
    schema=pyarrow_schema,
    auth=OAuth(client_id, client_secret),
    options=ArrowStreamConfigurationOptions(...),  # optional
)
```

The `auth` and `format` arguments are tagged-union types
(`OAuth`/`Headers`, `Format.JSON`/`Format.proto(...)`). New auth strategies and
formats can be added in future releases without breaking existing call sites.

### `ZerobusStream`

**Single record ingestion:**

| Method | Sync | Async | Notes |
|--------|------|-------|-------|
| `ingest_record_nowait(record)` | `→ None` | `→ None` (not async) | Fire-and-forget, highest throughput |
| `ingest_record_offset(record)` | `→ int` | `await → int` | Returns offset after queueing |

**Batch ingestion:**

| Method | Sync | Async | Notes |
|--------|------|-------|-------|
| `ingest_records_nowait(records)` | `→ None` | `→ None` (not async) | Fire-and-forget |
| `ingest_records_offset(records)` | `→ Optional[int]` | `await → Optional[int]` | Returns final offset |

**Accepted record types:**
- **JSON mode**: `dict` (SDK serializes) or `str` (pre-serialized JSON)
- **Protobuf mode**: `Message` object (SDK serializes) or `bytes` (pre-serialized)

**Offset tracking:**

```python
# Sync
offset = stream.ingest_record_offset(record)
# ... do other work ...
stream.wait_for_offset(offset)  # Block until durably written

# Async
offset = await stream.ingest_record_offset(record)
# ... do other work ...
await stream.wait_for_offset(offset)  # Block until durably written
```

**Stream management:**

```python
# Sync
stream.flush()   # Wait for all pending records to be acknowledged
stream.close()   # Flush and close gracefully (always call in finally)

# Async
await stream.flush()
await stream.close()
```

**Unacknowledged records:**

```python
# Sync
records = stream.get_unacked_records()   # List[bytes]
batches = stream.get_unacked_batches()  # List[List[bytes]]

# Async
records = await stream.get_unacked_records()
batches = await stream.get_unacked_batches()
```

### `OAuth` / `Headers`

Auth selectors. Pass one of these as the `auth` argument to `create_stream` /
`create_arrow_stream`:

```python
from zerobus import OAuth, Headers

OAuth(client_id="...", client_secret="...")  # OAuth client credentials
Headers(provider=MyHeadersProvider(...))     # Custom HeadersProvider subclass
```

### `Format`

Format selectors. Pass one of these as the `format` argument to `create_stream`:

```python
from zerobus import Format

Format.JSON                       # JSON records
Format.proto(MyMessage.DESCRIPTOR)  # Compiled-proto records
```

Arrow Flight uses a dedicated `create_arrow_stream(table=..., schema=...)` entry
point and does not go through `Format`.

### `StreamConfigurationOptions`

See [Configuration](#configuration) for the full parameter list.

### `AckCallback`

```python
from zerobus import AckCallback

class MyCallback(AckCallback):
    def on_ack(self, offset: int) -> None:
        pass
```

### `HeadersProvider`

For custom authentication, subclass `HeadersProvider` and pass it through
`Headers(provider=...)`. The provider must return a list of
`(header_name, header_value)` tuples including any authentication header
required by the server. See [`examples/sync_example_proto.py`](examples/sync_example_proto.py).

### Exceptions

- `ZerobusException(message, cause=None)` - Retriable errors
- `NonRetriableException(message, cause=None)` - Non-retriable errors (extends `ZerobusException`)

## Debugging

The SDK uses Rust's `tracing` framework. Control log levels via `RUST_LOG`:

```bash
export RUST_LOG=info           # Default
export RUST_LOG=debug          # Detailed debugging
export RUST_LOG=trace          # Very verbose
export RUST_LOG=zerobus_sdk=debug  # Only SDK components
```

## Building from Source

Building from source requires the **Rust toolchain** (install from [rustup.rs](https://rustup.rs/)).

```bash
git clone https://github.com/databricks/zerobus-sdk.git
cd zerobus-sdk/python
make dev    # Set up venv and install in editable mode
make test   # Run tests
make build  # Build release wheel
```

For development workflows and detailed instructions, see [CONTRIBUTING.md](CONTRIBUTING.md).

## Migrating from v1.x

v2.0.0 reorganises the stream-creation API around tagged-union selectors and
removes the long-deprecated `ingest_record()` / `*_with_headers_provider`
methods. The following changes are required:

**Imports:**

```python
# Before
from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties

# After
from zerobus import Format, OAuth, StreamConfigurationOptions, ZerobusSdk
```

**Stream creation:**

```python
# Before (v1.x)
props = TableProperties("catalog.schema.table", record_pb2.MyMessage.DESCRIPTOR)
stream = sdk.create_stream(client_id, client_secret, props, options)

# After (v2.0.0)
stream = sdk.create_stream(
    table="catalog.schema.table",
    auth=OAuth(client_id, client_secret),
    record_format=Format.proto(record_pb2.MyMessage.DESCRIPTOR),
    options=options,  # `record_type` is implied by `format`; do not set it
)
```

**Custom HeadersProvider:**

```python
# Before (v1.x)
stream = sdk.create_stream(client_id, client_secret, props, options, headers_provider=my_provider)
# or:
stream = sdk.create_stream_with_headers_provider(props, my_provider, options)  # removed

# After (v2.0.0)
from zerobus import Headers
stream = sdk.create_stream(
    table=..., auth=Headers(my_provider), record_format=..., options=options
)
```

**Removed methods:**

- `stream.ingest_record(...)` → `stream.ingest_record_offset(...)` (deprecated since v0.3.0)
- `sdk.create_stream_with_headers_provider(...)` → `auth=Headers(provider)`
- `sdk.create_arrow_stream_with_headers_provider(...)` → `auth=Headers(provider)`
- `TableProperties` class → no longer needed; pass `table=...` directly

**SDK identifier:** The Python SDK now identifies itself as
`zerobus-sdk-py/2.0.0` on the HTTP `user-agent` header (previously it
inherited the Rust SDK identifier `zerobus-sdk-rs/...`). Use the new
`application_name=` constructor argument to append a caller-supplied
identifier for server-side telemetry.

## Community and Contributing

We are keen to hear feedback. Please [file issues](https://github.com/databricks/zerobus-sdk/issues).

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and contribution guidelines.

## License

This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for the full text.
