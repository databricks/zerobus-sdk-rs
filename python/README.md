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
  - [Acknowledgments and throughput](#acknowledgments-and-throughput)
- [Configuration](#configuration)
- [Error Handling](#error-handling)
- [Handling Stream Failures](#handling-stream-failures)
- [Performance Tips](#performance-tips)
- [API Reference](#api-reference)
- [Debugging](#debugging)
- [Building from Source](#building-from-source)
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

Requires **Python 3.9-3.14**. The SDK is tested on CPython 3.9 through 3.14.

The wheel uses the CPython stable ABI (`abi3`), so one wheel works on every
supported version. The free-threaded builds (like `3.14t`) are not
supported, because the stable ABI does not cover them.

### Dependencies

- `protobuf` >= 4.25.0, < 7.0 (for Protocol Buffer schema handling)
- `requests` >= 2.28.1, < 3 (only for the `generate_proto` utility tool)

All core ingestion functionality (gRPC, OAuth, stream management) is handled by the native Rust implementation.

Arrow Flight ingestion needs `pyarrow`. Install it with the `arrow` extra:

```bash
pip install "databricks-zerobus-ingest-sdk[arrow]"
```

The extra selects the `pyarrow` version for your interpreter, because no single
`pyarrow` release covers Python 3.9 through 3.14. On Python 3.14 the extra
installs `pyarrow` 22.0.0 or later, which is the first release with 3.14 wheels.
On earlier versions it installs `pyarrow` below 22.0.0. Core ingestion (Protobuf
and JSON) does not need `pyarrow` at all.

## Quick Start

### Choose Your Serialization Format

1. **Protocol Buffers** (Recommended) - Strongly-typed schemas with compact binary encoding. More efficient over the wire and the best choice for production and high-throughput workloads.
2. **JSON** - Simple, no schema compilation needed. Good for getting started or quick prototyping, but each record carries higher per-record overhead (text serialization plus UTF-8 validation), so it is slower than Protocol Buffers for high-volume ingestion.

### Option 1: JSON (Simplest)

**Synchronous:**

```python
from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import TableProperties

server_endpoint = "https://1234567890123456.zerobus.us-west-2.cloud.databricks.com"
workspace_url = "https://dbc-a1b2c3d4-e5f6.cloud.databricks.com"

sdk = ZerobusSdk(server_endpoint, workspace_url)
table_properties = TableProperties("main.default.air_quality")
stream = sdk.create_stream(client_id, client_secret, table_properties)

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
from zerobus.sdk.aio import ZerobusSdk
from zerobus.sdk.shared import TableProperties

async def main():
    server_endpoint = "https://1234567890123456.zerobus.us-west-2.cloud.databricks.com"
    workspace_url = "https://dbc-a1b2c3d4-e5f6.cloud.databricks.com"

    sdk = ZerobusSdk(server_endpoint, workspace_url)
    table_properties = TableProperties("main.default.air_quality")
    stream = await sdk.create_stream(client_id, client_secret, table_properties)

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

### Acknowledgments and throughput

Ingestion is asynchronous. `ingest_record_offset()` returns as soon as the record is
queued; the SDK sends it and tracks its acknowledgment in the background. To confirm
records are durably committed, call `flush()` — it returns once everything queued so far
is acknowledged. The idiomatic flow is **ingest in a loop, then `flush()`** (once for a
bounded batch, or periodically for a long-running stream); or register an
[`AckCallback`](#ackcallback) to be notified as records commit.

Each ingest also returns the record's offset, and `wait_for_offset(offset)` blocks until
that offset is acknowledged — handy when a specific record must be confirmed before
continuing (acks are ordered, so waiting on the last offset confirms the whole run). Just
avoid calling `wait_for_offset()` after every record in a tight loop, since that limits
throughput to one record per round-trip.

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

**Load the descriptor** from the generated module and pass it to `TableProperties`:

```python
import record_pb2

# The DESCRIPTOR is the compiled schema — pass it so the SDK can validate records
table_properties = TableProperties("main.default.air_quality", record_pb2.AirQuality.DESCRIPTOR)
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
from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import TableProperties
import record_pb2

sdk = ZerobusSdk(server_endpoint, workspace_url)
table_properties = TableProperties("main.default.air_quality", record_pb2.AirQuality.DESCRIPTOR)
stream = sdk.create_stream(client_id, client_secret, table_properties)

try:
    for i in range(100):
        record = record_pb2.AirQuality(
            device_name=f"sensor-{i % 10}",
            temp=20 + (i % 15),
            humidity=50 + (i % 40)
        )
        stream.ingest_record_offset(record)
    stream.flush()
finally:
    stream.close()
```

**Asynchronous:**

```python
import asyncio
from zerobus.sdk.aio import ZerobusSdk
from zerobus.sdk.shared import TableProperties
import record_pb2

async def main():
    sdk = ZerobusSdk(server_endpoint, workspace_url)
    table_properties = TableProperties("main.default.air_quality", record_pb2.AirQuality.DESCRIPTOR)
    stream = await sdk.create_stream(client_id, client_secret, table_properties)

    try:
        for i in range(100):
            record = record_pb2.AirQuality(
                device_name=f"sensor-{i % 10}",
                temp=20 + (i % 15),
                humidity=50 + (i % 40)
            )
            await stream.ingest_record_offset(record)
        await stream.flush()
    finally:
        await stream.close()

asyncio.run(main())
```

See the [`examples/`](examples/) directory for complete runnable examples.

## Configuration

Configure stream behavior by passing a `StreamConfigurationOptions` object to `create_stream()`:

```python
from zerobus.sdk.shared import AckCallback, StreamConfigurationOptions

class MyCallback(AckCallback):
    def on_ack(self, offset: int):
        print(f"Acknowledged offset: {offset}")

    def on_error(self, offset: int, error_message: str):
        print(f"Error at offset {offset}: {error_message}")

options = StreamConfigurationOptions(
    max_inflight_records=10000,
    recovery=True,
    ack_callback=MyCallback()
)

stream = sdk.create_stream(client_id, client_secret, table_properties, options)
```

### Available Options

The record format is inferred from `TableProperties`: omitting `descriptor_proto` selects JSON,
while providing a Protobuf descriptor selects Protobuf. `record_type` is retained for backward
compatibility but does not select the format.

| Option                           | Type            | Default            | Description                                                                                                          |
| -------------------------------- | --------------- | ------------------ | -------------------------------------------------------------------------------------------------------------------- |
| `record_type`                    | `RecordType`    | `RecordType.PROTO` | Retained for backward compatibility; format comes from `TableProperties.descriptor_proto`                            |
| `max_inflight_records`           | `int`           | `1000000`          | Maximum number of unacknowledged records                                                                             |
| `recovery`                       | `bool`          | `True`             | Enable automatic stream recovery                                                                                     |
| `recovery_timeout_ms`            | `int`           | `15000`            | Timeout for recovery operations (ms)                                                                                 |
| `recovery_backoff_ms`            | `int`           | `2000`             | Delay between recovery attempts (ms)                                                                                 |
| `recovery_retries`               | `int`           | `4`                | Maximum number of recovery attempts                                                                                  |
| `flush_timeout_ms`               | `int`           | `300000`           | Timeout for flush operations (ms)                                                                                    |
| `server_lack_of_ack_timeout_ms`  | `int`           | `60000`            | Server acknowledgment timeout (ms)                                                                                   |
| `stream_paused_max_wait_time_ms` | `Optional[int]` | `None`             | Max wait during graceful stream close. `None` = full server duration, `0` = immediate, `x` = min(x, server_duration) |
| `callback_max_wait_time_ms`      | `Optional[int]` | `5000`             | Max wait for callbacks after `close()`. `None` = wait forever                                                        |
| `ack_callback`                   | `AckCallback`   | `None`             | Callback invoked once per successfully queued ingest submission that later acknowledges or fails                     |

## Error Handling

The SDK raises two categories of exception. Handle them differently:

- **Retriable** (`ZerobusException`): transient conditions such as network issues or
  temporary server errors. Safe to retry, ideally with backoff. The SDK's built-in
  recovery already handles many of these for you.
- **Non-retriable** (`NonRetriableException`): fatal conditions such as invalid
  credentials or a missing table. Retrying won't help. Fix the underlying problem.

`NonRetriableException` is a subclass of `ZerobusException`, so a bare
`except ZerobusException` still catches both.

```python
from zerobus.sdk.shared import ZerobusException, NonRetriableException

try:
    stream.ingest_record_offset(record)
except NonRetriableException as e:
    # Fatal: do not retry; fix the cause (credentials, table, schema)
    raise
except ZerobusException as e:
    # Transient: retry with backoff
    ...
```

## Handling Stream Failures

The SDK automatically handles retries for transient errors. Enqueue, flush, and close
failures surface as `ZerobusException` or `NonRetriableException`. `get_unacked_records()`
and `recreate_stream()` succeed only after the stream has already closed, which a terminal
failure does. An enqueue failure leaves the stream active, so those calls fail; raise
the original error and keep the stream. `recreate_stream()` re-queues records that were
already accepted; it does not retry a payload that failed to enqueue.

```python
from zerobus.sdk.shared import ZerobusException

try:
    for i in range(10000):
        stream.ingest_record_offset(record)
    stream.flush()
except ZerobusException as e:
    print(f"Ingestion failed: {e}")
    try:
        unacked = list(stream.get_unacked_records())
    except ZerobusException:
        raise e
    print(f"{len(unacked)} previously queued records were unacknowledged.")
    try:
        new_stream = sdk.recreate_stream(stream)
        try:
            new_stream.flush()
        finally:
            new_stream.close()
    except ZerobusException:
        raise e
else:
    stream.close()
```

Use `get_unacked_batches()` to inspect the original batch grouping after the stream closes:

```python
unacked_batches = list(stream.get_unacked_batches())
print(f"{len(unacked_batches)} batches remain unacknowledged")
```

**Decoding unacked records:**

- **JSON mode**: `json.loads(record_bytes.decode('utf-8'))`
- **Protobuf mode**: `YourMessage.FromString(record_bytes)`

## Performance Tips

The reliable bulk path is `ingest_records_offset()` plus one `flush()`. That call
amortizes the Python-to-Rust crossing and returns an offset after the batch is queued.
For single records, use `ingest_record_offset()` in a loop and `flush()` once. Ingest
calls queue immediately and the SDK acknowledges records in the background, so a single
`flush()` confirms everything queued so far. The ack watermark is monotonic, so if you
want a durability checkpoint mid-stream, waiting on the last offset returned confirms
every prior record. In async code, an [`AckCallback`](#ackcallback) tracks durability
without blocking. Calling `wait_for_offset()` after every record in a tight loop limits
throughput to one record per round-trip, so save it for confirming a specific record.

`ingest_record_nowait()` and `ingest_records_nowait()` spawn detached tasks and discard
enqueue errors. `flush()` can complete before those tasks allocate offsets, so they are
not a safe durability path. Prefer the offset APIs.

| Method                     | Throughput | Use case                                                                                          |
| -------------------------- | ---------- | ------------------------------------------------------------------------------------------------- |
| `ingest_records_offset()`  | Highest    | Recommended bulk path: queue a batch, then `flush()` once                                         |
| `ingest_record_offset()`   | Medium     | Recommended for single records: ingest in a loop, then `flush()` once                             |
| `ingest_record()`          | Low        | Deprecated; prefer offset-based APIs                                                              |
| `ingest_record_nowait()`   | Unsafe     | Detached fire-and-forget; enqueue errors can be lost and are not synchronized with `flush()`      |
| `ingest_records_nowait()`  | Unsafe     | Detached batch fire-and-forget; same durability caveats as `ingest_record_nowait()`               |

Idiomatic flow:

```python
async def ingest_all(stream, records):
    await stream.ingest_records_offset(records)     # queues the batch, no round-trip
    await stream.flush()                            # one wait for everything
```

**Confirming a specific record** (waiting on the last offset confirms all prior records):

```python
async def ingest_and_confirm(stream, records):
    offset = None
    for record in records:
        offset = await stream.ingest_record_offset(record)
    if offset is not None:
        await stream.wait_for_offset(offset)        # confirm the run before continuing
```

## API Reference

### `ZerobusSdk`

Main entry point. Sync: `from zerobus.sdk.sync import ZerobusSdk` / Async: `from zerobus.sdk.aio import ZerobusSdk`

```python
sdk = ZerobusSdk(
    host="https://<workspace>.zerobus.<region>.cloud.databricks.com",
    unity_catalog_url="https://<workspace-host>",
    application_name="my-app/1.0",
)
```

`application_name` is optional; when set it is appended to the `user-agent` header on gRPC requests to the Zerobus server (not on the OAuth token requests to the login service). It follows the `"<product>/<version>"` convention (e.g. `my-app/1.0`).

```python
# Sync
stream = sdk.create_stream(client_id, client_secret, table_properties, options=None, headers_provider=None)
# Async
async def create_async_stream(sdk):
    return await sdk.create_stream(client_id, client_secret, table_properties, options=None, headers_provider=None)
```

### `ZerobusStream`

**Single record ingestion:**

| Method                         | Sync                     | Async                | Notes                                                                 |
| ------------------------------ | ------------------------ | -------------------- | --------------------------------------------------------------------- |
| `ingest_record_offset(record)` | `→ int`                  | `await → int`        | Recommended for single records; returns offset after queueing         |
| `ingest_record(record)`        | `→ RecordAcknowledgment` | `await → Awaitable`  | Deprecated since v0.3.0                                               |
| `ingest_record_nowait(record)` | `→ None`                 | `→ None` (not async) | Detached fire-and-forget; enqueue errors are not synchronized with `flush()` |

**Batch ingestion:**

| Method                           | Sync     | Async                | Notes                                                                 |
| -------------------------------- | -------- | -------------------- | --------------------------------------------------------------------- |
| `ingest_records_offset(records)` | `→ int`  | `await → int`        | Recommended bulk path; returns the batch's final offset               |
| `ingest_records_nowait(records)` | `→ None` | `→ None` (not async) | Detached fire-and-forget; same durability caveats as `ingest_record_nowait()` |

**Accepted record types:**

- **JSON mode**: `dict` (SDK serializes) or `str` (pre-serialized JSON)
- **Protobuf mode**: `Message` object (SDK serializes) or `bytes` (pre-serialized)

**Offset tracking** (use to confirm a specific record before continuing; for bulk
durability, ingest in a loop and `flush()` once):

```python
# Sync
offset = stream.ingest_record_offset(record)
# ... do other work ...
stream.wait_for_offset(offset)  # Block until durably written

# Async
async def confirm_async(stream, record):
    offset = await stream.ingest_record_offset(record)
    # ... do other work ...
    await stream.wait_for_offset(offset)  # Block until durably written
```

Acks are ordered, so waiting on the last offset returned confirms all prior records too.

**Stream management:**

```python
# Sync
stream.flush()   # Wait for all pending records to be acknowledged
stream.close()   # Flush and close gracefully (always call in finally)

# Async
async def close_async(stream):
    await stream.flush()
    await stream.close()
```

**Unacknowledged records:**

```python
# Sync
records = stream.get_unacked_records()   # Iterator[bytes]
batches = stream.get_unacked_batches()  # Iterator[List[bytes]]

# Async
async def get_unacked_async(stream):
    records = await stream.get_unacked_records()
    batches = await stream.get_unacked_batches()
    return records, batches
```

### `TableProperties`

```python
# JSON mode
TableProperties("catalog.schema.table")

# Protobuf mode
TableProperties("catalog.schema.table", descriptor_proto=MyMessage.DESCRIPTOR)
```

### `StreamConfigurationOptions`

See [Configuration](#configuration) for full parameter list.

### `AckCallback`

```python
from zerobus.sdk.shared import AckCallback

class MyCallback(AckCallback):
    def on_ack(self, offset: int) -> None:
        # Called once for each acknowledged single-record or batch submission
        pass

    def on_error(self, offset: int, error_message: str) -> None:
        # Called once when a single-record or batch submission encounters an error
        pass
```

`close()` waits at most `callback_max_wait_time_ms` (default 5000 ms) for
in-flight callbacks. A callback per queued submission is not guaranteed if that
budget expires.

### `HeadersProvider`

For custom authentication (e.g. custom token providers), implement `HeadersProvider` and pass it to `create_stream()`. Must include both `authorization` and `x-databricks-zerobus-table-name` headers. See [`examples/`](examples/) for implementation details.

### `RecordAcknowledgment` (Sync only, deprecated)

```text
ack.wait_for_ack(timeout_sec=None)  # Block until acknowledged
ack.is_done() -> bool
```

### Exceptions

- `ZerobusException(message, cause=None)` - Base exception; retryable SDK failures are raised as this type
- `NonRetriableException(message, cause=None)` - Subclass for fatal errors (`ZerobusError::is_retryable()` is false)

## Debugging

The SDK uses Rust's `tracing` framework. Control log levels via `RUST_LOG`:

```bash
export RUST_LOG=info           # Default
export RUST_LOG=debug          # Detailed debugging
export RUST_LOG=trace          # Very verbose
export RUST_LOG=zerobus_sdk=debug  # Only SDK components
```

## Building from Source

Building from source requires the Rust toolchain 1.88 or newer (install from [rustup.rs](https://rustup.rs/)).

```bash
git clone https://github.com/databricks/zerobus-sdk.git
cd zerobus-sdk/python
make dev    # Set up venv and install in editable mode
make test   # Run tests
make build  # Build release wheel
```

For development workflows and detailed instructions, see [CONTRIBUTING.md](CONTRIBUTING.md).

## Community and Contributing

We are keen to hear feedback. Please [file issues](https://github.com/databricks/zerobus-sdk/issues).

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and contribution guidelines.

## License

This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for the full text.
