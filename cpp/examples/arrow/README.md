# Arrow Flight Example

This directory contains an example demonstrating Arrow Flight-based data
ingestion into Databricks Delta tables using the Zerobus C++ SDK.

> **Beta**: Arrow Flight ingestion is in Beta. The API is stabilising but may
> still change before reaching GA.

## Table of Contents

- [Overview](#overview)
- [Running the Example](#running-the-example)
- [Code Highlights](#code-highlights)
- [IPC Compression](#ipc-compression)
- [Adapting for Your Custom Table](#adapting-for-your-custom-table)

## Overview

Arrow Flight is a third record format option alongside JSON and Protocol Buffers:
it sends Apache Arrow `RecordBatch` data directly to Zerobus over the Arrow
Flight protocol. It is the best fit when your workload is naturally columnar or
batched — analytics pipelines, gateways aggregating short windows of rows, or
applications that already produce Arrow data via the Apache Arrow C++ library.

**Features:**
- Columnar Arrow data sent over the Arrow Flight protocol
- Per-batch acknowledgments with the same recovery semantics as standard streams

Send multiple rows per `RecordBatch`. Start with natural application-sized
batches; sending one row per call works but negates most of the performance
advantage of Arrow. For sparse, one-row-at-a-time traffic, the JSON or Protocol
Buffers examples are usually a better fit.

**Dependency:** the Apache Arrow C++ library. The example's CMake target is built
only when `find_package(Arrow)` succeeds; otherwise it is skipped and the other
examples still build. On Debian/Ubuntu, install `libarrow-dev`.

## Running the Example

1. Export the connection settings — see [Prerequisites](../README.md#prerequisites)
   for what each one is:
   ```bash
   export ZEROBUS_SERVER_ENDPOINT="https://<your-shard-id>.zerobus.<region>.cloud.databricks.com"
   export DATABRICKS_WORKSPACE_URL="https://<your-workspace>.cloud.databricks.com"
   export ZEROBUS_TABLE_NAME="catalog.schema.orders"
   export DATABRICKS_CLIENT_ID="<your_databricks_client_id>"
   export DATABRICKS_CLIENT_SECRET="<your_databricks_client_secret>"
   ```

2. Run:
   ```bash
   ./build/examples/arrow_ingest
   ```

The example ingests 10 `RecordBatch`es of 100 rows each, then `flush()`es and
`close()`s at the end.

**Expected output:**
```
Queued 10 batches (1000 rows); last offset ID: 9
Flushed — all batches acknowledged.
Stream closed successfully.
```

## Code Highlights

**Building an Arrow Flight stream.** The stream is created from schema-only Arrow
IPC bytes (an IPC stream with just the schema message), which tell the server
what the record batches will look like:

```cpp
std::shared_ptr<arrow::Schema> schema = orders_schema();
std::vector<std::uint8_t> schema_ipc = serialize_schema_ipc(schema);

zerobus::ArrowStream stream =
    sdk.create_arrow_stream(table_name, schema_ipc, client_id, client_secret);
```

**Ingest many `RecordBatch`es, then flush once.** Each `ingest_batch()` queues
one self-contained Arrow IPC stream (schema + one record-batch message) and
returns immediately with the assigned offset — there is no per-batch wait:

```cpp
for (int b = 0; b < kBatches; ++b) {
  std::shared_ptr<arrow::RecordBatch> batch = make_batch(...);
  last_offset = stream.ingest_batch(serialize_ipc(batch));  // queue only
}

stream.flush();   // wait once for all pending batches
stream.close();
```

**Batch semantics:**
- **All-or-nothing per `RecordBatch`** — a batch is acknowledged as a unit.
- **Single acknowledgment** — one offset ID for the whole `RecordBatch`.
- **Schema validation** — the `RecordBatch` schema must match the schema
  configured on the stream. The server validates on the first batch and fails
  fast with a descriptive error on a mismatch.

## IPC Compression

Arrow IPC payloads can be compressed on the wire. Enable compression only when
network bandwidth limits throughput — it reduces bytes on the wire at the cost of
client CPU. Set `ArrowStreamOptions::ipc_compression` and pass the options to
`create_arrow_stream`:

```cpp
zerobus::ArrowStreamOptions opts;
opts.ipc_compression = zerobus::IpcCompression::Zstd;   // or Lz4Frame

zerobus::ArrowStream stream = sdk.create_arrow_stream(
    table_name, schema_ipc, client_id, client_secret, opts);
```

- `Lz4Frame` — fast, low CPU overhead, modest compression ratio.
- `Zstd` — higher compression ratio, more CPU per batch.
- `NoCompression` (the default) — no compression.

A complete runnable version is in [`compression.cpp`](compression.cpp) — it is
`arrow_ingest.cpp` with `ipc_compression` set to `Zstd`. Build and run it the
same way:

```bash
./build/examples/arrow_compression
```

## Adapting for Your Custom Table

To ingest into your own table, change the Arrow schema and the array values to
match its columns.

1. **Update the Arrow schema** (must match the Delta table column names and types
   exactly): Delta `STRING` → `arrow::large_utf8()`, `INT` → `arrow::int32()`,
   `DOUBLE` → `arrow::float64()`, `TIMESTAMP` →
   `arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")`. These mirror the canonical
   Arrow schema the Arrow Flight server derives from Delta — note `STRING` maps to
   `large_utf8` (64-bit offsets), not `utf8`.
   ```cpp
   std::shared_ptr<arrow::Schema> orders_schema() {
     return arrow::schema({
         arrow::field("your_field_1", arrow::large_utf8()),
         arrow::field("your_field_2", arrow::int32()),
     });
   }
   ```
2. **Update `make_batch`** to populate builders matching the new schema.
3. **Point the environment at your table**: set `ZEROBUS_TABLE_NAME`,
   `DATABRICKS_WORKSPACE_URL`, and `ZEROBUS_SERVER_ENDPOINT` to your values.
