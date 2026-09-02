# Zerobus C++ SDK Examples

This directory contains examples demonstrating how to use the Zerobus C++ SDK to
ingest data into Databricks Delta tables.

## Table of Contents

- [Overview](#overview)
- [JSON Examples](json/README.md)
- [Protocol Buffers Examples](proto/README.md)
- [Arrow Flight Examples](arrow/README.md)
- [Prerequisites](#prerequisites)
  - [Create a Databricks Table](#1-create-a-databricks-table)
  - [Set Up OAuth Service Principal](#2-set-up-oauth-service-principal)
  - [Configure Credentials](#3-configure-credentials)
- [Building the Examples](#building-the-examples)
- [Common Code Patterns](#common-code-patterns)
- [Single-Record vs Batch Ingestion](#single-record-vs-batch-ingestion)
- [Choosing JSON vs Protocol Buffers](#choosing-json-vs-protocol-buffers)
- [Troubleshooting](#troubleshooting)

## Overview

The SDK supports two record wire formats on a standard `Stream`, plus a separate
columnar `ArrowStream`:

**Serialization Formats:**
- **[JSON](json/README.md)** — Simpler, no schema generation required. Great for
  getting started.
- **[Protocol Buffers](proto/README.md)** — Type-safe binary encoding. The
  examples build the descriptor at runtime from Unity Catalog metadata
  (`ProtoSchema::from_uc_json`), so **no `.proto` file or `protoc` is required**.
- **[Arrow Flight](arrow/README.md)** — Columnar Arrow `RecordBatch`
  ingestion over the Arrow Flight protocol.

Beyond the basic ingestion flow, several examples also demonstrate advanced
features inline: an async ack callback and a custom `HeadersProvider` in
[`json/batch.cpp`](json/batch.cpp), recovering unacknowledged records after a
failure in [`json/single.cpp`](json/single.cpp), and Arrow IPC compression in
[`arrow/arrow_ingest.cpp`](arrow/arrow_ingest.cpp).

**Ingestion Methods:**
- **Single-record** (`ingest_json_record` / `ingest_proto_record`) — ingest
  records one at a time.
- **Batch** (`ingest_json_records` / `ingest_proto_records`) — ingest multiple
  records at once with all-or-nothing semantics. Preferred in hot paths: it
  amortizes the per-call FFI crossing.
- **Arrow batch** (`ingest_batch`) — ingest one Arrow `RecordBatch` (one or many
  rows) over Arrow Flight.

**Available Examples:**

| Example | Format | Method | Binary |
|---------|--------|--------|--------|
| [JSON Single](json/README.md#single-record-example) | JSON | Single-record | `json_single` |
| [JSON Batch](json/README.md#batch-example) | JSON | Batch | `json_batch` |
| [Proto Single](proto/README.md#single-record-example) | Protocol Buffers | Single-record | `proto_single` |
| [Proto Batch](proto/README.md#batch-example) | Protocol Buffers | Batch | `proto_batch` |
| [Arrow](arrow/README.md) | Arrow Flight | `RecordBatch` | `arrow_ingest` |

## Prerequisites

### 1. Create a Databricks Table

Create a table in your Databricks workspace:

```sql
CREATE TABLE catalog.schema.orders (
  id INT,
  customer_name STRING,
  product_name STRING,
  quantity INT,
  price DOUBLE,
  status STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);
```

Replace `catalog.schema.orders` with your actual catalog, schema, and table name.

### 2. Set Up OAuth Service Principal

1. In your Databricks workspace, go to **Settings** > **Identity and Access**.
2. Create a service principal or use an existing one.
3. Generate OAuth credentials (client ID and secret).
4. Grant the service principal these permissions on your table:
   - `SELECT` — read table schema
   - `MODIFY` — write data to the table
   - `USE CATALOG` and `USE SCHEMA` — access catalog and schema

### 3. Configure Credentials

Every example reads its connection settings from the environment, so no value is
ever baked into source. Export these five variables before running any example
(the same names the Go, TypeScript, Java, and Python SDK examples use):

```bash
export ZEROBUS_SERVER_ENDPOINT="https://<your-shard-id>.zerobus.<region>.cloud.databricks.com"
export DATABRICKS_WORKSPACE_URL="https://<your-workspace>.cloud.databricks.com"
export ZEROBUS_TABLE_NAME="catalog.schema.orders"
export DATABRICKS_CLIENT_ID="<your_databricks_client_id>"
export DATABRICKS_CLIENT_SECRET="<your_databricks_client_secret>"
```

For Azure, use `.azuredatabricks.net` hosts in the endpoint and workspace URL.

The proto examples additionally read the Unity Catalog table metadata JSON from
the environment (the JSON examples don't). See the [Protocol Buffers
README](proto/README.md#how-the-schema-is-built-dynamic-proto) for why it's
needed and the full two-step fetch:

```bash
DATABRICKS_TOKEN="$(curl -sS --fail --request POST \
  --user "$DATABRICKS_CLIENT_ID:$DATABRICKS_CLIENT_SECRET" \
  "$DATABRICKS_WORKSPACE_URL/oidc/v1/token" \
  --data 'grant_type=client_credentials&scope=all-apis' \
  | jq -r .access_token)"

export ZEROBUS_UC_TABLE_JSON="$(curl -sS --fail \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  "$DATABRICKS_WORKSPACE_URL/api/2.1/unity-catalog/tables/$ZEROBUS_TABLE_NAME")"
```

**How to get these values:**
- **ZEROBUS_SERVER_ENDPOINT** — Zerobus ingestion endpoint (usually
  `https://<shard-id>.zerobus.<region>.cloud.databricks.com`).
- **DATABRICKS_WORKSPACE_URL** — your Databricks workspace URL (Unity Catalog endpoint).
- **ZEROBUS_TABLE_NAME** — full table name in the form `catalog.schema.table`.
- **DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET** — OAuth 2.0 credentials from your
  service principal.

## Building the Examples

The examples build as part of the SDK's top-level CMake build, gated on the
`ZEROBUS_BUILD_EXAMPLES` option (on by default for a top-level build). From
`cpp/`:

```bash
make build     # configure + build the SDK, tests, and examples
```

Or drive CMake directly:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

The binaries land in `build/examples/`:

```bash
./build/examples/json_single
./build/examples/json_batch
./build/examples/proto_single
./build/examples/proto_batch
./build/examples/arrow_ingest      # only if Apache Arrow C++ is installed
```

The JSON and proto examples need no extra dependencies. The Arrow example
requires the Apache Arrow C++ library (`find_package(Arrow)`); if it is not
installed the `arrow_ingest` target is skipped and the other four still build.

## Common Code Patterns

All examples follow the same general flow.

### 1. Initialize the SDK

```cpp
// server_endpoint, workspace_url, and table_name are read from the environment
// (ZEROBUS_SERVER_ENDPOINT / DATABRICKS_WORKSPACE_URL / ZEROBUS_TABLE_NAME).
zerobus::Sdk sdk = zerobus::Sdk::builder()
                       .endpoint(server_endpoint)
                       .unity_catalog_url(workspace_url)
                       .application_name("my-app")
                       .build();
```

### 2. Create a Stream

**JSON:**
```cpp
zerobus::TableProperties props;
props.table_name = table_name;         // empty descriptor => JSON stream

zerobus::StreamOptions options;
options.record_type = zerobus::RecordType::Json;

zerobus::Stream stream =
    sdk.create_stream(props, client_id, client_secret, options);
```

**Protocol Buffers (dynamic schema):**
```cpp
zerobus::ProtoSchema schema = zerobus::ProtoSchema::from_uc_json(uc_table_json);

zerobus::TableProperties props;
props.table_name = table_name;
props.descriptor_proto = schema.descriptor_bytes();

zerobus::StreamOptions options;
options.record_type = zerobus::RecordType::Proto;

zerobus::Stream stream =
    sdk.create_stream(props, client_id, client_secret, options);
```

**Arrow Flight:**
```cpp
zerobus::ArrowStream stream =
    sdk.create_arrow_stream(table_name, schema_ipc_bytes, client_id,
                            client_secret);
```

### 3. Ingest and Acknowledge

> **The cardinal performance rule.** `ingest_*` returns as soon as the record is
> **queued** — sending and acknowledgement happen on background tasks. **Never
> call `wait_for_offset()` / `flush()` after every record.** Waiting per record
> forces one server round-trip per record and collapses throughput. Ingest in a
> loop, then `flush()` once at the end (or periodically for a continuous stream).

```cpp
for (const auto& record : records) {
  stream.ingest_json_record(record);   // queue only — do NOT wait here
}
stream.flush();                        // wait once for all pending acks
```

`wait_for_offset()` behaves the same way: acks are monotonic, so waiting on the
**last** offset returned by a run of ingests confirms all prior ones too.

### 4. Close the Stream

```cpp
stream.close();   // flush + close at a controlled point; surfaces final errors
```

Prefer calling `close()` explicitly rather than relying on the destructor: it
flushes pending records and surfaces any error by throwing, whereas the
destructor swallows it.

## Single-Record vs Batch Ingestion

| Aspect | Single-Record | Batch |
|--------|---------------|-------|
| **Method** | `ingest_json_record()` / `ingest_proto_record()` | `ingest_json_records()` / `ingest_proto_records()` |
| **Use case** | Records arrive one at a time | Multiple records ready at once |
| **Semantics** | Each record independent | All-or-nothing (atomic) |
| **Acknowledgment** | Per record | Per batch (one offset for the batch) |
| **Throughput** | Lower | Higher (amortizes the FFI crossing) |

**Single-record:**
```cpp
for (const auto& record : records) {
  stream.ingest_json_record(record);
}
stream.flush();
```

**Batch:**
```cpp
std::int64_t batch_offset = stream.ingest_json_records(records);
if (batch_offset >= 0) {
  stream.wait_for_offset(batch_offset);   // one wait confirms the whole batch
}
```

An empty batch is a no-op and returns `-1`.

## Choosing JSON vs Protocol Buffers

| Feature | JSON | Protocol Buffers (dynamic) |
|---------|------|----------------------------|
| **Setup** | Simple — no schema | Fetches the descriptor from Unity Catalog at runtime |
| **Build deps** | none | none (`ProtoSchema`, no `protoc`) |
| **Type Safety** | Runtime validation | Runtime validation against the fetched schema |
| **Performance** | Text-based | Efficient binary encoding |
| **Best For** | Prototyping, flexible schemas | Production, high-throughput |

**Recommendation:** Start with JSON for quick prototyping, then move to
Protocol Buffers for production where binary encoding and performance matter.
Both paths need no protobuf toolchain in your build.

## Troubleshooting

### Failed to create the stream

**Possible causes:**
- Invalid credentials (client ID or secret).
- Service principal lacks permissions on the table.
- Incorrect workspace URL or endpoint.
- Table doesn't exist.

**Solution:** Verify your credentials and table permissions, and confirm the
endpoint and workspace URL.

### `environment variable ... is not set`

The example exits with code 2 before touching the SDK if a required environment
variable is missing. Export `ZEROBUS_SERVER_ENDPOINT`, `DATABRICKS_WORKSPACE_URL`,
`ZEROBUS_TABLE_NAME`, `DATABRICKS_CLIENT_ID`, and `DATABRICKS_CLIENT_SECRET` (and
`ZEROBUS_UC_TABLE_JSON` for the proto examples).

### Invalid token

**Possible causes:** OAuth credentials expired or invalid, or an incorrect Unity
Catalog endpoint. **Solution:** regenerate your service principal credentials and
verify the workspace URL.

### JSON parsing / encoding errors

**Possible causes:** the JSON record doesn't match the table schema, invalid JSON
syntax, or a type mismatch (e.g. a string where a number is expected). For proto,
`ProtoSchema::encode_json` throws if a record can't be encoded. **Solution:**
verify your record structure matches the Databricks table schema exactly.

### `arrow_ingest` target missing

Apache Arrow C++ is not installed. Install it (e.g. `libarrow-dev` on Debian/
Ubuntu) and re-run CMake; the target appears automatically.

## Additional Resources

- [Main C++ SDK Documentation](../README.md)
- [Databricks Unity Catalog Documentation](https://docs.databricks.com/unity-catalog/index.html)
