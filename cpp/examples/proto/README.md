# Protocol Buffers Examples

This directory contains examples demonstrating Protocol Buffers-based data
ingestion into Databricks Delta tables using the Zerobus C++ SDK.

## Table of Contents

- [Overview](#overview)
- [How the Schema Is Built (Dynamic Proto)](#how-the-schema-is-built-dynamic-proto)
- [Single-Record Example](#single-record-example)
  - [Running the Example](#running-the-example)
  - [Code Highlights](#code-highlights)
- [Batch Example](#batch-example)
  - [Running the Example](#running-the-example-1)
  - [Code Highlights](#code-highlights-1)
- [Adapting for Your Custom Table](#adapting-for-your-custom-table)

## Overview

Protocol Buffers provide efficient binary encoding. **No `.proto` file and no
`protoc` are required to run these examples** — the descriptor is built at
runtime from Unity Catalog table metadata via `ProtoSchema::from_uc_json()`,
which also encodes each record's JSON into protobuf bytes.

**Features:**
- Efficient binary encoding
- No protobuf toolchain in your build
- Schema always matches the live table (fetched from Unity Catalog)

**Available examples:**
- **`single.cpp`** — ingest records one at a time using `ingest_proto_record()`
- **`batch.cpp`** — ingest multiple records at once using `ingest_proto_records()`

Ack callbacks, a custom `HeadersProvider`, and unacked-record recovery apply
identically to proto streams — see the JSON examples
([`json/single.cpp`](../json/single.cpp),
[`json/batch.cpp`](../json/batch.cpp)) for worked demonstrations.

The `orders.proto` file in this directory is a **human-readable reference** for
the table's shape (and a starting point if you prefer the static-proto path). The
examples do not compile or use it.

## How the Schema Is Built (Dynamic Proto)

`ProtoSchema::from_uc_json()` takes the JSON body of
`GET /api/2.1/unity-catalog/tables/{full_name}` and produces:

1. **A descriptor** (`descriptor_bytes()`) — passed as
   `TableProperties::descriptor_proto` when creating the proto stream.
2. **A JSON→proto encoder** (`encode_json()`) — turns each record's JSON string
   into the protobuf bytes the stream ingests.

Fetch the metadata JSON once and pass it via the environment:

```bash
export ZEROBUS_UC_TABLE_JSON="$(curl -s \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  "$DATABRICKS_WORKSPACE_URL/api/2.1/unity-catalog/tables/$ZEROBUS_TABLE_NAME")"
```

Per-column value shaping rules (DATE/TIMESTAMP as integers, BINARY as base64,
etc.) are documented in the FFI README / `zerobus.h`.

## Single-Record Example

### Running the Example

1. Export the connection settings and the table metadata — see
   [Prerequisites](../README.md#prerequisites) for what each one is:
   ```bash
   export ZEROBUS_SERVER_ENDPOINT="https://<your-shard-id>.zerobus.<region>.cloud.databricks.com"
   export DATABRICKS_WORKSPACE_URL="https://<your-workspace>.cloud.databricks.com"
   export ZEROBUS_TABLE_NAME="catalog.schema.orders"
   export DATABRICKS_CLIENT_ID="<your_databricks_client_id>"
   export DATABRICKS_CLIENT_SECRET="<your_databricks_client_secret>"
   export ZEROBUS_UC_TABLE_JSON="$(curl -s \
     -H "Authorization: Bearer $DATABRICKS_TOKEN" \
     "$DATABRICKS_WORKSPACE_URL/api/2.1/unity-catalog/tables/$ZEROBUS_TABLE_NAME")"
   ```

2. Run:
   ```bash
   ./build/examples/proto_single
   ```

**Expected output:**
```
Record 1 queued with offset ID: 0
Record 2 queued with offset ID: 1
Record 3 queued with offset ID: 2
All records acknowledged.
Stream closed successfully.
```

### Code Highlights

Each iteration encodes the record's JSON to protobuf bytes and queues them; the
example ingests all of them and calls `flush()` ONCE at the end rather than
waiting per record:

```cpp
zerobus::ProtoSchema schema = zerobus::ProtoSchema::from_uc_json(uc_table_json);

std::vector<std::uint8_t> encoded = schema.encode_json(record_json);
std::int64_t offset = stream.ingest_proto_record(encoded);  // queue only
// ... encode + ingest more records ...

stream.flush();   // the single wait point — confirm all queued records at once
```

**Building a Protocol Buffers stream:**
```cpp
zerobus::TableProperties props;
props.table_name = table_name;
props.descriptor_proto = schema.descriptor_bytes();

zerobus::StreamOptions options;
options.record_type = zerobus::RecordType::Proto;

zerobus::Stream stream =
    sdk.create_stream(props, client_id, client_secret, options);
```

## Batch Example

### Running the Example

1. Export the connection settings and the table metadata as shown for the
   single-record example above (see [Prerequisites](../README.md#prerequisites)).

2. Run:
   ```bash
   ./build/examples/proto_batch
   ```

**Expected output:**
```
Batch of 3 records queued; last offset ID: 2
Batch acknowledged through offset ID: 2
Stream closed successfully.
```

### Code Highlights

Encode each record, collect the bytes into a batch, and hand the whole batch to
`ingest_proto_records()` in one call. It returns the offset of the **last**
record; waiting on that one offset confirms the batch:

```cpp
const std::vector<std::vector<std::uint8_t>> batch = {
    schema.encode_json(record1),
    schema.encode_json(record2),
    schema.encode_json(record3),
};

const std::int64_t last_offset = stream.ingest_proto_records(batch);
if (last_offset >= 0) {
  stream.wait_for_offset(last_offset);   // one wait confirms the batch
}
```

**Batch semantics:**
- **All-or-nothing** — the entire batch succeeds or fails as a unit.
- **Single acknowledgment** — one offset (the last record's) for the whole batch.
- **Empty batches** — a no-op; `ingest_proto_records()` returns `-1`.

In a hot path you would queue **many** batches and `flush()` once, rather than
waiting after each batch.

## Adapting for Your Custom Table

Because the schema is fetched from Unity Catalog at runtime, adapting to your own
table needs no code changes to the encoding:

1. **Point `ZEROBUS_UC_TABLE_JSON` at your table** — fetch the metadata for your
   own `catalog.schema.table` (see [How the Schema Is
   Built](#how-the-schema-is-built-dynamic-proto)).
2. **Update the record shape.** Change the JSON your records produce (the
   `make_order_json` helper) so its fields match your table's columns; unknown
   keys are ignored by `encode_json()`.
3. **Point the environment at your table**: set `ZEROBUS_TABLE_NAME`,
   `DATABRICKS_WORKSPACE_URL`, and `ZEROBUS_SERVER_ENDPOINT` to your values.

> **Note on static proto.** These examples use the dynamic path only, which needs
> no protobuf toolchain. If you instead want compile-time typing and no runtime
> Unity Catalog fetch, compile `orders.proto` with `protoc`, build a
> `DescriptorProto` from the generated C++ class, and pass its serialized bytes
> as `TableProperties::descriptor_proto`. That trades drift-safety (the `.proto`
> must be kept in sync with the table by hand) for those benefits.
