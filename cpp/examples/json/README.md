# JSON Examples

This directory contains examples demonstrating JSON-based data ingestion into
Databricks Delta tables using the Zerobus C++ SDK.

## Table of Contents

- [Overview](#overview)
- [Single-Record Example](#single-record-example)
  - [Running the Example](#running-the-example)
  - [Code Highlights](#code-highlights)
- [Batch Example](#batch-example)
  - [Running the Example](#running-the-example-1)
  - [Code Highlights](#code-highlights-1)
- [Adapting for Your Custom Table](#adapting-for-your-custom-table)

## Overview

JSON examples are recommended for getting started — they're simpler and don't
require any schema handling. The server maps each record's JSON fields onto the
table's columns by name.

**Features:**
- No schema generation required
- Easy to understand and modify
- Great for quick prototyping

**Available examples:**
- **`single.cpp`** — ingest records one at a time using `ingest_json_record()`
- **`batch.cpp`** — ingest multiple records at once using `ingest_json_records()`

Both open a JSON stream by setting `StreamOptions::record_type` to
`RecordType::Json` and leaving `TableProperties::descriptor_proto` empty.

## Single-Record Example

### Running the Example

1. Edit the placeholder constants at the top of `single.cpp` (table, endpoint,
   workspace URL) — see [Prerequisites](../README.md#prerequisites).

2. Export the OAuth secrets and run:
   ```bash
   export ZEROBUS_CLIENT_ID="<your_databricks_client_id>"
   export ZEROBUS_CLIENT_SECRET="<your_databricks_client_secret>"
   ./build/examples/json_single
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

Each `ingest_json_record()` returns as soon as the record is queued; the example
ingests all of them and calls `flush()` ONCE at the end rather than waiting per
record (waiting per record forces a server round-trip each time):

```cpp
std::int64_t offset = stream.ingest_json_record(record1);  // queue only
offset = stream.ingest_json_record(record2);               // queue only
offset = stream.ingest_json_record(record3);               // queue only

stream.flush();   // the single wait point — confirm all queued records at once
```

**Building a JSON stream:**
```cpp
zerobus::TableProperties props;
props.table_name = kTableName;         // empty descriptor => JSON stream

zerobus::StreamOptions options;
options.record_type = zerobus::RecordType::Json;

zerobus::Stream stream =
    sdk.create_stream(props, client_id, client_secret, options);
```

## Batch Example

### Running the Example

1. Edit the placeholder constants at the top of `batch.cpp` — see
   [Prerequisites](../README.md#prerequisites).

2. Export the OAuth secrets and run:
   ```bash
   export ZEROBUS_CLIENT_ID="<your_databricks_client_id>"
   export ZEROBUS_CLIENT_SECRET="<your_databricks_client_secret>"
   ./build/examples/json_batch
   ```

**Expected output:**
```
Batch of 3 records queued; last offset ID: 2
Batch acknowledged through offset ID: 2
Stream closed successfully.
```

### Code Highlights

`ingest_json_records()` hands a whole vector of records to the SDK in a single
FFI crossing and returns the offset of the **last** record. Waiting on that one
offset confirms the whole batch, because acks are monotonic:

```cpp
const std::vector<std::string> batch = { record1, record2, record3 };

const std::int64_t last_offset = stream.ingest_json_records(batch);
if (last_offset >= 0) {
  stream.wait_for_offset(last_offset);   // one wait confirms the batch
}
```

**Batch semantics:**
- **All-or-nothing** — the entire batch succeeds or fails as a unit.
- **Single acknowledgment** — one offset (the last record's) for the whole batch.
- **Empty batches** — a no-op; `ingest_json_records()` returns `-1`.

In a hot path you would queue **many** batches and `flush()` once, rather than
waiting after each batch.

## Adapting for Your Custom Table

JSON examples require no schema generation. To use your own table:

1. **Update the record shape.** Change the JSON your records produce (the
   `make_order_json` helper, or the raw JSON literals) to match your table's
   columns and types:
   ```cpp
   std::string record = R"({"your_field_1": "value", "your_field_2": 123})";
   ```
2. **Update the constants** at the top of the source file: `kTableName`,
   `kWorkspaceUrl`, and `kServerEndpoint`.

> **Tip.** Delta `TIMESTAMP` columns are int64 microseconds since the Unix epoch
> (UTC) — the examples fill them with `now_micros()`.
