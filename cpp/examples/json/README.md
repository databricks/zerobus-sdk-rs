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
- **`single.cpp`** — ingest records one at a time using `ingest_json_record()`,
  and recover unacknowledged records after a failure with `get_unacked_records()`
- **`batch.cpp`** — ingest multiple records at once using
  `ingest_json_records()`, with an async ack callback and a custom
  `HeadersProvider` (shown commented out)

Both open a JSON stream by setting `StreamOptions::record_type` to
`RecordType::Json` and leaving `TableProperties::descriptor_proto` empty.

## Single-Record Example

### Running the Example

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
   ./build/examples/json_single
   ```

**Expected output:**
```
Record 1 queued with offset ID: 0
Record 2 queued with offset ID: 1
Record 3 queued with offset ID: 2
All records acknowledged. Stream closed successfully.
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
props.table_name = table_name;         // empty descriptor => JSON stream

zerobus::StreamOptions options;
options.record_type = zerobus::RecordType::Json;

zerobus::Stream stream =
    sdk.create_stream(props, client_id, client_secret, options);
```

**Recovering unacknowledged records.** The SDK recovers transparently from
transient disconnects. If a stream fails *terminally*, `flush()`/`close()`
throws — and a failed `close()` keeps the handle alive so you can drain whatever
was never acknowledged with `get_unacked_records()` and re-ingest it on a fresh
stream. A flush timeout can leave the stream active; retrieval then throws, and
those records cannot be recovered until the stream has actually closed. After a
*successful* `close()` the handle is freed, so that call would throw instead —
recovery belongs on the failure path only.

```cpp
try {
  stream.flush();
  stream.close();
} catch (const zerobus::ZerobusException& e) {
  std::vector<zerobus::UnackedRecord> unacked;
  try {
    unacked = stream.get_unacked_records();
  } catch (const zerobus::ZerobusException& retrieval) {
    // Stream may still be active (for example a flush timeout).
    throw;
  }
  zerobus::Stream retry = open_stream(...);
  for (const auto& record : unacked) {
    retry.ingest_json_record(record.as_string());   // loop — no per-record wait
  }
  retry.flush();                                     // then flush once
  retry.close();
}
```

Each `UnackedRecord` exposes `is_json()`, the raw `data()` bytes, and
`as_string()`. Arrow streams have the mirror API,
`ArrowStream::get_unacked_batches()`.

## Batch Example

### Running the Example

1. Export the connection settings as shown for the single-record example above
   (see [Prerequisites](../README.md#prerequisites)).

2. Run:
   ```bash
   ./build/examples/json_batch
   ```

**Expected output:**
```
Batch of 3 records queued; batch offset ID: 0
Batch acknowledged at offset ID: 0
Stream closed successfully. Callback observed 1 logical submission acknowledgement(s).
```

### Code Highlights

`ingest_json_records()` hands a whole vector of records to the SDK in a single
FFI crossing and returns the single logical offset assigned to the batch.
Waiting on that one offset confirms the whole batch:

```cpp
const std::vector<std::string> batch = { record1, record2, record3 };

const std::int64_t batch_offset = stream.ingest_json_records(batch);
if (batch_offset >= 0) {
  stream.wait_for_offset(batch_offset);   // one wait confirms the batch
}
```

**Batch semantics:**
- **All-or-nothing** — the entire batch succeeds or fails as a unit.
- **Single acknowledgment** — one logical offset for the whole batch.
- **Empty batches** — a no-op; `ingest_json_records()` returns `-1`.

In a hot path you would queue **many** batches and `flush()` once, rather than
waiting after each batch.

**Async ack callback.** Register an `AckCallback` via
`StreamOptions::ack_callback` to observe acknowledgements on a background task
without ever blocking the ingest loop — for progress reporting or reacting to
failures. `AckCallback::from()` adapts two lambdas, so there is no subclass to
write:

```cpp
options.ack_callback = zerobus::AckCallback::from(
    [&acked](std::int64_t offset) noexcept { acked.fetch_add(1); },
    [](std::int64_t offset, const std::string& msg) noexcept { /* on error */ });
options.callback_wait_policy = zerobus::CallbackWaitPolicy::forever();
```

Both handlers are `noexcept` (an escaping exception calls `std::terminate`), run
serialized on another thread (synchronize shared state — the example uses
`std::atomic`), and must not call back into the `Stream`. `forever()` makes
`close()` wait for every in-flight callback, so none can touch captured state
after it goes out of scope. See
[`ack_callback.hpp`](../../include/zerobus/ack_callback.hpp) for the full
contract.

**Custom authentication.** Instead of OAuth client credentials, implement
`HeadersProvider` and pass it to the `create_stream()` overload that takes one
(shown commented out in `batch.cpp`). `get_headers()` returns the headers the
endpoint expects — at minimum an `authorization` bearer token and
`x-databricks-zerobus-table-name`. When a provider supplies auth,
`unity_catalog_url()` is optional on the builder. See
[`headers_provider.hpp`](../../include/zerobus/headers_provider.hpp).

## Adapting for Your Custom Table

JSON examples require no schema generation. To use your own table:

1. **Update the record shape.** Change the JSON your records produce (the
   `make_order_json` helper, or the raw JSON literals) to match your table's
   columns and types:
   ```cpp
   std::string record = R"({"your_field_1": "value", "your_field_2": 123})";
   ```
2. **Point the environment at your table**: set `ZEROBUS_TABLE_NAME`,
   `DATABRICKS_WORKSPACE_URL`, and `ZEROBUS_SERVER_ENDPOINT` to your values.

> **Tip.** Delta `TIMESTAMP` columns are int64 microseconds since the Unix epoch
> (UTC) — the examples fill them with `now_micros()`.
