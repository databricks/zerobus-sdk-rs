# Advanced Examples

This directory contains examples demonstrating advanced features of the Zerobus
C++ SDK, beyond the basic JSON/proto/Arrow ingestion flows. They all build on
the cardinal ingestion pattern (loop, then `flush()` once — see the
[top-level examples README](../README.md#3-ingest-and-acknowledge)).

## Table of Contents

- [Overview](#overview)
- [Ack Callback](#ack-callback)
  - [Running the Example](#running-the-example)
  - [Code Highlights](#code-highlights)
- [Recovery](#recovery)
  - [Running the Example](#running-the-example-1)
  - [Code Highlights](#code-highlights-1)
- [Custom Headers Provider](#custom-headers-provider)
  - [Running the Example](#running-the-example-2)
  - [Code Highlights](#code-highlights-2)

## Overview

**Available examples:**
- **`ack_callback.cpp`** — track durability asynchronously with an
  `AckCallback`, without ever blocking the ingest loop.
- **`recovery.cpp`** — recover unacknowledged records from a failed stream via
  `Stream::get_unacked_records()` and re-ingest them on a fresh stream.
- **`headers_provider.cpp`** — supply custom authentication headers by
  implementing `HeadersProvider`, instead of the built-in OAuth flow.

All three use JSON streams for brevity; the same APIs apply to proto and (where
noted) Arrow streams.

## Ack Callback

An ack callback observes acknowledgements as they arrive on a background task —
`on_ack(offset)` per durable record (in monotonic offset order),
`on_error(offset, msg)` on terminal failure — so you can track durability or
react to failures without ever calling `wait_for_offset()` in the ingest loop.

### Running the Example

1. Export the connection settings — see
   [Prerequisites](../README.md#prerequisites) for what each one is:
   ```bash
   export ZEROBUS_SERVER_ENDPOINT="https://<your-shard-id>.zerobus.<region>.cloud.databricks.com"
   export DATABRICKS_WORKSPACE_URL="https://<your-workspace>.cloud.databricks.com"
   export ZEROBUS_TABLE_NAME="catalog.schema.orders"
   export DATABRICKS_CLIENT_ID="<your_databricks_client_id>"
   export DATABRICKS_CLIENT_SECRET="<your_databricks_client_secret>"
   ```

2. Run:
   ```bash
   ./build/examples/advanced_ack_callback
   ```

**Expected output:**
```
Queued 100 records; last offset ID: 99
Done. acked=100 failed=0 of 100 records.
```

### Code Highlights

Register the callback via `StreamOptions::ack_callback`. `AckCallback::from()`
adapts two lambdas, so there is no subclass to write:

```cpp
options.ack_callback = zerobus::AckCallback::from(
    [&acked](std::int64_t offset) noexcept { acked.fetch_add(1); },
    [&failed](std::int64_t offset, const std::string& msg) noexcept {
      failed.fetch_add(1);
    });
options.callback_wait_policy = zerobus::CallbackWaitPolicy::forever();
```

**Contract (see [`ack_callback.hpp`](../../include/zerobus/ack_callback.hpp)):**
- Both handlers are `noexcept` — an escaping exception calls `std::terminate`.
- Handlers run serialized on another thread; synchronize shared state (the
  example uses `std::atomic`) and keep them light.
- Never call back into the owning `Stream` from a handler.
- Whatever the handlers capture must outlive the callback. The example declares
  its counters before the `Stream` and uses `CallbackWaitPolicy::forever()`, so
  `close()` blocks until every in-flight callback finishes.

## Recovery

The SDK recovers transparently from transient disconnects. If a stream fails
*terminally* (recovery exhausted, or `close()` throws), the records that were
queued but never acknowledged are still retrievable, so you can re-ingest them.

### Running the Example

1. Export the connection settings as shown above.

2. Run:
   ```bash
   ./build/examples/advanced_recovery
   ```

**Expected output (happy path — no failure):**
```
All 50 records acknowledged.
```

If the stream fails, the example instead reports how many records it recovered
and re-ingests them on a fresh stream.

### Code Highlights

Guard the durability barrier; only a *failed* `flush()`/`close()` leaves unacked
records, and the failed `close()` keeps the handle alive so recovery works:

```cpp
try {
  stream.flush();
  stream.close();
} catch (const zerobus::ZerobusException& e) {
  std::vector<zerobus::UnackedRecord> unacked = stream.get_unacked_records();
  zerobus::Stream retry = open_stream(...);
  for (const auto& record : unacked) {
    retry.ingest_json_record(record.as_string());   // loop — no per-record wait
  }
  retry.flush();                                     // then flush once
  retry.close();
}
```

Each `UnackedRecord` exposes `is_json()`, the raw `data()` bytes, and
`as_string()` (for JSON payloads). Re-ingest JSON via `ingest_json_record()` and
proto bytes via `ingest_proto_record()`.

> Arrow streams have the mirror API: `ArrowStream::get_unacked_batches()` returns
> the unacknowledged batches as Arrow IPC bytes, ready to re-`ingest_batch()`.

## Custom Headers Provider

By default the SDK authenticates with OAuth client credentials. To use a
different scheme — a token broker, a rotating bearer token, a pre-minted token —
implement `HeadersProvider` and pass it to the `create_stream()` overload that
takes a provider (no client id/secret). When a provider supplies authentication,
`unity_catalog_url()` is optional on the builder.

### Running the Example

This example authenticates with a bearer token instead of OAuth, so it reads a
different set of variables:

```bash
export ZEROBUS_SERVER_ENDPOINT="https://<your-shard-id>.zerobus.<region>.cloud.databricks.com"
export ZEROBUS_TABLE_NAME="catalog.schema.orders"
export DATABRICKS_TOKEN="<your_bearer_token>"
```

Then run:
```bash
./build/examples/advanced_headers_provider
```

**Expected output:**
```
Ingested 20 records using a custom HeadersProvider.
```

### Code Highlights

Implement `get_headers()` to return the headers the endpoint expects — at minimum
an `authorization` bearer token and `x-databricks-zerobus-table-name` (the same
headers the built-in OAuth provider produces):

```cpp
class BearerTokenProvider : public zerobus::HeadersProvider {
 public:
  std::map<std::string, std::string> get_headers() override {
    return {
        {"authorization", "Bearer " + token_},
        {"x-databricks-zerobus-table-name", table_name_},
    };
  }
  // ...
};

auto provider = std::make_shared<BearerTokenProvider>(table_name, token);
zerobus::Stream stream = sdk.create_stream(props, provider, options);
```

**Contract (see [`headers_provider.hpp`](../../include/zerobus/headers_provider.hpp)):**
- `get_headers()` may be called from another thread; be thread-safe with respect
  to your own state (the example guards its token with a `std::mutex`).
- Throwing surfaces the message to the core as a headers-provider error and fails
  the pending operation; the exception does not cross the FFI boundary.
- The provider must outlive the `Stream`. The `Stream` holds a `shared_ptr`,
  which is necessary but not always sufficient — keep `get_headers()` well under
  the ~1s `close()` budget, or keep the provider alive past the `Stream`.
