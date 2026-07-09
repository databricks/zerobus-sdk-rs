# Zerobus C++ SDK

A C++17 SDK for high-throughput ingestion into Databricks Zerobus. It is a thin,
RAII wrapper over the [Zerobus C FFI](../rust/ffi) (which wraps the Rust core),
so it shares the same gRPC streaming, OAuth, recovery, and ingestion engine as
every other Zerobus SDK.

- **RAII** — handles free themselves; wrapper objects are move-only.
- **Exceptions** — every failure throws `zerobus::ZerobusException`, which
  carries a message and an `is_retryable()` flag.
- **Proto and JSON** ingestion, single and batched.
- **Dynamic protobuf** — build a descriptor and encode records straight from
  Unity Catalog table metadata, with no `.proto` file or `protoc` required.
- **Arrow Flight** ingestion (Beta) — stream Arrow record batches with optional
  LZ4/ZSTD compression.

> Status: `0.1.0` — initial development. The API may change before `1.0.0`.

## Requirements

- A C++17 compiler (GCC, Clang, or MSVC)
- CMake ≥ 3.16
- A Rust toolchain (only when building the FFI library from source, which is the
  default)

## Building

From `cpp/`:

```bash
make build        # configure + build the SDK, tests, and examples
make test         # build + run the test suite
```

Or drive CMake directly:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
```

By default CMake builds the FFI static library from local Rust source
(`cargo build --release` in `rust/ffi`). To link a prebuilt library instead:

```bash
cmake -S . -B build \
  -DZEROBUS_FFI_LIBRARY=/path/to/libzerobus_ffi.a \
  -DZEROBUS_FFI_HEADER_DIR=/path/to/dir/containing/zerobus.h
```

## Using the SDK in your project

### With CMake `add_subdirectory` (or `FetchContent`)

```cmake
add_subdirectory(path/to/zerobus-sdk/cpp)
target_link_libraries(your_app PRIVATE zerobus::zerobus)
```

### With an installed package (`find_package`)

After `cmake --install`, the SDK ships a CMake package config so a separate
project can consume it without knowing its internals. The bundled Rust C FFI
archive is installed alongside and wired up automatically:

```cmake
find_package(zerobus REQUIRED)
target_link_libraries(your_app PRIVATE zerobus::zerobus)
```

However you link it, include the umbrella header:

```cpp
#include "zerobus/zerobus.hpp"
```

## Choosing an ingestion format

A record-oriented `Stream` accepts two wire formats (proto and JSON), and there
are three ways to get your data onto one. They differ only in how the record
schema is handled — the streaming, auth, and recovery machinery is identical.
(For columnar data there is also a separate [Arrow Flight
stream](#arrow-flight-ingestion-beta), Beta.)

| Path | Format | Schema source | Extra build deps | Best for |
|------|--------|---------------|------------------|----------|
| **JSON** | JSON | none — server maps fields to columns by name | none | getting started, flexible schemas |
| **Dynamic proto** | protobuf | fetched from Unity Catalog at runtime (`ProtoSchema`) | none | production proto without a `.proto` file |
| **Static proto** | protobuf | a checked-in `.proto` compiled by `protoc` | `protoc` + libprotobuf | offline builds, compile-time typing |

**If in doubt, start with JSON or dynamic proto** — neither needs a protobuf
toolchain in your build. Static proto trades that convenience for compile-time
type safety and no runtime schema fetch, at the cost of a hand-maintained
`.proto` that must be kept in sync with the table.

## The cardinal performance rule

Ingestion is asynchronous and pipelined. `ingest_*` returns as soon as the
record is **queued** — sending and acknowledgement happen on background tasks.
The returned offset is a handle you can wait on *later*, not a signal to wait
*now*.

**Never wait for an acknowledgement after every ingest.** Calling
`wait_for_offset()` (or `flush()`) inside the ingest loop forces a full server
round-trip per record and collapses throughput. Instead, loop and flush once at
the end (or periodically for a continuous stream):

```cpp
for (const auto& record : records) {
  stream.ingest_json_record(record);   // queue only — do NOT wait here
}
stream.flush();                        // wait once for all pending acks
```

For continuous/unbounded streams, call `flush()` every N records rather than per
record. Prefer the batch APIs (`ingest_*_records`) in hot paths — each FFI
crossing has a fixed cost that batching amortizes.

## Quickstart

### JSON ingestion

```cpp
#include "zerobus/zerobus.hpp"

zerobus::Sdk sdk = zerobus::Sdk::builder()
                       .endpoint("https://<id>.zerobus.<region>.cloud.databricks.com")
                       .unity_catalog_url("https://<workspace>.cloud.databricks.com")
                       .application_name("my-app")
                       .build();

zerobus::TableProperties table;
table.table_name = "main.analytics.events";   // empty descriptor => JSON stream

zerobus::StreamOptions options;
options.record_type = zerobus::RecordType::Json;

zerobus::Stream stream =
    sdk.create_stream(table, client_id, client_secret, options);

std::vector<std::string> batch = {
    R"({"id": 1, "payload": "hi"})",
    R"({"id": 2, "payload": "there"})",
};
stream.ingest_json_records(batch);   // queue the batch — no per-record wait
stream.flush();                      // wait once for all acks
stream.close();                      // flush + close at a controlled point
```

### Protobuf ingestion from a Unity Catalog schema (dynamic)

`ProtoSchema::from_uc_json()` builds both the descriptor and a JSON→proto
encoder from Unity Catalog table metadata — no `.proto` file or `protoc`
required.

```cpp
// uc_json = body of GET /api/2.1/unity-catalog/tables/{full_name}
zerobus::ProtoSchema schema = zerobus::ProtoSchema::from_uc_json(uc_json);

zerobus::TableProperties table;
table.table_name = "main.analytics.events";
table.descriptor_proto = schema.descriptor_bytes();   // => proto stream

zerobus::StreamOptions options;                        // record_type defaults to Proto

zerobus::Stream stream =
    sdk.create_stream(table, client_id, client_secret, options);

std::vector<std::vector<std::uint8_t>> batch;
batch.push_back(schema.encode_json(R"({"id": 1, "payload": "hi"})"));
stream.ingest_proto_records(batch);
stream.flush();
```

### Arrow Flight ingestion (Beta)

Stream Arrow record batches instead of proto/JSON records. Create the stream
with an Arrow IPC stream that encodes only the schema, then ingest each batch as
a self-contained Arrow IPC stream (schema message + one record batch message).

```cpp
// schema_ipc = an Arrow IPC stream containing just the schema message.
zerobus::ArrowStream stream = sdk.create_arrow_stream(
    "main.analytics.events", schema_ipc, client_id, client_secret);

for (const std::vector<std::uint8_t>& batch_ipc : batches) {
  stream.ingest_batch(batch_ipc);   // queue only — no per-batch wait
}
stream.flush();                     // wait once for all pending acks
stream.close();
```

Optional LZ4/ZSTD compression is set via `ArrowStreamOptions::ipc_compression`.
Arrow Flight is **Beta** — the API may change.

### Custom authentication

```cpp
class MyProvider : public zerobus::HeadersProvider {
 public:
  std::map<std::string, std::string> get_headers() override {
    return {{"Authorization", "Bearer " + current_token()}};
  }
};

auto provider = std::make_shared<MyProvider>();
zerobus::Stream stream = sdk.create_stream(table, provider, options);
```

### Error handling

```cpp
try {
  stream.ingest_json_record(record);
} catch (const zerobus::ZerobusException& e) {
  if (e.is_retryable()) { /* transient: back off and retry */ }
  else { /* permanent failure: log and drop */ }
}
```

## API overview

| Type | Purpose |
|------|---------|
| `zerobus::Sdk` / `zerobus::SdkBuilder` | Connection factory; creates streams |
| `zerobus::Stream` | Proto/JSON ingestion stream |
| `zerobus::ArrowStream` | Arrow Flight ingestion stream (Beta) |
| `zerobus::ProtoSchema` | UC table metadata → descriptor + JSON encoder |
| `zerobus::HeadersProvider` | Custom authentication headers |
| `zerobus::StreamOptions` / `zerobus::ArrowStreamOptions` | Stream configuration |
| `zerobus::ZerobusException` | Thrown on any failure; `is_retryable()` |
| `zerobus::UnackedRecord` | An unacknowledged record recovered from a failed stream |

Key `Stream` methods: `ingest_proto_record`, `ingest_json_record`,
`ingest_proto_records`, `ingest_json_records`, `wait_for_offset`, `flush`,
`get_unacked_records`, `close`.

## Examples

Runnable examples will live under `examples/`, covering the three ingestion
paths (JSON, dynamic proto, and static proto). Until they land, the
[Quickstart](#quickstart) above demonstrates the JSON and dynamic-proto paths
end to end.

## A note on credentials

Two different credentials appear in these flows; conflating them causes most
setup confusion:

- **OAuth client id/secret** — a service principal's credentials. The SDK uses
  them to mint short-lived, table-scoped Unity Catalog tokens for the stream. It
  is the only credential the SDK itself needs.
- **A workspace token (PAT)** — a user token for the Unity Catalog REST API. It
  is *not* required by the SDK. It only appears in the dynamic-proto example when
  the table metadata is fetched with a raw `curl`. That same fetch can instead be
  authorized by doing the OAuth client-credentials exchange first (which is what
  the schema-generation tooling does), needing no PAT.

## Generating a `.proto` for the static path

You do not hand-write the `.proto` for a real table — the monorepo ships a
generator that emits it (plus a binary descriptor) from the live Unity Catalog
schema, authenticated with the OAuth client credentials:

```bash
cd ../rust/tools/generate_files
cargo run -- \
  --uc-endpoint "https://<workspace>.cloud.databricks.com" \
  --client-id "$ZEROBUS_CLIENT_ID" \
  --client-secret "$ZEROBUS_CLIENT_SECRET" \
  --table "catalog.schema.table" \
  --output-dir ./out
```

Compile the generated `catalog.schema.table.proto` for C++ with
`protoc --cpp_out=...` and link libprotobuf, then build the descriptor from the
generated message class and pass it as `TableProperties::descriptor_proto`. Keep
the `.proto` in sync with the table — a mismatch places data in the wrong
columns silently.

## Resource management

Wrapper objects are move-only and free themselves (RAII). For streams, **prefer
calling `close()` explicitly** rather than relying on the destructor:

- `close()` flushes pending records and throws on failure; the destructor
  swallows any error.
- Closing flushes synchronously and can block up to the stream's
  `flush_timeout_ms` (default 5 minutes) if the server is unresponsive. Letting
  a `Stream` fall out of scope drags that blocking close into the destructor at
  an unpredictable point, so close at a controlled point in your code.

## Thread safety

A `Stream` or `ArrowStream` is **not** safe for concurrent use — serialize
access externally (the same contract as Java and the Rust core). A single `Sdk`
may create many streams. See [`CLAUDE.md`](CLAUDE.md) for the full
memory-ownership and threading contract.

## License

Apache 2.0. See the [root LICENSE](../LICENSE).
