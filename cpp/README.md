# Zerobus C++ SDK

A C++17 SDK for high-throughput ingestion into Databricks Zerobus. It is a thin,
RAII wrapper over the [Zerobus C FFI](../rust/ffi) (which wraps the Rust core),
so it shares the same gRPC streaming, OAuth, recovery, and ingestion engine as
every other Zerobus SDK.

- **RAII**: handles free themselves; wrapper objects are move-only.
- **Exceptions**: every failure throws `zerobus::ZerobusException`, which carries
  a message and an `is_retryable()` flag.
- **Proto and JSON** ingestion, single and batched, plus fire-and-forget.
- **Arrow Flight** streaming (Beta).
- **Dynamic protobuf**: build a descriptor and encode records straight from
  Unity Catalog table metadata — no `.proto` file or `protoc` required.

> Status: `0.1.0` — initial release.

## Requirements

- A C++17 compiler (GCC, Clang, or MSVC)
- CMake ≥ 3.16
- A Rust toolchain (only when building the FFI library from source, which is the
  default)

## Building

From `cpp/`:

```bash
make build        # configure + build SDK, tests, examples
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

With CMake `add_subdirectory` (or `FetchContent`):

```cmake
add_subdirectory(path/to/zerobus-sdk/cpp)
target_link_libraries(your_app PRIVATE zerobus::zerobus)
```

Then include the umbrella header:

```cpp
#include "zerobus/zerobus.hpp"
```

## Quickstart

### JSON ingestion

```cpp
#include "zerobus/zerobus.hpp"

zerobus::Sdk sdk = zerobus::Sdk::builder()
                       .endpoint("<id>.zerobus.<region>.cloud.databricks.com")
                       .unity_catalog_url("https://<workspace>.cloud.databricks.com")
                       .application_name("my-app")
                       .build();

zerobus::TableProperties table;
table.table_name = "main.analytics.events";   // empty descriptor => JSON stream

zerobus::StreamOptions options;
options.record_type = zerobus::RecordType::Json;

zerobus::Stream stream =
    sdk.create_stream(table, client_id, client_secret, options);

std::int64_t offset = stream.ingest_json_record(R"({"id": 1, "payload": "hi"})");
stream.flush();
stream.close();   // also happens automatically when `stream` goes out of scope
```

### Protobuf ingestion from a Unity Catalog schema

```cpp
// uc_json = body of GET /api/2.1/unity-catalog/tables/{name}
zerobus::ProtoSchema schema = zerobus::ProtoSchema::from_uc_json(uc_json);

zerobus::TableProperties table;
table.table_name = "main.analytics.events";
table.descriptor_proto = schema.descriptor_bytes();   // => proto stream

zerobus::Stream stream =
    sdk.create_stream(table, client_id, client_secret);  // record_type=Proto

std::vector<std::vector<std::uint8_t>> batch;
batch.push_back(schema.encode_json(R"({"id": 1, "payload": "hi"})"));
stream.ingest_proto_records(batch);
stream.flush();
```

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
  if (e.is_retryable()) { /* back off and retry */ }
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

Key `Stream` methods: `ingest_proto_record`, `ingest_json_record`,
`ingest_proto_records`, `ingest_json_records`, the `*_nowait` fire-and-forget
variants, `wait_for_offset`, `flush`, `get_unacked_records`, `close`.

## Examples

Runnable examples are under [`examples/`](examples):

| File | What it shows |
|------|---------------|
| `json_single.cpp` | Ingest one JSON record |
| `json_batch.cpp` | Batch JSON ingestion |
| `proto_uc_schema.cpp` | Proto ingestion using a UC-derived schema |
| `headers_provider.cpp` | Custom authentication |
| `arrow_stream.cpp` | Arrow Flight ingestion (Beta) |

They read connection settings from environment variables (`ZEROBUS_SERVER_ENDPOINT`,
`DATABRICKS_WORKSPACE_URL`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`,
`ZEROBUS_TABLE_NAME`); see each file's header for specifics.

## Thread safety

A `Stream`/`ArrowStream` is **not** safe for concurrent use — serialize access
externally. A single `Sdk` may create many streams. See [`CLAUDE.md`](CLAUDE.md)
for the full memory-ownership and threading contract.

## License

Apache 2.0. See [LICENSE](LICENSE).
