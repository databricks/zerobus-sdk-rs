# Version changelog

All notable changes to the Zerobus C++ SDK are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this
project adheres to [Semantic Versioning](https://semver.org/).

## Release v0.2.0

### New Features and Improvements

### Bug Fixes

- Fixed a use-after-free in which a custom `HeadersProvider` could be destroyed
  while the Rust core was still inside a `get_headers()` call into it during
  connection recovery. Provider ownership is now handed to the FFI as a
  heap-allocated `shared_ptr` released by a destroy callback
  (`detail::zerobus_cpp_headers_free`) only after any in-flight `get_headers()`
  has returned; the `Stream` / `ArrowStream` no longer keeps its own provider
  `shared_ptr`. Public API is unchanged — `create_stream` /
  `create_arrow_stream` still take a `std::shared_ptr<HeadersProvider>` — and you
  no longer need to keep your own reference alive past `create_stream`.

### Documentation

- Corrected custom-header examples and clarified that acknowledgment callbacks
  run once per logical ingest submission rather than once per record in a batch.
- Recovery after a flush timeout now treats unacked retrieval failure as an
  active stream rather than assuming the stream is terminal.
- Arrow Flight schema validation is documented at stream creation (the schema IPC
  bytes passed to `create_arrow_stream`), not on the first ingested batch.
- Built on Rust SDK 2.7.1. Wrapper-facing notes for that core are in
  `rust/CHANGELOG.md` and https://github.com/databricks/zerobus-sdk/releases/tag/rust/v2.7.1.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

## Release v0.1.1

### Bug Fixes

- Correct the batch-ingest offset docs. `ingest_json_records()` /
  `ingest_proto_records()` return the single logical offset assigned to the
  whole batch, not "the offset of the last record" — a 3-record batch on a fresh
  stream returns offset 0, not 2. Fixed the API docstrings, example comments,
  and expected-output samples.

### Documentation

- Fix release-bundle documentation. `cpp/README.md` was written for a source
  checkout; it now covers both audiences with a "from a release bundle" build
  path (prebuilt-FFI CMake flags, no Rust toolchain), notes that `make`/`ctest`
  and the test suite are source-checkout only, and rewrites links that pointed
  outside the bundle (prerequisites, FFI, contributing, license) so they resolve
  from a bundle too.
- Complete the proto examples' Unity Catalog metadata fetch: explain why the
  dynamic-proto path needs the table schema, acquire an OAuth token from the
  service-principal credentials first, and use `curl --fail` so an auth or
  permission error surfaces instead of storing an error body.
- Fix the credential variable names in the `generate_files` snippet
  (`DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET`, matching the rest of the
  docs).

## Release v0.1.0

Initial release of the Zerobus C++ SDK — a RAII C++17 wrapper over the Zerobus C
FFI for ingesting data into Databricks Delta tables.

### New Features and Improvements

- Ingest into Databricks Delta tables over a `Stream` (proto and JSON record
  formats) or an `ArrowStream` (Arrow Flight, Beta), with single-record and
  batch APIs. Streams are RAII, move-only handles over the Rust core; errors
  surface as `zerobus::ZerobusException`. Proto schemas are built at runtime from
  Unity Catalog table metadata via `ProtoSchema::from_uc_json()` (no `protoc`
  required).
- Added an async ack callback: implement `AckCallback` (or use the
  `AckCallback::from(on_ack, on_error)` lambda adapter) and register it via
  `StreamOptions::ack_callback` to track durability without blocking in
  `wait_for_offset()` / `flush()`. The callback methods are `noexcept`.
  `StreamOptions::callback_wait_policy` (a `CallbackWaitPolicy` of
  `use_default()` / `duration(ms)` / `forever()`) controls how long `close()`
  drains the callback task.
- Recover unacknowledged records after a failure via
  `Stream::get_unacked_records()` / `ArrowStream::get_unacked_batches()`, and
  supply per-stream request headers with a custom `HeadersProvider`.

### Documentation

- Added the C++ SDK `README.md` (build, install, quickstart for JSON / proto /
  Arrow Flight ingestion, ingestion-format guidance, credential model, API
  overview, `StreamOptions` / `ArrowStreamOptions` configuration tables, and an
  HTTP-proxy note) and `CLAUDE.md` (contributor guide covering the FFI boundary,
  RAII/memory ownership, thread-safety, and release process). Added
  `CONTRIBUTING.md` with C++-specific development setup and workflow. Added C++
  rows to the root `README.md` and `CLAUDE.md`, and reconciled the root
  Arrow-Flight and `examples/arrow/` notes with the C++ SDK's `0.1.0` state.
- Documented running the tests in `README.md`: the sanitizer runs
  (`make test SANITIZE=address` / `thread`) and the env-var-gated
  `integration_test` (which variables it needs and that it skips without them).
- Added runnable examples under `examples/` covering all three record formats —
  JSON and protobuf (dynamic schema built at runtime from Unity Catalog metadata
  via `ProtoSchema::from_uc_json`, no `protoc` required), each with a
  single-record and a batch variant, plus Arrow Flight (Beta). Every example
  reads its connection settings from the environment (`ZEROBUS_SERVER_ENDPOINT`,
  `DATABRICKS_WORKSPACE_URL`, `ZEROBUS_TABLE_NAME`, `DATABRICKS_CLIENT_ID`,
  `DATABRICKS_CLIENT_SECRET`). They build with
  the SDK via `ZEROBUS_BUILD_EXAMPLES` (the Arrow example is skipped when Apache
  Arrow C++ is not installed). Includes a top-level `examples/README.md` and
  per-format guides.
- The examples also demonstrate advanced features inline: an async ack callback
  (`StreamOptions::ack_callback`) and a custom `HeadersProvider` in
  `examples/json/batch.cpp`, recovery of unacknowledged records
  (`Stream::get_unacked_records()`) in `examples/json/single.cpp`, and Arrow IPC
  compression in `examples/arrow/arrow_ingest.cpp`.

### Internal Changes

- Hermetic unit-test suite covering the API surface: `ZerobusException` (message
  + retryable flag), `UnackedRecord`, `version()`, `ProtoSchema` (UC-JSON round
  trip, error paths, move semantics), the `HeadersProvider` FFI trampoline
  (marshalling, empty/embedded-NUL/throwing guards, null `user_data`), and
  `Sdk` / `SdkBuilder` (offline build, move, and `create_stream` /
  `create_arrow_stream` argument validation). Dependency-free and network-free;
  the suite also passes under AddressSanitizer.
- Added an env-var-gated live integration test (`integration_test`) covering the
  create-stream -> ingest -> flush -> close path against a real endpoint,
  mirroring the Java/TypeScript integration suites. It skips (passes) unless
  `ZEROBUS_SERVER_ENDPOINT`, `DATABRICKS_WORKSPACE_URL`, `ZEROBUS_TABLE_NAME`,
  `DATABRICKS_CLIENT_ID`, and `DATABRICKS_CLIENT_SECRET` are set, so `make test`
  and CI stay hermetic.
- Added a ThreadSanitizer CI job (`make test SANITIZE=thread`) and a
  `concurrency_test` that exercises the documented "concurrent readers on a
  shared `ProtoSchema`" contract under many threads, catching data races the
  AddressSanitizer job cannot.
