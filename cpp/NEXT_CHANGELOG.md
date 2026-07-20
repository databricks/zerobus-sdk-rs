# NEXT CHANGELOG

## Release v0.1.0

### New Features and Improvements

- Added an async ack callback: implement `AckCallback` (or use the
  `AckCallback::from(on_ack, on_error)` lambda adapter) and register it via
  `StreamOptions::ack_callback` to track durability without blocking in
  `wait_for_offset()` / `flush()`. The callback methods are `noexcept`.
  `StreamOptions::callback_wait_policy` (a `CallbackWaitPolicy` of
  `use_default()` / `duration(ms)` / `forever()`) controls how long `close()`
  drains the callback task.

### Bug Fixes

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
  the SDK via `ZEROBUS_BUILD_EXAMPLES` (the Arrow examples are skipped when
  Apache Arrow C++ is not installed). Includes a top-level `examples/README.md`
  and per-format guides.
- Added advanced examples under `examples/advanced/` (async ack callback via
  `StreamOptions::ack_callback`, recovery of unacknowledged records via
  `Stream::get_unacked_records()`, and custom authentication via a
  `HeadersProvider`), plus an Arrow IPC compression example
  (`examples/arrow/compression.cpp`).

### Internal Changes

- Expanded the hermetic unit-test suite to cover the previously untested API
  surface: `ZerobusException` (message + retryable flag), `UnackedRecord`,
  `version()`, `ProtoSchema` (UC-JSON round trip, error paths, move semantics),
  the `HeadersProvider` FFI trampoline (marshalling, empty/embedded-NUL/throwing
  guards, null `user_data`), and `Sdk` / `SdkBuilder` (offline build, move, and
  `create_stream` / `create_arrow_stream` argument validation). Still
  dependency-free and network-free; the suite also passes under AddressSanitizer.
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

### Breaking Changes

### Deprecations

### API Changes
