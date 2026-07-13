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

### Internal Changes

- Expanded the hermetic unit-test suite to cover the previously untested API
  surface: `ZerobusException` (message + retryable flag), `UnackedRecord`,
  `version()`, `ProtoSchema` (UC-JSON round trip, error paths, move semantics),
  the `HeadersProvider` FFI trampoline (marshalling, empty/embedded-NUL/throwing
  guards, null `user_data`), and `Sdk` / `SdkBuilder` (offline build, move, and
  `create_stream` / `create_arrow_stream` argument validation). Still
  dependency-free and network-free; the suite also passes under AddressSanitizer.

### Breaking Changes

### Deprecations

### API Changes
