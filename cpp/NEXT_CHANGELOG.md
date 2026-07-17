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

- Added the C++ SDK `README.md` (build, install, quickstart for JSON / proto /
  Arrow Flight ingestion, ingestion-format guidance, credential model, API
  overview, `StreamOptions` / `ArrowStreamOptions` configuration tables, and an
  HTTP-proxy note) and `CLAUDE.md` (contributor guide covering the FFI boundary,
  RAII/memory ownership, thread-safety, and release process). Added
  `CONTRIBUTING.md` with C++-specific development setup and workflow. Added C++
  rows to the root `README.md` and `CLAUDE.md`, and reconciled the root
  Arrow-Flight and `examples/arrow/` notes with the C++ SDK's `0.1.0` state.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
