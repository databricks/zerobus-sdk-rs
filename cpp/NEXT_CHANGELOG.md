# NEXT CHANGELOG

## Release v0.1.0

### Major Changes

- Initial release of the Zerobus C++ SDK: an RAII, exception-based C++17 wrapper
  over the Zerobus C FFI (`rust/ffi/zerobus.h`).

### New Features and Improvements

- `zerobus::Sdk` with a fluent `SdkBuilder` (endpoint, Unity Catalog URL, SDK
  identifier, application name, TLS toggle) plus a legacy `Sdk::create` path.
- Proto and JSON ingestion streams (`zerobus::Stream`): single-record, batch,
  and fire-and-forget (`*_nowait`) ingestion, `flush`, `wait_for_offset`,
  `get_unacked_records`, and graceful `close` with RAII cleanup.
- Custom authentication via `zerobus::HeadersProvider`.
- Arrow Flight streams (`zerobus::ArrowStream`, Beta): batch ingestion of Arrow
  IPC bytes, optional LZ4/ZSTD compression, and unacked-batch recovery.
- `zerobus::ProtoSchema`: build a protobuf descriptor and encode JSON records
  straight from Unity Catalog table metadata (no `.proto` / protoc required).
- CMake build that compiles the Rust FFI from local source by default, with
  `GTest`-based tests and runnable examples.

### Bug Fixes

### Documentation

- `README.md` quickstart, per-API documentation comments, and five examples
  (JSON single/batch, proto-from-UC-schema, custom headers, Arrow Flight).

### Internal Changes

### Behavior Changes

### Breaking Changes

### Deprecations

### API Changes
