# NEXT CHANGELOG

## Release v0.2.0

### New Features and Improvements

- Added a borrowing overload of `Stream::ingest_proto_records()`, taking
  `const ProtoRecordView*` and a count. Callers whose encoded records already
  live elsewhere (an arena, a ring buffer, their own record type) no longer have
  to copy every payload into a `std::vector<std::vector<std::uint8_t>>` just to
  hand the batch over. `zerobus::ProtoRecordView` is a non-owning `{data, size}`
  pair whose bytes must stay valid until the call returns. Existing calls are
  unaffected.

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

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

- New: `zerobus::ProtoRecordView` (in `zerobus/record.hpp`) and
  `Stream::ingest_proto_records(const ProtoRecordView*, std::size_t)`. Additive
  only — no existing signature changed.
