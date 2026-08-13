# NEXT CHANGELOG

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

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
