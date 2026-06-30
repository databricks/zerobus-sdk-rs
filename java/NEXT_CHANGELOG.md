# NEXT CHANGELOG

## Release v1.3.0

### Major Changes

### New Features and Improvements

- Added native library support for Linux musl (Alpine) on x86_64 and aarch64. The libc flavor is detected automatically at runtime; override with `-Dzerobus.libc=musl|glibc`.
- Added `ZerobusSdk.streamBuilder()`, a fluent builder for creating streams that mirrors the Rust SDK's `stream_builder()`. It supports JSON, Protocol Buffer, and Arrow Flight streams through a single chainable API and is now the recommended way to create streams. See `StreamBuilder`.

### Bug Fixes

- `StreamBuilder` now rejects null or blank table names and OAuth credentials before opening a
  stream.

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

- Deprecated `ZerobusSdk.createJsonStream`, `ZerobusSdk.createProtoStream`, and
  `ZerobusSdk.createArrowStream` (all overloads) in favor of `ZerobusSdk.streamBuilder()`. The
  deprecated methods continue to work unchanged and are scheduled for removal in the next major
  release.

### API Changes

- Added `StreamBuilder` and its typed sub-builders (`StreamBuilder.JsonStreamBuilder`,
  `StreamBuilder.ProtoStreamBuilder`, `StreamBuilder.ArrowStreamBuilder`), returned by
  `ZerobusSdk.streamBuilder()`.
