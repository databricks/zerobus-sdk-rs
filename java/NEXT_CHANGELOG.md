# NEXT CHANGELOG

## Release v1.3.0

### Major Changes

### New Features and Improvements

- Added native library support for Linux musl (Alpine) on x86_64 and aarch64. The libc flavor is detected automatically at runtime; override with `-Dzerobus.libc=musl|glibc`.
- Added `ZerobusSdk.streamBuilder()`, a fluent builder for creating streams that mirrors the Rust SDK's `stream_builder()`. It supports JSON, Protocol Buffer, and Arrow Flight streams through a single chainable API and is now the recommended way to create streams. See `StreamBuilder`.

### Bug Fixes

- Fixed the default `recoveryRetries`, which was `3` instead of the `4` used by the Rust core and every other SDK (Go, TypeScript, C++). A stream left with the default now makes 4 recovery attempts on transient failures instead of 3, matching the documented cross-SDK behavior. Callers that set `recoveryRetries` explicitly are unaffected. (#438)

### Documentation

- Reworked README and Javadoc to steer users toward the high-throughput ingestion pattern: ingest with `ingestRecordOffset()` in a loop without waiting, then `flush()` (or `waitForOffset()` on the last offset) once. Added a performance callout warning against waiting for an acknowledgment after every record, documented `AckCallback` as the non-blocking alternative, and rewrote the Quick Start example to use the offset-based API instead of the deprecated `ingestRecord().join()` loop.

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
