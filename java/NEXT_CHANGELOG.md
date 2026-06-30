# NEXT CHANGELOG

## Release v1.3.0

### Major Changes

### New Features and Improvements

- Added native library support for Linux musl (Alpine) on x86_64 and aarch64. The libc flavor is detected automatically at runtime; override with `-Dzerobus.libc=musl|glibc`.

### Bug Fixes

### Documentation

- Reworked README and Javadoc to steer users toward the high-throughput ingestion pattern: ingest with `ingestRecordOffset()` in a loop without waiting, then `flush()` (or `waitForOffset()` on the last offset) once. Added a performance callout warning against waiting for an acknowledgment after every record, documented `AckCallback` as the non-blocking alternative, and rewrote the Quick Start example to use the offset-based API instead of the deprecated `ingestRecord().join()` loop.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
