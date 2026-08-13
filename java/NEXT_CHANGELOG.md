# NEXT CHANGELOG

## Release v1.3.0

### Major Changes

### New Features and Improvements

- Added native library support for Linux musl (Alpine) on x86_64 and aarch64. The libc flavor is detected automatically at runtime; override with `-Dzerobus.libc=musl|glibc`.

### Bug Fixes

- Fixed proto, JSON, and Arrow stream recovery losing unacknowledged data during `close()`. Closed
  native streams now remain available until Java has cached their recovery records or batches.

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
