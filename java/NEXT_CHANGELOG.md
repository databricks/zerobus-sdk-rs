# NEXT CHANGELOG

## Release v1.4.0

### Major Changes

### New Features and Improvements

- Added `HeadersProvider` support to `StreamBuilder` for custom authentication on JSON, Protocol Buffer, and Arrow streams, including stream recreation and credential invalidation callbacks.

### Bug Fixes

- Arrow builders now reject unsupported ACK callbacks instead of silently
  discarding them. Configuring `ackCallback` before calling `ArrowStreamBuilder.build()`
  throws `IllegalStateException`.

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

- Added `HeadersProvider` and `StreamBuilder.headersProvider()` for custom authentication.
