# NEXT CHANGELOG

## Release v1.4.0

### Major Changes

### New Features and Improvements

- Added `HeadersProvider` support to `StreamBuilder` for custom authentication on JSON, Protocol Buffer, and Arrow streams, including stream recreation and credential invalidation callbacks.

### Bug Fixes

- Arrow builders now reject unsupported ACK callbacks instead of silently
  discarding them. Configuring `ackCallback` before calling `ArrowStreamBuilder.build()`
  throws `IllegalStateException`.
- Fixed proto, JSON, and Arrow stream recovery losing unacknowledged data during `close()`. Closed
  native streams now remain available until Java has cached their recovery records or batches, and
  recreation releases retained source streams after copying their recovery data. Failed recreation
  attempts also release their temporary native streams.

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

- Added `HeadersProvider` and `StreamBuilder.headersProvider()` for custom authentication.
