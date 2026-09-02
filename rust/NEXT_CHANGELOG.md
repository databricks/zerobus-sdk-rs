# NEXT CHANGELOG

## Release v2.9.0

### Major Changes

### New Features and Improvements

- JSON and protobuf streams now use a dedicated gRPC connection by default.
  Use `ZerobusSdk::builder().connection_per_stream(false)` to retain the prior
  shared HTTP/2 connection behavior. Arrow Flight streams are unchanged.

### Bug Fixes

- Fixed gRPC stream teardown to preserve unacknowledged records, return the
  terminal stream error from `close()`, and reliably cancel and reap background
  tasks.

### Documentation

### Internal Changes

- Hardened multiplexed-stream failure propagation, acknowledgment wakeups,
  capacity routing, and bounded concurrent shutdown.

### Breaking Changes

### Deprecations

### API Changes
