# NEXT CHANGELOG

## Release v2.9.0

### Major Changes

### New Features and Improvements

- JSON and protobuf streams now use a dedicated gRPC connection by default.
  Use `ZerobusSdk::builder().connection_per_stream(false)` to retain the prior
  shared HTTP/2 connection behavior. Arrow Flight streams are unchanged.

### Bug Fixes

### Documentation

### Internal Changes

- Updated multiplexed-stream failure handling to reject new ingestion and wake
  pending message waits after a lane fails, and to close lanes concurrently.

### Breaking Changes

### Deprecations

### API Changes
