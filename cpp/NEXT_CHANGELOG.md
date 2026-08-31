# NEXT CHANGELOG

## Release v0.4.0

### New Features and Improvements

- JSON and protobuf streams now use a dedicated gRPC connection by default.
  Set `Sdk::builder().connection_per_stream(false)` to share one HTTP/2
  connection across streams.

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
