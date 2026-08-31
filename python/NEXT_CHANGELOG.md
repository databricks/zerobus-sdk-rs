# NEXT CHANGELOG

## Release v1.9.0

### Major Changes

### New Features and Improvements

- JSON and protobuf streams now use a dedicated gRPC connection by default.
  Pass `connection_per_stream=False` to the synchronous or asynchronous
  `ZerobusSdk` constructor to share one HTTP/2 connection across streams.

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
