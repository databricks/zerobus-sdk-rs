# NEXT CHANGELOG

## Release v1.7.0

### New Features and Improvements

- JSON and protobuf streams now use a dedicated gRPC connection by default.
  Pass `WithConnectionPerStream(false)` to `NewZerobusSdkWithOptions` to share
  one HTTP/2 connection across streams.

### Deprecations

### Bug Fixes

### Documentation

### Internal Changes

### API Changes
