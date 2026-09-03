# NEXT CHANGELOG

## Release v1.4.0

### Major Changes

### New Features and Improvements

- JSON and protobuf streams now use a dedicated gRPC connection by default.
  Set `connectionPerStream: false` in `ZerobusSdkOptions` to share one HTTP/2
  connection across streams.

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
