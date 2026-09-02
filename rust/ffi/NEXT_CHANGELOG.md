# NEXT CHANGELOG

## Release v1.9.0

### Major Changes

### New Features and Improvements

- JSON and protobuf streams now use a dedicated gRPC connection by default.
  Call `zerobus_sdk_builder_connection_per_stream(builder, false)` before
  building the SDK to share one HTTP/2 connection across streams. Arrow Flight
  streams are unchanged.

### Bug Fixes

### Documentation

### Internal Changes

### Behavior Changes

### Breaking Changes

### Deprecations

### API Changes

- Added the additive `zerobus_sdk_builder_connection_per_stream` builder
  setter.
