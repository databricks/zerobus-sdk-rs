# NEXT CHANGELOG

## Release v1.3.0

### New Features and Improvements

### Deprecations

### Bug Fixes

### Documentation

### Internal Changes

- Add internal `zerobuspb` package with generated gRPC/protobuf bindings for the
  Zerobus service. This is the foundation for a pure-Go SDK (no cgo/FFI).

### API Changes

- The minimum required Go version is now 1.25.0 (raised from 1.21), pulled in by
  the `google.golang.org/grpc` dependency. Consumers must build with Go 1.25.0 or
  newer.
