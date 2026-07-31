# NEXT CHANGELOG

## Release v1.4.0

### Major Changes

### New Features and Improvements

- Added `HeadersProvider` support to `StreamBuilder` for custom authentication on JSON, Protocol Buffer, and Arrow streams, including stream recreation and credential invalidation callbacks.

### Bug Fixes

- `HeadersProvider` exceptions are now retryable during automatic recovery unless the provider
  throws `NonRetriableException`, including for Arrow streams. Malformed provider output and
  excessive distinct header names remain non-retryable.

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

- Added `HeadersProvider` and `StreamBuilder.headersProvider()` for custom authentication.
