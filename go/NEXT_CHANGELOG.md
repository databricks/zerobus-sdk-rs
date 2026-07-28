# NEXT CHANGELOG

## Release v1.4.0

### New Features and Improvements

### Deprecations

### Bug Fixes

- Fixed a use-after-free in which a custom `HeadersProvider` could be freed while
  a background worker was still calling into it during connection recovery. The
  provider's `cgo.Handle` ownership is now handed to the FFI, which releases it
  (via a new destroy callback) only after any in-flight `GetHeaders` has
  returned, instead of deleting it on stream close. This removes the per-stream
  handle registry. No public API change.
- The HTTP `user-agent` now identifies this wrapper as
  `zerobus-sdk-go/<version>` instead of using the Rust core's identifier.

### Documentation

### Internal Changes

### API Changes
