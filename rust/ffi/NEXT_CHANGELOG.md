# NEXT CHANGELOG

## Release v1.7.0

### Major Changes

### New Features and Improvements

### Bug Fixes

### Documentation

- Clarified that acknowledgment callbacks fire once per logical ingest
  submission, so one batch ingest call produces one callback.
- Documented flush, close/free, SDK free, and error-string cleanup in the
  copyable C lifecycle example.

### Internal Changes

### Behavior Changes

- `zerobus_arrow_stream_free` now blocks until Arrow background shutdown completes, every Flight request body reaches EOF or is dropped, and all retained Arrow C Data owners are released. Previously, a request body could retain an owner and run its release callback after free returned on an unacknowledged/failure path, risking callback-after-free use of wrapper state. When the calling restrictions below are respected, no Arrow C Data release callback for that stream can run after free returns. The function logs a warning every 30 seconds while shutdown remains incomplete; it does not return on a timeout. Free must not race another operation on the same stream handle. Freeing the same stream reentrantly from one of its SDK callbacks is unsupported because shutdown would wait for that callback; freeing a different stream from a callback remains supported. An internal native shutdown panic, a required helper-thread spawn failure, or a helper-thread panic terminates the process rather than returning without the release-callback guarantee.

### Breaking Changes

### Deprecations

### API Changes
