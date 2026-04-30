# NEXT CHANGELOG

## Release v1.2.0

### Major Changes

### New Features and Improvements

- **Arrow Flight — graceful stream close**: On server signaled close, the client stops sending new batches, drains in-flight acknowledgments within a bounded wait, then recovers; recoveries driven only by graceful close do not count against `recoveryRetries`.
- **`ArrowStreamConfigurationOptions`**: Added `streamPausedMaxWaitTimeMs` for the maximum time (milliseconds) to wait in the paused state during graceful close (`null` = full server duration, `0` = immediate recovery).

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
