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

- `zerobus_arrow_stream_free` now selects destruction behavior based on how the stream was used. IPC-only streams preserve best-effort, nonblocking destruction. Once a stream accepts an Arrow C Data batch, free blocks until Arrow background shutdown completes, every Flight request body reaches EOF or is dropped, and all retained foreign owners are released. Previously, a request body could retain an owner and run its release callback after free returned on an unacknowledged/failure path, risking callback-after-free use of producer state. When the calling restrictions below are respected, no Arrow C Data release callback for that stream can run after free returns. The function logs a warning every 30 seconds while required C Data shutdown remains incomplete; it does not return on a timeout. Free must not race another operation on the same stream handle. After C Data import, freeing the same stream reentrantly from one of its SDK callbacks is unsupported because complete shutdown would wait for that callback. IPC-only concurrent or reentrant free remains invalid because the opaque handle has single ownership; freeing a different stream from a callback remains supported. During required C Data shutdown, an internal native shutdown panic, a required helper-thread spawn failure, or a helper-thread panic terminates the process rather than returning without the release-callback guarantee.

### Breaking Changes

### Deprecations

### API Changes
