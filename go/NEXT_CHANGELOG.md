# NEXT CHANGELOG

## Release v1.2.0

### Major Changes

### New Features and Improvements

- **Arrow Flight — graceful stream close**: When the server signals an impending close, the client pauses sends, drains in-flight acks within a bounded wait, then recovers; graceful-close recoveries do not count toward `RecoveryRetries`.
- **`ArrowStreamConfigurationOptions.StreamPausedMaxWaitTimeMs`**: Optional `*uint64` limiting how long to wait (ms) while paused (`nil` = full server duration, `0` = immediate recovery).

### Deprecations

### Bug Fixes

### Documentation

### Internal Changes

### API Changes
