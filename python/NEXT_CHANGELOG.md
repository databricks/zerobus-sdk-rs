# NEXT CHANGELOG

## Release v1.3.0

### Major Changes

### New Features and Improvements

- **Arrow Flight — graceful stream close**: On server signaled close, the client pauses sending, drains in-flight acks within a bounded wait, then recovers; those recoveries do not consume the `recovery_retries` budget.
- **`stream_paused_max_wait_time_ms`** on `ArrowStreamConfigurationOptions`: Optional milliseconds cap for the paused wait (`None` = full server duration, `0` = immediate recovery).

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
