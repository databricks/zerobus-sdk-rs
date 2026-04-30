# NEXT CHANGELOG

## Release v1.1.0

### Major Changes

- **License: Migrated from the Databricks License to the Apache License 2.0**

### New Features and Improvements

- **Arrow Flight — graceful stream close**: When the server signals stream shutdown, the client pauses new sends, drains in-flight acknowledgments up to a configurable wait, then recovers; graceful-close recoveries do not count toward `recoveryRetries`.
- **`stream_paused_max_wait_time_ms`** on `ArrowStreamConfigurationOptions`: Optional cap (ms) on the paused wait (`undefined`/omitted = full server duration, `0` = recover immediately).

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

