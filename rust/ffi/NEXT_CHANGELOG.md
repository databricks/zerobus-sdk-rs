# NEXT CHANGELOG

## Release v1.2.0

### Major Changes

### New Features and Improvements

- **Arrow stream options (C API)**: `CArrowStreamConfigurationOptions.stream_paused_max_wait_time_ms` (`int64_t`) configures graceful-close paused wait: `-1` = None (full server duration), `0` = immediate recovery, `>0` = capped wait (see `zerobus.h` comments).

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
