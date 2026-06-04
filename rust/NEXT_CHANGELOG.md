# NEXT CHANGELOG

## Release v2.1.2

### Major Changes

### New Features and Improvements

- Added a process-wide stream churn warning: logs a `WARN` when 100 or more streams for the same table are opened within a 60-second sliding window, which may indicate a "one stream per record" misuse pattern. Applies to `ZerobusStream` and `ZerobusArrowStream`. Set `ZEROBUS_SDK_WARNINGS_ENABLED=false` to suppress.

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
