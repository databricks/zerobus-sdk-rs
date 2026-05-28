# NEXT CHANGELOG

## Release v2.1.2

### Major Changes

### New Features and Improvements

- Added process-wide client-side warnings to surface common misuse patterns:
  - **Concurrent open streams**: logs a `WARN` when 32 or more ingest streams for the same table are open simultaneously.
  - **High stream open rate (churn)**: logs a `WARN` when 100 or more streams for the same table are opened within a 60-second sliding window, which may indicate a "one stream per record" pattern.
  - Both warnings are keyed by table name and apply to `ZerobusStream` and `ZerobusArrowStream`.
  - Set `ZEROBUS_SDK_WARNINGS_ENABLED=false` to suppress all warnings.

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
