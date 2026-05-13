# NEXT CHANGELOG

## Release v1.2.0

### Major Changes

### New Features and Improvements

- **Arrow Flight — graceful stream close**: When the server signals an impending close, the client pauses sends, drains in-flight acks within a bounded wait, then recovers.
- **`ArrowStreamConfigurationOptions.StreamPausedMaxWaitTimeMs`**: Optional `*uint64` limiting how long to wait (ms) while paused (`nil` = full server duration, `0` = immediate recovery).

### Deprecations

### Bug Fixes

- **Reduced GC pressure in batch ingest FFI paths** ([#271](https://github.com/databricks/zerobus-sdk/issues/271)): `streamIngestJSONRecords` was allocating one heap-allocated closure per record per call (defer-in-loop). These closures are not pooled by the Go runtime, causing measurable allocation growth at high ingestion rates. Fixed by replacing N defers with a single closure. `streamIngestProtoRecords` was also allocating the pointer/length arrays on the Go heap and unnecessarily pinning them; both are now allocated in C memory via `C.malloc`.

### Documentation

### Internal Changes

### API Changes
