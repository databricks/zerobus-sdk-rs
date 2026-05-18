# NEXT CHANGELOG

## Release v1.2.0

### Major Changes

### New Features and Improvements

- **Arrow Flight — graceful stream close**: On server signaled close, the client stops sending new batches, drains in-flight acknowledgments within a bounded wait, then recovers.
- **`ArrowStreamConfigurationOptions`**: Added `streamPausedMaxWaitTimeMs` for the maximum time (milliseconds) to wait in the paused state during graceful close (`-1` = full server duration, `0` = immediate recovery).
- **Arrow Flight — zero-copy IPC ingestion**: Added `ZerobusArrowStream.ingestIpcBatch(byte[])` for callers that already hold serialized Arrow IPC stream bytes. Forwards bytes directly into Flight wire format, skipping the deserialize → re-serialize round-trip performed by `ingestBatch(VectorSchemaRoot)`. Not compatible with streams configured with `ipcCompression`.

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
