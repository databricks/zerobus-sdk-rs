# NEXT CHANGELOG

## Release v1.1.0

### Major Changes

### New Features and Improvements

- **[Experimental Arrow Flight] Zero-copy IPC ingestion via `ingest_ipc_batch`**: Added `ZerobusArrowStream::ingest_ipc_batch(Bytes)` for FFI callers (Go, Python, Java, TypeScript) that already hold Arrow IPC stream bytes. Raw bytes are forwarded directly to the Flight wire format without deserialising to a `RecordBatch` and re-serialising, eliminating one IPC round-trip per batch compared to `ingest_batch`. The existing `ingest_batch` API is unchanged.

### Bug Fixes

- Fixed proto generation tool to skip reserved field numbers 19000-19999 for tables with more than 19000 columns

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

