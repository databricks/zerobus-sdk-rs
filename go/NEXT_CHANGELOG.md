# NEXT CHANGELOG

## Release v1.1.0

### New Features and Improvements

**[Experimental] Arrow Flight Ingestion**: Added experimental Arrow Flight support for high-throughput Apache Arrow RecordBatch ingestion

- New `CreateArrowStream` and `CreateArrowStreamWithHeadersProvider` methods on `ZerobusSdk`
- New `ZerobusArrowStream` type with `IngestBatch`, `WaitForOffset`, `Flush`, `Close`, and `GetUnackedBatches` methods
- Configurable IPC compression via `ArrowStreamConfigurationOptions.IpcCompression` (supports `LZ4Frame` and `Zstd`)

### Deprecations

### Bug Fixes

### Documentation

### Internal Changes

### API Changes
