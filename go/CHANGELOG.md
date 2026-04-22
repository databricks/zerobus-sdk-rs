# Version changelog

## Release v1.1.0

### Major Changes

- **License: Migrated from the Databricks License to the Apache License 2.0**

### New Features and Improvements

**[Experimental] Arrow Flight Ingestion**: Added experimental Arrow Flight support for high-throughput Apache Arrow RecordBatch ingestion

- New `CreateArrowStream` and `CreateArrowStreamWithHeadersProvider` methods on `ZerobusSdk`
- New `ZerobusArrowStream` type with `IngestBatch`, `WaitForOffset`, `Flush`, `Close`, and `GetUnackedBatches` methods
- Configurable IPC compression via `ArrowStreamConfigurationOptions.IpcCompression` (supports `LZ4Frame` and `Zstd`)

## Release v1.0.0.

GA release of the Databricks Zerobus Ingest SDK for Go.

### New Features and Improvements
- Added HTTP proxy support via standard environment variables (`grpc_proxy`, `https_proxy`, `http_proxy`), following gRPC core conventions. Proxied connections use HTTP CONNECT tunneling with end-to-end TLS. Supports `no_grpc_proxy` / `no_proxy` for bypass rules.

### Deprecations


### Bug Fixes
**IMPORTANT**: Fixed memory safety issue where Go garbage collector could move data while Rust FFI was reading it, causing crashes          
    - Implemented proper memory pinning using `runtime.Pinner` in all FFI functions that pass Go slices to Rust
    - Updated `streamIngestProtoRecords`, `streamIngestProtoRecord`, `streamIngestJSONRecords`, `sdkCreateStream`, and
  `sdkCreateStreamWithHeadersProvider`
    - Uses `unsafe.SliceData()` for safe pointer conversion (requires Go 1.20+)
    - Pins data before passing to Rust, ensuring pointers remain valid during FFI calls

### Documentation


### Internal Changes


### API Changes


## Release v0.2.1

### Bug Fixes

- **Critical**: Fixed CGO pointer violations in batch ingestion APIs that caused runtime panics with "cgo argument has Go pointer to unpinned Go pointer"
  - Fixed `IngestRecordsOffset()` for both JSON and Protocol Buffer records
  - Fixed by allocating pointer arrays in C memory instead of Go memory
- Added NULL checks for all `malloc` calls to handle out-of-memory scenarios gracefully
  - Added checks in batch ingestion functions
  - Added checks in headers provider callback

### Internal Changes
- Updated all SDK pointer validation to work with wrapper structure

## Release v0.2.0

### New Features and Improvements

- Introduced simplified `IngestRecordOffset()` API that returns offsets directly as `(int64, error)` instead of returning a future-like `RecordAck`. This is now the recommended way to ingest records.
- Batch ingestion API `IngestRecordsOffset()` that accepts multiple records and returns one offset for the entire batch. Optimized for high-throughput scenarios where ingesting multiple records at once improves performance.
- Explicit control over waiting for server acknowledgments with `WaitForOffset()` method. Allows waiting for specific offsets without blocking on all records.
- Enabled retrieval of all records that have not been acknowledged, in case of stream failure, with `GetUnackedRecords()` method.
- Enabled Rust core tracing logs visible from Go applications via `RUST_LOG` environment variable. Provides detailed debugging information from the underlying SDK.
- Updated to `databricks-zerobus-ingest-sdk` v0.4.0 with latest improvements and bug fixes

### Deprecations

- `IngestRecord()` method is deprecated in favor of `IngestRecordOffset()`. The old API remains functional for backwards compatibility but will be removed in a future major version. IDEs will show deprecation warnings with migration guidance.

### Bug Fixes

- Fixed memory leaks caused by ACK tracking futures not being properly cleaned up when streams closed
- Corrected offset values to start from 0 (matching Rust SDK's `OffsetIdGenerator` behavior) instead of 1
- Fixed stream cleanup to properly free all resources without requiring manual ACK task abortion

### Documentation

- Updated READMEs
- Reorganized examples into `json/single`, `json/batch`, `proto/single`, and `proto/batch` directories
- Added batch ingestion examples demonstrating `IngestRecordsOffset()` in both JSON and protobuf examples

### Internal Changes

- Removed `ACK_REGISTRY`, `ACK_COUNTER`, and `STREAM_ACKS` global static state from FFI layer
- Removed async task spawning and future tracking in FFI layer
- Changed internal implementation to call Rust SDK's `ingest_record_offset()` and `ingest_records_offset()` instead of deprecated APIs

### API Changes

- `IngestRecordOffset(payload interface{}) (int64, error)` - Returns offset directly after queuing record for ingestion
- `IngestRecordsOffset(records []interface{}) (int64, error)` - Batch ingestion API that returns one offset for the entire batch
- `WaitForOffset(offset int64) error` - Explicitly wait for server acknowledgment of a specific offset
- `GetUnackedRecords() ([]interface{}, error)` - Retrieve all unacknowledged records that are still in-flight (call only after stream closes/fails)
- `IngestRecord()` now returns immediately with an offset wrapped in `RecordAck`
- `RecordAck.Await()` now blocks and waits for server acknowledgment (calls Rust SDK's `wait_for_offset()`)
- `RecordAck.Offset()` returns the offset immediately without waiting

## Release v0.1.0

Initial release of the Databricks Zerobus Ingest SDK for Go.

### Features

- **Static Linking** - Self-contained binaries with no runtime dependencies
- **Go SDK wrapper** around the high-performance Rust implementation
- **CGO/FFI integration** for seamless Go-to-Rust interoperability
- **JSON ingestion** support for simple data streaming
- **Protocol Buffer ingestion** for type-safe, efficient data encoding
- **OAuth 2.0 authentication** with Unity Catalog integration
- **Automatic retry and recovery** for transient failures
- **Configurable stream options** including inflight limits, timeouts, and recovery behavior
- **Async acknowledgments** for tracking record ingestion

### API

- Added `ZerobusSdk` struct for creating and managing ingestion streams
- Added `ZerobusStream` for bidirectional gRPC streaming
- `IngestRecord()` method that accepts both JSON (string) and Protocol Buffer ([]byte) data
- Added `StreamConfigurationOptions` for fine-tuning stream behavior
- Added `ZerobusError` for detailed error handling with retryability detection
- `Flush()` method to ensure all pending records are acknowledged
- `Close()` method for graceful stream shutdown

### Build System

- Static library compilation for portability
- Platform detection for Linux and macOS
- Automated build scripts for development and release
- No LD_LIBRARY_PATH configuration required

### Documentation

- Comprehensive README with quick start examples
- JSON and Protocol Buffer usage examples
- API reference documentation
- Troubleshooting guide
- Performance optimization tips
