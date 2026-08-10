# Version changelog

## Release v1.4.0

### Bug Fixes

- Fixed a use-after-free in which a custom `HeadersProvider` could be freed while
  a background worker was still calling into it during connection recovery. The
  provider's `cgo.Handle` ownership is now handed to the FFI, which releases it
  (via a new destroy callback) only after any in-flight `GetHeaders` has
  returned, instead of deleting it on stream close. This removes the per-stream
  handle registry. No public API change.
- The HTTP `user-agent` now identifies this wrapper as
  `zerobus-sdk-go/<version>` instead of using the Rust core's identifier.

## Release v1.3.0

### New Features and Improvements

- Added `NewZerobusSdkWithOptions` and `WithApplicationName`. The application
  name is appended to the HTTP `user-agent` header as
  `zerobus-sdk-go/<version> <application_name>` for server-side attribution.
  The existing two-argument `NewZerobusSdk` signature is unchanged.

### Bug Fixes

- The HTTP `user-agent` now identifies this wrapper as
  `zerobus-sdk-go/<version>` instead of using the Rust core's identifier.

### Documentation

- Documented application-name configuration and updated every Go example to
  demonstrate it.
- Clarified throughput guidance in the README, godoc, and examples: ingest records in a loop without waiting and call `Flush()` once, rather than calling `WaitForOffset()` after every record. Documented that the ack watermark is monotonic, so waiting on the last offset confirms all prior records.

### Internal Changes

- Added Darwin AMD64 and ARM64 static-library artifacts to the Go release build by cross-compiling the FFI with Zig, so release PRs can include the full supported platform matrix without a macOS runner.
- Added a Go SDK version constant used to construct the wrapper-specific
  user-agent identifier.
- Switched SDK construction to the additive C builder API and refreshed all
  five bundled FFI archives so they export the builder symbols. Removed the
  deprecated `zerobus_sdk_set_use_tls` call, which was already a no-op.
- Enabled thin LTO for release FFI builds so the bundled static libraries stay
  below repository file-size limits on every supported platform.
- Added the new ack-callback fields (`ack_on_ack`, `ack_on_error`, `ack_user_data`) to the cgo `CStreamConfigurationOptions` mirror to keep it byte-identical with the C FFI struct. The Go SDK has no ack-callback API yet and leaves these null, so behavior is unchanged.
- The integration-test protobuf bindings (`go/tests/pb`) and the pure-Go SDK bindings (`purego/internal/zerobuspb`) are now generated from the single canonical `rust/sdk/zerobus_service.proto`, instead of local per-module copies. Regenerate with `go/tests/generate_proto.sh` or `go generate ./...` in the purego package. No behavior change — the committed generated code is unchanged.

### API Changes

- Added `SdkOption`, `WithApplicationName`, and
  `NewZerobusSdkWithOptions`.

## Release v1.2.0

### New Features and Improvements

- **`IngestRecordNowait` / `IngestRecordsNowait`**: New fire-and-forget ingestion methods on `ZerobusStream`. Both return immediately after spawning a background task; ingestion errors are silently ignored. `IngestRecordNowait` accepts a single `[]byte` or `string` payload; `IngestRecordsNowait` accepts a batch as `[]interface{}`. Returns immediately after spawning a background task to queue the record; accepts `[]byte` (protobuf) or `string` (JSON). Ingestion errors from the background task are silently ignored.
- **Arrow Flight promoted to Beta**: The Arrow Flight ingestion API (`ZerobusArrowStream`, `CreateArrowStream`, `CreateArrowStreamWithHeadersProvider`, `ArrowStreamConfigurationOptions`) is no longer labelled experimental/unsupported. The API is stabilising but may still change before reaching GA.
- **Arrow Flight — graceful stream close**: When the server signals an impending close, the client pauses sends, drains in-flight acks within a bounded wait, then recovers.
- **`ArrowStreamConfigurationOptions.StreamPausedMaxWaitTimeMs`**: Optional `*uint64` limiting how long to wait (ms) while paused (`nil` = full server duration, `0` = immediate recovery).

### Bug Fixes

- **Reduced GC pressure in batch ingest FFI paths** ([#271](https://github.com/databricks/zerobus-sdk/issues/271)): `streamIngestJSONRecords` was allocating one heap-allocated closure per record per call (defer-in-loop). These closures are not pooled by the Go runtime, causing measurable allocation growth at high ingestion rates. Fixed by replacing N defers with a single closure. `streamIngestProtoRecords` was also allocating the pointer/length arrays on the Go heap and unnecessarily pinning them; both are now allocated in C memory via `C.malloc`.
- **Vendoring support**: `go mod vendor` now preserves the prebuilt FFI archives under `lib/<GOOS>_<GOARCH>/` when downstream consumers vendor this module. Previously, cgo `#cgo LDFLAGS` paths were invisible to the vendor tool's dependency analysis, so vendored builds failed to link.

## Release v1.1.1

Re-release of v1.1.0 with pre-built FFI libraries included. v1.1.0 is retracted due to missing native libraries.

### Major Changes

- **License: Migrated from the Databricks License to the Apache License 2.0**

### New Features and Improvements

**[Experimental] Arrow Flight Ingestion**: Added experimental Arrow Flight support for high-throughput Apache Arrow RecordBatch ingestion

- New `CreateArrowStream` and `CreateArrowStreamWithHeadersProvider` methods on `ZerobusSdk`
- New `ZerobusArrowStream` type with `IngestBatch`, `WaitForOffset`, `Flush`, `Close`, and `GetUnackedBatches` methods
- Configurable IPC compression via `ArrowStreamConfigurationOptions.IpcCompression` (supports `LZ4Frame` and `Zstd`)

## Release v1.1.0 (retracted)

Retracted — broken release. Use v1.1.1 instead.

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
