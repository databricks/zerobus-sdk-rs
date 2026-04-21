# Version changelog

## Release v1.0.1

### Bug Fixes

- Fixed npm packaging: v1.0.0 was published without the napi-rs generated `index.js` loader and `index.d.ts` type declarations, causing `MODULE_NOT_FOUND` on `require('@databricks/zerobus-ingest-sdk')`. The platform-specific native binary packages (e.g. `@databricks/zerobus-ingest-sdk-linux-x64-gnu`) were also missing from npm. This release includes all generated files and platform packages.

## Release v1.0.0

GA release of the Databricks Zerobus Ingest SDK for TypeScript.

### New Features and Improvements
- Added HTTP proxy support via standard environment variables (`grpc_proxy`, `https_proxy`, `http_proxy`), following gRPC core conventions. Proxied connections use HTTP CONNECT tunneling with end-to-end TLS. Supports `no_grpc_proxy` / `no_proxy` for bypass rules.

## Release v0.3.0

### Native Library Update

- Updated native Rust backend to v0.6.0
- Schemeless server endpoints now automatically get `https://` prepended
- All documentation and examples updated to explicitly use `https://` prefixed endpoints

## Release v0.2.0

### New Features and Improvements

- Upgraded to Rust SDK v0.4.0
- Added new offset-based ingestion APIs for better high-throughput patterns:
  - `ingestRecordOffset()` - Returns offset immediately after queuing
  - `ingestRecordsOffset()` - Batch version, returns offset immediately
  - `waitForOffset()` - Wait for specific offset to be acknowledged
- Added experimental Arrow Flight support (behind feature flag)
- Added `streamPausedMaxWaitTimeMs` configuration option
- Set user agent to identify as `zerobus-sdk-ts/0.2.0`
- Reorganized examples into `json/`, `proto/`, `arrow/` directories

### API Changes

- **New (Recommended):** `ingestRecordOffset()`, `ingestRecordsOffset()`, `waitForOffset()`
- **Deprecated:** `ingestRecord()`, `ingestRecords()` - still work but return Promise that blocks until ack
- Added `streamPausedMaxWaitTimeMs` to `StreamConfigurationOptions`
- Custom `headers_provider` now automatically includes TS SDK user agent if not specified

### Documentation

- Updated README with new APIs and deprecation notices
- Reorganized examples with separate directories for each format
- Added Arrow Flight examples (experimental)

---

## Release v0.1.0

Initial release of the Databricks Zerobus Ingest SDK for TypeScript.

### New Features and Improvements

- High-throughput data ingestion into Databricks Delta tables using native Rust implementation
- Support for JSON and Protocol Buffers serialization formats
- OAuth 2.0 client credentials authentication
- Batch ingestion API with `ingestRecords()` for higher throughput
- Type widening support for flexible record input:
  - JSON mode: Accept objects (auto-stringify) or strings (pre-serialized)
  - Protocol Buffers mode: Accept Message objects (auto-serialize) or Buffers (pre-serialized)
- Stream recovery mechanisms with `getUnackedRecords()` and `getUnackedBatches()`
- Automatic retry and recovery for transient failures
- Protocol Buffer descriptor utilities with `loadDescriptorProto()`
- Cross-platform support (Linux, macOS, Windows)

### API Changes

- Added `ZerobusSdk` class for creating ingestion streams
- Added `ZerobusStream` class for managing stateful gRPC streams
- Added `createStream()` method with optional `headers_provider` parameter
- Added `ingestRecord()` method accepting Buffer, string, or object types
- Added `ingestRecords()` method for batch ingestion
- Added `getUnackedRecords()` and `getUnackedBatches()` for recovery
- Added `TableProperties` interface for table configuration
- Added `StreamConfigurationOptions` interface with `recordType` parameter
- Added `RecordType` enum with `Json` and `Proto` values
- Added `HeadersProvider` interface for custom authentication
- Support for Node.js >= 16

### Documentation

- Comprehensive README with quick start guide
- Protocol Buffer setup instructions
- Type mapping guide (Delta ↔ Proto)
- API reference documentation
- Examples for JSON and Protocol Buffers ingestion
