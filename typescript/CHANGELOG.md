# Version changelog

## Release v1.2.0

### Major Changes

### New Features and Improvements

- `ZerobusSdk` constructor now accepts an optional `options` object
  (`ZerobusSdkOptions`) as its third argument. Its `applicationName` field is
  appended to the HTTP `user-agent` header sent on every request
  (e.g. `zerobus-sdk-ts/1.2.0 my-app/1.0`), enabling server-side attribution.
  The SDK now also correctly identifies itself as `zerobus-sdk-ts` rather than
  falling back to the underlying `zerobus-sdk-rs` identifier.

### Bug Fixes

- Added published CommonJS and type declaration files for
  `@databricks/zerobus-ingest-sdk/utils/descriptor.js`, so consumers can import
  `loadDescriptorProto()` from the npm package without running TypeScript
  source files directly. The package now installs the helper's `protobufjs`
  runtime dependency automatically, and the `.js` subpath supports CommonJS,
  native Node.js ESM, and NodeNext type resolution.
- Fixed descriptor file lookup so similarly suffixed names such as
  `not_air_quality.proto` cannot be selected for `air_quality.proto`.

### Documentation

- Simplified the README quick start to install the published npm package first
  and moved clone/build instructions into a source-development path.
- Corrected README, example, and JSDoc snippets for CommonJS async entry points,
  generated Protobuf field names, variable declarations, stream recovery, and
  custom-header callbacks.
- Documented that omitted `descriptorProto` does not select JSON, that the
  inherited inflight default is 1,000,000, and that `close()` is still required
  to flush. README `main().catch` handlers now set a non-zero exit code.
  The short `recreateStream()` example closes the replacement stream in `finally`.
- Binding rustdoc for `maxInflightRequests` now matches that 1,000,000 default.

- Clarified the high-throughput ingestion pattern across the README, API reference, JSDoc
  doc comments (`ingestRecordOffset`, `ingestRecordsOffset`, `waitForOffset`, `flush`), and
  examples: ingest in a loop without waiting, then wait once on the last offset (the ack
  watermark is monotonic) or `flush()` once at the end. Added explicit warnings that calling
  `waitForOffset()` after every record collapses throughput and should be reserved for
  low-volume cases.
- Built on Rust SDK 2.7.1. Wrapper-facing notes for that core are in
  `rust/CHANGELOG.md` and https://github.com/databricks/zerobus-sdk/releases/tag/rust/v2.7.1.

### Internal Changes

- The TypeScript release workflow can optionally import laptop-built darwin `.node`
  binaries from a GitHub Release when CI has no macOS runners.
- Updated TypeScript SDK development dependencies and the NAPI Cargo lockfile to
  resolve Dependabot security alerts without changing the public SDK API.
- Builds depend on the in-tree Rust SDK via `path = "../rust/sdk"` at 2.7.1.

### Breaking Changes

### Deprecations

### API Changes

- `HeadersProvider` is the shape `createStream()` actually accepts:
  `getHeadersCallback` returning header tuples synchronously. Classes with
  async `getHeaders()` are not compatible.

## Release v1.1.0

### New Features and Improvements

- Arrow Flight ingestion promoted to Beta. Mirrors the Rust SDK 2.0
  promotion. The API is stabilising but may still change before reaching GA.
  The `arrow-flight` feature is no longer labelled experimental/unsupported
  in docs and examples.
- macOS pre-built binaries. Added `@databricks/zerobus-ingest-sdk-darwin-x64`
  and `@databricks/zerobus-ingest-sdk-darwin-arm64` to `optionalDependencies`,
  so `npm install` on Intel and Apple Silicon Macs now fetches a pre-built
  `.node` binary instead of falling back to a source build.
- `waitForOffset` precision. Replaced the `Number(bigint)` round-trip
  with napi-rs's lossless `BigInt::get_i64()`. Both gRPC and Arrow streams
  now error cleanly on offsets that exceed `i64` range instead of silently
  truncating past `2^53 - 1`.

### Bug Fixes

- Fixed bogus `apache-arrow` peer dependency. v1.0.x declared
  `apache-arrow: "^56.0.0"`, which doesn't exist on npm (56 was a Rust
  crate version copied by mistake). Corrected to `^18.0.0` to match the
  current dev dep range.

### Internal Changes

- Depends on Rust SDK 2.0.1. The wrapper now goes through
  `sdk.stream_builder()` (Rust 2.0 removed the legacy
  `create_stream_with_headers_provider` / `create_arrow_stream` /
  `ingest_record` / `ingest_records` methods). The TS-facing API is
  unchanged — the v1 deprecated `ingestRecord` / `ingestRecords` methods
  still resolve after server ack (now via `ingest_record_offset` +
  `wait_for_offset` under the hood).
- Arrow crates bumped 56.2.0 → 58.2 to match the Rust SDK 2.0
  workspace. `bytes` added so the wrapper can hand IPC payloads to
  `ingest_ipc_batch` as `Bytes`.
- `napi6` feature on `napi-rs` so `BigInt::get_i64()` is available.
- CI install step switched from `npm ci` to `npm install --no-audit
  --no-fund`. `npm ci`'s strict lockfile validation rejects
  `optionalDependencies` referencing a not-yet-published version (every
  napi-rs major-version bump hits this); `npm install` tolerates it.

## Release v1.0.2

### Bug Fixes

- Split platform-specific native binaries into separate npm packages (`@databricks/zerobus-ingest-sdk-linux-x64-gnu`, `-linux-arm64-gnu`, `-win32-x64-msvc`). npm now auto-installs only the binary matching the user's OS/arch via `optionalDependencies`, reducing download size from ~15MB to ~5MB.

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
