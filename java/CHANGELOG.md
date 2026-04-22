# Version changelog

## Release v1.1.0

### Major Changes

- **License: Migrated from the Databricks License to the Apache License 2.0**

### New Features and Improvements

- **Arrow Flight Support (Experimental)**: Added support for ingesting Apache Arrow `VectorSchemaRoot` batches via Arrow Flight protocol
  - **Note**: Arrow Flight is not yet supported by default from the Zerobus server side.
  - New `ZerobusArrowStream` class with `ingestBatch()`, `waitForOffset()`, `flush()`, `close()`, `getUnackedBatches()` methods
  - New `ArrowStreamConfigurationOptions` for configuring Arrow streams (max inflight batches, recovery, timeouts, IPC compression)
  - Configurable IPC compression via `ArrowStreamConfigurationOptions.setIpcCompression()` (supports `LZ4_FRAME` and `ZSTD`)
  - New `createArrowStream()` and `recreateArrowStream()` methods on `ZerobusSdk`
  - Accepts `VectorSchemaRoot` directly via `ingestBatch()` (IPC serialization handled internally)
  - Arrow is opt-in: add `arrow-vector` and `arrow-memory-netty` as dependencies (provided scope, `>= 15.0.0`)

### Bug Fixes

- **Classloader Isolation Compatibility**: Fixed `NoClassDefFoundError` when using the SDK inside Spring Boot. JNI class references are now cached as `GlobalRef`s during `JNI_OnLoad`, so native daemon threads no longer rely on `FindClass` through the system classloader.
- Fixed proto generation tool to skip reserved field numbers 19000-19999 for tables with more than 19000 columns

### Internal Changes

- Added `arrow-vector` 17.0.0 as provided dependency for Arrow Flight support
- Added `arrow-memory-netty` 17.0.0 as test dependency for integration tests
- Uses existing JNI Arrow Flight bindings from Rust SDK (`nativeCreateArrowStream`, `nativeIngestBatch`, etc.)

### API Changes

- Added `createArrowStream(String tableName, Schema schema, String clientId, String clientSecret)` to `ZerobusSdk`
- Added `createArrowStream(String tableName, Schema schema, String clientId, String clientSecret, ArrowStreamConfigurationOptions options)` to `ZerobusSdk`
- Added `recreateArrowStream(ZerobusArrowStream closedStream)` to `ZerobusSdk`
- Added `ZerobusArrowStream` class with methods: `ingestBatch()`, `waitForOffset()`, `flush()`, `close()`, `getUnackedBatches()`, `isClosed()`, `getTableName()`, `getOptions()`
- Added `ArrowStreamConfigurationOptions` class with fields: `maxInflightBatches`, `recovery`, `recoveryTimeoutMs`, `recoveryBackoffMs`, `recoveryRetries`, `serverLackOfAckTimeoutMs`, `flushTimeoutMs`, `connectionTimeoutMs`, `ipcCompression`
- Added `IPCCompressionType` enum with values: `NONE`, `LZ4_FRAME`, `ZSTD`
- Added optional dependency: `org.apache.arrow:arrow-vector >= 15.0.0` (provided scope)

## Release v1.0.1

### Bug Fixes
- Fixed TLS certificate validation failure when behind corporate VPN/proxy with MITM certificates (e.g., GlobalProtect). The underlying Rust native library now loads CA certificates from the OS native trust store (respecting `SSL_CERT_FILE` and system certificate stores) while keeping bundled Mozilla roots as a fallback.

## Release v1.0.0

GA release of the Databricks Zerobus Ingest SDK for Java.

### New Features and Improvements
- Added HTTP proxy support via standard environment variables (`grpc_proxy`, `https_proxy`, `http_proxy`), following gRPC core conventions. Proxied connections use HTTP CONNECT tunneling with end-to-end TLS. Supports `no_grpc_proxy` / `no_proxy` for bypass rules.

## Release v0.3.0

### Native Library Update

- Updated native Rust backend to v0.6.0
- Schemeless server endpoints now automatically get `https://` prepended
- All documentation and examples updated to explicitly use `https://` prefixed endpoints
- Added Linux aarch64 (ARM64) native library support

## Release v0.2.0

### Native Rust Backend (JNI Migration)
- The SDK now uses JNI (Java Native Interface) to call the Zerobus Rust SDK instead of pure Java gRPC calls
- Native library is automatically loaded from the classpath or system library path
- Token management and background processing handled by native code

### New Stream Classes

**ZerobusProtoStream** - Protocol Buffer ingestion with method-level generics:
- `ingestRecordOffset(T record)` - Auto-encoded: SDK encodes Message to bytes
- `ingestRecordOffset(byte[] bytes)` - Pre-encoded: User provides encoded bytes
- `ingestRecordsOffset(List<T> records)` - Batch auto-encoded ingestion
- `ingestRecordsOffset(List<byte[]> bytes)` - Batch pre-encoded ingestion
- `getUnackedRecords()` - Returns `List<byte[]>` of unacked records
- `getUnackedRecords(Parser<T>)` - Returns parsed `List<T>` of unacked records
- `getUnackedBatches()` - Returns `List<EncodedBatch>` preserving batch grouping

**ZerobusJsonStream** - JSON ingestion without Protocol Buffer dependency:
- `ingestRecordOffset(String json)` - Pre-serialized: User provides JSON string
- `ingestRecordOffset(T obj, JsonSerializer<T>)` - Auto-serialized: SDK serializes object
- `ingestRecordsOffset(List<String> jsons)` - Batch pre-serialized ingestion
- `ingestRecordsOffset(List<T> objs, JsonSerializer<T>)` - Batch auto-serialized ingestion
- `getUnackedRecords()` - Returns `List<String>` of unacked JSON records
- `getUnackedRecords(JsonDeserializer<T>)` - Returns parsed `List<T>` of unacked records
- `getUnackedBatches()` - Returns `List<EncodedBatch>` preserving batch grouping

### New Factory Methods

- `ZerobusSdk.createProtoStream(tableName, descriptorProto, clientId, clientSecret)` - Create proto stream
- `ZerobusSdk.createProtoStream(tableName, descriptorProto, clientId, clientSecret, options)` - With options
- `ZerobusSdk.createJsonStream(tableName, clientId, clientSecret)` - Create JSON stream
- `ZerobusSdk.createJsonStream(tableName, clientId, clientSecret, options)` - With options
- `ZerobusSdk.recreateStream(ZerobusProtoStream)` - Recreate proto stream with unacked record re-ingestion
- `ZerobusSdk.recreateStream(ZerobusJsonStream)` - Recreate JSON stream with unacked record re-ingestion
- `ZerobusSdk.recreateStream(ZerobusStream)` - Recreate legacy stream (deprecated)

### New Supporting Types

- `BaseZerobusStream` - Abstract base class with native JNI methods
- `JsonSerializer<T>` - Functional interface for object-to-JSON serialization
- `JsonDeserializer<T>` - Functional interface for JSON-to-object deserialization
- `EncodedBatch` - Represents a batch of encoded records for recovery
- `AckCallback` - Callback interface with `onAck(long)` and `onError(long, String)`

### Deprecated (Backward Compatible)

**ZerobusStream<T>** - Use `ZerobusProtoStream` instead:
- `ingestRecord(T record)` - Returns `CompletableFuture<Void>`, use `ingestRecordOffset()` instead
- `getStreamId()` - No longer exposed by native backend, returns empty string
- `getState()` - Returns `OPENED` or `CLOSED` only
- `getUnackedRecords()` - **Breaking:** Returns empty iterator (records stored in native, type erasure prevents deserialization). Use `ZerobusProtoStream.getUnackedRecords(Parser<T>)` for typed access, or use `recreateStream()` which handles re-ingestion automatically using cached raw bytes.

**StreamConfigurationOptions**:
- `setAckCallback(Consumer<IngestRecordResponse>)` - No longer invoked by native backend. Use `setAckCallback(AckCallback)` instead

### Removed

- `TokenFactory` - Token management now handled by native Rust SDK
- `BackgroundTask` - Background processing now handled by native Rust SDK
- `ZerobusSdkStubUtils` - gRPC stub utilities no longer needed with native backend

### Platform Support

- Linux x86_64: Supported
- Windows x86_64: Supported
- macOS x86_64: Supported
- macOS aarch64 (Apple Silicon): Supported

## Release v0.1.0

Initial release of the Databricks Zerobus Ingest SDK for Java.

### API Changes

- Added `ZerobusSdk` class for creating ingestion streams
- Added `ZerobusStream` class for managing stateful gRPC streams
- Added `RecordAcknowledgment` for blocking until record acknowledgment
- Added `TableProperties` for configuring table schema and name
- Added `StreamConfigurationOptions` for stream behavior configuration
- Added `ZerobusException` and `NonRetriableException` for error handling
- Added `StreamState` enum for tracking stream lifecycle
- Added utility methods in `ZerobusSdkStubUtils` for gRPC stub management
- Support for Java 8 and higher
