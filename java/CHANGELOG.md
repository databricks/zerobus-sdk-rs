# Version changelog

## Release v1.6.0

### Major Changes

### New Features and Improvements

- Promoted Arrow Flight ingestion to general availability across its APIs,
  documentation, and examples.

### Bug Fixes

### Documentation

- Built on Rust SDK 2.8.0. Wrapper-facing notes for that core are in
  `rust/CHANGELOG.md` and https://github.com/databricks/zerobus-sdk/releases/tag/rust/v2.8.0.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

## Release v1.5.0

### Major Changes

### New Features and Improvements

### Bug Fixes

### Documentation

- Updated dependency snippets to version 1.4.0 and corrected README and example
  code for stream cleanup, recreation, unique local variables, and a single
  durability barrier after queued ingestion. Clarified that acknowledgment
  callbacks fire once per logical ingest submission, including one callback per
  batch ingest call.
- Documented that published JARs support Java 8 while source builds need JDK 11,
  that macOS JNI artifacts are not in the current release set, that
  `recoveryRetries` defaults to 4, and that `flush()` waits for durability rather
  than callback completion.
- Maven Central snippets no longer tell users to redeclare compile-scope
  transitives (`protobuf-java`, `slf4j-api`). Stream-builder examples use
  try-with-resources. GenerateProto docs no longer claim STRUCT support, JAR
  examples use `zerobus-ingest-sdk` 1.4.0, and proto examples generate
  `AirQualityProto.java` with `protoc` instead of treating it as checked in.
  Example README snippets and the JSON/proto `SingleRecordExample` programs
  queue records or batches and call `flush()` once instead of
  `waitForOffset()` after a single ingest.
- Built on Rust SDK 2.7.1. Wrapper-facing notes for that core are in
  `rust/CHANGELOG.md` and https://github.com/databricks/zerobus-sdk/releases/tag/rust/v2.7.1.

### Internal Changes

- Bumped `central-publishing-maven-plugin` from 0.5.0 to 0.11.0 so Maven Central
  deploy can parse the Portal status response that now includes a `warnings` field.
  0.5.0 crashed after a successful upload, which made the publisher job look failed
  even when the bundle was already in the Portal.
- The JNI and Java release workflows can optionally import laptop-built macOS
  dylibs from a GitHub Release when CI has no macOS runners.

### Breaking Changes

### Deprecations

### API Changes

## Release v1.4.0

### New Features and Improvements

- Added `HeadersProvider` support to `StreamBuilder` for custom authentication on JSON, Protocol Buffer, and Arrow streams, including stream recreation and credential invalidation callbacks.

### Bug Fixes

- Arrow builders now reject unsupported ACK callbacks instead of silently
  discarding them. Configuring `ackCallback` before calling `ArrowStreamBuilder.build()`
  throws `IllegalStateException`.
- Fixed proto, JSON, and Arrow stream recovery losing unacknowledged data during `close()`. Closed
  native streams now remain available until Java has cached their recovery records or batches, and
  recreation releases retained source streams after copying their recovery data. Failed recreation
  attempts also release their temporary native streams.

### API Changes

- Added `HeadersProvider` and `StreamBuilder.headersProvider()` for custom authentication.

## Release v1.3.0

### New Features and Improvements

- Built on Rust SDK 2.3.1: the JNI layer (`rust/jni/`) now depends on
  `databricks-zerobus-ingest-sdk = "2.3.1"` (was 2.0.1).
- Added native library support for Linux musl (Alpine) on x86_64 and aarch64. The libc flavor is detected automatically at runtime; override with `-Dzerobus.libc=musl|glibc`.
- Added `ZerobusSdk.streamBuilder()`, a fluent builder for creating streams that mirrors the Rust SDK's `stream_builder()`. It supports JSON, Protocol Buffer, and Arrow Flight streams through a single chainable API and is now the recommended way to create streams. See `StreamBuilder`.
- `ZerobusSdk` now has a three-argument constructor accepting an optional `applicationName`
  parameter. When set, it is appended to the HTTP `user-agent` header on gRPC requests to the
  Zerobus service (it is not sent on requests to the login service that mint the OAuth token),
  so callers can be identified in server-side telemetry. The wire value becomes
  `zerobus-sdk-java/<version> <applicationName>` (e.g. `zerobus-sdk-java/1.3.0 my-app/1.0`).
  The existing two-argument constructor is unchanged.

### Bug Fixes

- Lowered the glibc baseline for bundled Java JNI Linux artifacts to glibc 2.26, enabling the
  standard Java SDK JAR to run on Amazon Linux 2 environments such as Mule Runtime 4.11.6e without
  requiring customers to rebuild the native library.
- Fixed the `GenerateProto` tool rejecting `VARIANT` columns with `Unsupported column type` (and `Unsupported array/map element type` when nested). `VARIANT` now maps to `string` (unshredded JSON-encoded text) at the top level and inside `ARRAY`/`MAP`, matching the other SDKs.
- Fixed the default `recoveryRetries`, which was `3` instead of the `4` used by the Rust core and every other SDK (Go, TypeScript, C++). A stream left with the default now makes 4 recovery attempts on transient failures instead of 3, matching the documented cross-SDK behavior. Callers that set `recoveryRetries` explicitly are unaffected. (#438)

### Documentation

- Reworked README and Javadoc to steer users toward the high-throughput ingestion pattern: ingest with `ingestRecordOffset()` in a loop without waiting, then `flush()` (or `waitForOffset()` on the last offset) once. Added a performance callout warning against waiting for an acknowledgment after every record, documented `AckCallback` as the non-blocking alternative, and rewrote the Quick Start example to use the offset-based API instead of the deprecated `ingestRecord().join()` loop.

### Internal Changes

- The Java SDK now generates its protobuf classes from the canonical `rust/sdk/zerobus_service.proto` instead of a local copy under `src/main/proto/`. This reconciles schema drift: the generated classes now include the batch-ingest messages (`JsonRecordBatch`, `ProtoEncodedRecordBatch`, `IngestRecordBatchRequest`) that the canonical schema already defined. Purely additive — no change to the hand-written `com.databricks.zerobus` public API, and batch ingestion (`ingestRecordsOffset`) already worked via the JNI boundary.
- Added a `maven-enforcer-plugin` rule that fails `mvn package`/`install`/`deploy`
  if the JNI native libraries (`linux-x86_64`, `linux-aarch64`,
  `linux-musl-x86_64`, `linux-musl-aarch64`, `windows-x86_64`) aren't staged
  under `src/main/resources/native/`. v1.2.0 was published with an empty JAR
  because the release pipeline ran `mvn deploy` on a fresh checkout without
  staging natives; this guard makes that failure mode impossible going forward.
  Local Java-only builds that don't need packaged natives can skip this specific
  rule with `-Dzerobus.skipNativeLibCheck=true`.

### Deprecations

- Deprecated `ZerobusSdk.createJsonStream`, `ZerobusSdk.createProtoStream`, and
  `ZerobusSdk.createArrowStream` (all overloads) in favor of `ZerobusSdk.streamBuilder()`. The
  deprecated methods continue to work unchanged and are scheduled for removal in the next major
  release.

### API Changes

- Added `StreamBuilder` and its typed sub-builders (`StreamBuilder.JsonStreamBuilder`,
  `StreamBuilder.ProtoStreamBuilder`, `StreamBuilder.ArrowStreamBuilder`), returned by
  `ZerobusSdk.streamBuilder()`.
- Added a three-argument `ZerobusSdk(String serverEndpoint, String unityCatalogEndpoint, String
  applicationName)` constructor. The existing two-argument constructor delegates to it with a
  `null` application name.

## Release v1.2.0

### New Features and Improvements

- **Built on Rust SDK 2.0.1**: The JNI layer (`rust/jni/`) now depends on
  `databricks-zerobus-ingest-sdk = "2.0.1"` (was 2.0.0). The Java-facing API
  surface (`ZerobusSdk`, `ZerobusProtoStream`, `ZerobusJsonStream`,
  `ZerobusArrowStream`, `StreamConfigurationOptions`,
  `ArrowStreamConfigurationOptions`) is unchanged.
- **Arrow Flight promoted to Beta**: `ZerobusArrowStream` /
  `ArrowStreamConfigurationOptions` and the surrounding documentation
  (README, examples) are no longer labelled experimental. The API is
  stabilising but may still change before reaching GA. (Mirrors the Rust
  SDK 2.0.1 promotion.)
- **`ArrowStreamConfigurationOptions`**: Added `streamPausedMaxWaitTimeMs` for the maximum time (milliseconds) to wait in the paused state during graceful close (`-1` = full server duration, `0` = immediate recovery).
- **Java SDK identifier on the wire**: The SDK now reports itself as
  `zerobus-sdk-java/<version>` on the HTTP `user-agent` header (previously
  it inherited the Rust SDK identifier `zerobus-sdk-rs/<rust-version>`).
  Server-side telemetry can now distinguish Java clients from Rust clients.

### Behavior Changes

- **`StreamConfigurationOptions.maxInflightRecords` default raised from
  `50_000` to `1_000_000`** to match the Rust SDK 2.0.1 default. The Java
  wrapper previously hard-coded 50k while the Rust SDK quietly raised its
  default to 1M, so Java clients ran with a 20× lower in-flight ceiling
  than Rust clients. Callers who relied on the old cap can pin it back
  explicitly:

  ```java
  StreamConfigurationOptions.builder()
      .setMaxInflightRecords(50_000)
      .build();
  ```

### Documentation

- Updated `ArrowIngestionExample.java` to demonstrate all three IPC
  compression codecs end-to-end. Opens three streams in sequence (`NONE`,
  `LZ4_FRAME`, `ZSTD`), ingests 10 batches per stream, then
  `waitForOffset` + `flush` + `close`.
- Updated `README.md`, `examples/README.md`, `examples/arrow/README.md`,
  and Javadoc on `ZerobusArrowStream` / `ArrowStreamConfigurationOptions`
  / `ZerobusSdk.createArrowStream` to reflect the Beta promotion.
- Documented the JDK 9+ `--add-opens=java.base/java.nio=...` JVM flags
  required by `arrow-memory-netty` 17.x when using `ZerobusArrowStream`,
  in both the main README and the Arrow examples README.

### Internal Changes

- `maven-surefire-plugin` now passes the required `--add-opens` flags so
  Arrow integration tests run cleanly on JDK 9+ without per-developer
  setup.
- Bumped `zerobus-jni` crate version from `1.1.1` to `1.2.0` (used by the
  Java SDK identifier embedded at compile time via `CARGO_PKG_VERSION`).

## Release v1.1.1

### Bug Fixes

- Fixed `isClosed()` returning `false` after the stream internally closed due to non-retryable errors (e.g. server rejection, auth failure). The JNI layer now checks both the wrapper state and the Rust stream's internal closed flag. Affects both proto/JSON streams and Arrow streams.

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
