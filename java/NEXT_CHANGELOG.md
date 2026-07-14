# NEXT CHANGELOG

## Release v1.3.0

### Major Changes

### New Features and Improvements

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

### Breaking Changes

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
