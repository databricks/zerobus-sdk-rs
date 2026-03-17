# NEXT CHANGELOG

## Release v1.1.0

### Major Changes

### New Features and Improvements

- **Arrow Flight Support (Experimental)**: Added support for ingesting Apache Arrow `VectorSchemaRoot` batches via Arrow Flight protocol
  - **Note**: Arrow Flight is not yet supported by default from the Zerobus server side.
  - New `ZerobusArrowStream` class with `ingestBatch()`, `waitForOffset()`, `flush()`, `close()`, `getUnackedBatches()` methods
  - New `ArrowStreamConfigurationOptions` for configuring Arrow streams (max inflight batches, recovery, timeouts)
  - New `createArrowStream()` and `recreateArrowStream()` methods on `ZerobusSdk`
  - Accepts `VectorSchemaRoot` directly via `ingestBatch()` (IPC serialization handled internally)
  - Arrow is opt-in: add `arrow-vector` and `arrow-memory-netty` as dependencies (provided scope, `>= 15.0.0`)

### Bug Fixes

- Fixed proto generation tool to skip reserved field numbers 19000-19999 for tables with more than 19000 columns

### Documentation

### Internal Changes

- Added `arrow-vector` 17.0.0 as provided dependency for Arrow Flight support
- Added `arrow-memory-netty` 17.0.0 as test dependency for integration tests
- Uses existing JNI Arrow Flight bindings from Rust SDK (`nativeCreateArrowStream`, `nativeIngestBatch`, etc.)

### Breaking Changes

### Deprecations

### API Changes

- Added `createArrowStream(String tableName, Schema schema, String clientId, String clientSecret)` to `ZerobusSdk`
- Added `createArrowStream(String tableName, Schema schema, String clientId, String clientSecret, ArrowStreamConfigurationOptions options)` to `ZerobusSdk`
- Added `recreateArrowStream(ZerobusArrowStream closedStream)` to `ZerobusSdk`
- Added `ZerobusArrowStream` class with methods: `ingestBatch()`, `waitForOffset()`, `flush()`, `close()`, `getUnackedBatches()`, `isClosed()`, `getTableName()`, `getOptions()`
- Added `ArrowStreamConfigurationOptions` class with fields: `maxInflightBatches`, `recovery`, `recoveryTimeoutMs`, `recoveryBackoffMs`, `recoveryRetries`, `serverLackOfAckTimeoutMs`, `flushTimeoutMs`, `connectionTimeoutMs`
- Added optional dependency: `org.apache.arrow:arrow-vector >= 15.0.0` (provided scope)

