# NEXT CHANGELOG

## Release v0.2.0

### New Features and Improvements

- **Arrow Flight streams**: Added `ZerobusArrowStream` class for ingesting Apache Arrow IPC-encoded RecordBatches into Unity Catalog Delta tables. Includes `IngestBatch`, `WaitForOffset`, `Flush`, `Close`, and `GetUnackedBatches` APIs, with OAuth and `IHeadersProvider` authentication, and async variants (`IngestBatchAsync`, `WaitForOffsetAsync`, `FlushAsync`, `CloseAsync`, `GetUnackedBatchesAsync`). Configure via `ArrowStreamConfigurationOptions` record with IPC compression support (None, LZ4_FRAME, ZSTD). (#532, contributed by @nalyd2)
- **ProtoSchema**: Added `ProtoSchema` class for generating protobuf schemas from Unity Catalog table metadata. `ProtoSchema.FromUnityCatalogJson(ucJson)` creates a schema from the UC API response, `GetDescriptorBytes()` returns the compiled descriptor for stream creation, and `EncodeJson(json)` converts JSON records to protobuf bytes matching the table schema. (#532, contributed by @nalyd2)
- **Fluent StreamBuilder**: Added `StreamBuilder` fluent API as an alternative to factory methods on `ZerobusSdk`. Chain `.Table()`, `.OAuth()`, `.MaxInflightRequests()`, `.Recovery()` etc., then terminate with `.Json()`, `.CompiledProto(bytes)`, or `.Arrow(schema)` sub-builders. Both sync `Build()` and async `BuildAsync()` methods available. (#532, contributed by @nalyd2)

### Deprecations

### Bug Fixes

### Documentation

### Internal Changes

### API Changes
