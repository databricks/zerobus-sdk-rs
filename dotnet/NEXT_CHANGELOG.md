# NEXT CHANGELOG

## Release v0.2.0

### New Features and Improvements

- **Arrow Flight streams**: Added `ZerobusArrowStream` class for ingesting Apache Arrow IPC-encoded RecordBatches into Unity Catalog Delta tables. Includes `IngestBatch`, `WaitForOffset`, `Flush`, `Close`, and `GetUnackedBatches` APIs, with OAuth and `IHeadersProvider` authentication. Configure via `ArrowStreamConfigurationOptions` record with IPC compression support (None, LZ4_FRAME, ZSTD). Add to `ZerobusSdk`: `CreateArrowStream`, `CreateArrowStreamAsync`, `CreateArrowStreamWithHeadersProvider`, `CreateArrowStreamWithHeadersProviderAsync`. (#532, contributed by @nalyd2)
- **ProtoSchema**: Added `ProtoSchema` class for generating protobuf schemas from Unity Catalog table metadata. `ProtoSchema.FromUnityCatalogJson(ucJson)` creates a schema from the UC API response, `GetDescriptorBytes()` returns the compiled descriptor for stream creation, and `EncodeJson(json)` converts JSON records to protobuf bytes matching the table schema. (#532, contributed by @nalyd2)

### Deprecations

### Bug Fixes

### Documentation

### Internal Changes

### API Changes
