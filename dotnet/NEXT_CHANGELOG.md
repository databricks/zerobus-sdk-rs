# NEXT CHANGELOG

## Release v0.2.0

### New Features and Improvements

- **Arrow Flight streams**: Added `ZerobusArrowStream` class for ingesting Apache Arrow IPC-encoded RecordBatches into Unity Catalog Delta tables. Includes `IngestBatch`, `WaitForOffset`, `Flush`, `Close`, and `GetUnackedBatches` APIs, with OAuth and `IHeadersProvider` authentication, and async variants (`IngestBatchAsync`, `WaitForOffsetAsync`, `FlushAsync`, `CloseAsync`, `GetUnackedBatchesAsync`). Configure via `ArrowStreamConfigurationOptions` record with IPC compression support (None, LZ4_FRAME, ZSTD). (#532, contributed by @nalyd2)
- **ProtoSchema**: Added `ProtoSchema` class for generating protobuf schemas from Unity Catalog table metadata. `ProtoSchema.FromUnityCatalogJson(ucJson)` creates a schema from the UC API response, `GetDescriptorBytes()` returns the compiled descriptor for stream creation, and `EncodeJson(json)` converts JSON records to protobuf bytes matching the table schema. (#532, contributed by @nalyd2)
- **Fluent StreamBuilder**: Added `StreamBuilder` fluent API as an alternative to factory methods on `ZerobusSdk`. Chain `.Table()`, `.OAuth()`, `.MaxInflightRequests()`, `.Recovery()` etc., then terminate with `.Json()`, `.CompiledProto(bytes)`, or `.Arrow(schema)` sub-builders. Both sync `Build()` and async `BuildAsync()` methods available. (#532, contributed by @nalyd2)

### Deprecations

### Bug Fixes

- Fixed a use-after-free in which a custom `IHeadersProvider` could be freed
  while the Rust core was still inside a `GetHeaders()` call into it during
  connection recovery. Provider ownership is now handed to the FFI via the new
  `free_user_data` destroy callback, which releases the provider's `GCHandle`
  only after any in-flight `GetHeaders()` has returned; the stream no longer
  frees the handle on dispose. This applies to both the synchronous
  (`CreateStreamWithHeadersProvider`) and asynchronous
  (`CreateStreamWithHeadersProviderAsync`) creation paths. Tracks the FFI
  signature change to `zerobus_sdk_create_stream_with_headers_provider` and
  `zerobus_sdk_create_stream_with_headers_provider_async`. No public API change.

### Documentation

- Corrected installation and source-build prerequisites, separated JSON and
  protobuf stream examples, and added a runnable generated-message example.
- Documented the asynchronous ingestion model across the README, XML docs, and
  examples: ingest records in a loop, then call `Flush()` once instead of waiting
  for each record. `WaitForOffset()` is now presented as a targeted wait for a
  specific offset.
- Documented that `GetUnackedRecords()` can fail while the stream is still
  active after a flush timeout, and stopped reporting success after ingest
  failures. Pointed CONTRIBUTING at `src/Zerobus/Native/`.

### Internal Changes

- Made the .NET release workflow build-only, consistent with the other SDKs. It now packs the NuGet package and uploads it as an artifact; publishing and the GitHub Release happen downstream.
- Pin the full NuGet restore graph with `packages.lock.json` and fail CI restore when the lock files are stale (`RestoreLockedMode`).

### API Changes
