# NEXT CHANGELOG

## Release v2.9.0

### Major Changes

### New Features and Improvements

- JSON and protobuf streams now use a dedicated gRPC connection by default.
  Use `ZerobusSdk::builder().connection_per_stream(false)` to retain the prior
  shared HTTP/2 connection behavior. Arrow Flight streams are unchanged.
- On platforms where vendored `protoc` is not available, fallback to trying `protoc` on `PATH` or `PROTOC` environment variable override.

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

- **Adding the Avro record type extends the publicly re-exported gRPC types, which
  can break compilation for some downstream crates.** The additions
  (`RecordType::AVRO`, `CreateIngestStreamRequest.avro_schema_json`,
  `IngestRecordRequest.avro_encoded_record`, `IngestRecordBatchRequest.avro_batch`, and
  the new `AvroRecordBatch`) are wire-compatible — no runtime or behavior change. But
  because the crate re-exports the prost-generated types
  (`databricks_zerobus_ingest_sdk::databricks::zerobus`), code that matches or builds
  them exhaustively will no longer compile. The builder API (`.json()`,
  `.compiled_proto()`, `.dynamic_proto()`) and the other-language SDKs are unaffected.
  The long-term fix (hiding these generated types) is tracked in
  [#822](https://github.com/databricks/zerobus-sdk/issues/822).

  Migration — every fix is additive:
  - Exhaustive `match` on `RecordType` with no `_` arm → add `_ => { … }` (or a
    `RecordType::Avro` arm). (E0004)
  - Exhaustive `match` on the `ingest_record_request::Record` or
    `ingest_record_batch_request::Batch` oneof → add a `_ => { … }` arm. (E0004)
  - Full-field struct literal of `CreateIngestStreamRequest` → add
    `..Default::default()`. (E0063)
  - Full-field struct pattern/destructuring of `CreateIngestStreamRequest`
    (e.g. `let CreateIngestStreamRequest { table_name, descriptor_proto, record_type } = req`)
    → add `..`. (E0027)

  Unaffected: `x == RecordType::Json`, any `match` that already has a `_` arm, and
  struct literals/patterns that already use `..`.

### Deprecations

### API Changes

- Added the Avro wire surface to the generated gRPC types (`RecordType::AVRO`,
  `CreateIngestStreamRequest.avro_schema_json`, `IngestRecordRequest.avro_encoded_record`,
  `IngestRecordBatchRequest.avro_batch`, `AvroRecordBatch`). See **Breaking Changes** for
  the downstream-compilation impact and migration.
