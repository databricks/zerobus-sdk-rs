# NEXT CHANGELOG

## Release v2.0.0

### Major Changes

### New Features and Improvements

- **Arrow schema from UC schema** (feature `arrow-flight`):
  `schema::arrow_schema_from_uc_columns` and `schema::arrow_schema_from_uc_schema`
  build an `arrow_schema::Schema` directly from Unity Catalog metadata, parallel
  to the existing `descriptor_from_uc_*` functions. Emits native Arrow types
  (`Date32`, `Timestamp(Microsecond, ..)`, `LargeUtf8`, `LargeBinary`,
  `Map("entries", Struct{keys,values})`) matching the canonical Arrow schema
  the Databricks Arrow Flight server builds from Delta.
- **`ZerobusSdkBuilder::application_name`**: Set a custom application identifier
  appended to the HTTP `user-agent` header (sent on the underlying tonic
  `Endpoint`) on every request. The default `zerobus-sdk-rs/<version>` prefix
  is preserved for server-side telemetry, so the wire value becomes
  `zerobus-sdk-rs/<version> <application_name>`. The previous `x-zerobus-sdk`
  gRPC metadata header is no longer emitted; downstream consumers that parsed
  it should switch to reading `user-agent`.
- **`ZerobusSdkBuilder::sdk_identifier`**: Override the SDK prefix of the
  HTTP `user-agent` header, replacing the default `zerobus-sdk-rs/<version>`.
  Intended for wrapper SDKs that need to replace the SDK identification; most
  callers should prefer `application_name`, which preserves the SDK version
  prefix. When both are set, `application_name` is still appended, so the wire value becomes `<sdk_identifier> <application_name>`.

### Bug Fixes

- Corrected the values returned by the C FFI `zerobus_get_default_config()`
  for `callback_max_wait_time_ms` / `has_callback_max_wait_time_ms`. The
  function previously reported `0 / false` (i.e., "no callback timeout"),
  while the actual Rust SDK default is `Some(5000ms)`. The C-side defaults
  now correctly mirror the Rust defaults (`5000 / true`).

### Documentation

- Updated `rust/README.md`, `rust/examples/README.md`,
  `rust/examples/json/README.md`, and `rust/examples/proto/README.md` to
  remove all references to the deleted future-based APIs. The
  "Future-based API (Deprecated)" example sections and the deprecated
  method entries in the API Reference were removed.

### Internal Changes

- Consolidated Cargo workspace dependencies under `[workspace.dependencies]`
  in `rust/Cargo.toml`; member crates now use `dep.workspace = true` so
  versions are pinned in one place.
- Collapsed the four example packages (`example_json_{single,batch}`,
  `example_proto_{single,batch}`) into two packages,
  `rust-examples-json` and `rust-examples-proto`, each exposing two
  `[[example]]` targets. Examples are invoked as
  `cargo run -p rust-examples-json --example json_{single,batch}` and
  `cargo run -p rust-examples-proto --example proto_{single,batch}`.
- Bumped `prost` and `prost-types` from 0.13 to 0.14; `prost-reflect` from
  0.14 to 0.16. Public APIs that name `prost::Message` (e.g.
  `ProtoMessage<T: prost::Message>`) now require callers to use prost 0.14
  messages.
- Bumped `tonic` from 0.13 to 0.14. The 0.14 release splits code generation
  into separate crates: build-time codegen now uses `tonic-prost-build`
  (replacing `tonic-build`), and the runtime depends on the new
  `tonic-prost` crate for the prost codec. `sdk/build.rs`, `tests/build.rs`,
  and `tools/generate_files/src/generate.rs` were updated accordingly.
- Bumped Arrow crates (`arrow-flight`, `arrow-array`, `arrow-schema`,
  `arrow-ipc`) from 56.2.0 to 58.2. Switched `IpcDataGenerator::encoded_batch`
  to the non-deprecated `encode` API which takes an explicit
  `CompressionContext`.
- Raised minimum-version floors on several non-breaking dependencies to
  current latest minor: `tokio` 1.42 → 1.52, `tokio-stream` 0.1.16 →
  0.1.18, `tokio-util` 0.7.17 → 0.7.18, `once_cell` 1.19 → 1.21,
  `bytes` 1 → 1.11, `tempfile` 3.21 → 3.27, `clap` 4 → 4.6,
  `urlencoding` 2 → 2.1.
- Migrated the FFI and JNI crates off the deleted stream-creation methods.
  Both wrappers now build streams via `StreamBuilder`. Default config in
  `zerobus_get_default_config()` / `zerobus_arrow_get_default_config()`
  now reads `stream_options::defaults::*` constants directly instead of
  constructing `*ConfigurationOptions` (no longer needed at the FFI layer).
  No C ABI or JNI signature changes.
- FFI and JNI no longer construct `StreamConfigurationOptions` /
  `ArrowStreamConfigurationOptions`. They read C/Java struct fields
  directly and apply each via builder setters.

### Breaking Changes

- Removed `ZerobusSdk::create_stream()` (in deprecation since v1.3.0).
  Use `sdk.stream_builder().table(name).oauth(id, secret).json()` /
  `.compiled_proto(desc).build().await` instead. Removed from all
  examples, documentation, and tests.
- Removed `ZerobusSdk::create_stream_with_headers_provider()` (in
  deprecation since v1.3.0). Use
  `sdk.stream_builder().table(name).headers_provider(p).json()` /
  `.compiled_proto(desc).build().await` instead. Removed from all
  examples, documentation, and tests.
- Removed `ZerobusSdk::create_arrow_stream()` *(feature `arrow-flight`)*
  (in deprecation since v1.3.0). Use
  `sdk.stream_builder().table(name).oauth(id, secret).arrow(schema).build_arrow().await`
  instead. Removed from all examples, documentation, and tests.
- Removed `ZerobusSdk::create_arrow_stream_with_headers_provider()`
  *(feature `arrow-flight`)* (in deprecation since v1.3.0). Use
  `sdk.stream_builder().table(name).headers_provider(p).arrow(schema).build_arrow().await`
  instead. Removed from all examples, documentation, and tests.
- Removed `ZerobusStream::ingest_record()` (in deprecation since v0.4.0).
  Use `stream.ingest_record_offset(payload).await?` followed by
  `stream.wait_for_offset(offset).await?` to wait for acknowledgment.
  Removed from all examples, documentation, and tests.
- Removed `ZerobusStream::ingest_records()` (in deprecation since v0.4.0).
  Use `stream.ingest_records_offset(payloads).await?` followed by
  `stream.wait_for_offset(offset).await?`. Removed from all examples,
  documentation, and tests.
- Removed `ZerobusSdk::new()` (in deprecation since v0.5.0). Use
  `ZerobusSdk::builder().endpoint(...).unity_catalog_url(...).build()?`
  instead.
- Removed the `ZerobusSdk::use_tls` field (in deprecation since v0.5.0).
  TLS is controlled via `ZerobusSdkBuilder::tls_config(...)`. The C FFI
  `zerobus_sdk_set_use_tls()` function is retained as a no-op for ABI
  compatibility.
- Removed the `test_proto_stream_creation_without_descriptor_fails` test
  — the typestate `StreamBuilder` makes that scenario impossible at
  compile time.
- Added `#[non_exhaustive]` to `StreamConfigurationOptions`. External
  crates can no longer construct the struct via struct-literal syntax;
  all configuration must go through `StreamBuilder` setters. Field reads
  via `stream.options.*` are unaffected. Adding new config fields in
  future releases is now non-breaking.
- Added `#[non_exhaustive]` to `ArrowStreamConfigurationOptions`. Same
  semantics as above; reads via `stream.options().*` are unaffected.
- Added `#[non_exhaustive]` to `ZerobusError`, `StreamType`, and
  `SchemaError` enums. External `match` expressions on these types now
  require a `_ =>` wildcard arm. Adding new variants is non-breaking.
- Added `#[non_exhaustive]` to `ZerobusSdk`, `ZerobusStream`, and
  `ZerobusArrowStream` structs. Adding new fields to these top-level
  handle types is non-breaking.
- `TableProperties` and `ArrowTableProperties` are now `pub(crate)` and
  no longer part of the public API. They are only used internally by
  `StreamBuilder`; after the deletion of the deprecated
  `create_*_stream()` methods there are no external constructors.
- Removed `ZerobusArrowStream::table_properties()` getter (returned the
  now-private `ArrowTableProperties`). Use the existing `table_name()`
  and `schema()` getters instead.
- Major-version bumps of `prost` (0.13 → 0.14), `tonic` (0.13 → 0.14),
  `prost-reflect` (0.14 → 0.16), and the Arrow crates (56 → 58). Downstream
  consumers that directly handle SDK-exported `prost::Message` or
  `arrow_array::RecordBatch` values must move to the matching major
  versions of those crates.

### Deprecations

### API Changes
