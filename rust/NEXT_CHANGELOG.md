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
  appended to the `x-zerobus-sdk` gRPC metadata header on every request. The
  default `zerobus-sdk-rs/<version>` prefix is preserved for server-side
  telemetry, so the wire value becomes `zerobus-sdk-rs/<version> <application_name>`.
  The SDK now owns this header at the gRPC layer: any `x-zerobus-sdk` value
  returned by a `HeadersProvider` is ignored.

### Bug Fixes

### Documentation

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

### Breaking Changes

- Major-version bumps of `prost` (0.13 → 0.14), `tonic` (0.13 → 0.14),
  `prost-reflect` (0.14 → 0.16), and the Arrow crates (56 → 58). Downstream
  consumers that directly handle SDK-exported `prost::Message` or
  `arrow_array::RecordBatch` values must move to the matching major
  versions of those crates.

### Deprecations

### API Changes

## Release v1.3.0

### Major Changes

### New Features and Improvements

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
