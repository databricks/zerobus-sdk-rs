# Version changelog

## Release v2.0.1

### Major Changes

### New Features and Improvements

### Bug Fixes

- **Arrow Flight reconnect race condition fixed** (`arrow_stream.rs`): on unexpected stream failures (ack timeout, server-side error, network drop) the supervisor now sets `is_paused = true` before starting the reconnect sequence, matching the behaviour already present for the server-close-signal path. Previously, a concurrent `ingest_batch` call could acquire the new sender between its installation and the `ingest_mutex` acquisition in `reconnect()`, sending a batch with a stale physical offset on the fresh stream. The pause gate is lifted only after `reconnect()` completes and `physical_offset_generator` is repositioned to `replay_offset`.

- **Arrow Flight auto-chunking restored** (`arrow_stream.rs`): `ingest_batch` now splits `RecordBatch` payloads into multiple ≤2 MiB Flight messages when needed. The same chunking applies during reconnect replay so recovery of large pending batches is also safe.

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations


## Release v2.0.0

### New Features and Improvements

- **Arrow Flight ingestion promoted to Beta**: The `arrow-flight` feature
  (`ZerobusArrowStream`, `ArrowStreamConfigurationOptions`, and related types)
  is no longer labelled experimental/unsupported. The API is stabilising but
  may still change before reaching GA.
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
- Added an Arrow Flight example under `examples/arrow/` (`example_arrow`)
  demonstrating both `ingest_batch` (RecordBatch) and `ingest_ipc_batch`
  (Arrow IPC bytes).

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

## Release v1.3.0

### New Features and Improvements

- **Arrow Flight — graceful stream close**: When the server signals that the stream will close, the SDK enters a paused state: it stops sending new batches, drains in-flight acknowledgments up to a configurable wait, then recovers.
- **`stream_paused_max_wait_time_ms`** on `ArrowStreamConfigurationOptions`: Optional cap (milliseconds) on how long to wait during that paused phase (`None` = use full server duration, `Some(0)` = recover immediately, `Some(x)` = wait up to `min(x, server_duration)`).
- Added `ZerobusSdkBuilder::connector_factory` for programmatic proxy
  configuration. Callers can install a `ConnectorFactory` (a
  `Fn(&str) -> Option<ProxyConnector>` closure) that fully overrides the
  default env-var proxy detection — useful for embedders that already model
  proxy config in their own configuration system (e.g. Vector's `ProxyConfig`).
  When no factory is installed, the existing `grpc_proxy` / `https_proxy` /
  `http_proxy` env-var behavior is unchanged.
- The env-var proxy path now supports `https://` proxy URLs. The client→proxy
  hop does a TLS handshake using the system trust store; the CONNECT tunnel
  still carries raw TCP so tonic applies end-to-end TLS to the target endpoint
  on top.
- **`StreamBuilder` API**: New fluent builder for creating ingestion streams.
  Setters can be called in any order; the builder validates at `build()` time
  that both authentication and format have been configured.

### Bug Fixes

- **gRPC / HTTP/2 teardown on close and recovery**: Receive and send tasks now shut down with a per-stream `CancellationToken`, bounded waits before `abort`, and a separate `recv_drain_token` on the receiver. This avoids racing **`RST_STREAM` / `CANCEL`** from the client against **`END_STREAM`** from the server—failure modes that could show up as HTTP/2 protocol errors or broken pipe on the server.
- After the inbound receive loop exits, the response-stream drain is now split by exit reason: the close path (`recv_drain_token`) drains **inline** so the server sees `END_STREAM` before the client process exits and the runtime tears down; the recovery / error paths drain in a **detached** task so `flush()` and stream recovery aren't delayed.
- **`StreamBuilder::stream_paused_max_wait_time_ms`**: Now updates Arrow stream settings (`arrow_config`) as well as JSON/proto gRPC settings, so `.build_arrow()` respects this option (previously only JSON/proto streams saw the value).

### Internal Changes

- Reduced log verbosity in `wait_for_offset` / `wait_for_acks` polling loops.
  Per-iteration progress logs are now emitted at `trace` level, and the
  one-shot "completed" log is now at `debug` level (previously `info`). This
  removes repeated `info`-level noise observed when callers wait for flushes
  or graceful close.

### Deprecations

- **`ZerobusSdk::create_stream()`**: Use `sdk.stream_builder(table).oauth(id, secret).json().build().await` instead
- **`ZerobusSdk::create_stream_with_headers_provider()`**: Use `sdk.stream_builder(table).headers_provider(p).json().build().await` instead
- **`ZerobusSdk::create_arrow_stream()`**: Use `sdk.stream_builder(table).oauth(id, secret).arrow(schema).build_arrow().await` instead
- **`ZerobusSdk::create_arrow_stream_with_headers_provider()`**: Use `sdk.stream_builder(table).headers_provider(p).arrow(schema).build_arrow().await` instead

### API Changes

- New public exports: `ProxyConnector`, `ConnectorFactory`, `StreamBuilder`.
- New builder method: `ZerobusSdkBuilder::connector_factory`.
- New entry point: `ZerobusSdk::stream_builder()`.
- Changed `ZerobusSdk` fields `workspace_id` and `tls_config` to `pub(crate)` visibility (no public API impact).

## Release v1.2.0

### Major Changes

- **License: Migrated from the Databricks License to the Apache License 2.0**

### New Features and Improvements

- Added the `schema` module with `descriptor_from_uc_columns` /
  `descriptor_from_uc_schema`, which convert a Unity Catalog table schema
  (including nested `STRUCT`, `ARRAY`, and `MAP` columns via `type_json`) into
  a `prost_types::DescriptorProto` that can be passed to
  `TableProperties::descriptor_proto`. Enables building descriptors at runtime
  without pre-generating `.proto` files.

### Internal Changes

- The `generate_files` CLI tool now delegates schema → descriptor conversion
  to the SDK's new `schema` module instead of its own hand-rolled DDL-string
  parser, and renders the resulting `DescriptorProto` back to proto2 text.

### Breaking Changes

- `generate_files`: the emitted `.proto` files have changed shape for
  non-trivial schemas. Consumers regenerating existing files should expect:
  - Field numbers now follow Unity Catalog's `position + 1` (so gaps from
    `DROP COLUMN` under Delta column-mapping are preserved) instead of the
    previous 1,2,3… sequential numbering with a 19000-range skip.
  - Nested struct messages use path-based names (e.g. `OuterInner` instead of
    `Inner`) and are emitted hierarchically inside their parent message.
  - Struct field nullability now honors Unity Catalog's `nullable` flag
    instead of being forced to `optional`.

## Release v1.1.0

### New Features and Improvements

- **[Experimental Arrow Flight] Zero-copy IPC ingestion via `ingest_ipc_batch`**: Added `ZerobusArrowStream::ingest_ipc_batch(Bytes)` for FFI callers (Go, Python, Java, TypeScript) that already hold Arrow IPC stream bytes. Raw bytes are forwarded directly to the Flight wire format without deserialising to a `RecordBatch` and re-serialising, eliminating one IPC round-trip per batch compared to `ingest_batch`. The existing `ingest_batch` API is unchanged.

### Bug Fixes

- Fixed proto generation tool to skip reserved field numbers 19000-19999 for tables with more than 19000 columns

## Release v1.0.1

### Bug Fixes
- Fixed TLS certificate validation failure when behind corporate VPN/proxy with MITM certificates (e.g., GlobalProtect). Changed `reqwest` TLS configuration from `rustls-tls` to `rustls-tls-native-roots` + `rustls-tls-webpki-roots`, so the SDK now loads CA certificates from the OS native trust store (respecting `SSL_CERT_FILE` and system certificate stores) while keeping bundled Mozilla roots as a fallback for minimal environments.

### New Features and Improvements
- Exported `OAuthHeadersProvider` in the public API, allowing clients to directly construct and use the built-in OAuth 2.0 headers provider.

## Release v1.0.0

GA release of the Databricks Zerobus Ingest SDK for Rust.

### New Features and Improvements
- Added HTTP proxy support via standard environment variables (`grpc_proxy`, `https_proxy`, `http_proxy`), following gRPC core conventions. Proxied connections use HTTP CONNECT tunneling with end-to-end TLS. Supports `no_grpc_proxy` / `no_proxy` for bypass rules.

### Deprecations

### Bug Fixes
- Fixed a rare race condition in `wait_for_offset_internal` where the actual server error (e.g., `InvalidArgument`) was lost and replaced by a generic `StreamClosedError`. This occurred when `error_rx.changed()` fired but `is_closed` had not yet been set by the supervisor, causing the error to be missed on the next loop iteration.

## Release v0.6.0

### New Features and Improvements

- **Automatic `https://` scheme prepending**: Endpoints without a scheme now automatically get `https://` prepended. Previously, schemeless endpoints would fail with `InvalidUri` (builder) or fail to extract the workspace ID (deprecated `new()` constructor).

### Documentation

- Updated all examples to consistently include `https://` in endpoint URLs

## Release v0.5.0

### New Features and Improvements

- **Builder Pattern for SDK Initialization**: Added `ZerobusSdk::builder()` for fluent SDK configuration
  - `.endpoint()` - Set the Zerobus endpoint (~~scheme is optional, defaults work with or without `https://`~~ `https://` is required; schemeless endpoints are auto-prepended since v0.6.0)
  - `.unity_catalog_url()` - Set the Unity Catalog URL (optional when using custom headers providers)
  - `.tls_config()` - Provide a custom `TlsConfig` implementation (defaults to `SecureTlsConfig`)
- **Configurable TLS via `TlsConfig` trait**: TLS is now configured through a strategy pattern
  - `SecureTlsConfig` (default) - Production TLS with system CA certificates
  - `NoTlsConfig` - No-op TLS for testing with plaintext `http://` endpoints (requires `testing` feature)
  - Implement `TlsConfig` trait for custom certificate handling
- **SDK Identifier Header**: Renamed `user-agent` header to `x-zerobus-sdk` for clearer SDK identification in gRPC metadata
- **Type Widening for Record Ingestion**: Added wrapper types for record ingestion
  - **`ProtoMessage<T>`**: SDK handles encoding - pass any `prost::Message` directly
  - **`JsonValue<T>`**: SDK handles serialization - pass any `serde::Serialize` type directly
  - **`ProtoBytes`**: Client handles encoding - explicit wrapper for pre-encoded protobuf bytes
  - **`JsonString`**: Client handles serialization - explicit wrapper for pre-serialized JSON strings
  - **Backward compatible**: existing code using `Vec<u8>` and `String` continues to work
  - Works with both single record and batch ingestion methods

### Deprecations

- **`ZerobusSdk::new()`**: Use `ZerobusSdk::builder()` instead
- **`ZerobusSdk.use_tls` field**: TLS is now controlled via the `TlsConfig` trait passed to the builder


### Bug Fixes

- **[Experimental] Record-based acknowledgment tracking for Arrow Flight streams**: Added cumulative record counting to support proper ack tracking and correct recovery when batches are auto-chunked.

### Documentation

- Reorganized examples directory structure: `json/single`, `json/batch`, `proto/single`, `proto/batch`
- Added separate README files for JSON and Protocol Buffers examples with comprehensive documentation
- Updated all examples to demonstrate three data-passing approaches: auto-encoding/serializing wrappers, pre-encoded/serialized wrappers, and backward-compatible raw types

### Internal Changes


### API Changes

- **Added `ZerobusSdkBuilder`** for fluent SDK configuration (replaces `ZerobusSdk::new()`)
- **Added `TlsConfig` trait** with `SecureTlsConfig` (default) and `NoTlsConfig` (behind `testing` feature)
- **Renamed header** from `user-agent` to `x-zerobus-sdk` in gRPC metadata
- **Added type widening wrapper types** (backward compatible):
  - Added `ProtoMessage<T: prost::Message>` - SDK handles encoding for protobuf messages
  - Added `JsonValue<T: serde::Serialize>` - SDK handles serialization for JSON objects
  - Added `ProtoBytes` - for pre-encoded protobuf bytes (client handles encoding)
  - Added `JsonString` - for pre-serialized JSON strings (client handles serialization)
  - All new types implement `Into<EncodedRecord>` for seamless integration
  - Existing `Vec<u8>` and `String` types continue to work (backward compatible)

## Release v0.4.0

### New Features and Improvements

- **Acknowledgment Callbacks**: Added callback support for receiving notifications when records are acknowledged
  - New `AckCallback` trait with `on_ack()` and `on_error()` methods
  - Configurable via `ack_callback` field in `StreamConfigurationOptions` and `ArrowStreamConfigurationOptions`

- Added support for `TINYINT/BYTE`, `TIMESTAMP_NTZ`, and `VARIANT` data types in the proto generation tool

- **Alternative Ingestion API with Direct Offset Return**: Added `ingest_record_offset()` and `ingest_records_offset()` methods
  - Return `OffsetId` (logical offset) directly as an integer (after queuing) instead of wrapping it in a Future
  - Can be used with new `wait_for_offset()` method to block on acknowledgment when needed
  - Allows decoupling record ingestion from acknowledgment tracking
  - Useful for scenarios where you want to collect offsets and wait on them selectively

### Deprecations

- **Deprecated `ingest_record()` and `ingest_records()` methods**: Use `ingest_record_offset()` and `ingest_records_offset()` instead
  - The new methods return offsets directly (after queuing) without Future wrapping for a cleaner API
  - Use with `wait_for_offset()` to explicitly wait for acknowledgments when needed
  - Old methods will continue to work but may be removed in a future version

### Bug Fixes

- Improved error propagation in `wait_for_offset()` and `flush()`: errors from the server are now detected and returned immediately instead of waiting for timeout, providing faster feedback and more accurate error messages

- Improved error classification in OAuth token retrieval: 5xx server errors and network failures are now retryable, while 4xx client errors (invalid credentials, etc.) are non-retryable

### Documentation

### Internal Changes

- Refactored `wait_for_offset_internal` to remove unnecessary double loop
- Optimized gRPC channel reuse: `ZerobusSdk` now reuses a single gRPC channel across multiple stream creations instead of creating a new channel for each stream, improving connection efficiency and reducing resource overhead
- Enhanced background tasks with `is_closed` checks and proper error broadcasting to the shared error channel, ensuring timely shutdown and accurate error reporting

- Added `user-agent` header to all gRPC requests for SDK version tracking

- Refactored `flush()` and `wait_for_offset()` to share common waiting logic via `wait_for_offset_internal()`, reducing code duplication and ensuring consistent behavior

- Improved graceful close mechanism: when server signals stream closure, SDK now continues processing acknowledgments for in-flight records while pausing new record transmission until timeout.

### API Changes

- [**BREAKING**] Added `callback_max_wait_time_ms` to `StreamConfigurationOptions` to limit how long callbacks may run after stream closure (`None` = infinite, `Some(x)` = `x` ms).
- Added `ack_callback: Option<Arc<dyn AckCallback>>` field to `StreamConfigurationOptions` and `ArrowStreamConfigurationOptions` for acknowledgment callbacks
- Added `AckCallback` trait with `on_ack(offset_id)` and `on_error(offset_id, error_message)` methods

- Added Arrow IPC compression support via `ipc_compression: Option<CompressionType>` in `ArrowStreamConfigurationOptions` (supports `LZ4_FRAME` and `ZSTD`, default: `None`)
- **[BREAKING]** Changed `ZerobusArrowStream::ingest_batch()` to return `OffsetId` directly instead of `Future<Output = OffsetId>`. Use `wait_for_offset(offset)` to explicitly wait for acknowledgment
- Added `ZerobusArrowStream::wait_for_offset()` method to wait for acknowledgment of a specific offset
- Added `is_closed` check at the beginning of `flush()` for both `ZerobusStream` and `ZerobusArrowStream`

- Added `ingest_record_offset()` method to `ZerobusStream` for direct offset return without Future wrapping
- Added `ingest_records_offset()` method to `ZerobusStream` for batch ingestion with direct offset return
- Added `wait_for_offset()` method to `ZerobusStream` to wait for acknowledgment of a specific offset

- [**BREAKING**] Added `stream_paused_max_wait_time_ms` to `StreamConfigurationOptions` to configure maximum wait time during graceful stream close (`None` = wait for full server duration, `Some(0)` = immediate recovery, `Some(x)` = wait up to min(x, server_duration) milliseconds)

## Release v0.3.0

### New Features and Improvements

- **Arrow Flight Ingestion (Experimental)**: Added experimental Arrow Flight support for high-throughput Apache Arrow record batch ingestion
  - Opt-in feature: enable with `features = ["arrow-flight"]` in Cargo.toml
  - Transmits Arrow RecordBatches in native IPC format (no format conversion required)
  - Same recovery and retry semantics as gRPC streams
  - **Note**: This feature is currently experimental and unsupported

## Release v0.2.0

### New Features and Improvements

- **Batch Ingestion API**: Added `ingest_records()` method for ingesting multiple records at once
  - All-or-nothing semantics: entire batch succeeds or fails as a unit
  - Ingesting an empty batch is a no-op.
- **JSON Serialization Support**: Added support for JSON record serialization alongside Protocol Buffers (default)
  - No protobuf schema compilation required
- Added `HeadersProvider`, a trait for flexible authentication strategies
- Implemented `OAuthHeadersProvider` for OAuth 2.0 Client Credentials flow (default authentication method used by `create_stream()`)

### Bug Fixes

### Documentation

- Added JSON and protobuf serialization examples for batch ingestion
- Enhanced API Reference with batch ingestion documentation
- Added JSON and protobuf serialization examples
- Updated README's.
- Enhanced API Reference with JSON mode documentation
- Added Azure workspace and endpoint URL examples

### Internal Changes

- [**BREAKING**] Changed backpressure mechanism to track in-flight requests instead of in-flight records

### API Changes

- [**BREAKING**] changed `max_inflight_records` to `max_inflight_requests` in `StreamConfigurationOptions` as we now track in-flight requests
- [**BREAKING**] `get_unacked_records()` method now returns `impl Iterator<Item = EncodedRecord>` instead of `Vec<Vec<u8>>` - flattens all batches into individual records
- Added `get_unacked_batches()` method to `ZerobusStream` that returns `Vec<EncodedBatch>` to preserve batch structure - records ingested together remain grouped
- Added `ingest_records()` method to `ZerobusStream` for bulk record ingestion
- `recreate_stream` method in `ZerobusSdk` now accepts a reference to a stream, instead of taking ownership of it.
- `TableProperties` struct now has `descriptor_proto` field as optional (**breaking change**).
- Added `HeadersProvider` trait for custom header strategies
- Added `OAuthHeadersProvider` struct for OAuth 2.0 authentication with Databricks OIDC endpoint
- Added `create_stream_with_headers_provider` method to `ZerobusSdk` for custom authentication header providers

## Release v0.1.1

- Added comprehensive API documentation and fixed Cargo.toml metadata for crates.io publication

## Release v0.1.0

Initial release of the Databricks Zerobus Ingest SDK for Rust.

### API Changes

- Added `ZerobusSdk` struct for creating ingestion streams.
- Added `ZerobusStream` struct for managing the stateful gRPC stream.
- The `ingest_record` method returns a future that resolves to the record's acknowledgment offset.
- Added `TableProperties` for configuring the target table schema and name.
- Added `StreamConfigurationOptions` for fine-tuning stream behavior like recovery and timeouts.
- Added `ZerobusError` enum for detailed error handling, including a `is_retryable()` method.
- The SDK is built on `tokio` and is fully asynchronous.
