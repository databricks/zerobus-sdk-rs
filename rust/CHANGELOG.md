# Version changelog

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
