# Version changelog

## Release v2.0.0

### Major Changes

- **New stream-creation API**: `ZerobusSdk.create_stream(...)` now takes
  keyword-only `table=`, `auth=` and `record_format=` arguments. `auth` is a
  tagged-union — `OAuth(client_id, client_secret)` or `Headers(provider)` —
  and `record_format` is `Format.JSON` or `Format.proto(descriptor)`. The new
  shape is evolvable: future releases can add auth strategies and record
  formats by introducing new variants without breaking existing call sites.
  See the README's "Migrating from v1.x" section for examples.
- **Python SDK identifier**: The SDK now reports itself as
  `zerobus-sdk-py/<version>` on the HTTP `user-agent` header
  (previously it inherited the Rust SDK identifier `zerobus-sdk-rs/...`).
  A new `application_name` argument on `ZerobusSdk(...)` appends a
  caller-supplied identifier for server-side telemetry.
- **Zero-copy Arrow ingest path**: `ZerobusArrowStream.ingest_batch(...)` now
  forwards Arrow IPC bytes straight to Arrow Flight via the Rust SDK's
  `ingest_ipc_batch` API when `ipc_compression=IPCCompression.NONE` (the
  default), eliminating one parse + re-serialise round trip on the Rust side.
  Setting a compression codec falls back to the previous `RecordBatch` path.
- **Required by Rust SDK 2.0.0**: this release is built against the Rust SDK
  2.0.0 line, which removes the deprecated `create_stream` /
  `create_arrow_stream` family of methods on `ZerobusSdk`. The PyO3 binding
  has been rewritten to use the typestate-style `StreamBuilder` exclusively.

### Breaking Changes

- **Removed `TableProperties` class** (both Python-level and PyO3-level). Pass
  the table name and protobuf descriptor directly through the new
  `table=` / `record_format=Format.proto(...)` arguments to `create_stream`.
- **Removed `create_stream_with_headers_provider(...)`** (sync and async).
  Use `auth=Headers(my_provider)` instead.
- **Removed `create_arrow_stream_with_headers_provider(...)`** (sync and
  async). Use `auth=Headers(my_provider)` instead.
- **Removed `ZerobusStream.ingest_record()`** (deprecated since v0.3.0). Use
  `ingest_record_offset()` plus `wait_for_offset()`, or `ingest_record_nowait()`.
- **Removed `RecordAcknowledgment` class**. It only existed as the return type
  of the now-removed `ingest_record()`.
- **Removed `ZerobusSdk.set_use_tls()`** (no-op in v1.x; underlying Rust field
  was removed).
- **Removed `ZerobusStream.get_state()`** / `stream_id` placeholders. They
  always returned a stub value.
- **`create_stream` / `create_arrow_stream` are keyword-only**. The
  previous positional ordering (`client_id, client_secret, table_properties`,
  etc.) is no longer accepted.
- **`record_type` on `StreamConfigurationOptions` is set automatically** from
  the `record_format` argument and should no longer be specified explicitly.
  If the caller passes a `StreamConfigurationOptions` instance, the wrapper
  copies it before stamping `record_type`, so the caller's object is never
  mutated.

### New Features and Improvements

- **`OAuth` / `Headers` auth selectors** (frozen dataclasses) exposed at the
  top-level `zerobus` package. `OAuth.client_secret` is excluded from
  `__repr__` so credentials do not leak into logs or stack traces.
- **`Format` format selectors** (`Format.JSON`, `Format.proto(descriptor)`)
  exposed at the top-level `zerobus` package. When `Format.proto(...)` is
  given a `google.protobuf.descriptor.Descriptor` object, the wrapper extracts
  its `name` and selects exactly that message inside the
  `FileDescriptorProto` (fixes a v1.x bug where the *first* message in the
  file was always picked, regardless of which `Descriptor` was passed).
- **`application_name` constructor argument** on `ZerobusSdk` and
  `zerobus.aio.ZerobusSdk`. Appended to the HTTP `user-agent` after the
  SDK prefix. Empty strings are treated as unset.
- **`zerobus.aio` top-level shortcut module** — `from zerobus.aio import
  ZerobusSdk` replaces the more verbose `from zerobus.sdk.aio import ...`.
- **`AckCallback.on_error(offset, error_message)` is now delivered to
  Python**. The PyO3 wrapper previously only logged ack errors via
  `eprintln!`; subclasses overriding `on_error` would never see the call.
- **Arrow Flight promoted to Beta**: the `ZerobusArrowStream` /
  `ArrowStreamConfigurationOptions` surface is no longer labelled
  experimental/unsupported. The API is stabilising but may still change
  before reaching GA. (Mirrors the Rust SDK 2.0.0 promotion.)
- **Arrow Flight — graceful stream close**: on a server-signaled close, the
  client pauses sending, drains in-flight acks within a bounded wait, then
  recovers. (Inherited from the underlying Rust SDK 1.3.0 work.)
- **`stream_paused_max_wait_time_ms`** on both `StreamConfigurationOptions`
  and `ArrowStreamConfigurationOptions`: optional millisecond cap for the
  paused wait (`None` = full server duration, `0` = immediate recovery,
  `x > 0` = `min(x, server_duration)`).
- **Type stubs (`_zerobus_core.pyi`) updated** to match the new binding
  surface; previous gaps (Arrow stream methods, return types of
  `get_unacked_records`/`get_unacked_batches`) have been closed.

### Internal Changes

- Bumped `databricks-zerobus-ingest-sdk` to `2.0.0` (from the in-tree
  `1.3.0`-line patch used during development). Resolved from crates.io;
  the `[patch.crates-io]` redirect is gone.
- Bumped Rust dependencies to match the main workspace: `prost` 0.13 → 0.14,
  `prost-types` 0.13 → 0.14, `tonic` 0.13 → 0.14, `arrow-ipc` / `arrow-schema`
  / `arrow-array` 56.2.0 → 58.2.
- Bounded the `HeadersProviderWrapper` static-key leak: header names are now
  interned in a process-wide table so each distinct name is leaked at most
  once instead of once per `get_headers()` call.
- Shared payload-extraction, options-application, descriptor parsing and the
  `AckCallback` bridge moved into `python/rust/src/common.rs`, removing
  duplicate copies in `sync_wrapper.rs` and `async_wrapper.rs`.
- Bumped Python optional dependencies: `pyarrow` upper bound `<20.0` → `<22.0`
  (dependabot PR #255), `pytest-asyncio` upper bound `<1.0` → `<2.0`
  (dependabot PR #254).
- Rewrote the PyO3 binding (`python/rust/src/{sync_wrapper,async_wrapper,arrow,common,lib}.rs`)
  to use the `StreamBuilder` typestate API exclusively. `StreamConfigurationOptions`
  and `ArrowStreamConfigurationOptions` fields are now applied via builder
  setters because the underlying Rust structs are `#[non_exhaustive]`.

### Bug Fixes

- `get_unacked_records()` and `get_unacked_batches()` on the sync stream
  wrapper now return concrete `List[bytes]` / `List[List[bytes]]` instead of
  one-shot iterators. The previous `iter(...)` wrapping made `len(...)` and
  indexing fail on values that were already lists under the hood.
- `Format.proto(MyMessage.DESCRIPTOR)` now selects the message whose name
  matches the supplied descriptor, instead of always returning the first
  message in the `.proto` file (which silently mis-routed schemas for
  multi-message proto files in v1.x).

### Documentation

- Rewrote the README quick-start, configuration, and API reference sections
  around the new API. Added an "Arrow Flight (Beta)" quick-start and a
  "Migrating from v1.x" section.
- All `examples/` programs migrated to the new API and use
  `application_name=` for telemetry.

## Release v1.2.0

### Major Changes

- **License: Migrated from the Databricks License to the Apache License 2.0**

### New Features and Improvements

- **Arrow Flight Support (Experimental)**: Added support for ingesting `pyarrow.RecordBatch` and `pyarrow.Table` objects via Arrow Flight protocol
  - **Note**: Arrow Flight is not yet supported by default from the Zerobus server side.
  - New `ZerobusArrowStream` class (sync in `zerobus.sdk.sync`, async in `zerobus.sdk.aio`) with `ingest_batch()`, `wait_for_offset()`, `flush()`, `close()`, `get_unacked_batches()` methods
  - New `ArrowStreamConfigurationOptions` for configuring Arrow streams (max inflight batches, recovery, timeouts)
  - New `create_arrow_stream()` and `recreate_arrow_stream()` methods on both sync and async `ZerobusSdk`
  - Accepts both `pyarrow.RecordBatch` and `pyarrow.Table` (Tables are combined to a single batch internally)
  - Arrow is opt-in: install via `pip install databricks-zerobus-ingest-sdk[arrow]` (requires `pyarrow>=14.0.0`)
  - Arrow types gated behind `_core.arrow` submodule — not loaded unless pyarrow is installed
  - Available from both `zerobus.sdk.sync` and `zerobus.sdk.aio`, and re-exported from top-level `zerobus` package

### Bug Fixes

- Fixed proto generation tool to skip reserved field numbers 19000-19999 for tables with more than 19000 columns

### Internal Changes

- Bumped Rust SDK dependency to v1.0.1 with `arrow-flight` feature
- Added `arrow-ipc`, `arrow-schema`, `arrow-array` (v56.2.0) Rust dependencies for IPC serialization
- Added PyO3 arrow module (`arrow.rs`) with `ArrowStreamConfigurationOptions`, `ZerobusArrowStream`, `AsyncZerobusArrowStream` pyclasses
- Added Python-side serialization helpers in `zerobus.sdk.shared.arrow` (`_serialize_schema`, `_serialize_batch`, `_deserialize_batch`)

### API Changes

- Added `create_arrow_stream(table_name, schema, client_id, client_secret, options=None, headers_provider=None)` to sync and async `ZerobusSdk`
- Added `recreate_arrow_stream(old_stream)` to sync and async `ZerobusSdk`
- Added `ZerobusArrowStream` class (sync and async variants) with methods: `ingest_batch()`, `wait_for_offset()`, `flush()`, `close()`, `get_unacked_batches()`, properties: `is_closed`, `table_name`
- Added `ArrowStreamConfigurationOptions` class with fields: `max_inflight_batches`, `recovery`, `recovery_timeout_ms`, `recovery_backoff_ms`, `recovery_retries`, `server_lack_of_ack_timeout_ms`, `flush_timeout_ms`, `connection_timeout_ms`
- Added optional dependency: `pyarrow>=14.0.0` via `pip install databricks-zerobus-ingest-sdk[arrow]`

## Release v1.1.0

### New Features and Improvements
- Added HTTP proxy support via standard environment variables (`grpc_proxy`, `https_proxy`, `http_proxy`), following gRPC core conventions. Proxied connections use HTTP CONNECT tunneling with end-to-end TLS. Supports `no_grpc_proxy` / `no_proxy` for bypass rules.

## Release v1.0.0

GA release of the Databricks Zerobus Ingest SDK for Python.

### Breaking Changes

- **v0.3.0 was yanked** due to a breaking change introduced in that release: the `server_endpoint` parameter was changed to require the `https://` prefix, whereas v0.2.0 accepted URLs without it. v1.0.0 resolves this by accepting `server_endpoint` both with and without the `https://` prefix.

## Release v0.3.0 (YANKED)

### Major Changes

- **Rust-Backed Implementation**: Complete rewrite of the Python SDK as a thin wrapper around the [Databricks Zerobus Rust SDK](https://github.com/databricks/zerobus-sdk-rs)
  - All core logic (gRPC, authentication, recovery, stream management) now handled by native Rust code
  - Python bindings built using PyO3 and maturin
  - Significant performance improvements: 2-5x throughput, lower latency, reduced memory footprint
  - Single source of truth: Python SDK automatically inherits all Rust SDK improvements
  - **Architecture**: Native Rust core with PyO3 bindings and full type stubs (`_zerobus_core.pyi`)
  - **Build System**: Migrated from setuptools to maturin for Rust/Python integration
  - **Benefits**: Native performance, Rust's memory safety guarantees, easier maintenance, consistent behavior across all SDK languages


### New Features and Improvements

- **Configurable Logging**: Added support for `RUST_LOG` environment variable to control log levels
  - Users can now set `RUST_LOG=debug` or `RUST_LOG=trace` for detailed diagnostics
  - Default level is `info` when not specified
  - Supports granular control: `RUST_LOG=zerobus_sdk=trace,tokio=info`
- **Flexible Record Serialization**: `ingest_record()` now accepts multiple input types, giving clients control over serialization:
  - **JSON mode**: Accepts both `dict` (SDK serializes) and `str` (pre-serialized JSON string)
  - **Protobuf mode**: Accepts both `Message` objects (SDK serializes) and `bytes` (pre-serialized)
  - This allows clients to optimize serialization separately or use custom serialization logic while maintaining backward compatibility

### Bug Fixes

### Documentation

- Updated README with new Delta type mappings (TIMESTAMP_NTZ, VARIANT)
- Updated `ingest_record()` API documentation to show all accepted record types
- Added inline examples demonstrating both serialization approaches (SDK-controlled vs. client-controlled)
- Updated examples README with clear explanations of serialization options

### Internal Changes

- **Implemented `get_unacked_records()` and `get_unacked_batches()`**: Return actual unacknowledged records/batches (as bytes) for recovery and monitoring
  - `get_unacked_records()` returns `List[bytes]` of unacknowledged record payloads
  - `get_unacked_batches()` returns `List[List[bytes]]` where each batch contains record payloads
  - Available in both sync and async APIs
  - Useful for implementing custom retry logic or monitoring stream health
- Added `env-filter` feature to `tracing-subscriber` dependency for `RUST_LOG` support

- **generate_proto tool**: Added support for TIMESTAMP_NTZ and VARIANT data types
  - TIMESTAMP_NTZ maps to int64 (timestamp without timezone, microseconds since epoch)
  - VARIANT maps to string (unshredded, JSON string format)
- **generate_proto tool**: Added comprehensive unit tests for all pure functions (84 tests covering type parsing, type mapping, field validation, and proto file generation)
- Enhanced `ingest_record()` type validation to accept wider range of input types
- Added test coverage for both high-level objects (dict/Message) and pre-serialized data (str/bytes)

### Breaking Changes

- **BREAKING**: Host endpoints now require `https://` scheme
  - **Impact**: `SERVER_ENDPOINT` and `UNITY_CATALOG_ENDPOINT` must include `https://` prefix
  - **Migration**: Update endpoint URLs to include `https://`
  - Old: `SERVER_ENDPOINT = "your-shard-id.zerobus.region.cloud.databricks.com"`
  - New: `SERVER_ENDPOINT = "https://your-shard-id.zerobus.region.cloud.databricks.com"`

- **BREAKING**: Removed `create_stream_with_headers_provider()` method
  - **Migration**: Use `create_stream()` with the `headers_provider` parameter instead
  - Old: `sdk.create_stream_with_headers_provider(custom_provider, table_properties, options)`
  - New: `sdk.create_stream(client_id, client_secret, table_properties, options, headers_provider=custom_provider)`

- **BREAKING**: Removed `StreamState` enum
  - **Reason**: Internal state management now handled by Rust SDK
  - **Impact**: `get_state()` method no longer returns a meaningful state enum
  - **Migration**: Not typically used in primary workflows; remove any code that depends on `StreamState`

- **Changed**: `get_unacked_records()` implementation (backward compatible)
  - **Old**: Returned `Iterator` that yielded record payloads from the Python wrapper's internal queue
  - **New**: Returns `Iterator[bytes]` that yields unacknowledged record payloads directly from the Rust SDK
  - **Migration**: No migration needed - iteration pattern remains the same: `for record in stream.get_unacked_records():`
  - **Benefit**: Direct access to Rust SDK's unacked records; more accurate representation of what hasn't been acknowledged by the server
  - **Note**: Still returns an iterator for backward compatibility and memory efficiency

- **BREAKING**: Changed `ack_callback` signature in `StreamConfigurationOptions`
  - **Old**: Callback received detailed acknowledgment response object
  - **New**: Callback receives single `offset: int` parameter
  - **Migration**: Update callback signature from `def on_ack(self, response)` to `def on_ack(self, offset: int)`
  - **Impact**: Simplified API; offset is the primary acknowledgment information needed

### Deprecations

- **DEPRECATED**: `ingest_record()` method (both sync and async)
  - **Reason**: Offers significantly lower throughput compared to `ingest_record_offset()` and `ingest_record_nowait()`
  - **Migration**:
    - For sync API: Use `ingest_record_offset()` for offset tracking or `ingest_record_nowait()` for maximum throughput
    - For async API: Use `ingest_record_offset()` with batched `asyncio.gather()` pattern or `ingest_record_nowait()` for maximum throughput
  - **Performance Impact**: New methods are 2-40x faster depending on record size
  - **Note**: Method remains available for backward compatibility but will be removed in a future major version

### API Changes

- Added optional `headers_provider` parameter to `create_stream()` methods
  - Defaults to internal OAuth 2.0 Client Credentials authentication when not provided
- Widened `ingest_record()` type signature to accept:
  - JSON mode: `Union[dict, str]` (previously `str` only)
  - Protobuf mode: `Union[Message, bytes]` (previously `Message` only)
- All changes except removal of `create_stream_with_headers_provider()` are backward compatible

## Release v0.2.0

### New Features and Improvements

- Loosened protobuf dependency constraint to support versions >= 4.25.0 and < 7.0
- **JSON Serialization Support**: Added support for JSON record serialization alongside Protocol Buffers (default)
  - New `RecordType.JSON` mode for ingesting JSON-encoded strings
  - No protobuf schema compilation required
- Added `HeadersProvider` abstraction for flexible authentication strategies
- Implemented `OAuthHeadersProvider` for OAuth 2.0 Client Credentials flow (default authentication method used by `create_stream()`)

### Bug Fixes

- **generate_proto tool**: Fixed uppercase field names bug for nested fields
- **generate_proto tool**: Added validation for unsupported nested type combinations
  - Now properly rejects: `array<array<...>>`, `array<map<...>>`, `map<map<...>, ...>`, `map<array<...>, ...>`, `map<..., map<...>>`, `map<..., array<...>>`
- **Logging**: Fixed false alarm "Retriable gRPC error" logs when calling `stream.close()`
  - CANCELLED errors during intentional stream closure are no longer logged as errors
- **Logging**: Unified log messages between sync and async SDK implementations
  - Both SDKs now produce consistent logging output with same verbosity and format
- **Error handling**: Improved error messages to distinguish between recoverable and non-recoverable errors
  - "Stream closed due to a non-recoverable error" vs "Stream failed permanently after failed recovery attempt"

### Documentation

- Added JSON and protobuf serialization examples for both sync and async APIs
- Restructured Quick Start guide to present JSON first as the simpler option
- Enhanced API Reference with JSON mode documentation
- Added Azure workspace and endpoint URL examples

### Internal Changes

- **Build system**: Loosened setuptools requirement from `>=77` to `>=61`xw
- **License format**: Changed license specification to PEP 621 table format for setuptools <77 compatibility
  - Changed from `license = "LicenseRef-Proprietary"` to `license = {text = "LicenseRef-Proprietary"}`
- **generate_proto tool**: Added support for TINYINT and BYTE data types (both map to int32)
- **Logging**: Added detailed initialization logging to async SDK to match sync SDK
  - "Starting initializing stream", "Attempting retry X out of Y", "Sending CreateIngestStreamRequest", etc.

### API Changes

- **StreamConfigurationOptions**: Added `record_type` parameter to specify serialization format
  - `RecordType.PROTO` (default): For protobuf serialization
  - `RecordType.JSON`: For JSON serialization
  - Example: `StreamConfigurationOptions(record_type=RecordType.JSON)`
- **ZerobusStream.ingest_record**: Now accepts JSON strings (when using `RecordType.JSON`) in addition to protobuf messages and bytes
- Added `RecordType` enum with `PROTO` and `JSON` values
- Added `HeadersProvider` abstract base class for custom header strategies
- Added `OAuthHeadersProvider` class for OAuth 2.0 authentication with Databricks OIDC endpoint
- Added `create_stream_with_headers_provider` method to `ZerobusSdk` and `aio.ZerobusSdk` for custom authentication header providers
  - **Note**: Custom headers providers must include both `authorization` and `x-databricks-zerobus-table-name` headers

## Release v0.1.0

Initial release of the Databricks Zerobus Ingest SDK for Python.

### API Changes

- Added `ZerobusSdk` class for creating ingestion streams
- Added `ZerobusStream` class for managing stateful gRPC streams
- Added `RecordAcknowledgment` for blocking until record acknowledgment
- Added asynchronous versions: `zerobus.sdk.aio.ZerobusSdk` and `zerobus.sdk.aio.ZerobusStream`
- Added `TableProperties` for configuring table schema and name
- Added `StreamConfigurationOptions` for stream behavior configuration
- Added `ZerobusException` and `NonRetriableException` for error handling
- Added `StreamState` enum for tracking stream lifecycle
- Support for Python 3.9, 3.10, 3.11, 3.12, and 3.13
