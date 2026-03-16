# NEXT CHANGELOG

## Release v1.2.0

### Major Changes

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

### Documentation

### Internal Changes

- Bumped Rust SDK dependency to v1.0.1 with `arrow-flight` feature
- Added `arrow-ipc`, `arrow-schema`, `arrow-array` (v56.2.0) Rust dependencies for IPC serialization
- Added PyO3 arrow module (`arrow.rs`) with `ArrowStreamConfigurationOptions`, `ZerobusArrowStream`, `AsyncZerobusArrowStream` pyclasses
- Added Python-side serialization helpers in `zerobus.sdk.shared.arrow` (`_serialize_schema`, `_serialize_batch`, `_deserialize_batch`)

### Breaking Changes

### Deprecations

### API Changes

- Added `create_arrow_stream(table_name, schema, client_id, client_secret, options=None, headers_provider=None)` to sync and async `ZerobusSdk`
- Added `recreate_arrow_stream(old_stream)` to sync and async `ZerobusSdk`
- Added `ZerobusArrowStream` class (sync and async variants) with methods: `ingest_batch()`, `wait_for_offset()`, `flush()`, `close()`, `get_unacked_batches()`, properties: `is_closed`, `table_name`
- Added `ArrowStreamConfigurationOptions` class with fields: `max_inflight_batches`, `recovery`, `recovery_timeout_ms`, `recovery_backoff_ms`, `recovery_retries`, `server_lack_of_ack_timeout_ms`, `flush_timeout_ms`, `connection_timeout_ms`
- Added optional dependency: `pyarrow>=14.0.0` via `pip install databricks-zerobus-ingest-sdk[arrow]`
