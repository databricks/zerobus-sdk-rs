# Version changelog

## Release v1.2.0

### Major Changes

### New Features and Improvements

- **Arrow stream options (C API)**: `CArrowStreamConfigurationOptions.stream_paused_max_wait_time_ms` (`int64_t`) configures graceful-close paused wait: `-1` = None (full server duration), `0` = immediate recovery, `>0` = capped wait (see `zerobus.h` comments).
- **Zero-copy Arrow IPC ingestion**: `zerobus_arrow_stream_ingest_batch` now forwards IPC bytes directly via `ingest_ipc_batch`, skipping the deserialization round-trip. Use `zerobus_arrow_stream_ingest_batch_via_record_batch` for compression-enabled streams.
- **Fire-and-forget ingestion**: Added nowait variants that spawn a background task and return immediately — `zerobus_stream_ingest_proto_record_nowait`, `zerobus_stream_ingest_json_record_nowait`, `zerobus_stream_ingest_proto_records_nowait`, `zerobus_stream_ingest_json_records_nowait`.

### Bug Fixes

- **Arrow IPC compression fix**: Added `zerobus_arrow_stream_ingest_batch_via_record_batch` for streams created with `LZ4_FRAME` or `ZSTD` compression. The existing `zerobus_arrow_stream_ingest_batch` uses the zero-copy path and does not apply compression; callers must use the new function when compression is configured. This fixes a regression where compression was silently ignored.

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

- Added `zerobus_arrow_stream_ingest_batch_via_record_batch(stream, ipc_bytes, ipc_len, result)` for compression-enabled Arrow streams.
- Added `zerobus_stream_ingest_proto_record_nowait`, `zerobus_stream_ingest_json_record_nowait`, `zerobus_stream_ingest_proto_records_nowait`, `zerobus_stream_ingest_json_records_nowait` for fire-and-forget ingestion.

## Release v1.1.0

### Major Changes

- **License: Migrated from the Databricks License to the Apache License 2.0**
- Removed macOS x86_64 and macOS aarch64 support.

### New Features and Improvements

- Added dynamic library (.so / .dylib / .dll) output alongside static library

## Release v1.0.1

Initial tracked release of the FFI C bindings for the Zerobus SDK.

### Platforms
- Linux x86_64
- Linux aarch64
- macOS x86_64
- macOS aarch64
- Windows x86_64

### Libraries
- Static library (.a / .lib)
- Dynamic library (.so / .dylib / .dll)
- C header file (zerobus.h)
