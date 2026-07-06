# Version changelog

## Release v1.4.0

### New Features and Improvements

- Build the FFI library for Linux musl targets (`x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl`), enabling C/C++ and Go-on-Alpine consumers to link `libzerobus_ffi.a` / `libzerobus_ffi.so` on musl-based (Alpine) containers. Artifacts ship in the `linux-musl-x86_64` / `linux-musl-aarch64` directories.
- Build the FFI library for macOS (`x86_64-apple-darwin`, `aarch64-apple-darwin`) and MinGW Windows (`x86_64-pc-windows-gnu`) via `cargo-zigbuild`, cross-compiled from the Linux runner. Enables macOS (Intel and Apple Silicon) consumers and GNU-toolchain Windows consumers to link the released FFI archives. Artifacts ship in the `darwin-x86_64`, `darwin-aarch64`, and `windows-gnu-x86_64` directories.

### Bug Fixes

- Every panic-capable `#[no_mangle] extern "C"` entry point now runs its body inside a `catch_unwind` panic guard (`ffi_guard`). (The trivially-infallible `zerobus_sdk_set_use_tls` and the two `_get_default_config` getters are intentionally left unguarded.) Previously a panic anywhere in an FFI function body — a dependency `unwrap`/`expect`, an allocation failure, or a slicing/bounds error — would escape across the `extern "C"` boundary, aborting the host process (on current Rust toolchains; it was undefined behavior on pre-1.81 ones) with no recoverable error returned to the C/Go/Java caller. A caught panic is now converted into the function's normal failure channel: a non-retryable error is written to the `CResult` out-parameter (when present) and the per-signature failure sentinel is returned — `NULL` for pointer returns, `false` for most `bool` functions (but `true`, i.e. treat-as-closed, for `zerobus_arrow_stream_is_closed`), `-1` for the `i64` offset functions, an empty array struct for the `get_unacked_*` functions, and `()` for `void` functions. This guard only catches panics that originate in Rust: a caller-supplied headers-provider callback is an `extern "C" fn` invoked directly, so it must still not unwind across the ABI — its panic aborts at its own boundary before the guard runs. The guard relies on the `panic = "unwind"` strategy, which is pinned in the workspace release profile so a release build can't silently turn the `catch_unwind` guards into aborts. No signatures changed and `zerobus.h` is byte-identical, so this is ABI-compatible for existing Go/Java consumers.
- `zerobus_proto_schema_encode_json` now enforces proto2 `required` presence recursively instead of only on top-level columns. A record that omits a non-nullable field nested inside a `STRUCT`, inside an `ARRAY<STRUCT>` element, or inside a `MAP` value is now rejected locally at encode time (with the full field path, e.g. `addr.zip`, `items[2].id`, `props[home].zip`) rather than encoding successfully and being rejected by the server after a network round-trip.

### Internal Changes

- Split `src/lib.rs` into per-surface modules (`common`, `arrow`, `builder`, `sdk`, `stream`, `proto_schema`); `lib.rs` now holds only module declarations and re-exports. Pure refactor — no API, ABI, or behavior change; `zerobus.h` is byte-identical. `build.rs` now watches the whole `src/` tree so the header regenerates on any module change.

## Release v1.3.0

### Major Changes

### New Features and Improvements

- **C-builder API for SDK construction**: `zerobus_sdk_builder_new`, per-option setters (`_endpoint`, `_unity_catalog_url`, `_sdk_identifier`, `_application_name`, `_disable_tls`), and `_build` / `_free`. Mirrors the Rust `ZerobusSdkBuilder`; new options are added as setters without ABI breaks. Legacy `zerobus_sdk_new` is retained and delegates to the builder.
- **Dynamic protobuf from a Unity Catalog schema**: a pure-C consumer can now build a protobuf descriptor from UC table metadata and encode records without a companion Rust crate. New opaque type `CZerobusProtoSchema` and functions:
  - `zerobus_proto_schema_from_uc_json` — build a schema handle from UC table-metadata JSON (the body of `GET /api/2.1/unity-catalog/tables/{name}`).
  - `zerobus_proto_schema_descriptor_bytes` — borrow the serialized `DescriptorProto` to pass straight to `zerobus_sdk_create_stream` (byte-identical to the descriptor the encoder uses).
  - `zerobus_proto_schema_encode_json` — encode one JSON record into protobuf bytes; unknown keys are ignored. `DATE`/`TIMESTAMP`/`TIMESTAMP_NTZ` columns are integers (days / micros since epoch), `BINARY` is a base64 string, `DECIMAL` is a string, and large 64-bit integers are accepted as JSON strings (the protobuf-JSON canonical form) to avoid precision loss in producers that emit numbers as IEEE-754 doubles. Top-level non-nullable scalar/struct columns are proto2 `required`; a record missing one is rejected (ARRAY/MAP map to `repeated`, which has no presence, so an omitted one encodes as empty).
  - `zerobus_free_proto_bytes` / `zerobus_proto_schema_free` — free an encoded buffer / a schema handle.

### Bug Fixes

### Documentation

### Internal Changes

### Behavior Changes

### Breaking Changes

### Deprecations

### API Changes

## Release v1.2.1

### Major Changes

### New Features and Improvements

### Bug Fixes

- **`zerobus_arrow_stream_ingest_batch_via_record_batch` now works correctly on compression-enabled streams.** Previously the function performed its own IPC deserialization and called `ingest_batch` directly, bypassing the compression re-encoding step. It now delegates to `ingest_ipc_batch`, which handles compression transparently. The function is now fully equivalent to `zerobus_arrow_stream_ingest_batch` regardless of stream configuration.

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

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
