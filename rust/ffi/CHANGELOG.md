# Version changelog

## Release v1.8.0

### Major Changes

### New Features and Improvements

### Bug Fixes

### Documentation

- Built on Rust SDK 2.8.0. Wrapper-facing notes for that core are in
  `rust/CHANGELOG.md` and https://github.com/databricks/zerobus-sdk/releases/tag/rust/v2.8.0.

### Internal Changes

### Behavior Changes

### Breaking Changes

### Deprecations

### API Changes

## Release v1.7.0

### Major Changes

### New Features and Improvements

### Bug Fixes

### Documentation

- Clarified that acknowledgment callbacks fire once per logical ingest
  submission, so one batch ingest call produces one callback.
- Documented flush, close/free, SDK free, and error-string cleanup in the
  copyable C lifecycle example.
- Built on Rust SDK 2.7.1. Wrapper-facing notes for that core are in
  `rust/CHANGELOG.md` and https://github.com/databricks/zerobus-sdk/releases/tag/rust/v2.7.1.

### Internal Changes

### Behavior Changes

- `zerobus_arrow_stream_free` now selects destruction behavior based on how the stream was used. IPC-only streams preserve best-effort, nonblocking destruction. Once a stream accepts an Arrow C Data batch, free blocks until Arrow background shutdown completes, every Flight request body reaches EOF or is dropped, and all retained foreign owners are released. Previously, a request body could retain an owner and run its release callback after free returned on an unacknowledged/failure path, risking callback-after-free use of producer state. When the calling restrictions below are respected, no Arrow C Data release callback for that stream can run after free returns. The function logs a warning every 30 seconds while required C Data shutdown remains incomplete; it does not return on a timeout. Callers must not block the only thread, event loop, or runtime lock needed by a release callback: offload free, release required runtime locks, and continue servicing callback dependencies until it completes. Free must not race another operation on the same stream handle. After C Data import, freeing the same stream reentrantly from one of its SDK callbacks is unsupported because complete shutdown would wait for that callback. IPC-only concurrent or reentrant free remains invalid because the opaque handle has single ownership; freeing a different stream from a callback remains supported. During required C Data shutdown, an internal native shutdown panic, a required helper-thread spawn failure, or a helper-thread panic terminates the process rather than returning without the release-callback guarantee.

### Breaking Changes

### Deprecations

### API Changes

## Release v1.6.0

### New Features and Improvements

- Added `zerobus_arrow_stream_ingest_c_data`, an ownership-transferring Arrow C
  Data Interface ingestion API. It imports a canonical `ArrowArray` and
  `ArrowSchema` without an IPC encode/decode round trip, then uses the existing
  Flight encoder for dictionaries, compression, chunking, and framing.
- Add callback-based async overloads for all previously blocking stream operations: stream creation (`zerobus_sdk_create_stream_async`, `zerobus_sdk_create_stream_with_headers_provider_async`), stream recreation (`zerobus_sdk_recreate_stream_async`), offset-returning ingest calls (`zerobus_stream_ingest_proto_record_async`, `zerobus_stream_ingest_json_record_async`, `zerobus_stream_ingest_proto_records_async`, `zerobus_stream_ingest_json_records_async`), completion methods (`zerobus_stream_wait_for_offset_async`, `zerobus_stream_flush_async`, `zerobus_stream_close_async`), and unacked-record retrieval (`zerobus_stream_get_unacked_records_async`). These APIs return immediately after validation/scheduling and complete via callbacks; caller-owned string/descriptor/config inputs are copied before return, and SDK/stream handles must remain valid until callback completion.

### Bug Fixes

- Fixed a use-after-free in which a custom headers provider could be freed by the
  wrapper while a Rust worker thread was still inside a synchronous
  `get_headers()` callback into it during connection recovery. The FFI now takes
  ownership of the provider `user_data`: `CallbackHeadersProvider` invokes a
  caller-supplied `free_user_data` destroy callback from its `Drop`, which runs
  only after every task that could call `get_headers()` is gone (the supervisor
  task holds its own `Arc` across the in-flight call). The provider is
  constructed before any fallible work in `create_stream_with_headers_provider`,
  `create_arrow_stream_with_headers_provider`, and
  `create_stream_with_headers_provider_async`, so `free_user_data` is invoked
  exactly once on every path — on success after the last reference drops, on a
  failed create before returning (synchronously for the async variant's
  scheduling failures, in the spawned task for its asynchronous failures).

### Internal Changes

- Reused the Rust SDK's wrapper-only importer to implement the C Data API added
  in this release. This internal extraction introduces no further changes to
  that API's ABI, ownership contract, errors, or generated header beyond the
  additions described under New Features.
- Add headers-provider ownership tests: `free_user_data` fires once on `Drop` and once on failed create, a null destroy callback is a no-op, and a teardown test that reproduces the recovery-vs-teardown race (a blocking in-flight `get_headers` on one `Arc` clone while another is dropped) asserts the free is deferred until the callback returns. Test-only; no ABI or behavior change.
- Fix the darwin static-library build in `release-ffi.yml`: invoke `cargo-zigbuild rustc` (the binary directly) instead of `cargo zigbuild rustc`, which routed `rustc` as a positional into the zigbuild subcommand and failed with `unexpected argument 'rustc'`. Release tooling only; no ABI or behavior change.
- Add ack-callback live-teardown / use-after-free tests. They drive the real `CallbackAckCallback` bridge over a heap-allocated `user_data` through the real callback-handler task, then tear it down via the production teardown code, asserting no callback fires after teardown returns and that `user_data` is safe to release at that point. All teardown paths are covered: drain-within-`callback_max_wait_time_ms`, wait-indefinitely, and a callback still synchronously in-flight when a bounded budget expires — which the drain aborts but cannot preempt, so the callback outlives `teardown()` and `user_data` must stay alive until it finishes. Test-only; no ABI or behavior change.
- Add tests asserting the ack-callback `Arc` is released, not leaked to a background task, when `zerobus_sdk_create_stream` / `zerobus_sdk_create_stream_with_headers_provider` fails. Uses a `#[cfg(test)]` drop hook keyed to a test-only sentinel `user_data`. Test-only; no ABI or behavior change.
- Move the header-callback helpers `zerobus_alloc_header_array`, `zerobus_alloc_cstring`, and `zerobus_free_headers` from `arrow.rs` into the shared `common.rs` module, next to the `CHeader`/`CHeaders` types they serve. No behavior, signature, or ABI change; their declarations move ahead of the Arrow functions in the generated `zerobus.h`.

### Behavior Changes

- Server-initiated Arrow Flight rotation now waits only for records submitted on the active connection, half-closes its request, and drains late responses before reconnecting. `stream_paused_max_wait_time_ms = 0` skips the ACK wait but still permits bounded transport cleanup.

### Breaking Changes

- `zerobus_sdk_create_stream_with_headers_provider`,
  `zerobus_sdk_create_arrow_stream_with_headers_provider`, and
  `zerobus_sdk_create_stream_with_headers_provider_async` take a new
  `free_user_data` parameter (a nullable `void (*)(void *user_data)`) after
  `user_data`. Callers must hand ownership of `user_data` across and supply a
  destroy callback (or pass null to opt out and manage the lifetime themselves).
  This changes the generated `zerobus.h` signatures, so Go, .NET, and any other
  C FFI consumer must update their call sites.

### API Changes

- Add `CreateStreamAsyncCallback`, `OffsetAsyncCallback`, `BoolAsyncCallback`, and `RecordArrayAsyncCallback` plus the full async stream API set (`*_async` overloads for create/recreate/ingest/wait/flush/get_unacked_records/close) to `zerobus.h`. Callback `const CResult *` values are valid only for the duration of each callback; any error text must be copied during the call.

## Release v1.5.0

### New Features and Improvements

- Expose the core ack callback through the C FFI. `CStreamConfigurationOptions` gains `ack_on_ack` (`void (*)(int64_t offset_id, void *user_data)`), `ack_on_error` (`void (*)(int64_t offset_id, const char *error_message, void *user_data)`), and `ack_user_data`. When either pointer is non-null, an `AckCallback` is registered on the stream so acks and errors are delivered asynchronously instead of only via `wait_for_offset` / `flush`. Both create paths (`zerobus_sdk_create_stream` and `zerobus_sdk_create_stream_with_headers_provider`) read the new fields. Callbacks fire on a background task (serialized, never re-entered concurrently), so the callback and its `user_data` must synchronize any shared state and stay alive until the callback object is destroyed. `close()` drains the handler task only up to `callback_max_wait_time_ms` then aborts it, and abort cancels only at an await — so a synchronously-running callback can outlive `close()`; keeping the pointers alive merely until `close()` returns is not sufficient. Panics are contained at the boundary and logged. `zerobus.h` documents the ack semantics (per-record, monotonic), error delivery, and lifetime contract.

### Internal Changes

- Move the header-callback helpers `zerobus_alloc_header_array`, `zerobus_alloc_cstring`, and `zerobus_free_headers` from `arrow.rs` into the shared `common.rs` module, next to the `CHeader`/`CHeaders` types they serve. No behavior, signature, or ABI change; their declarations move ahead of the Arrow functions in the generated `zerobus.h`.

### API Changes

- `CStreamConfigurationOptions` (in `zerobus.h`) gains three trailing fields: `ack_on_ack`, `ack_on_error`, and `ack_user_data`. This is an additive struct-layout change — existing fields keep their order and offsets, and `zerobus_get_default_config()` zero-initializes the new fields (no callback). Consumers that hand-mirror the struct (e.g. the Go cgo preamble) must add the matching trailing fields to keep the layout byte-identical.
- Add `zerobus_alloc_header_array` and `zerobus_alloc_cstring` so a non-Rust headers callback can allocate the `CHeaders` it returns through this library instead of its own allocator. The buffers are then freed by `zerobus_free_headers` with the matching allocator, keeping each allocate/free pair inside one library. This removes a cross-allocator free that could corrupt the heap on Windows when the consumer and this statically linked library resolve to different CRT heaps. Additive only — existing functions and `zerobus_free_headers` are unchanged.

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
