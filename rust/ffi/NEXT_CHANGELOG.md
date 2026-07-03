# NEXT CHANGELOG

## Release v1.4.0

### Major Changes

### New Features and Improvements

- Build the FFI library for Linux musl targets (`x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl`), enabling C/C++ and Go-on-Alpine consumers to link `libzerobus_ffi.a` / `libzerobus_ffi.so` on musl-based (Alpine) containers. Artifacts ship in the `linux-musl-x86_64` / `linux-musl-aarch64` directories.
- Expose the core ack callback through the C FFI. `CStreamConfigurationOptions` gains `ack_on_ack` (`void (*)(int64_t offset_id, void *user_data)`), `ack_on_error` (`void (*)(int64_t offset_id, const char *error_message, void *user_data)`), and `ack_user_data`. When either pointer is non-null, an `AckCallback` is registered on the stream so acks and errors are delivered asynchronously instead of only via `wait_for_offset` / `flush`. Both create paths (`zerobus_sdk_create_stream` and `zerobus_sdk_create_stream_with_headers_provider`) read the new fields. Callbacks fire on a background task (serialized, never re-entered concurrently), so the callback and its `user_data` must outlive the stream and synchronize any shared state; panics are contained at the boundary and logged. `zerobus.h` documents the ack semantics (per-record, monotonic), error delivery, and lifetime contract.

### Bug Fixes

- Every panic-capable `#[no_mangle] extern "C"` entry point now runs its body inside a `catch_unwind` panic guard (`ffi_guard`). (The trivially-infallible `zerobus_sdk_set_use_tls` and the two `_get_default_config` getters are intentionally left unguarded.) Previously a panic anywhere in an FFI function body — a dependency `unwrap`/`expect`, an allocation failure, or a slicing/bounds error — would escape across the `extern "C"` boundary, aborting the host process (on current Rust toolchains; it was undefined behavior on pre-1.81 ones) with no recoverable error returned to the C/Go/Java caller. A caught panic is now converted into the function's normal failure channel: a non-retryable error is written to the `CResult` out-parameter (when present) and the per-signature failure sentinel is returned — `NULL` for pointer returns, `false` for most `bool` functions (but `true`, i.e. treat-as-closed, for `zerobus_arrow_stream_is_closed`), `-1` for the `i64` offset functions, an empty array struct for the `get_unacked_*` functions, and `()` for `void` functions. This guard only catches panics that originate in Rust: a caller-supplied headers-provider callback is an `extern "C" fn` invoked directly, so it must still not unwind across the ABI — its panic aborts at its own boundary before the guard runs. The guard relies on the `panic = "unwind"` strategy, which is pinned in the workspace release profile so a release build can't silently turn the `catch_unwind` guards into aborts. No signatures changed and `zerobus.h` is byte-identical, so this is ABI-compatible for existing Go/Java consumers.
- `zerobus_proto_schema_encode_json` now enforces proto2 `required` presence recursively instead of only on top-level columns. A record that omits a non-nullable field nested inside a `STRUCT`, inside an `ARRAY<STRUCT>` element, or inside a `MAP` value is now rejected locally at encode time (with the full field path, e.g. `addr.zip`, `items[2].id`, `props[home].zip`) rather than encoding successfully and being rejected by the server after a network round-trip.

### Documentation

### Internal Changes

- Split `src/lib.rs` into per-surface modules (`common`, `arrow`, `builder`, `sdk`, `stream`, `proto_schema`); `lib.rs` now holds only module declarations and re-exports. Pure refactor — no API, ABI, or behavior change; `zerobus.h` is byte-identical. `build.rs` now watches the whole `src/` tree so the header regenerates on any module change.

### Behavior Changes

### Breaking Changes

### Deprecations

### API Changes

- `CStreamConfigurationOptions` (in `zerobus.h`) gains three trailing fields: `ack_on_ack`, `ack_on_error`, and `ack_user_data`. This is an additive struct-layout change — existing fields keep their order and offsets, and `zerobus_get_default_config()` zero-initializes the new fields (no callback). Consumers that hand-mirror the struct (e.g. the Go cgo preamble) must add the matching trailing fields to keep the layout byte-identical.
