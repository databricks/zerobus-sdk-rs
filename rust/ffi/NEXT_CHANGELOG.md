# NEXT CHANGELOG

## Release v1.4.0

### Major Changes

### New Features and Improvements

- Build the FFI library for Linux musl targets (`x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl`), enabling C/C++ and Go-on-Alpine consumers to link `libzerobus_ffi.a` / `libzerobus_ffi.so` on musl-based (Alpine) containers. Artifacts ship in the `linux-musl-x86_64` / `linux-musl-aarch64` directories.

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

- Add `zerobus_alloc_header_array` and `zerobus_alloc_cstring` so a non-Rust headers callback can allocate the `CHeaders` it returns through this library instead of its own allocator. The buffers are then freed by `zerobus_free_headers` with the matching allocator, keeping each allocate/free pair inside one library. This removes a cross-allocator free that could corrupt the heap on Windows when the consumer and this statically linked library resolve to different CRT heaps. Additive only — existing functions and `zerobus_free_headers` are unchanged.
