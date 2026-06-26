# NEXT CHANGELOG

## Release v1.4.0

### Major Changes

### New Features and Improvements

- Build the FFI library for Linux musl targets (`x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl`), enabling C/C++ and Go-on-Alpine consumers to link `libzerobus_ffi.a` / `libzerobus_ffi.so` on musl-based (Alpine) containers. Artifacts ship in the `linux-musl-x86_64` / `linux-musl-aarch64` directories.

### Bug Fixes

- Every `#[no_mangle] extern "C"` entry point now runs its body inside a `catch_unwind` panic guard (`ffi_guard`). Previously a panic anywhere in an FFI function body (a dependency `unwrap`/`expect`, an allocation failure, a slicing/bounds error, or a panicking callback) would escape across the `extern "C"` boundary, aborting the host process (on current Rust toolchains; it was undefined behavior on pre-1.81 ones) with no recoverable error returned to the C/Go/Java caller. A caught panic is now converted into the function's normal failure channel: a non-retryable error is written to the `CResult` out-parameter (when present) and the per-signature failure sentinel is returned (`NULL` for pointer returns, `false` for `bool`, `-1` for the `i64` offset functions, an empty array struct for the `get_unacked_*` functions). The guard relies on the default `panic = "unwind"` strategy. No signatures changed and `zerobus.h` is byte-identical, so this is ABI-compatible for existing Go/Java consumers.

### Documentation

### Internal Changes

- Split `src/lib.rs` into per-surface modules (`common`, `arrow`, `builder`, `sdk`, `stream`, `proto_schema`); `lib.rs` now holds only module declarations and re-exports. Pure refactor — no API, ABI, or behavior change; `zerobus.h` is byte-identical. `build.rs` now watches the whole `src/` tree so the header regenerates on any module change.

### Behavior Changes

### Breaking Changes

### Deprecations

### API Changes
