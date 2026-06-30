# NEXT CHANGELOG

## Release v1.4.0

### Major Changes

### New Features and Improvements

- Build the FFI library for Linux musl targets (`x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl`), enabling C/C++ and Go-on-Alpine consumers to link `libzerobus_ffi.a` / `libzerobus_ffi.so` on musl-based (Alpine) containers. Artifacts ship in the `linux-musl-x86_64` / `linux-musl-aarch64` directories.

### Bug Fixes

### Documentation

### Internal Changes

- Split `src/lib.rs` into per-surface modules (`common`, `arrow`, `builder`, `sdk`, `stream`, `proto_schema`); `lib.rs` now holds only module declarations and re-exports. Pure refactor — no API, ABI, or behavior change; `zerobus.h` is byte-identical. `build.rs` now watches the whole `src/` tree so the header regenerates on any module change.

### Behavior Changes

### Breaking Changes

### Deprecations

### API Changes

- Add `zerobus_alloc_header_array` and `zerobus_alloc_cstring` so a non-Rust headers callback can allocate the `CHeaders` it returns through this library instead of its own allocator. The buffers are then freed by `zerobus_free_headers` with the matching allocator, keeping each allocate/free pair inside one library. This removes a cross-allocator free that could corrupt the heap on Windows when the consumer and this statically linked library resolve to different CRT heaps. Additive only — existing functions and `zerobus_free_headers` are unchanged.
