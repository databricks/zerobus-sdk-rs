# NEXT CHANGELOG

## Release v1.4.0

### Major Changes

### New Features and Improvements

- Build the FFI library for Linux musl targets (`x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl`), enabling C/C++ and Go-on-Alpine consumers to link `libzerobus_ffi.a` / `libzerobus_ffi.so` on musl-based (Alpine) containers. Artifacts ship in the `linux-musl-x86_64` / `linux-musl-aarch64` directories.

### Bug Fixes

- `zerobus_proto_schema_encode_json` now enforces proto2 `required` presence recursively instead of only on top-level columns. A record that omits a non-nullable field nested inside a `STRUCT`, inside an `ARRAY<STRUCT>` element, or inside a `MAP` value is now rejected locally at encode time (with the full field path, e.g. `addr.zip`, `items[2].id`, `props[home].zip`) rather than encoding successfully and being rejected by the server after a network round-trip.

### Documentation

### Internal Changes

- Split `src/lib.rs` into per-surface modules (`common`, `arrow`, `builder`, `sdk`, `stream`, `proto_schema`); `lib.rs` now holds only module declarations and re-exports. Pure refactor — no API, ABI, or behavior change; `zerobus.h` is byte-identical. `build.rs` now watches the whole `src/` tree so the header regenerates on any module change.

### Behavior Changes

### Breaking Changes

### Deprecations

### API Changes
