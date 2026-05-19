# Rust SDK

This is the core implementation. All other SDKs depend on it.

## Structure

```
rust/
├── sdk/          # Core SDK crate (databricks-zerobus-ingest-sdk)
├── ffi/          # C FFI crate — cbindgen generates zerobus.h (used by Go, Java)
├── jni/          # JNI crate — Java Native Interface bindings
├── examples/     # JSON and proto ingestion examples
├── tests/        # Integration tests
└── tools/        # Schema generation CLI
```

This is a Cargo workspace. The workspace root is `rust/Cargo.toml`.

## Key modules in `sdk/src/`

- `builder/sdk_builder.rs` — Typestate builder pattern for `ZerobusSdk`
- `errors.rs` — `ZerobusError` enum with `is_retryable()` classification
- `headers_provider.rs` — `HeadersProvider` trait + OAuth implementation
- `landing_zone.rs` — Batches records before sending over gRPC
- `record_types.rs` — `EncodedRecord`, `ProtoMessage`, `JsonString`
- `arrow_stream.rs` — Arrow Flight ingestion (Beta; behind `arrow-flight` feature flag)

## Build commands

Run from `rust/`:

- `make build` / `make build-release` — Build SDK
- `make build-ffi` — Build C FFI library (generates `zerobus.h` + static/dynamic libs)
- `make build-jni` — Build JNI library for Java
- `make test` — Run all workspace tests
- `make lint` — Clippy
- `make fmt` — rustfmt
- `make check` — fmt + lint

## Breaking change rules

Any change to the Rust SDK's public API surface has cascading effects:

1. **Public types/traits** (`pub` items in `lib.rs`) — used directly by Rust consumers. Removing or renaming is a breaking change.
2. **FFI functions** (`rust/ffi/`) — changing a C function signature, struct layout, or removing a function breaks Go and Java. The header `zerobus.h` is auto-generated; always review the diff.
3. **JNI exports** (`rust/jni/`) — changing native method signatures breaks Java. Must stay in sync with `native` declarations in Java source.
4. **PyO3 bindings** (`python/rust/src/lib.rs`) and **NAPI-RS bindings** (`typescript/src/lib.rs`) wrap the Rust SDK directly, so internal refactors can break them if module paths or type signatures change.

**Safe changes**: adding new public items, adding optional parameters with defaults, adding new enum variants (if non-exhaustive), adding fields to `#[non_exhaustive]` structs, adding feature-gated modules.

**Adding methods to public traits** (`HeadersProvider`, `AckCallback`, `TlsConfig`): always provide a default implementation. Users implement these traits externally; adding a method without a default breaks every external impl.

**Deprecation path**: mark with `#[deprecated(since = "x.y.z", note = "Use X instead")]`, keep for at least one minor release, remove in next major.

## Feature flags

- `arrow-flight` — Arrow Flight support (Beta). API is stabilising but may still change before GA.
- `testing` — Test utilities.

## Error handling

`ZerobusError` variants are classified as retryable or non-retryable via `is_retryable()`. This distinction propagates through FFI (`CResult.is_retryable`) to all wrapper SDKs. When adding new error variants, always implement `is_retryable()` for them.

## Async runtime

The SDK uses tokio. The FFI and JNI crates each manage their own tokio runtime instance — do not assume a runtime exists in the caller's context.

## Changelog and documentation

- Every PR must update `rust/NEXT_CHANGELOG.md` under the appropriate section if it changes user-facing behavior.
- Update `rust/README.md` if the change affects usage, setup, or API surface.
- Add or update examples in `rust/examples/` for new or modified APIs.
- Add doc comments (`///`) for all new public items — these render on docs.rs.

## Release

- Version source: `rust/sdk/Cargo.toml` (`version = "x.y.z"`).
- Tag: `rust/v<semver>` → triggers `release-rust.yml` → publishes to crates.io.
- The FFI crate (`rust/ffi/`) has a separate version and tag (`ffi/v*`). An FFI release must happen before Go or Java can pick up Rust core changes, since they link the pre-built static library.
- On version bump PR: move `NEXT_CHANGELOG.md` contents to `CHANGELOG.md`, reset `NEXT_CHANGELOG.md` with the next version header.
- Rust core version bumps may cascade: if the change affects FFI signatures or wrapper behavior, coordinate releases across affected SDKs.
