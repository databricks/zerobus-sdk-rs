# NEXT CHANGELOG

## Release v1.3.0

### New Features and Improvements

- **`WithApplicationName`**: `SdkOption` for `NewZerobusSdk` that appends a caller-supplied identifier (e.g. `my-app/1.0`) to the HTTP `user-agent` header. Wire value becomes `zerobus-sdk-go/<version> <application_name>`. The SDK owns the `user-agent` at the gRPC channel level; values returned by a custom `HeadersProvider` cannot override it.
- **`WithNoTLS`**: `SdkOption` for `NewZerobusSdk` that selects a no-TLS gRPC channel. Intended for local development; do not use against production.

### Behavior changes

- `NewZerobusSdk` no longer infers TLS-off from `http://` endpoints. Use `WithNoTLS()` explicitly. Matches the Rust SDK's model.

### Deprecations

### Bug Fixes

- `user-agent` header now correctly identifies the Go SDK as `zerobus-sdk-go/<version>` instead of the underlying Rust default.

### Documentation

- README: added section for `WithApplicationName`.
- All examples under `go/examples/` now demonstrate `WithApplicationName`.

### Internal Changes

- Rust FFI: introduced a C-builder API (`zerobus_sdk_builder_*`) mirroring the Rust `ZerobusSdkBuilder`. Future options are exposed as additive setters. The legacy `zerobus_sdk_new` entrypoint delegates to the builder.

### API Changes

- `NewZerobusSdk` is now variadic: `func(zerobusEndpoint, unityCatalogURL string, opts ...SdkOption) (*ZerobusSdk, error)`. Strictly additive; existing two-arg callers are unaffected.
