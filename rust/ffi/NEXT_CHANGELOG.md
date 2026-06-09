# NEXT CHANGELOG

## Release v1.3.0

### Major Changes

### New Features and Improvements

- **C-builder API for SDK construction**: `zerobus_sdk_builder_new`, per-option setters (`_endpoint`, `_unity_catalog_url`, `_sdk_identifier`, `_application_name`, `_disable_tls`), and `_build` / `_free`. Mirrors the Rust `ZerobusSdkBuilder`; new options are added as setters without ABI breaks. Legacy `zerobus_sdk_new` is retained and delegates to the builder.

### Bug Fixes

### Documentation

### Internal Changes

### Behavior Changes

- `zerobus_sdk_new` no longer infers TLS-off from `http://` endpoints; it uses the default `SecureTlsConfig` regardless of scheme. Callers needing a plain-HTTP channel must use the builder API and call `zerobus_sdk_builder_disable_tls` explicitly.

### Breaking Changes

### Deprecations

### API Changes
