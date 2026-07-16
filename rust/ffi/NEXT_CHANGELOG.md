# NEXT CHANGELOG

## Release v1.6.0

### Major Changes

### New Features and Improvements

### Bug Fixes

### Documentation

### Internal Changes

- Add tests asserting the ack-callback `Arc` is released, not leaked to a background task, when `zerobus_sdk_create_stream` / `zerobus_sdk_create_stream_with_headers_provider` fails. Uses a `#[cfg(test)]` drop hook keyed to a test-only sentinel `user_data`. Test-only; no ABI or behavior change.
- Move the header-callback helpers `zerobus_alloc_header_array`, `zerobus_alloc_cstring`, and `zerobus_free_headers` from `arrow.rs` into the shared `common.rs` module, next to the `CHeader`/`CHeaders` types they serve. No behavior, signature, or ABI change; their declarations move ahead of the Arrow functions in the generated `zerobus.h`.

### Behavior Changes

### Breaking Changes

### Deprecations

### API Changes
