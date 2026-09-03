# NEXT CHANGELOG

## Release v1.9.0

### Major Changes

### New Features and Improvements

- Add the Avro record format (Beta) behind the `avro` feature, exposed in the C
  header under `#if defined(ZEROBUS_AVRO)`: `zerobus_sdk_create_avro_stream`
  (+ `_async` / `_with_headers_provider` / `_with_headers_provider_async`) and
  `zerobus_stream_ingest_avro_record` / `_records` (+ `_async` / `_nowait`).
  Additive — existing functions and `CStreamConfigurationOptions` are unchanged.
  Ephemeral streams only; server support is pending.

### Bug Fixes

### Documentation

### Internal Changes

### Behavior Changes

### Breaking Changes

### Deprecations

### API Changes
