# NEXT CHANGELOG

## Release v1.4.0

### Major Changes

### New Features and Improvements

- **`ZerobusSdk(application_name=...)`**: Both the sync and async `ZerobusSdk`
  constructors accept an optional `application_name` argument. When set, it is
  appended to the HTTP `user-agent` header sent on every request, so callers
  can be identified in server-side telemetry. The SDK prefix is preserved, so
  the wire value becomes `zerobus-sdk-py/<version> <application_name>`. By
  convention use `<product>/<version>` (e.g. `"my-app/1.0"`).

### Bug Fixes

### Documentation

- README: documented the `application_name` constructor argument.
- Examples: all examples under `examples/` now demonstrate `application_name`.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

- `ZerobusSdk.__init__` gains an optional `application_name: Optional[str] = None`
  parameter (sync and async). Strictly additive; existing two-argument callers
  are unaffected.
