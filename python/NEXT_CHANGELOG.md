# NEXT CHANGELOG

## Release v1.4.0

### Major Changes

### New Features and Improvements

- **`ZerobusSdk(application_name=...)`**: Both the sync and async `ZerobusSdk`
  constructors accept an optional `application_name` argument. When set, it is
  appended to the HTTP `user-agent` header on gRPC requests to the Zerobus
  server (it is not sent on the requests to the login service that mint the
  OAuth token), so callers can be identified in server-side telemetry. The SDK
  prefix is preserved, so the wire value becomes
  `zerobus-sdk-py/<version> <application_name>`. By convention use
  `<product>/<version>` (e.g. `"my-app/1.0"`).

### Bug Fixes

- Fixed the default `recovery_retries`, which was `3` instead of the `4` used by the Rust core and every other SDK (Go, TypeScript, C++). A stream left with the default now makes 4 recovery attempts on transient failures instead of 3, matching the documented cross-SDK behavior. Callers that pass `recovery_retries` explicitly are unaffected. (#438)

### Documentation

- README: documented the `application_name` constructor argument.
- Examples: all examples under `examples/` now demonstrate `application_name`.
- Documented the high-throughput ingestion pattern across the README, docstrings, and
  examples: ingest records in a loop without waiting, then `flush()` once, rather than
  calling `wait_for_offset()` after every record. Added a prominent performance callout
  to the README, throughput notes to the `ingest_record_offset`, `ingest_record_nowait`,
  `wait_for_offset`, and `flush` docstrings (sync and async, including Arrow streams), and
  steering comments to the examples. Added a "Client code patterns" section to
  `python/CLAUDE.md`.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

- `ZerobusSdk.__init__` gains an optional `application_name: Optional[str] = None`
  parameter (sync and async). Strictly additive; existing two-argument callers
  are unaffected.
