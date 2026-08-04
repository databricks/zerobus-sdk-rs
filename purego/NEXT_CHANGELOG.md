# NEXT CHANGELOG

## Release v0.1.0

### New Features and Improvements

- Added context-aware single-record and batch ingestion methods so cancellation
  can interrupt buffer backpressure.
- Exposed recovery timeout/backoff, lack-of-ack timeout, maximum batch records,
  and server-pause wait controls as stream options.
- Added opt-in wait-ready stream creation whose context bounds the complete
  first-open process; asynchronous creation now preserves context values while
  detaching caller cancellation.

### Deprecations

### Bug Fixes

- Correctly normalize IPv6 Zerobus endpoints and reject plaintext HTTP
  endpoints because the SDK always uses TLS.
- Validate application names before adding them to the gRPC user-agent.
- Return offset `-1` (not `0`) from failed ingest calls, matching the
  original Go SDK so callers can distinguish errors from the first real offset.

### Documentation

- Clarified that callbacks may call stream methods, including `Close`.
- Finalized the README for the initial PureGo release.

### Internal Changes

- Added a build-only release validation workflow for the SDK and examples.
- Added a module-local Apache 2.0 license for Go module distribution.

### API Changes

- Protocol Buffers are now the default record type, matching the other SDKs.
