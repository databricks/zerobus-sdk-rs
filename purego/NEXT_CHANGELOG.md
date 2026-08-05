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
- Added `FetchProtoDescriptor` for building protobuf descriptors from Unity
  Catalog table schemas.
- Added `Stream.IngestJSONOffset` and `Stream.IngestJSONRecordsOffset`. JSON
  streams queue JSON directly; proto streams convert it before ingestion.

### Deprecations

### Bug Fixes

- Correctly normalize IPv6 Zerobus endpoints and reject plaintext HTTP
  endpoints because the SDK always uses TLS.
- Validate application names before adding them to the gRPC user-agent.
- Return offset `-1` from failed ingest calls so callers can distinguish errors
  from the first real offset.
- Reuse compatible map entry descriptors when UC field names normalize to the
  same protobuf name, and report incompatible collisions.
- Report invalid Unity Catalog positions as schema errors.
- Apply `MaxPayloadBytes` to encoded protobuf payloads instead of source JSON.
- Reject unknown JSON fields instead of silently dropping them during dynamic
  protobuf conversion.
- Reject nullable collections and null collection values that protobuf cannot
  represent without losing their semantics.
- Classify Unity Catalog HTTP client timeouts and connection failures as
  retryable while keeping TLS certificate failures terminal.

### Documentation

- Clarified that callbacks may call stream methods, including `Close`.
- Added an example that builds protobuf descriptors and messages directly in Go.
- Finalized the README for the initial PureGo release.

### Internal Changes

- Added UC schema and runtime dynamic-protobuf conversion helpers.
- Added a build-only release validation workflow for the SDK and examples.
- Added a module-local Apache 2.0 license for Go module distribution.

### API Changes

- Protocol Buffers are now the default record type.
- Added `Stream.MessageDescriptor` for constructing protobuf messages directly
  from the configured schema.
