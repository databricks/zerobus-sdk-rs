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
- Generate valid map entry names regardless of column order and report invalid
  Unity Catalog positions as schema errors.
- Apply `MaxPayloadBytes` to encoded protobuf payloads instead of source JSON.

### Documentation

- Clarified that callbacks may call stream methods, including `Close`.
- Added an example that builds protobuf descriptors and messages directly in Go.

### Internal Changes

- Added UC schema and runtime dynamic-protobuf conversion helpers.

### API Changes

- Protocol Buffers are now the default record type.
- Added `Stream.MessageDescriptor` for constructing protobuf messages directly
  from the configured schema.
