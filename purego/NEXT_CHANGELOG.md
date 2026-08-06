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
- Added `FetchProtoDescriptorFromUC` for building protobuf descriptors from
  Unity Catalog table schemas.
- Added `RefreshProtoDescriptorFromUC` for bypassing and replacing a cached
  descriptor after a table schema change.
- Coalesced compatible descriptor requests while keeping refreshes newer than
  ordinary fetches, preserving caller cancellation, and stopping abandoned work.
- Added `Stream.IngestJSONOffset` and `Stream.IngestJSONRecordsOffset`. JSON
  streams queue JSON directly; proto streams convert it before ingestion.
- Added Beta Arrow Flight ingestion for typed `arrow.RecordBatch` values and
  self-contained IPC batches, with LZ4 Frame or Zstd compression, 2 MiB
  row-based framing, and record-count recovery for partially acknowledged
  batches.

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
- Added a runnable typed Arrow example that releases each RecordBatch after
  queuing, flushes once, and demonstrates owned unacknowledged-batch replay.
- Finalized the README for the initial PureGo release.

### Internal Changes

- Added UC schema and runtime dynamic-protobuf conversion helpers.
- Added a build-only release validation workflow for the SDK and examples.
- Added a module-local Apache 2.0 license for Go module distribution.
- Bounded Arrow materialization before serialization, streamed Flight chunks
  incrementally, and reused connection dictionary state across chunks.
- Preflighted compressed Arrow IPC expansion, bounded local chunk-search work,
  retained late ACKs after partial sends, and removed a redundant recovery copy.

### API Changes

- Protocol Buffers are now the default record type.
- Added `Stream.MessageDescriptor` for constructing protobuf messages directly
  from the configured schema.
- Added Beta `ArrowStream`, typed and schema-IPC constructors, typed and IPC
  ingestion and recovery methods, Arrow compression, and Flight connection
  timeout options.
