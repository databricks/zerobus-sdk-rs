# Version changelog

## Release v0.1.0

Initial release of the Zerobus pure-Go SDK — a Go client for ingesting data into
Databricks Delta tables. It speaks gRPC directly and links no Rust FFI, so it
needs no cgo and no prebuilt native libraries. Requires Go 1.25 or later.

### New Features and Improvements

- Ingest into Databricks Delta tables over a `Stream` created from an `SDK`:
  `New` dials lazily, `CreateStream` opens a stream for a table, and `Close`
  tears down streams and the shared connection.
- Two record formats: Protocol Buffers, the default, selected with
  `WithProto(descriptorProto)`, and JSON, selected with `WithJSON()`. The
  `IngestJSON*` methods queue JSON directly on JSON streams and convert it to
  protobuf on proto streams, following protobuf JSON value rules for UC types
  (`DATE`, `TIMESTAMP`, `TIMESTAMP_NTZ`, `BINARY`, `DECIMAL`, `VARIANT`, and
  nested `STRUCT`/`ARRAY`/`MAP`).
- Asynchronous, pipelined ingestion. `IngestRecordOffset` /
  `IngestRecordsOffset` queue a record or batch and return the assigned offset
  as a handle to wait on later; `Flush` waits once for all pending
  acknowledgments, and `WaitForOffset` confirms a single offset for low-volume
  callers that need per-record durability.
- Context-aware variants of every blocking call — `IngestRecordOffsetContext`,
  `IngestRecordsOffsetContext`, `IngestJSONOffsetContext`,
  `IngestJSONRecordsOffsetContext`, `FlushContext`, and
  `WaitForOffsetContext` — so cancellation can interrupt buffer backpressure.
- Asynchronous acknowledgment notification via `WithAckCallback`, for continuous
  streams that should not block in `Flush` or `WaitForOffset`.
- Dynamic protobuf schemas from Unity Catalog. `FetchProtoDescriptorFromUC`
  builds a descriptor from a UC table schema with no `protoc` step; fetch it
  once and reuse the bytes for every stream on that table.
  `Stream.MessageDescriptor`
  exposes the compiled descriptor so callers can build `dynamicpb` messages and
  skip JSON conversion entirely.
- Stream creation is asynchronous by default: `CreateStream` returns after local
  validation while first-open proceeds in the background. `WithWaitForReady`
  makes it block until first-open succeeds or fails terminally.
- Automatic recovery. Streams reconnect on recoverable failures (4 retries by
  default) and can be made strict with `WithRecovery(RecoveryDisabled)`. After a
  close or failure, `GetUnackedRecords` / `GetUnackedBatches` return the records
  that were never acknowledged so they can be replayed.
- Unity Catalog OAuth 2.0 client-credentials authentication, with the minted
  token cached and shared across every stream on the SDK. For custom
  authentication, implement `HeadersProvider` and use
  `CreateStreamWithProvider`; `NewStaticHeadersProvider` supplies fixed headers
  for tests or externally managed credentials.
- Errors surface as `*Error`, carrying the failing operation, a wrapped cause
  reachable through `errors.Is` / `errors.As`, and a retryability verdict that
  the `Retryable(err)` helper reports.
- Tunable buffering and timeouts through stream options:
  `WithRecoveryRetries`, `WithRecoveryTimeout`, `WithRecoveryBackoff`,
  `WithLackOfAckTimeout`, `WithMaxInflight`, `WithMaxBufferedPayloadBytes`,
  `WithMaxBatchRecords`, `WithMaxPayloadBytes`, `WithStreamPausedMaxWait`, and
  `WithFlushTimeout`.
- SDK-level options for `WithApplicationName` (appended to the gRPC user-agent),
  `WithTLSConfig`, `WithHTTPClient`, and `WithProtoDescriptorFetchTimeout`.
- Stream introspection through `ID`, `ServerID`, and `IsClosed`.

### Documentation

- Added the SDK `README.md`, covering the quick start, the loop-then-`Flush`
  ingestion pattern that keeps throughput off a per-record round trip, the
  credential model, both record formats, dynamic proto with UC schema fetch, the
  JSON value rules for UC types, error handling, recovery, the stream option
  reference, and the release process.
- Added package-level godoc for `zerobus` and doc comments across the public
  API.
- Added runnable examples under `examples/` for JSON (single and batch),
  protobuf (single, batch, and a descriptor built at runtime), and Unity
  Catalog-derived descriptors (JSON single, JSON batch, and runtime `dynamicpb`
  messages), each reading its connection settings from the environment.

### Internal Changes

- Hermetic, network-free test suite covering the ingestion core (buffer,
  watermark, ack model, supervisor), the gRPC transport and handshake, OAuth
  token caching and credential invalidation, UC schema conversion, and dynamic
  protobuf conversion. CI runs it under the race detector and again with cgo
  disabled.
- Added a build-only release validation workflow for the SDK and examples
  modules.
- Added a module-local Apache 2.0 license for Go module distribution.
