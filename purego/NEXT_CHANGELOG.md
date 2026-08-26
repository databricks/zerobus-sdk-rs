# NEXT CHANGELOG

## Release v0.2.0

### New Features and Improvements

- Tear the connection down gracefully when the server requests a stream pause
  (`CloseStreamSignal`), as a clean `Close` already did: the client half-closes
  the request stream and drains remaining acknowledgments before reconnecting,
  so the server observes an orderly `END_STREAM` instead of an abrupt cancel. A
  request still being sent is allowed to finish, so an acknowledgment already
  received for it makes the record durable instead of replaying the record on
  the new connection. Teardown stays bounded by the drain budget.
- `ColumnsFromDescriptor` returns the set of column names a serialized protobuf
  descriptor declares, so callers can inspect a table's columns without parsing
  the descriptor themselves. It accepts the bytes returned by
  `FetchProtoDescriptorFromUC` or passed to `WithProto`.

### Bug Fixes

### Documentation

- Flush recovery no longer treats every flush error as terminal. The JSON single
  example retrieves unacknowledged records on flush failure before teardown and
  replays them on a fresh stream. The JSON batch example demonstrates that a
  batch produces a single ack callback event and waits for that callback before
  exit.
- Corrected the README's dynamic JSON rules: a payload carrying a field the
  descriptor does not declare fails conversion; unknown fields are not ignored.

### Internal Changes

- Generalize the stream core's durability model so one implementation serves both
  the atomic proto and JSON protocols and the record-count protocol the Arrow
  Flight path needs: acknowledgments are tracked as cumulative durability units,
  a partially acknowledged item replays only its unacknowledged suffix,
  submission receipts report how much of a multi-frame send reached the server,
  and encoder, ack-model, and opener seams let a protocol instantiate the core
  over its own payload type. Proto and JSON behavior is unchanged, and nothing is
  exposed through a public API yet.
- Add `internal/arrowproto`, the Arrow IPC payload for the upcoming Arrow
  ingestion path. A payload is a canonical self-contained IPC stream materialized
  when it is built, so the core can hold it across a reconnect without pinning the
  caller's `RecordBatch`, and it can be sliced by row so a partially acknowledged
  batch replays only its unacknowledged suffix. Admission sizes a batch from the
  rows it covers rather than the whole buffers it points at, because a slice
  shares its parent's buffers and would otherwise be charged for the entire
  parent. Nested children are sized from the same row window, since slicing
  rebases only the top-level node and leaves a struct's fields and a list's
  values spanning the whole parent, and a batch's custom metadata is charged
  separately because it is written into the IPC message header rather than into
  any column buffer. Nothing is exposed through a public API yet.
- Add the `github.com/apache/arrow-go/v18` dependency. arrow-go requires
  `google.golang.org/grpc` v1.82.0, which raises this module's grpc minimum from
  v1.81.1.

### Breaking Changes

### Deprecations

### API Changes
