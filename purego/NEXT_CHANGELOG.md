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

### Bug Fixes

### Documentation

- Flush recovery no longer treats every flush error as terminal. The JSON single
  example retrieves unacknowledged records on flush failure before teardown and
  replays them on a fresh stream. The JSON batch example demonstrates that a
  batch produces a single ack callback event and waits for that callback before
  exit.

### Internal Changes

- Generalize the stream core's durability model so one implementation serves both
  the atomic proto and JSON protocols and the record-count protocol the Arrow
  Flight path needs: acknowledgments are tracked as cumulative durability units,
  a partially acknowledged item replays only its unacknowledged suffix,
  submission receipts report how much of a multi-frame send reached the server,
  and encoder, ack-model, and opener seams let a protocol instantiate the core
  over its own payload type. Proto and JSON behavior is unchanged, and nothing is
  exposed through a public API yet.
- Add `internal/arrowproto`, the Arrow IPC payload and Flight frame encoder for
  the upcoming Arrow ingestion path. It fills the stream core's encoder seam with
  the two hooks that are trivial for proto and JSON but not for Arrow: row counts
  as durability units, and real row-range slicing so a partially acknowledged
  batch replays only its unacknowledged suffix. Frames are chunked to 2 MiB based
  on measured protobuf size. Nothing is exposed through a public API yet.
- Charge Arrow payloads against the buffered-bytes limit before decoding them.
  Compressed Arrow IPC input is inspected for its declared uncompressed buffer
  sizes, so a highly compressible payload cannot pass admission and then expand
  past the limit while Arrow materializes it.
- Add `github.com/apache/arrow-go/v18` and `github.com/google/flatbuffers`
  dependencies. arrow-go requires `google.golang.org/grpc` v1.82.0, which raises
  this module's grpc minimum from v1.81.1.

### Breaking Changes

### Deprecations

### API Changes
