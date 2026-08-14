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

### Internal Changes

- Generalize the stream core's durability model so one implementation serves both
  the atomic proto and JSON protocols and the record-count protocol the Arrow
  Flight path needs: acknowledgments are tracked as cumulative durability units,
  a partially acknowledged item replays only its unacknowledged suffix,
  submission receipts report how much of a multi-frame send reached the server,
  and encoder, ack-model, and opener seams let a protocol instantiate the core
  over its own payload type. Proto and JSON behavior is unchanged, and nothing is
  exposed through a public API yet.

### Breaking Changes

### Deprecations

### API Changes
