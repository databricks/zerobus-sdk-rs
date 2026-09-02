# NEXT CHANGELOG

## Release v2.9.0

### Major Changes

### New Features and Improvements

- JSON and protobuf streams now use a dedicated gRPC connection by default.
  Use `ZerobusSdk::builder().connection_per_stream(false)` to retain the prior
  shared HTTP/2 connection behavior. Arrow Flight streams are unchanged.
- Added `StreamBuilder::multiplexed(stream_count)` for atomically constructing
  1–64 managed JSON or protobuf sub-streams with capacity-aware round-robin
  routing, recovery, acknowledgments, flush, close, and unacknowledged-record
  aggregation.
- Multi-lane multiplexed construction opens sub-streams concurrently with
  bounded random startup jitter and cleans up every successful open if any
  sibling fails or construction is cancelled. Single-lane construction opens
  immediately.
- Multiplexed streams divide the mux-wide `max_inflight_requests` budget evenly
  across sub-streams instead of allocating the full capacity for every lane.

### Bug Fixes

- Fixed multiplexed teardown to avoid polling a completed receiver task twice,
  poison all lanes after any terminal lane failure, and close stalled lanes
  concurrently within one flush-timeout window.
- Multiplexed failure handling now preserves the terminal server error, fails
  pending sibling callbacks, retains every unacknowledged record for recovery,
  and avoids a second flush-timeout window during poisoning.

### Documentation

- Added multiplexed-stream guidance and a complete compiled-protobuf example
  with queued ingestion, periodic flushing, `MessageId` callbacks, and close.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

- Exported `MultiplexedStream`, `MultiplexedStreamBuilder`, and `MessageId` with
  default features.
- Added `StreamBuilder::multiplexed_ack_callback` for `MessageId` callbacks
  while preserving `ack_callback` for ordinary `OffsetId` callbacks; each
  terminal mode rejects the other mode's callback.
- Added `MultiplexedStream::new_record()` for dynamic-protobuf records.
