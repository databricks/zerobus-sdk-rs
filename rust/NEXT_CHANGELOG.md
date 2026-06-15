# NEXT CHANGELOG

## Release v2.3.0

### Major Changes

### New Features and Improvements

- **Prototype**: persistent (resumable) ingestion streams. New `PersistentStream` type plus
  `StreamBuilder::build_persistent()` / `resume_persistent(stream_id)`. `build_persistent` opens a
  new stream and exposes the server-assigned id via `PersistentStream::stream_id()`;
  `resume_persistent` reopens it by id and continues after the server's `last_committed_offset`.
  Both go through the existing `EphemeralStream` RPC (sending `CreateStream` without vs. with a
  `stream_id`), using the newly un-reserved `CreateIngestStreamRequest.stream_id` /
  `CreateIngestStreamResponse.last_committed_offset` proto fields. Separate, additive path from the
  production `ZerobusStream` (no landing zone / recovery yet).

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
