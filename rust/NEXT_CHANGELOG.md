# NEXT CHANGELOG

## Release v2.3.0

### Major Changes

### New Features and Improvements

### Bug Fixes

- `ZerobusStream::get_unacked_records` / `get_unacked_batches` now also return
  records still sitting in the landing zone when the stream was closed without
  a stream failure (e.g. the flush inside `close` timed out). Previously only
  records captured by the failure path were reported, so such records were
  silently missing from the result.

### Documentation

### Internal Changes

- Added `ZerobusStream::signal_shutdown` (crate-private), a `&self`-callable
  helper that flips `is_closed` and cancels the cancellation token. Lets
  `MultiplexedStream` tear down sub-stream background tasks from its poison
  path and `Drop` without needing `&mut`. JoinHandle reaping still happens in
  `close` or the existing `Drop` impl.

### Breaking Changes

### Deprecations

### API Changes
