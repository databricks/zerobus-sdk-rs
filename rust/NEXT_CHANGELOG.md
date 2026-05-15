# NEXT CHANGELOG

## Release v2.1.0

### Major Changes

### New Features and Improvements

### Bug Fixes

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
