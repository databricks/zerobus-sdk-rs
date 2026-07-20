# NEXT CHANGELOG

## Release v2.5.0

### Major Changes

### New Features and Improvements

### Bug Fixes

- **Arrow Flight — `close()` now propagates flush errors** (Beta): `ZerobusArrowStream::close()` previously swallowed a failed final `flush()` and always returned `Ok(())`, contradicting its documentation and diverging from the proto stream's `close()`. It now returns the flush error after still tearing down the stream and moving pending batches to the failed set (retrievable via `get_unacked_batches()`).

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
