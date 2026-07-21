# NEXT CHANGELOG

## Release v2.5.0

### Major Changes

### New Features and Improvements

- Arrow Flight: schema-validation rejections now surface as the new
  `ZerobusError::InvalidSchema` variant (carrying the server-reported `causes` as
  typed `SchemaValidationCause` values) instead of a generic `CreateStreamError`.
  This lets callers detect a table/stream schema mismatch — e.g. a column added
  to or dropped from the target table — and re-resolve their schema rather than
  treating it as an opaque invalid-argument failure. The variant is not
  SDK-retryable. This applies both to initial stream setup and to mid-stream
  reconnects: previously a schema change detected during recovery was retried
  until the recovery budget drained and then reported as a generic failure;
  now the non-retriable `InvalidSchema` is surfaced to a blocked `wait_for_offset`
  (or `flush`) immediately so callers can rebuild the stream without downtime.

### Bug Fixes

- **Arrow Flight — `close()` now propagates flush errors** (Beta): `ZerobusArrowStream::close()` previously swallowed a failed final `flush()` and always returned `Ok(())`, contradicting its documentation and diverging from the proto stream's `close()`. It now returns the flush error after still tearing down the stream and moving pending batches to the failed set (retrievable via `get_unacked_batches()`).

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
