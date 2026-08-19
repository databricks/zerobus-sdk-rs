# NEXT CHANGELOG

## Release v1.7.0

### Major Changes

### New Features and Improvements

### Bug Fixes

- Map non-retryable Rust errors to `NonRetriableException` instead of always
  raising `ZerobusException`. Retryability follows `ZerobusError::is_retryable()`
  (invalid credentials, missing table, schema/argument errors, and other fatal
  conditions). `NonRetriableException` remains a subclass of `ZerobusException`.

### Documentation

- Corrected README, example, and docstring snippets for record-format selection,
  exception handling, recovery, iterator return values, custom headers, async
  contexts, and durability-aware throughput measurement.
- Documented that `get_unacked_records()` and `recreate_stream()` require an already
  closed stream. An enqueue failure leaves the stream active and that payload was
  never queued, so it is not recovered; close first only to inspect records that
  were already accepted.
- Removed nowait APIs from featured examples. Those calls spawn detached tasks and
  are not safely synchronized with `flush()`. Recommend `ingest_records_offset()`
  plus one `flush()` for bulk ingestion.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
