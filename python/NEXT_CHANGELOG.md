# NEXT CHANGELOG

## Release v1.7.0

### Major Changes

### New Features and Improvements

### Bug Fixes

### Documentation

- Corrected README, example, and docstring snippets for record-format selection,
  exception handling, recovery, iterator return values, custom headers, async
  contexts, and durability-aware throughput measurement.
- Documented that `get_unacked_records()` and `recreate_stream()` require a closed
  stream, and that enqueue failures must be closed before recovery.
- Removed nowait APIs from featured examples. Those calls spawn detached tasks and
  are not safely synchronized with `flush()`. Recommend `ingest_records_offset()`
  plus one `flush()` for bulk ingestion.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
