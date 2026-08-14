# NEXT CHANGELOG

## Release v2.8.0

### Major Changes

### New Features and Improvements

### Bug Fixes

- Arrow Flight now rolls back logical offsets and record ranges when an enqueue
  fails with recovery disabled. `ingest_batch()` waits for terminal finalization
  and returns the request-stream error; `flush()` and `close()` no longer wait on
  the withdrawn offset. An already-acknowledged flush target still succeeds, while
  `close()` preserves the terminal error and retained batches are immediately available.

### Documentation

- Corrected README and rustdoc examples so their dependencies, feature flags,
  imports, and mutable stream bindings compile as shown.
- Batch examples and primary rustdoc now queue all records and wait once with
  `flush()` or the last offset, and no longer refer to removed `ingest_record()`
  / `ingest_records()` methods.
- Example READMEs and `get_unacked_*` rustdoc now name `ingest_record_offset()` /
  `ingest_records_offset()`. The generate-files tool README quoting is valid shell.
  Arrow example docs place schema validation at stream creation, not the first batch.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
