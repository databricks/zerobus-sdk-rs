# NEXT CHANGELOG

## Release v1.2.0

### Major Changes

### New Features and Improvements

### Bug Fixes

### Documentation

- Clarified the high-throughput ingestion pattern across the README, API reference, JSDoc
  doc comments (`ingestRecordOffset`, `ingestRecordsOffset`, `waitForOffset`, `flush`), and
  examples: ingest in a loop without waiting, then wait once on the last offset (the ack
  watermark is monotonic) or `flush()` once at the end. Added explicit warnings that calling
  `waitForOffset()` after every record collapses throughput and should be reserved for
  low-volume cases.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

