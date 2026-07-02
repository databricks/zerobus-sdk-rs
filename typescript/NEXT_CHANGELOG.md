# NEXT CHANGELOG

## Release v1.2.0

### Major Changes

### New Features and Improvements

- `ZerobusSdk` constructor now accepts an optional `options` object
  (`ZerobusSdkOptions`) as its third argument. Its `applicationName` field is
  appended to the HTTP `user-agent` header sent on every request
  (e.g. `zerobus-sdk-ts/1.2.0 my-app/1.0`), enabling server-side attribution.
  The SDK now also correctly identifies itself as `zerobus-sdk-ts` rather than
  falling back to the underlying `zerobus-sdk-rs` identifier.

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

