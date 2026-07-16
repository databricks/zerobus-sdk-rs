# NEXT CHANGELOG

## Release v2.4.0

### Major Changes

### New Features and Improvements

### Bug Fixes

### Documentation

- Reworked ingestion docs to lead with the high-throughput pattern (ingest in a loop, then `flush()` once) and explicitly warn against calling `wait_for_offset()` after every record. Updated the README, crate- and method-level doc comments (`ingest_record_offset`, `ingest_records_offset`, `wait_for_offset`, `flush`), and the `json`/`proto` single-record examples accordingly.

### Internal Changes

- Established `rust/sdk/zerobus_service.proto` as the single canonical gRPC schema, now referenced directly by the cgo Go SDK tests and the Java SDK build instead of their own duplicated (and drifted) copies. No schema or behavior change for the Rust core — the file stays where it was.

### Breaking Changes

### Deprecations

### API Changes
