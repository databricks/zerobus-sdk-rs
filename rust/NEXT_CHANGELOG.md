# NEXT CHANGELOG

## Release v2.4.0

### Major Changes

### New Features and Improvements

### Bug Fixes

- Fixed `VARIANT` columns nested inside a `STRUCT`, `ARRAY`, or `MAP` failing with `unknown primitive type 'variant'` when building a descriptor from a Unity Catalog schema. Nested `VARIANT` now maps to `string` (unshredded JSON-encoded text) at any depth, matching the top-level behavior. Applies to both the protobuf and Arrow Flight schema paths (`descriptor_from_uc_columns` / `arrow_schema_from_uc_columns`), and so also fixes the Go and C++ SDKs, which build descriptors through the C FFI.

### Documentation

- Reworked ingestion docs to lead with the high-throughput pattern (ingest in a loop, then `flush()` once) and explicitly warn against calling `wait_for_offset()` after every record. Updated the README, crate- and method-level doc comments (`ingest_record_offset`, `ingest_records_offset`, `wait_for_offset`, `flush`), and the `json`/`proto` single-record examples accordingly.

### Internal Changes

- Established `rust/sdk/zerobus_service.proto` as the single canonical gRPC schema, now referenced directly by the cgo Go SDK tests and the Java SDK build instead of their own duplicated (and drifted) copies. No schema or behavior change for the Rust core — the file stays where it was.

### Breaking Changes

### Deprecations

### API Changes
