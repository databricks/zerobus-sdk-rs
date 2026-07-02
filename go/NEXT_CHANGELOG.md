# NEXT CHANGELOG

## Release v1.3.0

### New Features and Improvements

### Deprecations

### Bug Fixes

### Documentation

- Clarified throughput guidance in the README, godoc, and examples: ingest records in a loop without waiting and call `Flush()` once, rather than calling `WaitForOffset()` after every record. Documented that the ack watermark is monotonic, so waiting on the last offset confirms all prior records.

### Internal Changes

- Added the new ack-callback fields (`ack_on_ack`, `ack_on_error`, `ack_user_data`) to the cgo `CStreamConfigurationOptions` mirror to keep it byte-identical with the C FFI struct. The Go SDK has no ack-callback API yet and leaves these null, so behavior is unchanged.

### API Changes
