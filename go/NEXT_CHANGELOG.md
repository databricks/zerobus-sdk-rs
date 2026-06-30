# NEXT CHANGELOG

## Release v1.3.0

### New Features and Improvements

### Deprecations

### Bug Fixes

### Documentation

- Clarified throughput guidance in the README, godoc, and examples: ingest records in a loop without waiting and call `Flush()` once, rather than calling `WaitForOffset()` after every record. Documented that the ack watermark is monotonic, so waiting on the last offset confirms all prior records.

### Internal Changes

### API Changes
