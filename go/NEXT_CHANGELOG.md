# NEXT CHANGELOG

## Release v1.3.0

### New Features and Improvements

### Deprecations

### Bug Fixes

### Documentation

- Clarified throughput guidance in the README, godoc, and examples: ingest records in a loop without waiting and call `Flush()` once, rather than calling `WaitForOffset()` after every record. Documented that the ack watermark is monotonic, so waiting on the last offset confirms all prior records.

### Internal Changes

- The integration-test protobuf bindings (`go/tests/pb`) and the pure-Go SDK bindings (`purego/internal/zerobuspb`) are now generated from the single canonical `rust/sdk/zerobus_service.proto`, instead of local per-module copies. Regenerate with `go/tests/generate_proto.sh` or `go generate ./...` in the purego package. No behavior change — the committed generated code is unchanged.

### API Changes
