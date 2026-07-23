# NEXT CHANGELOG

## Release v1.3.0

### New Features and Improvements

### Deprecations

### Bug Fixes

- Fixed a use-after-free in which a custom `HeadersProvider` could be freed while
  a background worker was still calling into it during connection recovery. The
  provider's `cgo.Handle` ownership is now handed to the FFI, which releases it
  (via a new destroy callback) only after any in-flight `GetHeaders` has
  returned, instead of deleting it on stream close. This removes the per-stream
  handle registry. No public API change.

### Documentation

- Clarified throughput guidance in the README, godoc, and examples: ingest records in a loop without waiting and call `Flush()` once, rather than calling `WaitForOffset()` after every record. Documented that the ack watermark is monotonic, so waiting on the last offset confirms all prior records.

### Internal Changes

- Added Darwin AMD64 and ARM64 static-library artifacts to the Go release build by cross-compiling the FFI with Zig, so release PRs can include the full supported platform matrix without a macOS runner.
- Added the new ack-callback fields (`ack_on_ack`, `ack_on_error`, `ack_user_data`) to the cgo `CStreamConfigurationOptions` mirror to keep it byte-identical with the C FFI struct. The Go SDK has no ack-callback API yet and leaves these null, so behavior is unchanged.
- The integration-test protobuf bindings (`go/tests/pb`) and the pure-Go SDK bindings (`purego/internal/zerobuspb`) are now generated from the single canonical `rust/sdk/zerobus_service.proto`, instead of local per-module copies. Regenerate with `go/tests/generate_proto.sh` or `go generate ./...` in the purego package. No behavior change — the committed generated code is unchanged.

### API Changes
