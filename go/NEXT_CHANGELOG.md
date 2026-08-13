# NEXT CHANGELOG

## Release v1.5.0

### New Features and Improvements

### Deprecations

### Bug Fixes

### Documentation

- Corrected consumer installation so tagged releases do not require Rust or
  `go generate`. Documented that `make build-go` depends on the Rust FFI build.
- Documented that `GetUnackedRecords()` must run before `Close()`, that
  `RecordAck.Await()` waits for server durability, and that one stream can be
  used from multiple goroutines.
- Added `Flush()` to copyable example snippets.
- Batch examples name the offset returned by `IngestRecordsOffset` `batchOffset`.
  JSON and protobuf example modules declare `go 1.21` to match the SDK minimum.
  The Arrow example and tests declare `go 1.22.0`, which is what `arrow-go` v18 requires.

### Internal Changes

### API Changes
