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
  Example and test modules keep the CI toolchain Go versions; the documented
  SDK minimum remains Go 1.21+.

### Internal Changes

### API Changes
