# NEXT CHANGELOG

## Release v2.9.0

### Major Changes

### New Features and Improvements

- Added `ZerobusArrowStream::take_offset_details`, which returns
  `Some(OffsetDetails)` with, for the batch at a given offset, both `wire_byte_size`
  (actual bytes on the wire, after IPC compression) and `uncompressed_byte_size`
  (encoded size before compression), plus a running total for each. Call it after
  `wait_for_offset` to report "bytes sent" metrics (e.g. network bytes vs. component
  bytes) without re-serialising the `RecordBatch` yourself. All sizes accumulate every
  transmission, so a batch re-sent during connection recovery counts each send. Sizes
  are best-effort instrumentation captured as the SDK encodes each batch and are
  consumed on read; it returns `None` when no size is recorded (not yet encoded, or
  evicted from the bounded cache). The uncompressed size is codec-independent (measured
  from the Arrow buffers, excluding IPC framing), so a batch reports the same value with
  or without compression. `OffsetDetails` is a new public type. (Beta Arrow API.)

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
