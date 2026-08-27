# NEXT CHANGELOG

## Release v0.3.0

### New Features and Improvements

- Added a borrowing overload of `Stream::ingest_proto_records()`, taking
  `const ProtoRecordView*` and a count. Callers whose encoded records already
  live elsewhere (an arena, a ring buffer, their own record type) no longer have
  to copy every payload into a `std::vector<std::vector<std::uint8_t>>` just to
  hand the batch over. `zerobus::ProtoRecordView` is a non-owning `{data, size}`
  pair whose bytes must stay valid until the call returns. Existing calls are
  unaffected.

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

- New: `zerobus::ProtoRecordView` (in `zerobus/record.hpp`) and
  `Stream::ingest_proto_records(const ProtoRecordView*, std::size_t)`. Additive
  only — no existing signature changed.
