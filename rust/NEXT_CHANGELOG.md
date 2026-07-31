# NEXT CHANGELOG

## Release v2.4.0

### Major Changes

### New Features and Improvements

- Added the callback bridge used by multiplexed streams to report `MessageId`
  values while preserving the existing `AckCallback` API. Each sub-stream
  callback converts its stream-local `OffsetId` into a message ID containing
  both the sub-stream index and offset.

### Bug Fixes

- Custom gRPC metadata headers now reject invalid names without panicking, recognize reserved
  headers case-insensitively, and keep authorization values marked sensitive.
- Arrow recovery now preserves non-retryable custom headers-provider errors instead of replacing
  them with a retryable `Unavailable` status.

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

- Generalized `AckCallback` over its identifier type while preserving
  `OffsetId` as the default for existing single-stream callbacks. Multiplexed
  callbacks use the same trait with `MessageId` as the identifier type.
