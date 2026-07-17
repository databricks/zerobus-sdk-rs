# NEXT CHANGELOG

## Release v2.4.0

### Major Changes

### New Features and Improvements

- Added the callback bridge used by multiplexed streams to report `MessageId`
  values while preserving the existing `AckCallback` API. Each sub-stream
  callback converts its stream-local `OffsetId` into a message ID containing
  both the sub-stream index and offset.

### Bug Fixes

- Fixed `VARIANT` columns in Arrow Flight schemas generated from Unity Catalog
  metadata. `arrow_schema_from_uc_columns` / `arrow_schema_from_uc_schema` now
  project `VARIANT` as `Struct<metadata: LargeBinary not null, value:
  LargeBinary not null>` instead of `LargeUtf8`, matching the server's expected
  binary variant representation. Protobuf descriptor generation continues to
  expose `VARIANT` as `string`.

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

- Generalized `AckCallback` over its identifier type while preserving
  `OffsetId` as the default for existing single-stream callbacks. Multiplexed
  callbacks use the same trait with `MessageId` as the identifier type.
