# NEXT CHANGELOG

## Release v1.4.0

### Major Changes

### New Features and Improvements

### Bug Fixes

- Arrow builders now reject unsupported ACK callbacks instead of silently
  discarding them. Configuring `ackCallback` before calling `ArrowStreamBuilder.build()`
  throws `IllegalStateException`.

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
