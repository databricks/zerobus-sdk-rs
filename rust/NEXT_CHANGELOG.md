# NEXT CHANGELOG

## Release v2.4.0

### Major Changes

### New Features and Improvements

### Bug Fixes

### Documentation

### Internal Changes

- Bumped the Arrow dependencies (`arrow-array`, `arrow-schema`, `arrow-ipc`, and
  the vendored `arrow-flight` fork) from `58.3` to `59.1`. The vendored
  `arrow-flight` crate was re-synced to upstream `59.1.0` with the slice-aware
  batch-split fix (arrow-rs#9388 / #5352) re-applied. Consumers using the Beta
  `arrow-flight` feature with the re-exported Arrow types must align their own
  Arrow dependencies to `59.x`.

### Breaking Changes

### Deprecations

### API Changes
