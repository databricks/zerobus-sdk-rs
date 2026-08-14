# NEXT CHANGELOG

## Release v1.5.0

### Major Changes

### New Features and Improvements

### Bug Fixes

### Documentation

### Internal Changes

- Bumped `central-publishing-maven-plugin` from 0.5.0 to 0.11.0 so Maven Central
  deploy can parse the Portal status response that now includes a `warnings` field.
  0.5.0 crashed after a successful upload, which made the publisher job look failed
  even when the bundle was already in the Portal.
- The JNI and Java release workflows can optionally import laptop-built macOS
  dylibs from a GitHub Release when CI has no macOS runners.

### Breaking Changes

### Deprecations

### API Changes
