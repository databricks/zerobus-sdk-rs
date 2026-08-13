# NEXT CHANGELOG

## Release v1.5.0

### Major Changes

### New Features and Improvements

### Bug Fixes

### Documentation

- Updated dependency snippets to version 1.3.0 and corrected README and example
  code for stream cleanup, recreation, unique local variables, and a single
  durability barrier after queued ingestion. Clarified that acknowledgment
  callbacks fire once per logical ingest submission, including one callback per
  batch ingest call.
- Documented that published JARs support Java 8 while source builds need JDK 11,
  that macOS JNI artifacts are not in the current release set, that
  `recoveryRetries` defaults to 4, and that `flush()` waits for durability rather
  than callback completion. `recreateStream()` is no longer presented as a safe
  production recovery path.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
