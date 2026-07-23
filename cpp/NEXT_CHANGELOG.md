# NEXT CHANGELOG

## Release v0.2.0

### New Features and Improvements

### Bug Fixes

### Documentation

- Correct the batch-ingest offset docs. `ingest_json_records()` /
  `ingest_proto_records()` return the single logical offset assigned to the
  whole batch, not "the offset of the last record" — a 3-record batch on a fresh
  stream returns offset 0, not 2. Fixed the API docstrings, example comments,
  and expected-output samples.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
