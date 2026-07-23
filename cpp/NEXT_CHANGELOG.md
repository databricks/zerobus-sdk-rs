# NEXT CHANGELOG

## Release v0.2.0

### New Features and Improvements

### Bug Fixes

### Documentation

- Fix release-bundle documentation. `cpp/README.md` was written for a source
  checkout; it now covers both audiences with a "from a release bundle" build
  path (prebuilt-FFI CMake flags, no Rust toolchain), notes that `make`/`ctest`
  and the test suite are source-checkout only, and rewrites links that pointed
  outside the bundle (prerequisites, FFI, contributing, license) so they resolve
  from a bundle too.
- Complete the proto examples' Unity Catalog metadata fetch: explain why the
  dynamic-proto path needs the table schema, acquire an OAuth token from the
  service-principal credentials first, and use `curl --fail` so an auth or
  permission error surfaces instead of storing an error body.
- Fix the credential variable names in the `generate_files` snippet
  (`DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET`, matching the rest of the
  docs).
- Correct the batch-ingest offset docs. `ingest_json_records()` /
  `ingest_proto_records()` return the single logical offset assigned to the
  whole batch, not "the offset of the last record" — a 3-record batch on a fresh
  stream returns offset 0, not 2. Fixed the API docstrings, example comments,
  and expected-output samples.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
