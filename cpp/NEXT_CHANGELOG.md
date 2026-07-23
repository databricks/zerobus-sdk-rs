# NEXT CHANGELOG

## Release v0.2.0

### New Features and Improvements

### Bug Fixes

### Documentation

- Fix release-bundle documentation. `cpp/README.md` was written for a source
  checkout; it now covers both audiences with a "from a release bundle" build
  path (prebuilt-FFI CMake flags, no Rust toolchain), notes that `make`/`ctest`
  and the test suite are source-checkout only, and links prerequisites to a page
  that ships in the bundle.
- Complete the proto examples' Unity Catalog metadata fetch: acquire an OAuth
  token from the service-principal credentials first, and use `curl --fail` so an
  auth or permission error surfaces instead of storing an error body.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
