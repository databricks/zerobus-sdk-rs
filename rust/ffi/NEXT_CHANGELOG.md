# NEXT CHANGELOG

## Release v1.5.0

### Major Changes

### New Features and Improvements

### Bug Fixes

### Documentation

### Internal Changes

### Behavior Changes

### Breaking Changes

### Deprecations

### API Changes

- Add `zerobus_alloc_header_array` and `zerobus_alloc_cstring` so a non-Rust headers callback can allocate the `CHeaders` it returns through this library instead of its own allocator. The buffers are then freed by `zerobus_free_headers` with the matching allocator, keeping each allocate/free pair inside one library. This removes a cross-allocator free that could corrupt the heap on Windows when the consumer and this statically linked library resolve to different CRT heaps. Additive only — existing functions and `zerobus_free_headers` are unchanged.
