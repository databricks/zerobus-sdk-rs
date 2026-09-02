# NEXT CHANGELOG

## Release v1.7.0

### New Features and Improvements

**Dynamic proto descriptor generation from UC schema**: Added `DescriptorFromUcColumns` and `DescriptorFromUcSchema` functions that build a `*descriptorpb.DescriptorProto` at runtime from Unity Catalog table metadata, eliminating the need for offline `.proto` file generation. Supports all UC scalar types plus `STRUCT`, `ARRAY`, and `MAP` complex types.

### Deprecations

### Bug Fixes

### Documentation

### Internal Changes

### API Changes
