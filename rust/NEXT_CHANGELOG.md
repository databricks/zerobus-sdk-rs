# NEXT CHANGELOG

## Release v1.2.0

### Major Changes

### New Features and Improvements

- Added the `schema` module with `descriptor_from_uc_columns` /
  `descriptor_from_uc_schema`, which convert a Unity Catalog table schema
  (including nested `STRUCT`, `ARRAY`, and `MAP` columns via `type_json`) into
  a `prost_types::DescriptorProto` that can be passed to
  `TableProperties::descriptor_proto`. Enables building descriptors at runtime
  without pre-generating `.proto` files.

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

