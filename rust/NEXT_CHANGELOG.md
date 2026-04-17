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

- The `generate_files` CLI tool now delegates schema → descriptor conversion
  to the SDK's new `schema` module instead of its own hand-rolled DDL-string
  parser, and renders the resulting `DescriptorProto` back to proto2 text.

### Breaking Changes

- `generate_files`: the emitted `.proto` files have changed shape for
  non-trivial schemas. Consumers regenerating existing files should expect:
  - Field numbers now follow Unity Catalog's `position + 1` (so gaps from
    `DROP COLUMN` under Delta column-mapping are preserved) instead of the
    previous 1,2,3… sequential numbering with a 19000-range skip.
  - Nested struct messages use path-based names (e.g. `OuterInner` instead of
    `Inner`) and are emitted hierarchically inside their parent message.
  - Struct field nullability now honors Unity Catalog's `nullable` flag
    instead of being forced to `optional`.

### Deprecations

### API Changes

