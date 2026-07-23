# NEXT CHANGELOG

## Release v2.6.0

### Major Changes

### New Features and Improvements

- Arrow Flight: `arrow_schema_from_uc_columns` / `arrow_schema_from_uc_schema`
  now annotate `VARIANT` fields (top-level and nested inside structs, arrays, and
  maps) with the canonical `arrow.parquet.variant` Arrow extension marker
  (`ARROW:extension:name` + empty `ARROW:extension:metadata`), so downstream
  consumers can recover which fields are variants — previously lost when a UC
  schema was converted to Arrow. The physical `Struct<metadata, value>` shape is
  unchanged, and the Databricks Arrow Flight server ignores and strips the marker
  on the write path, so ingestion behavior is unaffected.

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
