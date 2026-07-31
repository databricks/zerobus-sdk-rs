# NEXT CHANGELOG

## Release v2.6.0

### Major Changes

### New Features and Improvements

- Arrow Flight: new `arrow_schema_from_uc_columns_with_options` /
  `arrow_schema_from_uc_schema_with_options` accept an `ArrowSchemaOptions`. When
  `annotate_variant_extension` is set, `VARIANT` fields (top-level and nested
  inside structs, arrays, and maps) are annotated with the canonical
  `arrow.parquet.variant` Arrow extension marker (`ARROW:extension:name` + empty
  `ARROW:extension:metadata`), so downstream consumers can recover which fields
  are variants — previously lost when a UC schema was converted to Arrow. The
  physical `Struct<metadata, value>` shape is unchanged. The option defaults to
  off (and `arrow_schema_from_uc_columns` / `arrow_schema_from_uc_schema` are
  unchanged) because the Arrow Flight server's target schema is unmarked, so a
  marked schema forces a per-batch server-side cast; enable it only when a
  consumer needs the annotation and that cost is acceptable.

### Bug Fixes

- **Rust gRPC — reject server over-acks**: cumulative durability
  acknowledgements that exceed the highest request sent on the active
  connection now fail the stream before any record is reported durable or the
  acknowledgement watermark advances.

- **Arrow Flight — invalid acknowledgment watermarks are rejected** (Beta): ack progress is now monotonic, so delayed or duplicate responses cannot move the durable watermark backward. A response claiming more records than were actually submitted on the active connection is rejected without making buffered, unsent records appear durable.

### Documentation

- README: document the opt-in variant extension annotation
  (`ArrowSchemaOptions`) for the Arrow Flight UC→Arrow schema conversion.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
