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

- **Arrow Flight — acknowledgment deadlines remain active during graceful close** (Beta): a server close signal no longer pauses or extends an already-running batch deadline. Repeated close signals preserve the earliest recovery deadline, while a valid acknowledgement already ready at either deadline is applied before recovery is considered.
- **Arrow Flight — acknowledgment deadlines now start when work becomes pending** (Beta): no timer runs while the stream is idle. The timeout is an absolute deadline for the oldest pending batch: acknowledgments that leave that batch pending do not refresh it, replayed batches receive a fresh deadline on their recovered connection, and malformed or non-progressing responses cannot indefinitely postpone recovery. A valid acknowledgment already ready at expiry is applied before recovery is considered.
- **Arrow Flight — acknowledgment watermarks are handled monotonically** (Beta): delayed, duplicate, or backward watermarks are absorbed without moving durable progress backward. A forward watermark that claims more records than were submitted on the active connection is rejected without making buffered, unsent records appear durable.

### Documentation

- README: document the opt-in variant extension annotation
  (`ArrowSchemaOptions`) for the Arrow Flight UC→Arrow schema conversion.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
