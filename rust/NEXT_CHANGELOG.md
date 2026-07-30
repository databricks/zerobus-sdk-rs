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

- **Arrow Flight — invalid acknowledgment watermarks are rejected** (Beta): ack progress is now monotonic, so delayed or duplicate responses cannot move the durable watermark backward. A response claiming more records than were actually submitted on the active connection is rejected without making buffered, unsent records appear durable.
- Fixed Arrow Flight stream rotation to half-close the request and drain the server response before reconnecting, avoiding HTTP/2 stream resets during graceful close. Request EOF and response draining now make progress concurrently under one deadline, so queued acknowledgments are still applied while the request is settling. `stream_paused_max_wait_time_ms = Some(0)` now skips only the ACK wait; it still allows up to 500ms for bounded transport cleanup, including a best-effort local EOF attempt when the advertised server grace is shorter. Explicit close now preserves the newest permanent peer error and reports a connection timeout if request EOF or response draining does not finish before the cleanup deadline, including when a replacement request cannot produce a drainable response. Recovery timeouts and reconnect-ready timeouts also perform bounded request EOF and response draining when a replacement request is already live, applying any acknowledgments received during that cleanup before reporting the timeout. Streams configured with `recovery = false` now honor server close signals: they pause, half-close, and terminate after cleanup without reconnecting; batches accepted during the grace window remain available from `get_unacked_batches()`.

### Documentation

- README: document the opt-in variant extension annotation
  (`ArrowSchemaOptions`) for the Arrow Flight UC→Arrow schema conversion.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
