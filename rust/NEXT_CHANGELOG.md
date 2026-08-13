# NEXT CHANGELOG

## Release v2.7.0

### Major Changes

### New Features and Improvements

### Bug Fixes

- Arrow builders now reject unsupported ACK callbacks instead of silently
  discarding them. Remove `.ack_callback(...)` before calling `build_arrow()`;
  otherwise it returns `InvalidArgument`.
- Arrow Flight acknowledgment deadlines are pending-relative: no timer runs
  while a stream is idle. During normal stream operation, each batch receives
  an absolute deadline when it becomes pending; responses and partial
  acknowledgments do not extend it. Recovery refreshes the deadline when
  the full replay completes and ACK processing can resume on the replacement
  connection.
- Arrow Flight rejects unrepresentable timeout values: stream creation returns
  `InvalidArgument` when ACK, recovery, or flush deadlines exceed the platform
  monotonic-clock range. Server-advertised graceful-rotation periods are capped
  at one year.
- Arrow Flight close is cancellation-safe and half-closes the active request before
  bounded response draining. ACK success is decided when the durable watermark is
  applied relative to the original flush deadline. Close during recovery cancels the
  attempt, retains the unacknowledged suffix, and returns the error that triggered the
  current attempt. Close during an existing recovery or server-requested rotation keeps
  that trigger even if every record is durable, so an error can coexist with an empty
  unacknowledged-batch set. After a request-send failure, one ready response may still
  be applied; later stream items are not discarded in order to start recovery.
- Fixed Arrow Flight recovery sender lifetime: replacement senders are now published
  only after pending replay succeeds, while initial supervisor handoff and failed or
  cancelled replay promptly drop redundant senders instead of retaining incomplete
  `DoPut` request channels until later teardown.

### Documentation

- Corrected README and rustdoc examples so their dependencies, feature flags,
  imports, and mutable stream bindings compile as shown.
- Batch examples and primary rustdoc now queue all records and wait once with
  `flush()` or the last offset, and no longer refer to removed `ingest_record()`
  / `ingest_records()` methods.

### Internal Changes

- Added Arrow C Data `RecordBatch` conversion behind a disabled-by-default
  wrapper-only SDK feature so current and future native bindings can share one
  ownership implementation. No supported Rust SDK or Flight behavior changed.
- Reorganized Arrow Flight under `stream/arrow/` with focused API, connection,
  ACK, supervisor, and batch modules and no public API changes. Its tracing
  target now follows the module path:
  `databricks_zerobus_ingest_sdk::stream::arrow`.

### Breaking Changes

### Deprecations

### API Changes
