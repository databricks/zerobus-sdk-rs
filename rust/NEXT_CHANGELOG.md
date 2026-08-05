# NEXT CHANGELOG

## Release v2.7.0

### Major Changes

### New Features and Improvements

### Bug Fixes

- Arrow Flight acknowledgment deadlines are pending-relative: no timer runs
  while a stream is idle. During normal stream operation, each batch receives
  an absolute deadline when it becomes pending; responses and partial
  acknowledgments do not extend it. Recovery refreshes the deadline when
  the full replay completes and ACK processing can resume on the replacement
  connection.
- Arrow Flight rejects unrepresentable timeout values: stream creation returns
  `InvalidArgument` when ACK or recovery deadlines exceed the platform
  monotonic-clock range. Server-advertised graceful-rotation periods are capped
  at one year.
- Fixed Arrow Flight explicit close during recovery. Close now interrupts recovery
  backoff, connection setup, ready-signal waiting, and replay; the supervisor finalizes
  exactly once, preserves real peer/authentication errors, and retains only the correct
  unacknowledged batch suffixes.
- Fixed Arrow Flight recovery sender lifetime: replacement senders are now published
  only after pending replay succeeds, while initial supervisor handoff and failed or
  cancelled replay promptly drop redundant senders instead of retaining incomplete
  `DoPut` request channels until later teardown.

### Documentation

### Internal Changes

- Reorganized Arrow Flight under `stream/arrow/` with focused API, connection,
  ACK, supervisor, and batch modules and no public API changes. Its tracing
  target now follows the module path:
  `databricks_zerobus_ingest_sdk::stream::arrow`.

### Breaking Changes

### Deprecations

### API Changes
