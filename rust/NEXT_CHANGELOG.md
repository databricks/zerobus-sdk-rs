# NEXT CHANGELOG

## Release v2.1.0

### Major Changes

### New Features and Improvements

### Bug Fixes

- **Arrow Flight: fix race condition causing stale wire offsets after non-close-signal
  recovery.** When a stream broke via a server error or ack timeout (rather than a graceful
  close signal), the supervisor did not set the ingest-pause gate before starting reconnect.
  A concurrent `ingest_batch` call could send a batch with a pre-recovery wire offset,
  which the server rejects with error code 4002 (`NonIncrementalOffset`), exhausting
  recovery retries and failing the entire stream. Fix: set `is_paused = true` immediately
  when entering the retriable-error retry branch, symmetric with the existing close-signal
  path.

- **Arrow Flight: restore automatic batch chunking at 2 MiB.** Reverted the manual
  zero-copy IPC encoding introduced in v2.0.0 back to `FlightDataEncoderBuilder`, which
  automatically chunks large `RecordBatch` values at 2 MiB. The zero-copy refactor had
  removed this chunking, causing large batches to exceed tonic's default 4 MiB server
  decode limit and be silently dropped. `ingest_ipc_batch` now deserialises IPC bytes
  into a `RecordBatch` before encoding, so it correctly benefits from the same chunking
  and supports streams with `ipc_compression` enabled.

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
