# NEXT CHANGELOG

## Release v2.5.0

### Major Changes

### New Features and Improvements

### Bug Fixes

- **Arrow Flight — `close()` now propagates flush errors** (Beta): `ZerobusArrowStream::close()` previously swallowed a failed final `flush()` and always returned `Ok(())`, contradicting its documentation and diverging from the proto stream's `close()`. It now returns the flush error after still tearing down the stream and moving pending batches to the failed set (retrievable via `get_unacked_batches()`).
- **Arrow Flight — `max_inflight_batches` now bounds batches awaiting acknowledgment** (Beta): it previously limited only the pre-encode channel, so pending batches could grow unbounded under a slow-acking server. `ingest_batch` now holds a permit until the batch is acked, applying backpressure (it blocks) at the configured limit. `max_inflight_batches = 0` is now rejected with `InvalidArgument` instead of panicking.
- **Arrow Flight — recovery replay is now failure-safe** (Beta): if a batch send failed while replaying after a reconnect, the pending set was drained and lost (unrecoverable via automatic replay or `get_unacked_batches()`). Pending batches (and their in-flight accounting) are now retained so the next recovery attempt replays them.
- **Arrow Flight — no spurious ingest error during recovery handoff** (Beta): a race between starting recovery and an in-flight `ingest_batch` could make ingest return a "stream sender is closed" error for a batch that was actually retained and replayed. The pause and sender-detach is now atomic with respect to ingest, so ingest either sends or buffers (returns `Ok`).
- **Arrow Flight — records ingested during recovery are always replayed** (Beta): `reconnect` reset the recovery counters before rebuilding the pending record ranges, so a record ingested in that window could be assigned a stale range and skipped by replay as already acknowledged. The counter reset and range rebuild are now applied atomically, so a record ingested during a recovery handoff is always replayed.

### Documentation

### Internal Changes

- Added a test-only `test-hooks` Cargo feature that exposes deterministic synchronization seams in the Arrow stream for recovery-race tests. It has zero footprint in default and FFI builds.

### Breaking Changes

### Deprecations

### API Changes
