# NEXT CHANGELOG

## Release v2.5.0

### Major Changes

### New Features and Improvements

### Bug Fixes

- **Arrow Flight — `close()` now propagates flush errors** (Beta): `ZerobusArrowStream::close()` previously swallowed a failed final `flush()` and always returned `Ok(())`, contradicting its documentation and diverging from the proto stream's `close()`. It now returns the flush error after still tearing down the stream and moving pending batches to the failed set (retrievable via `get_unacked_batches()`).
- **Arrow Flight — `max_inflight_batches` now bounds batches awaiting acknowledgment** (Beta): it previously limited only the pre-encode channel, so pending batches could grow unbounded under a slow-acking server. `ingest_batch` now holds a permit until the batch is acked, applying backpressure (it blocks) at the configured limit. `max_inflight_batches = 0` is now rejected with `InvalidArgument` instead of panicking.
- **Arrow Flight — recovery replay is now failure-safe** (Beta): if a batch send failed while replaying after a reconnect, the pending set was drained and lost (unrecoverable via automatic replay or `get_unacked_batches()`). Pending batches (and their in-flight accounting) are now retained so the next recovery attempt replays them.

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
