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
- **Arrow Flight — unacknowledged batches no longer duplicate durably-acked records** (Beta): after a terminal failure, a partially-acknowledged auto-chunked batch was retained whole, so retrying it via `get_unacked_batches()` re-sent the already-persisted prefix. Retained batches are now sliced to their un-acknowledged suffix, and `get_unacked_batches()` returns a consistent, idempotent snapshot: closure and the terminal drain are serialized with `ingest_batch`, so a batch accepted concurrently with recovery/close is included rather than omitted from the first snapshot and revealed by a later call.
- **Arrow Flight — `flush()`/`wait_for_offset()` return the real terminal error** (Beta): on a terminal failure a blocked `flush()`/`wait_for_offset()` could return a generic "timed out" or "stream is closed" error instead of the actual cause. All terminal paths — mid-stream server error, server stream end, and ack timeout — now publish the error and wake waiters with it. Additionally, an acknowledgment that lands just before the stream closes now resolves as `Ok(())` instead of a spurious closed error (which could otherwise trigger a duplicate retry of an already-durable batch).
- **Arrow Flight — recovery surfaces the original reconnect failure** (Beta): after reconnect attempts were exhausted, the stream terminated with a synthetic "Reconnection failed" error, losing the underlying cause and its retry classification. The real reconnect error is now carried through: its message is surfaced, an auth rejection still invalidates cached credentials and retries (so a fresh token can be minted), and if retries are exhausted the original error is reported rather than a synthetic one.

### Documentation

### Internal Changes

- Added a test-only `test-hooks` Cargo feature that exposes deterministic synchronization seams in the Arrow stream for recovery-race tests. It has zero footprint in default and FFI builds.

### Breaking Changes

### Deprecations

### API Changes
