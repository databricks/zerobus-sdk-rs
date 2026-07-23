# NEXT CHANGELOG

## Release v2.5.0

### Major Changes

### New Features and Improvements

- Arrow Flight: schema-validation rejections now surface as the new
  `ZerobusError::InvalidSchema` variant (carrying the server-reported `causes` as
  typed `SchemaValidationCause` values) instead of a generic `CreateStreamError`.
  This lets callers detect a table/stream schema mismatch — e.g. a column added
  to or dropped from the target table — and re-resolve their schema rather than
  treating it as an opaque invalid-argument failure. The variant is not
  SDK-retryable. This applies both to initial stream setup and to mid-stream
  reconnects: on a reconnect, the typed error flows through the terminal
  recovery path (a non-retriable failure ends recovery and is reported as-is),
  so a schema change detected during recovery is surfaced to a blocked
  `wait_for_offset` / `flush` as `InvalidSchema` — letting callers rebuild the
  stream without downtime — rather than being retried until the recovery budget
  drains and reported as a generic failure.

### Bug Fixes

- **Proxy target TLS is now applied exactly once**: Standard and Arrow Flight streams now keep the CONNECT tunnel raw after establishing an HTTP or HTTPS proxy connection, allowing tonic to apply endpoint TLS once instead of attempting a second TLS handshake for HTTPS targets.
- **Arrow Flight — proxy configuration now applies to all connections** (Beta): Arrow streams now honor the same `grpc_proxy`/`https_proxy`/`http_proxy`, `no_grpc_proxy`/`no_proxy`, and caller-supplied `connector_factory` policy as standard streams, including replacement channels created during recovery.
- **Arrow Flight — initial setup refreshes one stale credential** (Beta): an `Unauthenticated` or `PermissionDenied` response during initial stream setup previously failed after invalidating the rejected credential. When recovery is enabled and a recovery retry remains, initial setup now spends at most one such retry so the headers provider can refresh the credential; a repeated auth rejection remains terminal, and auth errors remain globally non-retryable. Provider invalidation shares the setup deadline and preserves the auth rejection if it stalls instead of consuming the remaining budget as generic timeout retries.
- **Arrow Flight — `close()` now propagates flush errors and survives cancellation** (Beta): `ZerobusArrowStream::close()` previously swallowed a failed final `flush()` and always returned `Ok(())`, contradicting its documentation and diverging from the proto stream's `close()`. It now returns the flush error after still tearing down the stream and moving pending batches to the failed set (retrievable via `get_unacked_batches()`). If the close future is cancelled after teardown starts, the stream enters a non-ingestable `Closing` state and a later `close()` resumes teardown without waiting for another flush.
- **Arrow Flight — `max_inflight_batches` now bounds batches awaiting acknowledgment** (Beta): it previously limited only the pre-encode channel, so pending batches could grow unbounded under a slow-acking server. `ingest_batch` now holds a permit until the batch is acked, applying backpressure (it blocks) at the configured limit. `max_inflight_batches = 0` is now rejected with `InvalidArgument` instead of panicking.
- **Arrow Flight — recovery replay is now failure-safe** (Beta): if a batch send failed while replaying after a reconnect, the pending set was drained and lost (unrecoverable via automatic replay or `get_unacked_batches()`). Pending batches (and their in-flight accounting) are now retained so the next recovery attempt replays them.
- **Arrow Flight — no spurious ingest error during recovery handoff** (Beta): a race between starting recovery and an in-flight `ingest_batch` could make ingest return a "stream sender is closed" error for a batch that was actually retained and replayed. The pause and sender-detach is now atomic with respect to ingest, so ingest either sends or buffers (returns `Ok`).
- **Arrow Flight — records ingested during recovery are always replayed** (Beta): `reconnect` reset the recovery counters before rebuilding the pending record ranges, so a record ingested in that window could be assigned a stale range and skipped by replay as already acknowledged. The counter reset and range rebuild are now applied atomically, so a record ingested during a recovery handoff is always replayed.
- **Arrow Flight — unacknowledged batches no longer duplicate durably-acked records** (Beta): after a terminal failure, a partially-acknowledged auto-chunked batch was retained whole, so retrying it via `get_unacked_batches()` re-sent the already-persisted prefix. Retained batches are now sliced to their un-acknowledged suffix, and `get_unacked_batches()` returns a consistent, idempotent snapshot: closure and the terminal drain are serialized with `ingest_batch`, so a batch accepted concurrently with recovery/close is included rather than omitted from the first snapshot and revealed by a later call.
- **Arrow Flight — `flush()`/`wait_for_offset()`/`close()` return the real terminal error** (Beta): on a terminal failure a blocked `flush()`/`wait_for_offset()` could return a generic "timed out" or "stream is closed" error instead of the actual cause. All terminal paths — mid-stream server error, server stream end, and ack timeout — now publish the error and wake waiters with it. `close()` likewise returns the terminal error when the stream was already closed by a background failure (rather than `Ok(())`), so the common ingest-then-`close()` pattern no longer hides failed batches. Additionally, an acknowledgment that lands just before the stream closes now resolves as `Ok(())` instead of a spurious closed error (which could otherwise trigger a duplicate retry of an already-durable batch).
- **Arrow Flight — empty (zero-row) batches are rejected** (Beta): `ingest_batch()`/`ingest_ipc_batch()` now return `InvalidArgument` for a zero-row `RecordBatch`. Previously it entered the pending set but the Flight encoder emits no data message for zero rows, so it was never sent or acknowledged and `flush()`/`wait_for_offset()` would hang until they timed out.
- **Arrow Flight — recovery surfaces the original reconnect failure** (Beta): after reconnect attempts were exhausted, the stream terminated with a synthetic "Reconnection failed" error, losing the underlying cause and its retry classification. The real reconnect error is now carried through: its message is surfaced, an auth rejection still invalidates cached credentials and retries (so a fresh token can be minted), and if retries are exhausted the original error is reported rather than a synthetic one. A single `recovery_timeout_ms` deadline bounds reconnect plus credential invalidation, and terminal cleanup is bounded separately; a stalled custom provider therefore cannot hang recovery or leave the supervisor alive indefinitely.
- **Arrow Flight — authorization metadata is now sensitive** (Beta): Bearer credentials are marked sensitive in tonic metadata, matching the standard gRPC stream and preventing token values from appearing in metadata debug output. Invalid authorization header values now return `InvalidUCTokenError` instead of `InvalidArgument`, also matching the standard gRPC stream.

### Documentation

### Internal Changes

- Added a test-only `test-hooks` Cargo feature that exposes deterministic synchronization seams in the Arrow stream for recovery-race tests. It has zero footprint in default and FFI builds.

### Breaking Changes

### Deprecations

### API Changes
