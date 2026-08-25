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
- Fixed Arrow Flight recovery sender lifetime: replacement senders are now published
  only after pending replay succeeds, while initial supervisor handoff and failed or
  cancelled replay promptly drop redundant senders instead of retaining incomplete
  `DoPut` request channels until later teardown.
- The OAuth `expires_in` field is now parsed from a quoted integer (`"3600"`) in
  addition to a plain JSON integer. A value that is missing or does not represent
  a positive integer still yields no token lifetime, as before.
- OAuth token lifetime is now anchored to when the token request starts rather
  than when the response arrives, so a slow response no longer makes the SDK treat
  a token as valid longer than the issuer does. A token already past its
  (start-anchored) expiry is dead on arrival and surfaces a retryable
  `TokenFetchError` when there is no still-valid cached token to serve instead,
  including when the token cache is disabled.
- A stalled proactive token refresh no longer hangs stream creation. For streams
  built with `.oauth(...)`, the refresh is capped at half the stream's configured
  setup budget (`recovery_timeout_ms`) — but only when the cached token has more
  life left than that cap, so a hung endpoint becomes a prompt failure and the
  still-valid cached token is served in time. When too little of the token's life
  remains for that fallback to help, the refresh runs unbounded (like a cold miss)
  so a slow-but-working mint isn't cut off early.
- When a proactive token refresh fails — for any error, including a non-retryable
  one such as revoked credentials — the still-valid cached token is served rather
  than failing the caller. The token was validly issued and hasn't expired, and the
  server re-validates it on every connection, so the error surfaces only when there
  is no still-valid token to fall back to. A refresh that returns a dead-on-arrival
  token falls back the same way.
- After a proactive refresh falls back to the cached token, a short backoff paces
  further attempts so a burst of stream creations cannot turn one token-endpoint
  failure into a mint stampede. The interval scales with the configured token
  refresh buffer and shrinks as the token nears expiry, down to a floor, and never
  extends past expiry.
- OAuth credential invalidation after an auth rejection is more precise and never
  waits on an in-flight mint. Each cached token carries a monotonic generation, and
  invalidation records the rejected generation on the cache entry with a lock-free
  atomic, so it never takes the per-token lock or detaches a mint. A token at or
  below the recorded generation is dropped before its next use and is never refreshed
  from, so the next fetch re-mints; a newer token installed by a concurrent refresh
  has a higher generation and is kept and reused.

### Documentation

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
