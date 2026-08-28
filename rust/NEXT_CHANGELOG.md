# NEXT CHANGELOG

## Release v2.8.0

### Major Changes

### New Features and Improvements

- Promoted Arrow Flight ingestion to general availability across its APIs,
  documentation, and examples. The `arrow-flight` Cargo feature remains opt-in.

### Bug Fixes

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

### Breaking Changes

### Deprecations

### API Changes

- Added the in-development persistent-stream protobuf contract for creating,
  resuming, ingesting into, and retiring durable streams.
