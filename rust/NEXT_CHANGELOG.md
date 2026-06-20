# NEXT CHANGELOG

## Release v2.3.0

### Major Changes

### New Features and Improvements

- Token caching for the default OAuth path. Tokens obtained via `.oauth(...)` are now cached per table on the `ZerobusSdk` instance and reused across stream creations and recoveries until they near expiry, instead of minting a fresh token on every stream. This reduces load on the Unity Catalog token endpoint for clients that create many short-lived streams. Caching is on by default and can be tuned via `ZerobusSdkBuilder::token_cache_enabled` and `ZerobusSdkBuilder::token_refresh_buffer`.
- On a server-side authentication rejection during stream creation, the cached token is invalidated so the next attempt re-mints (re-checking grants at Unity Catalog), rather than reusing a rejected token until the refresh window.
- `OAuthHeadersProvider::new` now caches tokens for the lifetime of the returned provider (previously it minted a fresh token on every call). Behavior is unchanged for the common path of constructing streams through `ZerobusSdk`, which already shares a cache.
- Stream creation and recovery now default to exponential backoff with jitter instead of a fixed retry interval. `recovery_backoff_ms` remains the fixed interval for `RetryStrategy::Fixed` and becomes the base delay for `RetryStrategy::ExponentialBackoffWithJitter` before jitter, capped by `max_recovery_backoff_ms` (default 30 seconds).

### Bug Fixes

- Redacted the OAuth authorization token from an error log and error message on the gRPC stream-setup path; a malformed token value is no longer written to logs.
- A UC token that cannot be encoded as an HTTP `authorization` header value is now rejected at mint time rather than cached, so it cannot poison the cache and fail every stream creation until its refresh window.
- Arrow Flight stream errors now preserve the server's gRPC status code instead of flattening it to `Unknown`. Previously a `FlightError` was wrapped via `tonic::Status::from_error`, which dropped the inner code, so non-retryable rejections (for example `PermissionDenied`) were misclassified as retryable and auth-rejection detection did not fire on the Arrow path.

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

- Added `ZerobusSdkBuilder::token_cache_enabled(bool)` to enable or disable OAuth token caching (default enabled).
- Added `ZerobusSdkBuilder::token_refresh_buffer(Duration)` to configure how long before a cached token's expiry it is refreshed (default 5 minutes).
- Added `HeadersProvider::invalidate` with a default no-op implementation; the SDK calls it when the server rejects the supplied credentials so a provider can drop cached auth state. Existing trait implementations are unaffected.
- Added `RetryStrategy`, `retry_strategy(...)`, and `max_recovery_backoff_ms(...)` for Rust stream recovery configuration.
