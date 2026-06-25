# NEXT CHANGELOG

## Release v2.3.0

### Major Changes

### New Features and Improvements

- Token caching for the default OAuth path. Tokens obtained via `.oauth(...)` are now cached per table on the `ZerobusSdk` instance and reused across stream creations and recoveries until they near expiry, instead of minting a fresh token on every stream. This reduces load on the Unity Catalog token endpoint for clients that create many short-lived streams. Caching is on by default and can be tuned via `ZerobusSdkBuilder::token_cache_enabled` and `ZerobusSdkBuilder::token_refresh_buffer`.
- On a server-side authentication rejection during stream creation, the cached token is invalidated so the next attempt re-mints (re-checking grants at Unity Catalog), rather than reusing a rejected token until the refresh window.
- `OAuthHeadersProvider::new` now caches tokens for the lifetime of the returned provider (previously it minted a fresh token on every call). Behavior is unchanged for the common path of constructing streams through `ZerobusSdk`, which already shares a cache.
- Add `StreamBuilder::no_auth()` and `NoAuthHeadersProvider` for local testing
  against Zerobus endpoints that do not enforce authentication. Both are gated
  behind the `testing` feature flag.
- Add `ZerobusSdkBuilder::no_tls()` convenience method as a shortcut for
  `.tls_config(Arc::new(NoTlsConfig))` when connecting to plaintext `http://`
  endpoints. Gated behind the `testing` feature flag.
- Added a configurable payload size limit per `ingest_record_offset` / `ingest_records_offset` call. Attempts to ingest more than the limit of encoded record data in a single call now return `ZerobusError::InvalidArgument` immediately, before any network I/O. The default is set slightly below the 10 MiB server limit to leave headroom for the request envelope (protobuf framing/stream metadata), so payloads accepted client-side are not later rejected by the server's transport layer. The limit is tunable per stream via `StreamBuilder::max_ingest_payload_bytes` (gRPC JSON/proto streams only; Arrow Flight streams do not enforce it and log a warning if it is set before `build_arrow()`).
- Added stream lifecycle logging to make recovery observable. The SDK now logs (at `info`) when recovery starts and how many records are pending, and when a recovered stream re-sends unacknowledged records and how many. Each failed stream-creation attempt is logged (at `warn`) with its attempt number and retryability, and a non-retryable failure logs (at `error`) how many records were left unacknowledged (these are retained for retrieval via `get_unacked_records`/`get_unacked_batches`). These counts now distinguish in-flight batches from the true record count they carry (a single `ingest_records` can be one batch but many records), and a terminal recovery failure now always emits a single `error` even when no records remain pending.
- `ZerobusSdkBuilder::application_name` is now normalized and validated in `build()`: the value is trimmed of surrounding whitespace, a blank value is ignored (the default `zerobus-sdk-rs/<version>` identifier is used), and values containing control characters are rejected with `ZerobusError::InvalidArgument` rather than producing a malformed `user-agent` header. Centralizing this in the core means all wrapper SDKs inherit the same handling.

### Bug Fixes
- Redacted the OAuth authorization token from an error log and error message on the gRPC stream-setup path; a malformed token value is no longer written to logs.
- A UC token that cannot be encoded as an HTTP `authorization` header value is now rejected at mint time rather than cached, so it cannot poison the cache and fail every stream creation until its refresh window.
- Arrow Flight stream errors now preserve the server's gRPC status code instead of flattening it to `Unknown`. Previously a `FlightError` was wrapped via `tonic::Status::from_error`, which dropped the inner code, so non-retryable rejections (for example `PermissionDenied`) were misclassified as retryable and auth-rejection detection did not fire on the Arrow path.
- Fixed Arrow Flight streams over-splitting batches that were deserialized from Arrow IPC bytes. The zero-copy IPC reader makes every column buffer report its whole allocation size, so the Flight encoder's `split_batch_for_grpc_response` over-estimated batch size and split it into many small `FlightData` messages — inflating message counts and rendering IPC compression ineffective. The encoder now sizes batches with a slice-aware calculation (`ArrayData::get_slice_memory_size`) so already-sliced/IPC-decoded batches are measured accurately, with no extra data copy. Shipped via a vendored `arrow-flight` (`58.3.0`) referenced as a workspace `path` dependency (see `rust/third_party/arrow-flight`); fixes [arrow-rs#9388](https://github.com/apache/arrow-rs/issues/9388) / [#5352](https://github.com/apache/arrow-rs/issues/5352).

### Documentation

### Internal Changes

- Added `ZerobusStream::signal_shutdown` (crate-private), a `&self`-callable
  helper that flips `is_closed` and cancels the cancellation token. Lets
  `MultiplexedStream` tear down sub-stream background tasks from its poison
  path and `Drop` without needing `&mut`. JoinHandle reaping still happens in
  `close` or the existing `Drop` impl.

### Breaking Changes

### Deprecations

### API Changes

- Added `ZerobusSdkBuilder::token_cache_enabled(bool)` to enable or disable OAuth token caching (default enabled).
- Added `ZerobusSdkBuilder::token_refresh_buffer(Duration)` to configure how long before a cached token's expiry it is refreshed (default 5 minutes).
- Added `HeadersProvider::invalidate` with a default no-op implementation; the SDK calls it when the server rejects the supplied credentials so a provider can drop cached auth state. Existing trait implementations are unaffected.
