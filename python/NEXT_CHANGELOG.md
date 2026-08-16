# NEXT CHANGELOG

## Release v1.7.0

### Major Changes

### New Features and Improvements

- Added `FederatedToken` for external-IdP (for example Entra ID) authentication.
  Pass `auth=FederatedToken(idp_token_supplier=..., databricks_client_id=...)`
  to `create_stream` and the SDK exchanges the external IdP token for a
  Zerobus-scoped Databricks token (RFC 8693 token exchange), caching and
  refreshing it. Supports account-level federation (omit `databricks_client_id`,
  identity synced via Automatic Identity Management) and workload identity
  federation (set `databricks_client_id` to the service principal, no secret).
  The `idp_token_supplier` callback may be synchronous or asynchronous. Existing
  `client_id`/`client_secret` and `headers_provider` calls are unchanged.
- Forwarded the `HeadersProvider.invalidate()` hook through the Python bridge, so
  a custom provider can drop cached auth state when the server rejects a token.

### Bug Fixes

### Documentation

- Corrected README, example, and docstring snippets for record-format selection,
  exception handling, recovery, iterator return values, custom headers, async
  contexts, and durability-aware throughput measurement.
- Documented that `get_unacked_records()` and `recreate_stream()` require an already
  closed stream. An enqueue failure leaves the stream active and that payload was
  never queued, so it is not recovered; close first only to inspect records that
  were already accepted.
- Removed nowait APIs from featured examples. Those calls spawn detached tasks and
  are not safely synchronized with `flush()`. Recommend `ingest_records_offset()`
  plus one `flush()` for bulk ingestion.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
