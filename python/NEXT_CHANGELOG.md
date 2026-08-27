# NEXT CHANGELOG

## Release v1.9.0

### Major Changes

### New Features and Improvements

- Added `FederatedToken` for external-IdP (for example Entra ID) authentication.
  Pass `auth=FederatedToken(idp_token_supplier=..., databricks_client_id=...)`
  to `create_stream` and the SDK exchanges the external IdP token for a
  Zerobus-scoped Databricks token (RFC 8693 token exchange), caching and
  refreshing it. Supports account-level federation (omit `databricks_client_id`,
  identity synced via Automatic Identity Management) and workload identity
  federation (set `databricks_client_id` to the service principal, no secret).
  The `idp_token_supplier` callback may be synchronous or asynchronous. A
  transient failure in the callback (it raised) surfaces as a retryable
  `ZerobusException`, matching OAuth mint failures; caller misuse (a non-string
  return, or an async callback on the sync SDK) surfaces as a non-retryable
  `NonRetriableException`. Each `FederatedToken` instance also partitions the
  shared token cache under account-level federation, so two different identities
  used from one `ZerobusSdk` do not collide and serve each other's token; reusing
  the same `FederatedToken` keeps the cache shared. Existing
  `client_id`/`client_secret` and `headers_provider` calls are unchanged.
- Forwarded the `HeadersProvider.invalidate()` hook through the Python bridge, so
  a custom provider can drop cached auth state when the server rejects a token.

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
