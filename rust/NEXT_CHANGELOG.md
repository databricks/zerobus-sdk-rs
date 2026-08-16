# NEXT CHANGELOG

## Release v2.8.0

### Major Changes

### New Features and Improvements

- Added first-class external-IdP token federation (`FederatedTokenProvider`,
  `IdpTokenSupplier`) alongside the existing OAuth client-credentials path. It
  exchanges an external IdP token (for example an Entra ID token) for a
  Zerobus-scoped Databricks token via the RFC 8693 token-exchange grant, caches
  and refreshes it through the existing `TokenCache`, and supports both
  account-level federation (no `client_id`, identity synced via Automatic
  Identity Management) and workload identity federation (a service principal
  `client_id` with no secret). Opt in via `StreamBuilder::federated(...)` or
  `StreamBuilder::federated_with_client_id(...)`. The client-credentials and
  token-exchange grants now share one request-shaping path, keeping them at
  parity. Existing `oauth(...)` and `headers_provider(...)` paths are unchanged.

### Bug Fixes

### Documentation

- Corrected README and rustdoc examples so their dependencies, feature flags,
  imports, and mutable stream bindings compile as shown.
- Batch examples and primary rustdoc now queue all records and wait once with
  `flush()` or the last offset, and no longer refer to removed `ingest_record()`
  / `ingest_records()` methods.
- Example READMEs and `get_unacked_*` rustdoc now name `ingest_record_offset()` /
  `ingest_records_offset()`. The generate-files tool README quoting is valid shell.
  Arrow example docs place schema validation at stream creation, not the first batch.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
