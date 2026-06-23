# NEXT CHANGELOG

## Release v2.4.0

### Major Changes

### New Features and Improvements

- Token caching for the default OAuth path. Tokens obtained via `.oauth(...)` are now cached per table on the `ZerobusSdk` instance and reused across stream creations and recoveries until they near expiry, instead of minting a fresh token on every stream. This reduces load on the Unity Catalog token endpoint for clients that create many short-lived streams. Caching is on by default and can be tuned via `ZerobusSdkBuilder::token_cache_enabled` and `ZerobusSdkBuilder::token_refresh_buffer`.
- On a server-side authentication rejection during stream creation, the cached token is invalidated so the next attempt re-mints (re-checking grants at Unity Catalog), rather than reusing a rejected token until the refresh window.
- `OAuthHeadersProvider::new` now caches tokens for the lifetime of the returned provider (previously it minted a fresh token on every call). Behavior is unchanged for the common path of constructing streams through `ZerobusSdk`, which already shares a cache.
- Dynamic protobuf ingestion: ingest on the efficient proto path without a compiled `.proto` or generated Rust types. The canonical record contract is a **JSON value encoded against the descriptor** (consistent with the C/FFI encode path), turned into protobuf wire bytes client-side. Three pieces ship together:
  - `StreamBuilder::proto_from_uc()` — fetches the table's schema from Unity Catalog at stream creation and derives the protobuf descriptor (no local `.proto`).
  - `schema::TableDescriptorBuilder` — build a descriptor in code from Databricks column types when there is no Unity Catalog metadata.
  - `DynamicProtoEncoder` (via `ZerobusStream::encoder()`) — encode JSON records (string, `serde_json::Value`, or any `serde::Serialize`) into protobuf bytes against the stream's descriptor, then ingest them through the existing `ingest_record_offset` / `ingest_records_offset` API.

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

- Added `ZerobusSdkBuilder::token_cache_enabled(bool)` to enable or disable OAuth token caching (default enabled).
- Added `ZerobusSdkBuilder::token_refresh_buffer(Duration)` to configure how long before a cached token's expiry it is refreshed (default 5 minutes).
- Added `HeadersProvider::invalidate` with a default no-op implementation; the SDK calls it when the server rejects the supplied credentials so a provider can drop cached auth state. Existing trait implementations are unaffected.
- Added `StreamBuilder::proto_from_uc()` to create a proto stream whose descriptor is fetched from Unity Catalog at `build()` time.
- Added `ZerobusStream::encoder()`, returning a `DynamicProtoEncoder` bound to the stream's descriptor.
- Added `DynamicProtoEncoder` (module `dynamic`) with `new`, `encode`, `encode_value`, `encode_record`, `descriptor`, and `descriptor_bytes`.
- Added `schema::TableDescriptorBuilder` for constructing a `DescriptorProto` in code, plus `schema::fetch_uc_table_schema` and `schema::descriptor_from_uc` for fetching a table's schema/descriptor from the Unity Catalog REST API.
- Added `DefaultTokenFactory::get_workspace_token` to mint an `all-apis` token for Unity Catalog REST calls.
- `prost-reflect` is now a regular dependency of the SDK crate (previously a dev-dependency).
