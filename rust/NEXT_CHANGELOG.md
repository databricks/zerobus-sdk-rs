# NEXT CHANGELOG

## Release v2.9.0

### Major Changes

### New Features and Improvements

- JSON and protobuf streams now use a dedicated gRPC connection by default.
  Use `ZerobusSdk::builder().connection_per_stream(false)` to retain the prior
  shared HTTP/2 connection behavior. Arrow Flight streams are unchanged.

- Dynamic protobuf streams can resolve their schema from Unity Catalog, so a
  runtime descriptor no longer has to be assembled by hand. Fetch the descriptor
  from the live table metadata and pass it to the existing `.dynamic_proto(...)`
  selector:

  ```rust
  let descriptor = sdk
      .fetch_message_descriptor("catalog.schema.table", client_id, client_secret)
      .await?;

  let stream = sdk
      .stream_builder()
      .table("catalog.schema.table")
      .oauth(client_id, client_secret)
      .dynamic_proto(descriptor)
      .build()
      .await?;
  ```

  `ZerobusSdk::fetch_message_descriptor()` uses the SDK's configured
  `unity_catalog_url`; the underlying `uc_schema` module takes the endpoint
  directly. The fetch needs OAuth credentials able to read the table's metadata.

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

- Added `ZerobusSdk::fetch_message_descriptor()`, the `uc_schema` module
  (`fetch_message_descriptor`, `fetch_table_schema`), and the
  `ZerobusError::SchemaFetchError { message, retryable }` variant. All additive.
