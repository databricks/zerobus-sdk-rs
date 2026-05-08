# NEXT CHANGELOG

## Release v1.3.0

### Major Changes

### New Features and Improvements

- Added `ZerobusSdkBuilder::connector_factory` for programmatic proxy
  configuration. Callers can install a `ConnectorFactory` (a
  `Fn(&str) -> Option<ProxyConnector>` closure) that fully overrides the
  default env-var proxy detection — useful for embedders that already model
  proxy config in their own configuration system (e.g. Vector's `ProxyConfig`).
  When no factory is installed, the existing `grpc_proxy` / `https_proxy` /
  `http_proxy` env-var behavior is unchanged.
- The env-var proxy path now supports `https://` proxy URLs. The client→proxy
  hop does a TLS handshake using the system trust store; the CONNECT tunnel
  still carries raw TCP so tonic applies end-to-end TLS to the target endpoint
  on top.
- **`StreamBuilder` API**: New fluent builder for creating ingestion streams.
  Setters can be called in any order; the builder validates at `build()` time
  that both authentication and format have been configured.


### Bug Fixes

- **gRPC / HTTP/2 teardown on close and recovery**: Receive and send tasks now shut down with a per-stream `CancellationToken`, bounded waits before `abort`, and a separate `recv_drain_token` on the receiver. This avoids racing **`RST_STREAM` / `CANCEL`** from the client against **`END_STREAM`** from the server—failure modes that could show up as HTTP/2 protocol errors or broken pipe on the server.
- After the inbound receive loop exits, the response-stream drain is now split by exit reason: the close path (`recv_drain_token`) drains **inline** with a 500ms cap so the server sees `END_STREAM` before the client process exits and the runtime tears down; the recovery / error paths drain in a **detached** task so `flush()` and stream recovery aren't delayed.

### Documentation

### Internal Changes

- Reduced log verbosity in `wait_for_offset` / `wait_for_acks` polling loops.
  Per-iteration progress logs are now emitted at `trace` level, and the
  one-shot "completed" log is now at `debug` level (previously `info`). This
  removes repeated `info`-level noise observed when callers wait for flushes
  or graceful close.

### Breaking Changes

### Deprecations

- **`ZerobusSdk::create_stream()`**: Use `sdk.stream_builder(table).oauth(id, secret).json().build().await` instead
- **`ZerobusSdk::create_stream_with_headers_provider()`**: Use `sdk.stream_builder(table).headers_provider(p).json().build().await` instead
- **`ZerobusSdk::create_arrow_stream()`**: Use `sdk.stream_builder(table).oauth(id, secret).arrow(schema).build_arrow().await` instead
- **`ZerobusSdk::create_arrow_stream_with_headers_provider()`**: Use `sdk.stream_builder(table).headers_provider(p).arrow(schema).build_arrow().await` instead

### API Changes

- New public exports: `ProxyConnector`, `ConnectorFactory`, `StreamBuilder`.
- New builder method: `ZerobusSdkBuilder::connector_factory`.
- New entry point: `ZerobusSdk::stream_builder()`.
- Changed `ZerobusSdk` fields `workspace_id` and `tls_config` to `pub(crate)` visibility (no public API impact).
