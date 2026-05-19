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

### Bug Fixes

- Preserve pending records for recovery when `ZerobusStream::close()` fails while flushing.

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

- New public exports: `ProxyConnector`, `ConnectorFactory`.
- New builder method: `ZerobusSdkBuilder::connector_factory`.
