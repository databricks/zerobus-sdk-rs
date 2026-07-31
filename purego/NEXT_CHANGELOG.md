# NEXT CHANGELOG

## Release v0.1.0

### New Features and Improvements

- Added context-aware single-record and batch ingestion methods so cancellation
  can interrupt buffer backpressure.
- Exposed recovery timeout/backoff, lack-of-ack timeout, maximum batch records,
  and server-pause wait controls as stream options.

### Deprecations

### Bug Fixes

- Correctly normalize IPv6 Zerobus endpoints and reject plaintext HTTP
  endpoints because the SDK always uses TLS.
- Validate application names before adding them to the gRPC user-agent.

### Documentation

- Clarified that callbacks may call stream methods, including `Close`.

### Internal Changes

### API Changes

- Protocol Buffers are now the default record type, matching the other SDKs.
