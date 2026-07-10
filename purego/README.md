# Zerobus pure-Go SDK (work in progress)

A native, pure-Go implementation of the Zerobus ingestion SDK that talks to the
Zerobus gRPC service directly.

It lives in its own top-level module (`github.com/databricks/zerobus-sdk/purego`)
so it is fully isolated from the existing cgo-based SDK under `go/` while it is
built out. The two do not share a `go.mod`: the cgo SDK keeps its lean
dependency set, and this module owns the gRPC/protobuf dependencies.

## Current scope

Implemented packages:

- `internal/zerobuspb` — protobuf message types and the gRPC `ZerobusClient`
  (the bidirectional `EphemeralStream` RPC), generated from `zerobus_service.proto`.
- `internal/transport` — dials the service (TLS by default), performs the
  create-stream handshake, and exposes send/receive operations over the
  bidirectional stream. It validates stream-open inputs (`TableName`,
  `RecordType`, descriptor requirement for `PROTO`) and sets auth metadata from
  `StreamParams.Token` (`"Bearer <token>"` for bare tokens, verbatim when a
  known scheme — `Bearer`, `Basic`, or `DPoP` — is already present).
- `internal/auth` — token providers for stream authentication. `OAuthTokenProvider`
  implements the Unity Catalog OAuth 2.0 client-credentials flow with per-table
  token caching and proactive refresh; `StaticTokenProvider` wraps a fixed token
  for tests or externally managed lifecycles. Obtain a token with `Token(ctx)`
  and pass it as `StreamParams.Token`.

Planned layers: ingest/ack state management, recovery, and the public API.

## Regenerating the protobuf bindings

See `internal/zerobuspb/gen.go`. From this module root:

```
go generate ./...
```
