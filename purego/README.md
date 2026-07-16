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
  the `StreamParams.HeadersProvider`. The authorization value is sent verbatim as
  the provider formats it (e.g. `"Bearer <token>"`); the transport does not add a
  scheme prefix.

Planned layers: OAuth/auth, ingest/ack state management, recovery, and the
public API.

## Regenerating the protobuf bindings

See `internal/zerobuspb/gen.go`. From this module root:

```
go generate ./...
```
