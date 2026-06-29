# Zerobus pure-Go SDK (work in progress)

A native, pure-Go implementation of the Zerobus ingestion SDK that talks to the
Zerobus gRPC service directly — **no cgo and no Rust FFI**.

It lives in its own top-level module (`github.com/databricks/zerobus-sdk/purego`)
so it is fully isolated from the existing cgo-based SDK under `go/` while it is
built out. The two do not share a `go.mod`: the cgo SDK keeps its lean
dependency set, and this module owns the gRPC/protobuf dependencies.

## Status

Foundational only so far:

- `internal/zerobuspb` — protobuf message types and the gRPC `ZerobusClient`
  (the bidirectional `EphemeralStream` RPC), generated from `zerobus_service.proto`.

Planned layers (each added incrementally): transport, OAuth/auth, the
ingest/ack state machine, recovery, and the public API.

## Regenerating the protobuf bindings

See `internal/zerobuspb/gen.go`. From this module root:

```
go generate ./...
```
