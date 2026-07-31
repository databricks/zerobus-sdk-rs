# Zerobus pure-Go SDK (work in progress)

A native, pure-Go implementation of the Zerobus ingestion SDK that talks to the
Zerobus gRPC service directly — **no cgo, no FFI, no C toolchain**. It builds and
cross-compiles as an ordinary Go module (`go get`, `CGO_ENABLED=0` friendly).

It lives in its own top-level module (`github.com/databricks/zerobus-sdk/purego`)
so it is fully isolated from the existing cgo-based SDK under `go/` while it is
built out. The two do not share a `go.mod`: the cgo SDK keeps its lean
dependency set, and this module owns the gRPC/protobuf dependencies.

## Quick start

```go
import "github.com/databricks/zerobus-sdk/purego/zerobus"

sdk, err := zerobus.New(
    "https://your-workspace.zerobus.region.cloud.databricks.com",
    "https://your-workspace.cloud.databricks.com",
)
if err != nil {
    log.Fatal(err)
}
defer sdk.Close()

stream, err := sdk.CreateStream(ctx, "catalog.schema.table",
    clientID, clientSecret, zerobus.WithJSON())
if err != nil {
    log.Fatal(err)
}
defer stream.Close()
```

## Ingesting data

Ingestion is asynchronous and pipelined. `IngestRecordOffset` returns as soon as
the record is queued and the offset is assigned; sending and acknowledgement
happen in the background. **Queue records in a loop and confirm durability with a
single `Flush`** — never wait for an acknowledgement after every record, which
collapses throughput to one record per server round-trip.

```go
for _, rec := range records {
    if _, err := stream.IngestRecordOffset(rec); err != nil { // queue only — do NOT wait here
        log.Fatal(err)
    }
}
if err := stream.Flush(); err != nil { // wait once for all pending acks
    log.Fatal(err)
}
```

For continuous, unbounded streams, call `Flush` periodically (every N records) to
bound in-flight memory, or register an ack callback with `WithAckCallback` for
async notification. Prefer the batch API `IngestRecordsOffset` in hot paths: one
batch is one buffer entry and one atomic ack.

Reserve per-record `WaitForOffset` for genuinely low-volume cases where each
record must be confirmed durable before continuing. Because acks are ordered and
the watermark is monotonic, waiting on the last offset of a group confirms all
prior offsets too.

When buffer backpressure may block longer than the caller can wait, use
`IngestRecordOffsetContext` or `IngestRecordsOffsetContext`. Cancellation only
interrupts admission; once queued, a record remains owned by the stream.

## Authentication

`CreateStream` uses the Unity Catalog OAuth 2.0 client-credentials flow. For
custom authentication (externally managed credentials, a custom token source, or
tests), implement `HeadersProvider` and use `CreateStreamWithProvider`:

```go
stream, err := sdk.CreateStreamWithProvider(ctx, "catalog.schema.table",
    myProvider, zerobus.WithProto(descriptorProto))
```

`NewStaticHeadersProvider` returns a fixed-header provider for tests or
externally managed credentials.

## Record types

- **Protocol Buffers** (default record type) —
  `WithProto(descriptorProto)`; records are marshaled protobuf bytes and the
  required serialized message descriptor is supplied once.
- **JSON** — `WithJSON()`; records are UTF-8 JSON bytes.

## Error handling

Operations return an `*Error` that reports whether a retry could succeed. Use the
package-level `Retryable` helper, which also classifies wrapped core errors:

```go
if _, err := stream.IngestRecordOffset(rec); err != nil {
    if zerobus.Retryable(err) {
        // transient — a retry or a fresh stream may succeed
    } else {
        // permanent — fix configuration or input before retrying
    }
}
```

## Recovery

Streams reconnect automatically on recoverable failures (default 4 retries).
Disable with `WithRecovery(zerobus.RecoveryDisabled)`. After a stream closes or
fails, `GetUnackedRecords` / `GetUnackedBatches` return the records that were
never acknowledged, for replay or persistence.

Recovery and buffering can be tuned with `WithRecoveryRetries`,
`WithRecoveryTimeout`, `WithRecoveryBackoff`, `WithLackOfAckTimeout`,
`WithMaxInflight`, `WithMaxBufferedPayloadBytes`, `WithMaxBatchRecords`, and
`WithStreamPausedMaxWait`.

## Package layout

```
purego/
├── zerobus/              PUBLIC API — SDK, Stream, options, errors
└── internal/
    ├── stream/           generic ingestion core (buffer, watermark, supervisor)
    ├── transport/        gRPC connection, TLS, EphemeralStream handshake
    ├── auth/             HeadersProvider, token cache, UC OAuth
    └── zerobuspb/        generated protobuf bindings
```

## Regenerating the protobuf bindings

See `internal/zerobuspb/gen.go`. From this module root:

```
go generate ./...
```
