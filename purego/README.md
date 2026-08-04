# Zerobus pure-Go SDK (work in progress)

A native pure-Go Zerobus ingestion SDK.

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

Ingestion is asynchronous. `IngestRecordOffset` queues records and returns quickly.
Use **loop-then-`Flush()`**; avoid per-record waits in hot paths.

```go
for _, rec := range records {
    if _, err := stream.IngestRecordOffset(rec); err != nil { // queue only
        log.Fatal(err)
    }
}
if err := stream.Flush(); err != nil { // wait once for all pending acks
    log.Fatal(err)
}
```

For continuous streams, flush periodically or use `WithAckCallback`.
Prefer `IngestRecordsOffset` in hot paths.

Use per-record `WaitForOffset` only for low-volume strict confirmation flows.

Use `IngestRecordOffsetContext` / `IngestRecordsOffsetContext` to bound admission wait time.

## Authentication

`CreateStream` uses Unity Catalog OAuth client-credentials.
For custom auth, use `HeadersProvider` with `CreateStreamWithProvider`:

```go
stream, err := sdk.CreateStreamWithProvider(ctx, "catalog.schema.table",
    myProvider, zerobus.WithProto(descriptorProto))
```

`NewStaticHeadersProvider` returns fixed headers for tests or external credentials.

By default, stream open is asynchronous: `CreateStream` returns after argument
validation while first-open runs in the background, and terminal open failures
surface on `Flush`/`WaitForOffset`/ack callbacks. Values from the creation
context propagate to the connection and later reconnects, but cancellation and
deadlines are detached because first-open outlives the call.

Pass `WithWaitForReady()` to make `CreateStream` /
`CreateStreamWithProvider` block until first-open succeeds or fails. In this
mode, the creation context directly bounds token resolution, handshake, retry
backoff, and every attempt before success. Cancellation is detached after the
first successful open, so it does not terminate the live stream.

## Record types

- **Protocol Buffers** (default): `WithProto(descriptorProto)`.
- **JSON**: `WithJSON()`.

## Dynamic proto with UC schema fetch

`CreateDynamicProtoStream` fetches table schema from Unity Catalog, converts it
to a runtime protobuf descriptor, and accepts raw JSON records that are
converted to protobuf before ingest.

```go
stream, err := sdk.CreateDynamicProtoStream(
    ctx,
    "catalog.schema.table",
    clientID,
    clientSecret,
)
if err != nil {
    log.Fatal(err)
}
defer stream.Close()

for _, rec := range records {
    if _, err := stream.IngestJSONOffset([]byte(rec)); err != nil { // queue only
        log.Fatal(err)
    }
}
if err := stream.Flush(); err != nil { // wait once at the end
    log.Fatal(err)
}
```

See `examples/dynamic/single/main.go` for a complete example.

Dynamic JSON follows protobuf JSON value rules:

- `DATE` is days since 1970-01-01.
- `TIMESTAMP` is UTC microseconds since the Unix epoch.
- `TIMESTAMP_NTZ` is local-wall-clock microseconds since 1970-01-01, with no
  timezone.
- `BINARY` is a base64-encoded string.
- `DECIMAL` is a string, such as `"123.45"`.
- `VARIANT` is a string containing JSON text.
- `BIGINT` values above 2^53 should be strings to avoid JSON-number precision
  loss.
- `STRUCT`, `ARRAY`, and `MAP` use JSON objects, arrays, and objects.
- Unknown JSON fields are ignored.

## Error handling

Operations return `*Error`. Use `Retryable(err)` to check retryability:

```go
if _, err := stream.IngestRecordOffset(rec); err != nil {
    if zerobus.Retryable(err) {
        // retryable
    } else {
        // non-retryable
    }
}
```

## Recovery

Streams reconnect automatically on recoverable failures (default 4 retries).
Disable with `WithRecovery(zerobus.RecoveryDisabled)`.
After close/failure, use `GetUnackedRecords` / `GetUnackedBatches` to replay.
For `DynamicProtoStream`, these methods return the converted protobuf bytes.
Replay them through the embedded `Stream.IngestRecordOffset` /
`Stream.IngestRecordsOffset`, not through the `IngestJSON*` methods.

Recovery and buffering can be tuned with `WithRecoveryRetries`,
`WithRecoveryTimeout`, `WithRecoveryBackoff`, `WithLackOfAckTimeout`,
`WithMaxInflight`, `WithMaxBufferedPayloadBytes`, `WithMaxBatchRecords`, and
`WithStreamPausedMaxWait`.

## Package layout

```
purego/
├── zerobus/              PUBLIC API: SDK, Stream, options, errors
└── internal/
    ├── stream/           generic ingestion core (buffer, watermark, supervisor)
    ├── transport/        gRPC connection, TLS, EphemeralStream handshake
    ├── auth/             HeadersProvider, token cache, UC OAuth
    ├── schema/           UC schema -> protobuf descriptor conversion
    ├── ucschema/         UC REST schema fetch client
    ├── dynamicproto/     JSON -> protobuf runtime conversion
    └── zerobuspb/        generated protobuf bindings
```

## Regenerating the protobuf bindings

See `internal/zerobuspb/gen.go`. From this module root:

```
go generate ./...
```
