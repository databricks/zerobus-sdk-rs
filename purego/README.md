# Zerobus pure-Go SDK

A native pure-Go Zerobus ingestion SDK.

## Requirements

PureGo requires Go 1.25 or later.

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
- **Avro** *(Beta, requires the `avro` build tag)*: `WithAvro(schemaJSON)`; ingest pre-encoded datums via `IngestRecordOffset`. Server support is pending.

`IngestJSONOffset` and `IngestJSONRecordsOffset` queue JSON directly on JSON
streams and convert it to protobuf on proto streams.

## Dynamic proto with UC schema fetch

Fetch the table schema explicitly, then create a regular proto stream. The
stream can accept either protobuf bytes or JSON converted at ingest time.

> **Fetch the descriptor once and reuse it.** `FetchProtoDescriptorFromUC`
> performs a Unity Catalog request on every call and the SDK does not cache the
> result. Call it once per table at startup, hold on to the returned bytes, and
> pass them to `WithProto` for every stream you open on that table. Fetch again
> only when the table schema changes. Calling it per stream — or per record —
> puts a UC round-trip on your ingestion path.

UC schema conversion rejects nullable array/map fields and collections that
allow null elements or values because protobuf cannot preserve those
distinctions. JSON ingestion also rejects explicit `null` collection fields.

```go
// Fetch once at startup, then reuse `descriptor` for every stream on this table.
descriptor, err := sdk.FetchProtoDescriptorFromUC(
    ctx,
    "catalog.schema.table",
    clientID,
    clientSecret,
)
if err != nil {
    log.Fatal(err)
}

stream, err := sdk.CreateStream(
    ctx,
    "catalog.schema.table",
    clientID,
    clientSecret,
    zerobus.WithProto(descriptor),
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

`ColumnsFromDescriptor` reports which columns a descriptor declares, without
parsing the protobuf yourself:

```go
columns, err := zerobus.ColumnsFromDescriptor(descriptor)
if err != nil {
    log.Fatal(err)
}
if _, ok := columns["event_id"]; !ok {
    log.Fatal("table has no event_id column")
}
```

Only top-level fields are columns; the fields of a nested `STRUCT` belong to
that column's value.

To skip JSON conversion while retaining UC schema discovery, build dynamic
protobuf messages from the stream descriptor:

```go
descriptor := stream.MessageDescriptor()
idField := descriptor.Fields().ByName("id")

for _, id := range ids {
    message := dynamicpb.NewMessage(descriptor)
    message.Set(idField, protoreflect.ValueOfInt32(int32(id)))
    record, err := proto.Marshal(message)
    if err != nil {
        log.Fatal(err)
    }
    if _, err := stream.IngestRecordOffset(record); err != nil {
        log.Fatal(err)
    }
}
if err := stream.Flush(); err != nil {
    log.Fatal(err)
}
```

See `examples/dynamic/proto/main.go` for a complete example.

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
- Unknown JSON fields are rejected; conversion fails the ingest call rather
  than dropping them.

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
For proto streams, records queued through `IngestJSON*` are returned as
converted protobuf bytes. Replay them through `IngestRecordOffset` /
`IngestRecordsOffset`, not through the `IngestJSON*` methods.

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

## Releasing

PureGo is distributed as a Go module through Git tags; there are no binary
artifacts or package-registry uploads. Releases use `purego/v<version>` tags.
Consumers can install a tagged module with:

```bash
go get github.com/databricks/zerobus-sdk/purego@v0.1.0
```

Cutting a release:

- On the version-bump PR, move `NEXT_CHANGELOG.md` into `CHANGELOG.md` under a
  `## Release v<semver>` heading — the release notes are extracted by matching
  that exact heading — and reset `NEXT_CHANGELOG.md` for the next version.
- Merge the bump PR, then tag its merge commit `purego/v<semver>`. The tag must
  be reachable from `main`.
- Releases are driven from a separate secure release environment rather than
  from this repository: a maintainer there dispatches the `zerobus-sdk-purego`
  workflow with the tag, first with `dry-run` enabled as a rehearsal.
- That workflow validates the tag and the `## Release v<semver>` changelog
  heading, invokes `release-purego.yml` here to build, vet, and test the module,
  and scans the `purego/` source. On a real (non-dry) run it then creates the
  GitHub Release with notes taken from `CHANGELOG.md`.
- The Git tag is the version of record. Keep the `version` const in
  `zerobus/sdk.go` (the gRPC user-agent) in sync with it; `release-purego.yml`
  fails when a `purego/v*` tag disagrees.

## Regenerating the protobuf bindings

See `internal/zerobuspb/gen.go`. From this module root:

```
go generate ./...
```
