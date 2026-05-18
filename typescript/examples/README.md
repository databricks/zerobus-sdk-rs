# Zerobus TypeScript SDK Examples

Runnable examples for the **v2.0** API. They cover all three serialization
formats and exercise the production-shaped happy path: open a stream, queue
records, wait for server acknowledgment, close.

| Path | What it demonstrates |
|------|----------------------|
| `json/single.ts`  | Queue records one at a time with `ingestRecord`. |
| `json/batch.ts`   | Queue many records atomically with `ingestRecords`. |
| `proto/single.ts` | Protobuf-encoded ingestion using a `protobufjs` message. |
| `proto/batch.ts`  | Protobuf batch ingestion. |
| `arrow/single.ts` | Arrow Flight (Beta) — zero-copy IPC ingest. |
| `arrow/batch.ts`  | Arrow Flight with `zstd` compression. |

All examples share `_config.ts`, which reads environment variables and
builds the `SdkOptions` and `auth` object. The same example can target a
real Databricks workspace or a local test server depending on the env.

## Running against a Databricks workspace

```bash
export ZEROBUS_ENDPOINT='https://<workspace>.zerobus.<region>.cloud.databricks.com'
export DATABRICKS_WORKSPACE_URL='https://<workspace>.cloud.databricks.com'
export DATABRICKS_CLIENT_ID='<client id>'
export DATABRICKS_CLIENT_SECRET='<client secret>'
export ZEROBUS_TABLE_NAME='catalog.schema.air_quality'

npm install
npm run build:debug          # builds napi + facade (Arrow Flight included)
npm run build:proto          # only needed for the proto examples

npm run example:json:single
npm run example:json:batch
npm run example:proto:single
npm run example:proto:batch
npm run example:arrow:single
npm run example:arrow:batch
```

## Running against a local test server

```bash
export ZEROBUS_ENDPOINT='http://[::1]:50051'
export ZEROBUS_TLS=none
export ZEROBUS_NO_AUTH=1
export ZEROBUS_TABLE_NAME='test_data/test_table'

npm run example:json:single
# ... etc
```

The `_config.ts` helper supplies the canonical headers the wire protocol
requires (with a placeholder token) when `ZEROBUS_NO_AUTH=1`.

## Schema

All examples target the `air_quality` schema:

```protobuf
message AirQuality {
    optional string device_name = 1;
    optional int32  temp        = 2;
    optional int64  humidity    = 3;
}
```

For Arrow examples the equivalent schema is declared with explicit nullable
fields so the IPC payload validates against the Delta target.

## Ingestion patterns

The v2.0 API exposes one queue-time API per cardinality:

| Method | Returns | Resolves at |
|--------|---------|-------------|
| `stream.ingestRecord(record)`     | `Promise<bigint>`        | Record is in the SDK's landing zone. |
| `stream.ingestRecords(records)`   | `Promise<bigint \| null>`| Batch is in the landing zone; `null` for an empty batch. |
| `stream.waitForOffset(offset)`    | `Promise<void>`          | Server has acked through that offset. |
| `stream.flush()`                  | `Promise<void>`          | All currently-queued records have been acked. |
| `stream.close()`                  | `Promise<void>`          | Graceful shutdown. |

The v1.x deprecated blocking-on-ack variants of `ingestRecord` /
`ingestRecords` are gone. Use `waitForOffset` to wait for acknowledgment.

## Authentication

`createStream` and `createArrowStream` take an `auth` field — a discriminated
union with three arms:

```typescript
auth: { type: 'oauth', clientId, clientSecret }

auth: {
    type: 'headersProvider',
    getHeaders: async () => [
        ['authorization', `Bearer ${myToken}`],
        ['x-databricks-zerobus-table-name', 'catalog.schema.table'],
    ],
}

auth: { type: 'noAuth' }   // local-only / sidecar-proxy
```

The helper `bearerTokenProvider(table, getToken)` produces a
`headersProvider`-shaped callback for the common bearer-token case.
