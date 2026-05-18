# Arrow Flight Examples (Beta)

> **Beta**: API is stabilising but may still change before reaching GA.

High-throughput columnar ingestion via Arrow Flight. Each batch is supplied
as an Arrow IPC stream (`tableToIPC(table, 'stream')`).

| File | What it shows |
|------|---------------|
| `single.ts` | Zero-copy IPC ingestion — no compression, no RecordBatch round trip. |
| `batch.ts`  | `compression: 'zstd'` — SDK re-encodes with the codec on the way to Flight. |

## Prerequisite

Arrow Flight is compiled into the default npm prebuilds — no extra build
flag needed. If you're working from source:

```bash
npm run build             # release
npm run build:debug       # debug, faster iteration
```

## Compression and the zero-copy path

| `compression` | Path |
|---------------|------|
| `'none'` (default) | JS IPC bytes are forwarded straight to the Rust SDK's `ingest_ipc_batch`. No parse / re-encode. |
| `'lz4_frame'`      | Parse → re-encode with LZ4_FRAME. Fast compression, modest ratio. |
| `'zstd'`           | Parse → re-encode with ZSTD. Slower but smaller payloads. |

Pick `'none'` when the network can carry the raw IPC bytes; pick a codec
when bandwidth is the bottleneck.

## Building Arrow tables in JS

`apache-arrow` JS dictionary-encodes string columns by default and infers
non-nullable fields for typed-array-backed columns. The examples construct
a `RecordBatch` with an explicit `Schema` so every field is nullable to
match the Delta target:

```typescript
const arrowSchema = new Schema([
    new Field('device_name', new Utf8(), true),
    new Field('temp',        new Int32(), true),
    new Field('humidity',    new Int64(), true),
]);
const dev = vectorFromArray(['s1', 's2'], new Utf8());
const t   = makeVector(Int32Array.from([21, 19]));
const h   = makeVector(BigInt64Array.from([55n, 60n]));
const data = makeData({
    type: new Struct(arrowSchema.fields),
    length: 2,
    children: [dev.data[0], t.data[0], h.data[0]],
});
const ipc = Buffer.from(tableToIPC(new Table(new RecordBatch(arrowSchema, data)), 'stream'));
const offset = await stream.ingestBatch(ipc);
```

## Run

```bash
ZEROBUS_ENDPOINT='http://[::1]:50051' \
ZEROBUS_TLS=none \
ZEROBUS_NO_AUTH=1 \
ZEROBUS_TABLE_NAME='test_data/test_table' \
npm run example:arrow:single
```
