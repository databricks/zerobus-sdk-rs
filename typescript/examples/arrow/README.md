# Arrow Flight Examples (Beta)

> **Beta**: API is stabilising but may still change before reaching GA.

High-throughput columnar ingestion via Arrow Flight. Each batch is supplied
as an Arrow IPC stream (`tableToIPC(table, 'stream')` from `apache-arrow`).

| File | What it shows |
|------|---------------|
| `single.ts` | Single-batch ingestion with default (uncompressed) IPC. |
| `batch.ts`  | `ipcCompression: IpcCompressionType.Zstd` — the SDK re-encodes with the codec on the way to Flight. |

## Compression

| `ipcCompression` | Behaviour |
|---|---|
| `undefined` (default) / `null` | No compression on the wire. |
| `IpcCompressionType.Lz4Frame` | LZ4_FRAME — fast compression, modest ratio. |
| `IpcCompressionType.Zstd`     | ZSTD — slower but smaller payloads. |

## Building Arrow tables in JS

`apache-arrow` JS dictionary-encodes string columns by default and infers
non-nullable fields for typed-array-backed columns. The examples construct
a `RecordBatch` with an explicit `Schema` so every field is nullable to
match the Delta target:

```typescript
const schema = new Schema([
    new Field('device_name', new Utf8(), true),
    new Field('temp',        new Int32(), true),
    new Field('humidity',    new Int64(), true),
]);
const dev = vectorFromArray(['s1', 's2'], new Utf8());
const t   = makeVector(Int32Array.from([21, 19]));
const h   = makeVector(BigInt64Array.from([55n, 60n]));
const data = makeData({
    type: new Struct(schema.fields),
    length: 2,
    children: [dev.data[0], t.data[0], h.data[0]],
});
const ipc = Buffer.from(tableToIPC(new Table(new RecordBatch(schema, data)), 'stream'));
const offset = await stream.ingestBatch(ipc);
```

## Run

Build with Arrow support and run the examples:

```bash
npm run build:debug:arrow
npm run example:arrow:single
npm run example:arrow:batch
```
