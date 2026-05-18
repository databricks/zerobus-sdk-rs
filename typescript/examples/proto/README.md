# Protocol Buffers Examples

Protobuf-encoded ingestion. The SDK accepts a `protobufjs` message
(auto-encoded via `.encode().finish()`) or a `Buffer` of pre-encoded bytes.

| File | What it shows |
|------|---------------|
| `single.ts` | `ingestRecord` with a `protobufjs` message instance. |
| `batch.ts`  | `ingestRecords` with an array of messages. |

## Prerequisite

Compile the proto schema once. This produces:

- `examples/generated/air_quality.{js,d.ts}` — the `protobufjs` message classes.
- `schemas/air_quality_descriptor.pb` — the `FileDescriptorSet`.

```bash
npm run build:proto
```

The example then extracts the `AirQuality` message's `DescriptorProto` from
the `FileDescriptorSet` (via the bundled `loadDescriptorProto` helper in
`utils/descriptor.ts`) and passes it base64-encoded to the stream.

## Run

```bash
ZEROBUS_ENDPOINT='http://[::1]:50051' \
ZEROBUS_TLS=none \
ZEROBUS_NO_AUTH=1 \
ZEROBUS_TABLE_NAME='test_data/test_table' \
npm run example:proto:single
```

## Code shape

```typescript
import { ZerobusSdk } from '@databricks/zerobus-ingest-sdk';
import { loadDescriptorProto } from '@databricks/zerobus-ingest-sdk/utils/descriptor';

const descriptor = loadDescriptorProto({
    descriptorPath: 'schemas/air_quality_descriptor.pb',
    protoFileName:  'air_quality.proto',
    messageName:    'AirQuality',
});

const stream = await sdk.createStream({
    table: 'catalog.schema.air_quality',
    auth:   { type: 'oauth', clientId, clientSecret },
    format: { type: 'proto', descriptor },
});

const msg = examples.AirQuality.create({ deviceName: 'sensor-1', temp: 21, humidity: 55 });
const offset = await stream.ingestRecord(msg);
await stream.waitForOffset(offset);
```

## Type mapping (Delta ↔ proto2)

| Delta type | Proto type    |
|------------|---------------|
| `STRING`   | `optional string`  |
| `INT`      | `optional int32`   |
| `BIGINT`   | `optional int64`   |
| `FLOAT`    | `optional float`   |
| `DOUBLE`   | `optional double`  |
| `BOOLEAN`  | `optional bool`    |
| `BINARY`   | `optional bytes`   |

Use `optional` (proto2) so the server can distinguish unset from zero-valued
fields. Field numbers (`= 1`, `= 2`, …) must be stable across schema
evolution.
