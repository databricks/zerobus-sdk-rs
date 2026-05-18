# JSON Examples

JSON-encoded ingestion. The SDK accepts plain JS objects (auto-`JSON.stringify`'d)
or pre-serialized JSON strings.

| File | What it shows |
|------|---------------|
| `single.ts` | `ingestRecord` queues records one at a time and resolves to an offset. `waitForOffset` blocks until the server acks. |
| `batch.ts`  | `ingestRecords` queues a batch atomically; the offset is shared across the batch. |

## Run

Set the env, then run the example. See [`../README.md`](../README.md) for the
full env-var list. The minimal local-server invocation:

```bash
ZEROBUS_ENDPOINT='http://[::1]:50051' \
ZEROBUS_TLS=none \
ZEROBUS_NO_AUTH=1 \
ZEROBUS_TABLE_NAME='test_data/test_table' \
npm run example:json:single
```

## Code shape

```typescript
import { ZerobusSdk } from '@databricks/zerobus-ingest-sdk';

const sdk = new ZerobusSdk({ endpoint, unityCatalogUrl });

const stream = await sdk.createStream({
    table: 'catalog.schema.air_quality',
    auth: { type: 'oauth', clientId, clientSecret },
    format: { type: 'json' },
    maxInflightRequests: 1000,
    recovery: true,
});

const offset = await stream.ingestRecord({
    device_name: 'sensor-1', temp: 21, humidity: 55,
});
await stream.waitForOffset(offset);
await stream.close();
```

## JSON payload rules

- Plain object → `JSON.stringify`'d by the SDK.
- `string` → forwarded as-is (must be valid JSON).
- `BigInt` is not supported by `JSON.stringify`; for `BIGINT` columns,
  pass a JS `number` (safe up to 2^53) or a pre-serialized string with a
  numeric (not quoted) value.
