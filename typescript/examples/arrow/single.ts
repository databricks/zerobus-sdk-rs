/**
 * Arrow Flight single-batch ingestion example (Beta).
 *
 * Arrow Flight is compiled into the default npm build — no special flag.
 *
 * The TS SDK forwards the IPC bytes directly to the Rust SDK's
 * `ingest_ipc_batch` whenever compression is off (the default), so this
 * example exercises the zero-copy path end-to-end.
 *
 * Build Arrow vectors with explicit types so the IPC schema matches what the
 * stream declared — `apache-arrow` JS otherwise dictionary-encodes string
 * columns and infers non-nullable fields for typed-array-backed numeric
 * columns. We construct a `RecordBatch` with an explicit `Schema` to keep
 * every field nullable (matching the target Delta table).
 */

import {
    Field,
    Int32,
    Int64,
    RecordBatch,
    Schema,
    Struct,
    Table,
    Utf8,
    makeData,
    makeVector,
    tableToIPC,
    vectorFromArray,
} from 'apache-arrow';
import { ZerobusSdk, ArrowDataType } from '../../dist/index.js';
import { loadConfig } from '../_config.js';

function buildBatch(deviceName: string[], temp: Int32Array, humidity: BigInt64Array): Buffer {
    const schema = new Schema([
        new Field('device_name', new Utf8(), true),
        new Field('temp', new Int32(), true),
        new Field('humidity', new Int64(), true),
    ]);
    const dev = vectorFromArray(deviceName, new Utf8());
    const t = makeVector(temp);
    const h = makeVector(humidity);
    const data = makeData({
        type: new Struct(schema.fields),
        length: deviceName.length,
        children: [dev.data[0], t.data[0], h.data[0]],
    });
    const batch = new RecordBatch(schema, data);
    const table = new Table(batch);
    return Buffer.from(tableToIPC(table, 'stream'));
}

async function main() {
    const cfg = loadConfig();
    const sdk = new ZerobusSdk(cfg.sdkOptions);

    const stream = await sdk.createArrowStream({
        table: cfg.tableName,
        auth: cfg.auth,
        schema: [
            { name: 'device_name', dataType: ArrowDataType.Utf8 },
            { name: 'temp', dataType: ArrowDataType.Int32 },
            { name: 'humidity', dataType: ArrowDataType.Int64 },
        ],
        // compression: 'none' (default) keeps the SDK on the zero-copy path.
        maxInflightBatches: 50,
    });
    console.log('Arrow stream created');

    try {
        const ipc = buildBatch(
            ['sensor-1', 'sensor-2', 'sensor-3'],
            Int32Array.from([21, 19, 23]),
            BigInt64Array.from([55n, 60n, 50n]),
        );
        const offset = await stream.ingestBatch(ipc);
        console.log(`Queued Arrow batch → offset=${offset}`);
        await stream.waitForOffset(offset);
        console.log(`Server acknowledged offset ${offset}`);
        await stream.flush();
    } finally {
        await stream.close();
    }
}

main().catch((err) => {
    console.error('FAILED:', err);
    process.exit(1);
});
