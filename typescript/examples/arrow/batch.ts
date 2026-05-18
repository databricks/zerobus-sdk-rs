/**
 * Arrow Flight batch ingestion example (Beta).
 *
 * Demonstrates `compression: 'zstd'` — the SDK parses the IPC bytes and
 * re-encodes them with the configured codec on the way to Flight. Without
 * compression the SDK forwards the bytes zero-copy.
 *
 * Arrow Flight is compiled into the default npm build — no special flag.
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

const ARROW_SCHEMA = new Schema([
    new Field('device_name', new Utf8(), true),
    new Field('temp', new Int32(), true),
    new Field('humidity', new Int64(), true),
]);

function buildBatch(b: number, rows: number): Buffer {
    const deviceName = Array.from({ length: rows }, (_, i) => `sensor-${b * rows + i}`);
    const temp = Int32Array.from({ length: rows }, (_, i) => 20 + (i % 5));
    const humidity = BigInt64Array.from({ length: rows }, (_, i) => BigInt(50 + (i % 10)));

    const dev = vectorFromArray(deviceName, new Utf8());
    const t = makeVector(temp);
    const h = makeVector(humidity);
    const data = makeData({
        type: new Struct(ARROW_SCHEMA.fields),
        length: rows,
        children: [dev.data[0], t.data[0], h.data[0]],
    });
    const batch = new RecordBatch(ARROW_SCHEMA, data);
    return Buffer.from(tableToIPC(new Table(batch), 'stream'));
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
        // Trade CPU for network: `'zstd'` shrinks payloads at modest CPU cost,
        // `'lz4_frame'` is faster with less ratio, `'none'` (default) keeps
        // the SDK on the zero-copy IPC path.
        compression: 'zstd',
        maxInflightBatches: 100,
    });
    console.log('Arrow stream created (compression=zstd)');

    try {
        const NUM_BATCHES = 5;
        const ROWS_PER_BATCH = 10;
        let lastOffset: bigint | null = null;

        for (let b = 0; b < NUM_BATCHES; b++) {
            const ipc = buildBatch(b, ROWS_PER_BATCH);
            lastOffset = await stream.ingestBatch(ipc);
            console.log(`Batch ${b + 1}/${NUM_BATCHES} → offset=${lastOffset}`);
        }
        if (lastOffset !== null) {
            await stream.waitForOffset(lastOffset);
            console.log(`Server acknowledged up to offset ${lastOffset}`);
        }
        await stream.flush();
    } finally {
        await stream.close();
    }
}

main().catch((err) => {
    console.error('FAILED:', err);
    process.exit(1);
});
