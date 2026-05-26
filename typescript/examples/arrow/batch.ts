/**
 * Arrow Flight batch ingestion example (Beta).
 *
 * Demonstrates `ipcCompression: IpcCompressionType.Zstd` — the SDK
 * re-encodes the IPC payload with the codec on the way to Flight. See
 * `single.ts` for the default (uncompressed) path.
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
import {
    ArrowDataType,
    ArrowStreamConfigurationOptions,
    ArrowTableProperties,
    IpcCompressionType,
    ZerobusSdk,
} from '../../index';

const SERVER_ENDPOINT =
    process.env.ZEROBUS_SERVER_ENDPOINT ||
    'https://your-workspace-id.zerobus.region.cloud.databricks.com';
const DATABRICKS_WORKSPACE_URL =
    process.env.DATABRICKS_WORKSPACE_URL || 'https://your-workspace.cloud.databricks.com';
const TABLE_NAME = process.env.ZEROBUS_TABLE_NAME || 'catalog.schema.table';
const CLIENT_ID = process.env.DATABRICKS_CLIENT_ID || 'your-client-id';
const CLIENT_SECRET = process.env.DATABRICKS_CLIENT_SECRET || 'your-client-secret';

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
    return Buffer.from(tableToIPC(new Table(new RecordBatch(ARROW_SCHEMA, data)), 'stream'));
}

async function main() {
    console.log('Arrow Flight Batch Example (Beta, ipcCompression = Zstd)');
    console.log('='.repeat(60));

    const sdk = new ZerobusSdk(SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL);

    const tableProperties: ArrowTableProperties = {
        tableName: TABLE_NAME,
        schemaFields: [
            { name: 'device_name', dataType: ArrowDataType.Utf8 },
            { name: 'temp', dataType: ArrowDataType.Int32 },
            { name: 'humidity', dataType: ArrowDataType.Int64 },
        ],
    };

    const options: ArrowStreamConfigurationOptions = {
        maxInflightBatches: 100,
        // Trade CPU for network: Zstd shrinks payloads at modest CPU cost.
        // Use IpcCompressionType.Lz4Frame for faster compression with less
        // ratio, or leave `ipcCompression` unset for no compression.
        ipcCompression: IpcCompressionType.Zstd,
    };

    const stream = await sdk.createArrowStream(tableProperties, CLIENT_ID, CLIENT_SECRET, options);
    console.log('Arrow stream created (ipcCompression=Zstd)');

    try {
        const NUM_BATCHES = 5;
        const ROWS_PER_BATCH = 10;
        let lastOffset: bigint = 0n;

        for (let b = 0; b < NUM_BATCHES; b++) {
            const ipc = buildBatch(b, ROWS_PER_BATCH);
            lastOffset = await stream.ingestBatch(ipc);
            console.log(`Batch ${b + 1}/${NUM_BATCHES} → offset=${lastOffset}`);
        }

        await stream.waitForOffset(lastOffset);
        console.log(`Server acknowledged up to offset ${lastOffset}`);
        await stream.flush();
    } finally {
        await stream.close();
        console.log('Stream closed');
    }
}

main().catch((err) => {
    console.error('FAILED:', err);
    process.exit(1);
});
