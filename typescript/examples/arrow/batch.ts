/**
 * Arrow Flight ingestion example (Beta).
 *
 * Opens one Arrow Flight stream, ingests 10 batches (5 rows each) in a
 * loop, waits for the last offset to be acknowledged, flushes, and
 * closes. Each iteration calls `ingestBatch` with an Arrow IPC stream
 * containing one RecordBatch — the offset-based API resolves as soon
 * as the batch is queued, so the loop runs without blocking on the
 * server for each batch.
 *
 * `apache-arrow` JS dictionary-encodes string columns by default and
 * marks typed-array-backed numeric columns as non-nullable. We build a
 * `RecordBatch` with an explicit `Schema` so every field is nullable
 * and the IPC payload matches the schema declared on the stream.
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

const SCHEMA = new Schema([
    new Field('device_name', new Utf8(), true),
    new Field('temp', new Int32(), true),
    new Field('humidity', new Int64(), true),
]);

function buildBatchIpc(batchIndex: number, rowsPerBatch: number): Buffer {
    const start = batchIndex * rowsPerBatch;
    const deviceName = Array.from({ length: rowsPerBatch }, (_, i) => `sensor-${start + i}`);
    const temp = Int32Array.from({ length: rowsPerBatch }, (_, i) => 20 + ((start + i) % 15));
    const humidity = BigInt64Array.from(
        { length: rowsPerBatch },
        (_, i) => BigInt(50 + ((start + i) % 40)),
    );

    const dev = vectorFromArray(deviceName, new Utf8());
    const t = makeVector(temp);
    const h = makeVector(humidity);
    const data = makeData({
        type: new Struct(SCHEMA.fields),
        length: rowsPerBatch,
        children: [dev.data[0], t.data[0], h.data[0]],
    });
    return Buffer.from(tableToIPC(new Table(new RecordBatch(SCHEMA, data)), 'stream'));
}

async function main() {
    console.log('Arrow Flight Example (Beta)');
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
        maxInflightBatches: 50,
    };

    const stream = await sdk.createArrowStream(tableProperties, CLIENT_ID, CLIENT_SECRET, options);
    console.log('Arrow stream created');

    try {
        const NUM_BATCHES = 10;
        const ROWS_PER_BATCH = 5;
        let lastOffset = 0n;

        for (let i = 0; i < NUM_BATCHES; i++) {
            const ipc = buildBatchIpc(i, ROWS_PER_BATCH);
            lastOffset = await stream.ingestBatch(ipc);
            console.log(`Queued batch ${i + 1}/${NUM_BATCHES} (${ROWS_PER_BATCH} rows) → offset=${lastOffset}`);
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
