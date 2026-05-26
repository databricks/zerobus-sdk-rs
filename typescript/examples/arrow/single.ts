/**
 * Arrow Flight single-batch ingestion example (Beta).
 *
 * Default (uncompressed) IPC: `ipcCompression` is left unset, so the SDK
 * sends the IPC payload to the server without applying a codec.
 *
 * `apache-arrow` JS dictionary-encodes string columns by default and
 * marks typed-array-backed numeric columns as non-nullable. We build a
 * `RecordBatch` with an explicit `Schema` so every field is nullable and
 * the IPC payload matches the schema declared on the stream.
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

function buildIpc(
    deviceName: string[],
    temp: Int32Array,
    humidity: BigInt64Array,
): Buffer {
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
    return Buffer.from(tableToIPC(new Table(new RecordBatch(schema, data)), 'stream'));
}

async function main() {
    console.log('Arrow Flight Single-Batch Example (Beta)');
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
        // ipcCompression left unset → no compression on the wire
    };

    const stream = await sdk.createArrowStream(tableProperties, CLIENT_ID, CLIENT_SECRET, options);
    console.log('Arrow stream created');

    try {
        const ipc = buildIpc(
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
        console.log('Stream closed');
    }
}

main().catch((err) => {
    console.error('FAILED:', err);
    process.exit(1);
});
