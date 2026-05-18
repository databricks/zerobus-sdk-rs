/**
 * Protocol Buffers batch ingestion example.
 *
 * Run `npm run build:proto` first to generate the JS/TS stubs and the
 * `.pb` descriptor file. The SDK encodes each protobufjs message into bytes
 * before queuing.
 */

import * as path from 'node:path';
import { ZerobusSdk } from '../../dist/index.js';
import { loadDescriptorProto } from '../../utils/descriptor.js';
import { loadConfig } from '../_config.js';
// eslint-disable-next-line @typescript-eslint/no-require-imports
const { examples } = require('../generated/air_quality.js');

async function main() {
    const cfg = loadConfig();
    const descriptor = loadDescriptorProto({
        descriptorPath: path.resolve(__dirname, '../../schemas/air_quality_descriptor.pb'),
        protoFileName: 'air_quality.proto',
        messageName: 'AirQuality',
    });
    const sdk = new ZerobusSdk(cfg.sdkOptions);

    const stream = await sdk.createStream({
        table: cfg.tableName,
        auth: cfg.auth,
        format: { type: 'proto', descriptor },
        maxInflightRequests: 100,
    });
    console.log('Stream created');

    try {
        const NUM_BATCHES = 5;
        const ROWS_PER_BATCH = 10;
        let lastOffset: bigint | null = null;

        for (let b = 0; b < NUM_BATCHES; b++) {
            const records = Array.from({ length: ROWS_PER_BATCH }, (_, i) =>
                examples.AirQuality.create({
                    deviceName: `sensor-${b * ROWS_PER_BATCH + i}`,
                    temp: 20 + (i % 5),
                    humidity: 50 + (i % 10),
                }),
            );
            lastOffset = await stream.ingestRecords(records);
            console.log(`Batch ${b + 1}/${NUM_BATCHES} queued → offset=${lastOffset}`);
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
