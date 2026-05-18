/**
 * Protocol Buffers single-record ingestion example.
 *
 * Pre-requisite: run `npm run build:proto` once to compile
 * `schemas/air_quality.proto` into JS/TS stubs and a `.pb` descriptor file.
 *
 * Demonstrates passing a protobufjs message instance — the SDK invokes
 * `.encode().finish()` to produce the wire bytes.
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
        const records = [
            examples.AirQuality.create({ deviceName: 'sensor-1', temp: 21, humidity: 55 }),
            examples.AirQuality.create({ deviceName: 'sensor-2', temp: 19, humidity: 60 }),
            examples.AirQuality.create({ deviceName: 'sensor-3', temp: 23, humidity: 50 }),
        ];

        let lastOffset: bigint | null = null;
        for (const r of records) {
            lastOffset = await stream.ingestRecord(r);
            console.log(`Queued proto record → offset=${lastOffset}`);
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
