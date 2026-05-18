/**
 * JSON batch ingestion example.
 *
 * `ingestRecords(array)` queues all records atomically and resolves to the
 * batch's offset ID once the records are in the SDK's landing zone.
 */

import { ZerobusSdk } from '../../dist/index.js';
import { loadConfig } from '../_config.js';

interface AirQuality {
    device_name: string;
    temp: number;
    humidity: number;
}

async function main() {
    const cfg = loadConfig();
    const sdk = new ZerobusSdk(cfg.sdkOptions);

    const stream = await sdk.createStream({
        table: cfg.tableName,
        auth: cfg.auth,
        format: { type: 'json' },
        maxInflightRequests: 100,
    });
    console.log('Stream created');

    try {
        const NUM_BATCHES = 5;
        const ROWS_PER_BATCH = 10;
        let lastOffset: bigint | null = null;

        for (let b = 0; b < NUM_BATCHES; b++) {
            const records: AirQuality[] = Array.from({ length: ROWS_PER_BATCH }, (_, i) => ({
                device_name: `sensor-${b * ROWS_PER_BATCH + i}`,
                temp: 20 + (i % 5),
                humidity: 50 + (i % 10),
            }));
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
