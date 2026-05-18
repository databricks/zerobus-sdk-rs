/**
 * JSON single-record ingestion example.
 *
 * Demonstrates `ingestRecord` (queue + offset) followed by `waitForOffset`
 * to confirm server acknowledgment. The record is serialized to JSON by the
 * SDK — pass a plain object or a pre-serialized string.
 */

import { ZerobusSdk } from '../../dist/index.js';
import { loadConfig } from '../_config.js';

interface AirQuality {
    device_name: string;
    temp: number;
    humidity: number; // BIGINT — JS numbers OK up to 2^53
}

async function main() {
    const cfg = loadConfig();
    const sdk = new ZerobusSdk(cfg.sdkOptions);

    const stream = await sdk.createStream({
        table: cfg.tableName,
        auth: cfg.auth,
        format: { type: 'json' },
        maxInflightRequests: 100,
        recovery: true,
    });
    console.log('Stream created');

    try {
        const records: AirQuality[] = [
            { device_name: 'sensor-1', temp: 21, humidity: 55 },
            { device_name: 'sensor-2', temp: 19, humidity: 60 },
            { device_name: 'sensor-3', temp: 23, humidity: 50 },
        ];

        let lastOffset: bigint | null = null;
        for (const r of records) {
            lastOffset = await stream.ingestRecord(r);
            console.log(`Queued record device=${r.device_name} → offset=${lastOffset}`);
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
