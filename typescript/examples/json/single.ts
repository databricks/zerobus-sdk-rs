/**
 * JSON Single-Record Ingestion Example
 *
 * This example demonstrates:
 * - Creating a stream with JSON record type
 * - Ingesting individual records with ingestRecordOffset() (recommended)
 * - Using waitForOffset() for selective acknowledgment
 * - Deprecated ingestRecord() API for comparison
 * - Type widening: object vs string
 */

import { ZerobusSdk, StreamConfigurationOptions, TableProperties, RecordType } from '../../index';

// Configuration - set via environment variables or modify these defaults
const SERVER_ENDPOINT = process.env.ZEROBUS_SERVER_ENDPOINT || 'https://your-workspace-id.zerobus.region.cloud.databricks.com';
const DATABRICKS_WORKSPACE_URL = process.env.DATABRICKS_WORKSPACE_URL || 'https://your-workspace.cloud.databricks.com';
const TABLE_NAME = process.env.ZEROBUS_TABLE_NAME || 'catalog.schema.table';
const CLIENT_ID = process.env.DATABRICKS_CLIENT_ID || 'your-oauth-client-id';
const CLIENT_SECRET = process.env.DATABRICKS_CLIENT_SECRET || 'your-oauth-client-secret';

/**
 * AirQuality record structure matching the table schema:
 * - device_name: STRING
 * - temp: INT
 * - humidity: BIGINT (use number in JS - JSON.stringify doesn't support BigInt)
 */
interface AirQuality {
    device_name: string;
    temp: number;
    humidity: number;  // BIGINT in table, but use number for JSON serialization
}

async function main() {
    console.log('JSON Single-Record Ingestion Example');
    console.log('='.repeat(60));

    // Validate configuration
    if (CLIENT_ID === 'your-oauth-client-id' || CLIENT_SECRET === 'your-oauth-client-secret') {
        console.error('Error: Please set DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET environment variables');
        process.exit(1);
    }

    if (SERVER_ENDPOINT === 'https://your-workspace-id.zerobus.region.cloud.databricks.com') {
        console.error('Error: Please set ZEROBUS_SERVER_ENDPOINT environment variable');
        process.exit(1);
    }

    if (TABLE_NAME === 'catalog.schema.table') {
        console.error('Error: Please set ZEROBUS_TABLE_NAME environment variable');
        process.exit(1);
    }

    // Initialize SDK
    const sdk = new ZerobusSdk(SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL);

    // Configure table properties (no descriptor needed for JSON)
    const tableProperties: TableProperties = {
        tableName: TABLE_NAME
    };

    // Configure stream with JSON record type
    const options: StreamConfigurationOptions = {
        recordType: RecordType.Json,
        maxInflightRequests: 100,
        recovery: true
    };

    // Create stream
    const stream = await sdk.createStream(
        tableProperties,
        CLIENT_ID,
        CLIENT_SECRET,
        options
    );
    console.log('Stream created');

    try {
        await ingestWithOffsetApi();
        await ingestWithFutureApi();

        await stream.close();
        console.log('Stream closed successfully');
    } catch (error) {
        await stream.close();
        throw error;
    }

    /**
     * Recommended API: returns offset directly after queuing.
     */
    async function ingestWithOffsetApi() {
        console.log('\n=== Offset-based API (Recommended) ===');

        // NOTE: Examples 1 and 2 below confirm each record with waitForOffset(), which is
        // handy when a specific record must be confirmed before continuing (and fine for
        // this single-record demo). For high volume, the idiomatic flow is to ingest in a
        // loop and flush() once at the end — see example 3 below.

        // 1. Auto-serializing: object - SDK handles JSON.stringify()
        const record1: AirQuality = {
            device_name: 'sensor-001',
            temp: 22,
            humidity: 65
        };

        const offset1 = await stream.ingestRecordOffset(record1);
        console.log(`[Auto-serializing] Record sent with offset ID: ${offset1}`);
        await stream.waitForOffset(offset1);
        console.log(`[Auto-serializing] Record acknowledged with offset ID: ${offset1}`);

        // 2. Pre-serialized: string - pass JSON string directly
        const jsonString = JSON.stringify({
            device_name: 'sensor-002',
            temp: 24,
            humidity: 70
        });

        const offset2 = await stream.ingestRecordOffset(jsonString);
        console.log(`[Pre-serialized] Record sent with offset ID: ${offset2}`);
        await stream.waitForOffset(offset2);
        console.log(`[Pre-serialized] Record acknowledged with offset ID: ${offset2}`);

        // 3. High-throughput pattern: send many, wait once
        console.log('\n[High-throughput] Sending 10 records...');
        let lastOffset: bigint = BigInt(0);
        for (let i = 0; i < 10; i++) {
            lastOffset = await stream.ingestRecordOffset({
                device_name: `sensor-${i.toString().padStart(3, '0')}`,
                temp: 20 + i,
                humidity: 50 + i * 2
            });
        }
        await stream.waitForOffset(lastOffset);
        console.log(`[High-throughput] All 10 records acknowledged (last offset: ${lastOffset})`);
    }

    /**
     * Deprecated API: returns future that resolves to offset.
     */
    async function ingestWithFutureApi() {
        console.log('\n=== Future-based API (Deprecated) ===');

        // 1. Auto-serializing: object
        const record1: AirQuality = {
            device_name: 'sensor-legacy-001',
            temp: 23,
            humidity: 68
        };

        const offset1 = await stream.ingestRecord(record1);
        console.log(`[Auto-serializing] Record acknowledged with offset ID: ${offset1}`);

        // 2. Pre-serialized: string
        const jsonString = JSON.stringify({
            device_name: 'sensor-legacy-002',
            temp: 25,
            humidity: 72
        });

        const offset2 = await stream.ingestRecord(jsonString);
        console.log(`[Pre-serialized] Record acknowledged with offset ID: ${offset2}`);
    }
}

// Run the example
main().catch((error) => {
    console.error('Fatal error:', error);
    process.exit(1);
});
