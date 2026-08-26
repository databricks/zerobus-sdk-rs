/**
 * Protocol Buffers Single-Record Ingestion Example
 *
 * This example demonstrates:
 * - Creating a stream with Protocol Buffers record type
 * - Loading and using protobuf descriptors
 * - Ingesting individual records with ingestRecordOffset() (recommended)
 * - Using waitForOffset() for selective acknowledgment
 * - Deprecated ingestRecord() API for comparison
 * - Type widening: Message object vs Buffer
 */

import * as path from 'path';
import * as fs from 'fs';
import { ZerobusSdk, StreamConfigurationOptions, TableProperties, RecordType } from '../../index';
import { loadDescriptorProto } from '../../utils/descriptor';
import * as airQuality from '../generated/air_quality';

// Configuration - set via environment variables or modify these defaults
const SERVER_ENDPOINT = process.env.ZEROBUS_SERVER_ENDPOINT || 'https://your-workspace-id.zerobus.region.cloud.databricks.com';
const DATABRICKS_WORKSPACE_URL = process.env.DATABRICKS_WORKSPACE_URL || 'https://your-workspace.cloud.databricks.com';
const TABLE_NAME = process.env.ZEROBUS_TABLE_NAME || 'catalog.schema.table';
const CLIENT_ID = process.env.DATABRICKS_CLIENT_ID || 'your-oauth-client-id';
const CLIENT_SECRET = process.env.DATABRICKS_CLIENT_SECRET || 'your-oauth-client-secret';

async function main() {
    console.log('Protocol Buffers Single-Record Ingestion Example');
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

    // Load protobuf descriptor
    const descriptorPath = path.join(__dirname, '..', '..', 'schemas', 'air_quality_descriptor.pb');

    if (!fs.existsSync(descriptorPath)) {
        console.error('Error: Protobuf descriptor not found at:', descriptorPath);
        console.log('\nGenerate it using: npm run build:proto');
        process.exit(1);
    }

    const descriptorBase64 = loadDescriptorProto({
        descriptorPath,
        protoFileName: 'air_quality.proto',
        messageName: 'AirQuality'
    });
    console.log('Protobuf descriptor loaded');

    // Initialize SDK
    const sdk = new ZerobusSdk(SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL);

    // Configure table properties with descriptor (required for Protocol Buffers)
    const tableProperties: TableProperties = {
        tableName: TABLE_NAME,
        descriptorProto: descriptorBase64
    };

    // Configure stream with Protocol Buffers record type
    const options: StreamConfigurationOptions = {
        recordType: RecordType.Proto,
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

    const AirQuality = airQuality.examples.AirQuality;

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

        // 1. Auto-encoding: Message object - SDK handles encoding
        const record1 = AirQuality.create({
            deviceName: 'sensor-001',
            temp: 22,
            humidity: 65
        });

        const offset1 = await stream.ingestRecordOffset(record1);
        console.log(`[Auto-encoding] Record sent with offset ID: ${offset1}`);
        await stream.waitForOffset(offset1);
        console.log(`[Auto-encoding] Record acknowledged with offset ID: ${offset1}`);

        // 2. Pre-encoded: Buffer - pass pre-serialized bytes
        const record2 = AirQuality.create({
            deviceName: 'sensor-002',
            temp: 24,
            humidity: 70
        });
        const buffer = Buffer.from(AirQuality.encode(record2).finish());

        const offset2 = await stream.ingestRecordOffset(buffer);
        console.log(`[Pre-encoded] Record sent with offset ID: ${offset2}`);
        await stream.waitForOffset(offset2);
        console.log(`[Pre-encoded] Record acknowledged with offset ID: ${offset2}`);

        // 3. High-throughput pattern: send many, wait once
        console.log('\n[High-throughput] Sending 10 records...');
        let lastOffset: bigint = BigInt(0);
        for (let i = 0; i < 10; i++) {
            const record = AirQuality.create({
                deviceName: `sensor-${i.toString().padStart(3, '0')}`,
                temp: 20 + i,
                humidity: 50 + i * 2
            });
            lastOffset = await stream.ingestRecordOffset(record);
        }
        await stream.waitForOffset(lastOffset);
        console.log(`[High-throughput] All 10 records acknowledged (last offset: ${lastOffset})`);
    }

    /**
     * Deprecated API: returns future that resolves to offset.
     */
    async function ingestWithFutureApi() {
        console.log('\n=== Future-based API (Deprecated) ===');

        // 1. Auto-encoding: Message object
        const record1 = AirQuality.create({
            deviceName: 'sensor-legacy-001',
            temp: 23,
            humidity: 68
        });

        const offset1 = await stream.ingestRecord(record1);
        console.log(`[Auto-encoding] Record acknowledged with offset ID: ${offset1}`);

        // 2. Pre-encoded: Buffer
        const record2 = AirQuality.create({
            deviceName: 'sensor-legacy-002',
            temp: 25,
            humidity: 72
        });
        const buffer = Buffer.from(AirQuality.encode(record2).finish());

        const offset2 = await stream.ingestRecord(buffer);
        console.log(`[Pre-encoded] Record acknowledged with offset ID: ${offset2}`);
    }
}

// Run the example
main().catch((error) => {
    console.error('Fatal error:', error);
    process.exit(1);
});
