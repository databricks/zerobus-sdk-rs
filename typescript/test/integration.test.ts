/**
 * Integration tests for the v2.0 TypeScript SDK.
 *
 * These tests need a running Zerobus server. They are skipped unless
 * `ZEROBUS_ENDPOINT` is set. The same env vars used by examples apply:
 *
 *   ZEROBUS_ENDPOINT       e.g. http://[::1]:50051
 *   ZEROBUS_TLS            'none' for plaintext
 *   ZEROBUS_NO_AUTH        '1' to skip OAuth
 *   ZEROBUS_TABLE_NAME     fully-qualified table name
 *   DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET / DATABRICKS_WORKSPACE_URL
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert';
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
import { ZerobusSdk, ArrowDataType, type SdkOptions, type Auth } from '../dist/index.js';

const ENABLED = !!process.env.ZEROBUS_ENDPOINT;
const TABLE = process.env.ZEROBUS_TABLE_NAME ?? 'test_data/test_table';

function buildConfig(): { sdkOptions: SdkOptions; auth: Auth } {
    const tls: 'secure' | 'none' = process.env.ZEROBUS_TLS === 'none' ? 'none' : 'secure';
    const sdkOptions: SdkOptions = {
        endpoint: process.env.ZEROBUS_ENDPOINT!,
        unityCatalogUrl: process.env.DATABRICKS_WORKSPACE_URL,
        tls,
        applicationName: 'zerobus-sdk-ts-tests/2.0.0',
    };
    const auth: Auth =
        process.env.ZEROBUS_NO_AUTH === '1' || tls === 'none'
            ? { type: 'noAuth' }
            : {
                  type: 'oauth',
                  clientId: process.env.DATABRICKS_CLIENT_ID!,
                  clientSecret: process.env.DATABRICKS_CLIENT_SECRET!,
              };
    return { sdkOptions, auth };
}

describe('JSON stream', { skip: !ENABLED }, () => {
    it('ingestRecord + waitForOffset', async () => {
        const { sdkOptions, auth } = buildConfig();
        const sdk = new ZerobusSdk(sdkOptions);
        const stream = await sdk.createStream({
            table: TABLE,
            auth,
            format: { type: 'json' },
        });
        try {
            const offset = await stream.ingestRecord({
                device_name: 'integration-test',
                temp: 21,
                humidity: 55,
            });
            assert.strictEqual(typeof offset, 'bigint');
            await stream.waitForOffset(offset);
            await stream.flush();
        } finally {
            await stream.close();
        }
    });

    it('ingestRecords returns null for empty batch', async () => {
        const { sdkOptions, auth } = buildConfig();
        const sdk = new ZerobusSdk(sdkOptions);
        const stream = await sdk.createStream({
            table: TABLE,
            auth,
            format: { type: 'json' },
        });
        try {
            const offset = await stream.ingestRecords([]);
            assert.strictEqual(offset, null);
        } finally {
            await stream.close();
        }
    });

    it('ingestRecords with a non-empty batch', async () => {
        const { sdkOptions, auth } = buildConfig();
        const sdk = new ZerobusSdk(sdkOptions);
        const stream = await sdk.createStream({
            table: TABLE,
            auth,
            format: { type: 'json' },
        });
        try {
            const offset = await stream.ingestRecords([
                { device_name: 'a', temp: 1, humidity: 1 },
                { device_name: 'b', temp: 2, humidity: 2 },
            ]);
            assert.strictEqual(typeof offset, 'bigint');
            await stream.waitForOffset(offset!);
        } finally {
            await stream.close();
        }
    });
});

describe('Arrow Flight stream (Beta)', { skip: !ENABLED }, () => {
    function buildIpc(): Buffer {
        const schema = new Schema([
            new Field('device_name', new Utf8(), true),
            new Field('temp', new Int32(), true),
            new Field('humidity', new Int64(), true),
        ]);
        const dev = vectorFromArray(['arrow-test'], new Utf8());
        const t = makeVector(Int32Array.from([42]));
        const h = makeVector(BigInt64Array.from([99n]));
        const data = makeData({
            type: new Struct(schema.fields),
            length: 1,
            children: [dev.data[0], t.data[0], h.data[0]],
        });
        return Buffer.from(tableToIPC(new Table(new RecordBatch(schema, data)), 'stream'));
    }

    it('zero-copy IPC path (no compression)', async () => {
        const { sdkOptions, auth } = buildConfig();
        const sdk = new ZerobusSdk(sdkOptions);
        const stream = await sdk.createArrowStream({
            table: TABLE,
            auth,
            schema: [
                { name: 'device_name', dataType: ArrowDataType.Utf8 },
                { name: 'temp', dataType: ArrowDataType.Int32 },
                { name: 'humidity', dataType: ArrowDataType.Int64 },
            ],
        });
        try {
            const offset = await stream.ingestBatch(buildIpc());
            assert.strictEqual(typeof offset, 'bigint');
            await stream.waitForOffset(offset);
        } finally {
            await stream.close();
        }
    });

    it('parsed path with zstd compression', async () => {
        const { sdkOptions, auth } = buildConfig();
        const sdk = new ZerobusSdk(sdkOptions);
        const stream = await sdk.createArrowStream({
            table: TABLE,
            auth,
            schema: [
                { name: 'device_name', dataType: ArrowDataType.Utf8 },
                { name: 'temp', dataType: ArrowDataType.Int32 },
                { name: 'humidity', dataType: ArrowDataType.Int64 },
            ],
            compression: 'zstd',
        });
        try {
            const offset = await stream.ingestBatch(buildIpc());
            assert.strictEqual(typeof offset, 'bigint');
            await stream.waitForOffset(offset);
        } finally {
            await stream.close();
        }
    });
});
