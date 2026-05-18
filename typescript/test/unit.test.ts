/**
 * Unit tests for the v2.0 TypeScript SDK facade.
 *
 * These exercise the public API shape: constructor options, builder defaults,
 * and the discriminated-union types for `auth` and `format`. They do not open
 * any network connections — integration tests live in integration.test.ts.
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import {
    ZerobusSdk,
    ArrowDataType,
    bearerTokenProvider,
    type Auth,
    type GrpcFormat,
    type SdkOptions,
    type CreateStreamOptions,
    type CreateArrowStreamOptions,
    type Compression,
} from '../dist/index.js';

describe('ZerobusSdk', () => {
    describe('constructor', () => {
        it('accepts a minimal options bag', () => {
            const sdk = new ZerobusSdk({
                endpoint: 'https://1234567890.zerobus.us-west-2.cloud.databricks.com',
            });
            assert.ok(sdk);
        });

        it('accepts every documented option', () => {
            const sdk = new ZerobusSdk({
                endpoint: 'https://example.zerobus.databricks.com',
                unityCatalogUrl: 'https://example.cloud.databricks.com',
                tls: 'secure',
                applicationName: 'my-app/1.0',
                sdkIdentifier: 'custom-sdk/9.9.9',
            });
            assert.ok(sdk);
        });

        it('accepts tls: "none" for plaintext endpoints', () => {
            const sdk = new ZerobusSdk({
                endpoint: 'http://[::1]:50051',
                tls: 'none',
            });
            assert.ok(sdk);
        });

        it('throws when endpoint is missing', () => {
            assert.throws(
                () => new ZerobusSdk({ endpoint: '' }),
                /endpoint is required/,
            );
        });
    });
});

describe('Auth variants compile', () => {
    it('oauth shape', () => {
        const auth: Auth = { type: 'oauth', clientId: 'a', clientSecret: 'b' };
        assert.strictEqual(auth.type, 'oauth');
    });

    it('headersProvider shape', () => {
        const auth: Auth = {
            type: 'headersProvider',
            getHeaders: async () => [['authorization', 'Bearer x']],
        };
        assert.strictEqual(auth.type, 'headersProvider');
    });

    it('noAuth shape', () => {
        const auth: Auth = { type: 'noAuth' };
        assert.strictEqual(auth.type, 'noAuth');
    });
});

describe('Format variants compile', () => {
    it('json shape', () => {
        const fmt: GrpcFormat = { type: 'json' };
        assert.strictEqual(fmt.type, 'json');
    });

    it('proto shape requires a descriptor', () => {
        const fmt: GrpcFormat = { type: 'proto', descriptor: 'base64==' };
        assert.strictEqual(fmt.type, 'proto');
    });
});

describe('CreateStreamOptions', () => {
    it('typechecks a JSON stream with OAuth', () => {
        const o: CreateStreamOptions = {
            table: 'catalog.schema.table',
            auth: { type: 'oauth', clientId: 'a', clientSecret: 'b' },
            format: { type: 'json' },
            recovery: true,
            recoveryRetries: 5,
            maxInflightRequests: 1000,
        };
        assert.strictEqual(o.format.type, 'json');
    });

    it('typechecks a proto stream with custom headers', () => {
        const o: CreateStreamOptions = {
            table: 'catalog.schema.table',
            auth: {
                type: 'headersProvider',
                getHeaders: async () => [
                    ['authorization', 'Bearer tok'],
                    ['x-databricks-zerobus-table-name', 'catalog.schema.table'],
                ],
            },
            format: { type: 'proto', descriptor: 'base64==' },
        };
        assert.strictEqual(o.auth.type, 'headersProvider');
    });
});

describe('CreateArrowStreamOptions', () => {
    it('typechecks all compression modes', () => {
        const compressions: Compression[] = ['none', 'lz4_frame', 'zstd'];
        compressions.forEach((c) => {
            const o: CreateArrowStreamOptions = {
                table: 'c.s.t',
                auth: { type: 'noAuth' },
                schema: [{ name: 'id', dataType: ArrowDataType.Int64 }],
                compression: c,
            };
            assert.strictEqual(o.compression, c);
        });
    });

    it('schema accepts ArrowField with optional nullable', () => {
        const o: CreateArrowStreamOptions = {
            table: 'c.s.t',
            auth: { type: 'noAuth' },
            schema: [
                { name: 'id', dataType: ArrowDataType.Int64, nullable: false },
                { name: 'name', dataType: ArrowDataType.Utf8 },
            ],
        };
        assert.strictEqual(o.schema.length, 2);
    });
});

describe('ArrowDataType', () => {
    it('is a plain object with stable values', () => {
        assert.strictEqual(ArrowDataType.Boolean, 0);
        assert.strictEqual(ArrowDataType.Int64, 4);
        assert.strictEqual(ArrowDataType.Utf8, 11);
        assert.strictEqual(ArrowDataType.TimestampMicros, 17);
    });
});

describe('bearerTokenProvider', () => {
    it('returns the canonical header pair', async () => {
        const provider = bearerTokenProvider('catalog.schema.table', () => 'abc');
        const headers = await provider();
        assert.deepStrictEqual(headers, [
            ['authorization', 'Bearer abc'],
            ['x-databricks-zerobus-table-name', 'catalog.schema.table'],
        ]);
    });

    it('awaits a promise-returning token getter', async () => {
        const provider = bearerTokenProvider('t', async () => 'lazy');
        const headers = await provider();
        assert.strictEqual(headers[0][1], 'Bearer lazy');
    });
});

describe('SdkOptions type ergonomics', () => {
    it('treats every field besides endpoint as optional', () => {
        const o: SdkOptions = { endpoint: 'https://x.example' };
        assert.ok(o);
    });
});
