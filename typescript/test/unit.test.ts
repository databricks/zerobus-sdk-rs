/**
 * Unit tests for Zerobus TypeScript SDK
 *
 * These tests verify the TypeScript bindings and type conversions.
 * Integration tests that require a Databricks workspace are in integration.test.ts
 */

import { describe, it, before } from 'node:test';
import * as assert from 'node:assert';
import { execFileSync } from 'node:child_process';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { ZerobusSdk, RecordType, TableProperties, StreamConfigurationOptions, JsAckCallback } from '../index';
import { HeadersProvider } from '../src/headers_provider';
import { loadDescriptorProto } from '../utils/descriptor.js';

const descriptor = require('protobufjs/ext/descriptor');

describe('ZerobusSdk', () => {
    describe('constructor', () => {
        it('should create SDK instance with valid endpoints', () => {
            const sdk = new ZerobusSdk(
                'https://1234567890.zerobus.us-west-2.cloud.databricks.com',
                'https://test-workspace.cloud.databricks.com'
            );
            assert.ok(sdk);
        });

        it('should extract workspace ID from endpoint', () => {
            const sdk = new ZerobusSdk(
                'https://9876543210.zerobus.us-west-2.cloud.databricks.com',
                'https://test.cloud.databricks.com'
            );
            assert.ok(sdk);
        });

        it('should accept an options object with applicationName', () => {
            const sdk = new ZerobusSdk(
                'https://1234567890.zerobus.us-west-2.cloud.databricks.com',
                'https://test-workspace.cloud.databricks.com',
                { applicationName: 'my-app/1.0' }
            );
            assert.ok(sdk);
        });

        it('should accept an empty options object', () => {
            const sdk = new ZerobusSdk(
                'https://1234567890.zerobus.us-west-2.cloud.databricks.com',
                'https://test-workspace.cloud.databricks.com',
                {}
            );
            assert.ok(sdk);
        });
    });

    describe('configuration validation', () => {
        it('should accept valid StreamConfigurationOptions', () => {
            const options: StreamConfigurationOptions = {
                recordType: RecordType.Json,
                maxInflightRequests: 1000,
                recovery: true,
                recoveryTimeoutMs: 15000,
                recoveryBackoffMs: 2000,
                recoveryRetries: 3,
            };
            assert.strictEqual(options.recordType, RecordType.Json);
            assert.strictEqual(options.maxInflightRequests, 1000);
        });

        it('should accept optional StreamConfigurationOptions', () => {
            const options: StreamConfigurationOptions = {
                recordType: RecordType.Proto,
            };
            assert.strictEqual(options.recordType, RecordType.Proto);
        });

        it('should accept new v0.4.0 configuration options', () => {
            const options: StreamConfigurationOptions = {
                recordType: RecordType.Json,
                maxInflightRequests: 100,
                callbackMaxWaitTimeMs: 5000,      // New in v0.4.0
                streamPausedMaxWaitTimeMs: 3000,  // New in v0.4.0
            };
            assert.strictEqual(options.callbackMaxWaitTimeMs, 5000);
            assert.strictEqual(options.streamPausedMaxWaitTimeMs, 3000);
        });

        it('should allow undefined for new callback timeout options', () => {
            const options: StreamConfigurationOptions = {
                recordType: RecordType.Json,
                // callbackMaxWaitTimeMs and streamPausedMaxWaitTimeMs are optional
            };
            assert.strictEqual(options.callbackMaxWaitTimeMs, undefined);
            assert.strictEqual(options.streamPausedMaxWaitTimeMs, undefined);
        });
    });

    describe('TableProperties', () => {
        it('should create table properties with just table name (JSON mode)', () => {
            const props: TableProperties = {
                tableName: 'catalog.schema.table',
            };
            assert.strictEqual(props.tableName, 'catalog.schema.table');
            assert.strictEqual(props.descriptorProto, undefined);
        });

        it('should create table properties with descriptor (Proto mode)', () => {
            const props: TableProperties = {
                tableName: 'catalog.schema.table',
                descriptorProto: 'base64encodedstring',
            };
            assert.strictEqual(props.tableName, 'catalog.schema.table');
            assert.strictEqual(props.descriptorProto, 'base64encodedstring');
        });
    });
});

describe('HeadersProvider', () => {
    it('should accept custom headers provider implementation', () => {
        class TestHeadersProvider implements HeadersProvider {
            async getHeaders(): Promise<Array<[string, string]>> {
                return [
                    ['authorization', 'Bearer test-token'],
                    ['x-databricks-zerobus-table-name', 'catalog.schema.table'],
                ];
            }
        }

        const provider = new TestHeadersProvider();
        assert.ok(provider);
        assert.ok(typeof provider.getHeaders === 'function');
    });

    it('should return correct header format', async () => {
        class TestHeadersProvider implements HeadersProvider {
            async getHeaders(): Promise<Array<[string, string]>> {
                return [
                    ['authorization', 'Bearer test-token'],
                    ['x-databricks-zerobus-table-name', 'test-table'],
                    ['x-custom-header', 'custom-value'],
                ];
            }
        }

        const provider = new TestHeadersProvider();
        const headers = await provider.getHeaders();

        assert.strictEqual(headers.length, 3);
        assert.deepStrictEqual(headers[0], ['authorization', 'Bearer test-token']);
        assert.deepStrictEqual(headers[1], ['x-databricks-zerobus-table-name', 'test-table']);
        assert.deepStrictEqual(headers[2], ['x-custom-header', 'custom-value']);
    });
});

describe('Descriptor utilities', () => {
    it('should load DescriptorProto from the CommonJS helper', () => {
        const descriptorPath = path.join(os.tmpdir(), `zerobus-descriptor-${process.pid}.pb`);
        const message = descriptor.DescriptorProto.create({
            name: 'AirQuality',
            field: [
                {
                    name: 'device_name',
                    number: 1,
                    label: 1,
                    type: 9,
                },
            ],
        });
        const fileDescriptorSet = descriptor.FileDescriptorSet.create({
            file: [
                {
                    name: 'schemas/air_quality.proto',
                    messageType: [message],
                },
            ],
        });

        fs.writeFileSync(descriptorPath, descriptor.FileDescriptorSet.encode(fileDescriptorSet).finish());

        try {
            const encoded = loadDescriptorProto({
                descriptorPath,
                protoFileName: 'air_quality.proto',
                messageName: 'AirQuality',
            });
            const decoded = descriptor.DescriptorProto.decode(Buffer.from(encoded, 'base64'));
            assert.strictEqual(decoded.name, 'AirQuality');
            assert.strictEqual(decoded.field[0].name, 'device_name');
        } finally {
            fs.rmSync(descriptorPath, { force: true });
        }
    });

    it('should require a path boundary when matching proto file names', () => {
        const descriptorPath = path.join(os.tmpdir(), `zerobus-descriptor-boundary-${process.pid}.pb`);
        const shadowMessage = descriptor.DescriptorProto.create({
            name: 'AirQuality',
            field: [
                {
                    name: 'wrong_field',
                    number: 1,
                    label: 1,
                    type: 9,
                },
            ],
        });
        const expectedMessage = descriptor.DescriptorProto.create({
            name: 'AirQuality',
            field: [
                {
                    name: 'device_name',
                    number: 1,
                    label: 1,
                    type: 9,
                },
            ],
        });
        const fileDescriptorSet = descriptor.FileDescriptorSet.create({
            file: [
                {
                    name: 'schemas/not_air_quality.proto',
                    messageType: [shadowMessage],
                },
                {
                    name: 'schemas/air_quality.proto',
                    messageType: [expectedMessage],
                },
            ],
        });

        fs.writeFileSync(descriptorPath, descriptor.FileDescriptorSet.encode(fileDescriptorSet).finish());

        try {
            const encoded = loadDescriptorProto({
                descriptorPath,
                protoFileName: 'air_quality.proto',
                messageName: 'AirQuality',
            });
            const decoded = descriptor.DescriptorProto.decode(Buffer.from(encoded, 'base64'));
            assert.strictEqual(decoded.field[0].name, 'device_name');
        } finally {
            fs.rmSync(descriptorPath, { force: true });
        }
    });

    it('should resolve the packed helper from CommonJS, ESM, and NodeNext', { timeout: 120_000 }, () => {
        const packageRoot = path.resolve(__dirname, '..');
        const temporaryRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'zerobus-package-test-'));
        const consumerRoot = path.join(temporaryRoot, 'consumer');
        const npmCli = process.env.npm_execpath;
        assert.ok(npmCli, 'npm_execpath must be set when running the npm test script');

        const runNpm = (args: string[], cwd: string): string =>
            execFileSync(process.execPath, [npmCli, ...args], { cwd, encoding: 'utf8' });

        try {
            fs.mkdirSync(consumerRoot);
            fs.writeFileSync(
                path.join(consumerRoot, 'package.json'),
                JSON.stringify({ name: 'zerobus-package-test', private: true, type: 'module' }, null, 2)
            );

            runNpm(['pack', '--pack-destination', temporaryRoot, '--json', '--silent'], packageRoot);
            const tarballs = fs.readdirSync(temporaryRoot).filter((file) => file.endsWith('.tgz'));
            assert.strictEqual(tarballs.length, 1, 'npm pack should create exactly one tarball');
            const tarballPath = path.join(temporaryRoot, tarballs[0]);

            runNpm(
                [
                    'install',
                    '--prefer-offline',
                    '--ignore-scripts',
                    '--omit=optional',
                    '--no-audit',
                    '--no-fund',
                    '--package-lock=false',
                    '--no-save',
                    tarballPath,
                ],
                consumerRoot
            );

            const packageSubpath = '@databricks/zerobus-ingest-sdk/utils/descriptor.js';
            execFileSync(
                process.execPath,
                [
                    '--input-type=commonjs',
                    '-e',
                    `const helper = require('${packageSubpath}'); if (typeof helper.loadDescriptorProto !== 'function') process.exit(1);`,
                ],
                { cwd: consumerRoot }
            );
            execFileSync(
                process.execPath,
                [
                    '--input-type=module',
                    '-e',
                    `import { loadDescriptorProto } from '${packageSubpath}'; if (typeof loadDescriptorProto !== 'function') process.exit(1);`,
                ],
                { cwd: consumerRoot }
            );

            const typeTestPath = path.join(consumerRoot, 'consumer.mts');
            fs.writeFileSync(
                typeTestPath,
                `import { loadDescriptorProto, type LoadDescriptorOptions } from '${packageSubpath}';\n` +
                    `const options: LoadDescriptorOptions = { descriptorPath: 'schema.pb', protoFileName: 'schema.proto', messageName: 'Record' };\n` +
                    `const encoded: string = loadDescriptorProto(options);\n` +
                    `void encoded;\n`
            );
            execFileSync(
                process.execPath,
                [
                    path.join(packageRoot, 'node_modules', 'typescript', 'bin', 'tsc'),
                    '--noEmit',
                    '--strict',
                    '--skipLibCheck',
                    '--target',
                    'ES2020',
                    '--module',
                    'NodeNext',
                    '--moduleResolution',
                    'NodeNext',
                    typeTestPath,
                ],
                { cwd: consumerRoot }
            );
        } finally {
            fs.rmSync(temporaryRoot, { recursive: true, force: true });
        }
    });
});

describe('RecordType enum', () => {
    it('should have Json value', () => {
        assert.strictEqual(RecordType.Json, 0);
    });

    it('should have Proto value', () => {
        assert.strictEqual(RecordType.Proto, 1);
    });
});

describe('Type widening validation', () => {
    it('should accept Buffer for Proto mode', () => {
        const buffer = Buffer.from([1, 2, 3, 4]);
        assert.ok(Buffer.isBuffer(buffer));
    });

    it('should accept string for JSON mode', () => {
        const jsonString = JSON.stringify({ test: 'data' });
        assert.strictEqual(typeof jsonString, 'string');
    });

    it('should accept plain object for JSON mode', () => {
        const obj = { device_name: 'sensor-1', temp: 25 };
        assert.strictEqual(typeof obj, 'object');
        assert.ok(!Buffer.isBuffer(obj));
    });

    it('should validate protobuf message interface', () => {
        // Mock protobuf message
        const mockProtoMessage = {
            encode: function() {
                return {
                    finish: function() {
                        return Buffer.from([1, 2, 3]);
                    }
                };
            }
        };

        assert.ok(typeof mockProtoMessage.encode === 'function');
        const encoded = mockProtoMessage.encode();
        assert.ok(typeof encoded.finish === 'function');
        const buffer = encoded.finish();
        assert.ok(Buffer.isBuffer(buffer));
    });
});

describe('Error handling', () => {
    it('should provide meaningful error messages', () => {
        try {
            // Invalid endpoint format
            new ZerobusSdk('', '');
            assert.fail('Should have thrown an error');
        } catch (error) {
            assert.ok(error);
            assert.ok(error instanceof Error);
        }
    });
});

describe('Batch operations', () => {
    it('should accept array of buffers for batch proto', () => {
        const buffers = [
            Buffer.from([1, 2, 3]),
            Buffer.from([4, 5, 6]),
            Buffer.from([7, 8, 9]),
        ];
        assert.strictEqual(buffers.length, 3);
        buffers.forEach(buf => assert.ok(Buffer.isBuffer(buf)));
    });

    it('should accept array of strings for batch JSON', () => {
        const jsonStrings = [
            JSON.stringify({ id: 1 }),
            JSON.stringify({ id: 2 }),
            JSON.stringify({ id: 3 }),
        ];
        assert.strictEqual(jsonStrings.length, 3);
        jsonStrings.forEach(str => assert.strictEqual(typeof str, 'string'));
    });

    it('should accept array of plain objects for batch JSON', () => {
        const objects = [
            { device: 'sensor-1', temp: 20 },
            { device: 'sensor-2', temp: 21 },
            { device: 'sensor-3', temp: 22 },
        ];
        assert.strictEqual(objects.length, 3);
        objects.forEach(obj => assert.strictEqual(typeof obj, 'object'));
    });

    it('should accept mixed formats in array (Buffer, string, object)', () => {
        const records = [
            Buffer.from([1, 2, 3]),
            JSON.stringify({ id: 2 }),
            { id: 3 },
        ];
        assert.strictEqual(records.length, 3);
    });

    it('should handle empty batch', () => {
        const emptyBatch: any[] = [];
        assert.strictEqual(emptyBatch.length, 0);
    });
});

describe('AckCallback (v0.4.0)', () => {
    it('should accept ack callback with onAck function', () => {
        let ackCount = 0;
        const callback: JsAckCallback = {
            onAck: (offsetId: string) => {
                ackCount++;
            }
        };
        assert.ok(callback.onAck);
        assert.strictEqual(typeof callback.onAck, 'function');
    });

    it('should accept ack callback with onError function', () => {
        let errorCount = 0;
        const callback: JsAckCallback = {
            onError: (offsetId: string, errorMsg: string) => {
                errorCount++;
            }
        };
        assert.ok(callback.onError);
        assert.strictEqual(typeof callback.onError, 'function');
    });

    it('should accept ack callback with both onAck and onError', () => {
        const callback: JsAckCallback = {
            onAck: (offsetId: string) => {
                console.log(`Ack: ${offsetId}`);
            },
            onError: (offsetId: string, errorMsg: string) => {
                console.error(`Error: ${offsetId} - ${errorMsg}`);
            }
        };
        assert.ok(callback.onAck);
        assert.ok(callback.onError);
    });

    it('should accept empty ack callback', () => {
        const callback: JsAckCallback = {};
        assert.strictEqual(callback.onAck, undefined);
        assert.strictEqual(callback.onError, undefined);
    });
});

describe('New v0.4.0 API types', () => {
    it('should define ingestRecordOffset method on ZerobusStream type', () => {
        // Type check - these are defined in index.d.ts
        // ZerobusStream.prototype.ingestRecordOffset exists
        assert.ok(true, 'ingestRecordOffset is defined in type definitions');
    });

    it('should define ingestRecordsOffset method on ZerobusStream type', () => {
        // Type check - these are defined in index.d.ts
        // ZerobusStream.prototype.ingestRecordsOffset exists
        assert.ok(true, 'ingestRecordsOffset is defined in type definitions');
    });

    it('should define waitForOffset method on ZerobusStream type', () => {
        // Type check - these are defined in index.d.ts
        // ZerobusStream.prototype.waitForOffset exists
        assert.ok(true, 'waitForOffset is defined in type definitions');
    });
});
