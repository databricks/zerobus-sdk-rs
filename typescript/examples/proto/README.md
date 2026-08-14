# Protocol Buffers Examples

This directory contains examples demonstrating Protocol Buffers-based data ingestion into Databricks Delta tables using the Zerobus TypeScript SDK.

## Overview

Protocol Buffers provide type-safe, efficient binary encoding for production workloads.

**Features:**
- Compile-time type validation
- Efficient binary encoding
- Strong typing with generated TypeScript interfaces

**Available examples:**
- **`single.ts`** - Ingest records one at a time using `ingestRecordOffset()` / `ingestRecord()`
- **`batch.ts`** - Ingest multiple records at once using `ingestRecordsOffset()` / `ingestRecords()`

## Prerequisites

### Generate Proto Files

Before running the examples, generate the protobuf files:

```bash
npm run build:proto
```

This creates:
- `examples/generated/air_quality.js` - Generated protobuf code
- `examples/generated/air_quality.d.ts` - TypeScript definitions
- `schemas/air_quality_descriptor.pb` - Binary descriptor for the SDK

## Two Ways to Pass Data

The SDK supports two approaches for passing Protocol Buffers data:

| Approach | Type | Description |
|----------|------|-------------|
| **Auto-encoding** | Message object | Pass protobuf message; SDK calls `.encode().finish()` |
| **Pre-encoded** | Buffer | Pass pre-serialized bytes directly |

**When to use each:**
- **Message object** - Most convenient; SDK handles encoding
- **Pre-encoded Buffer** - When you have bytes from external sources or need control over encoding

## Single-Record Example

### Running the Example

1. Generate proto files:
   ```bash
   npm run build:proto
   ```

2. Set environment variables (see [Prerequisites](../README.md#prerequisites))

3. Run the example:
   ```bash
   npm run example:proto:single
   ```

**Expected output:**
```
Protocol Buffers Single-Record Ingestion Example
============================================================
Protobuf descriptor loaded
Stream created

=== Offset-based API (Recommended) ===
[Auto-encoding] Record sent with offset ID: 0
[Auto-encoding] Record acknowledged with offset ID: 0
[Pre-encoded] Record sent with offset ID: 1
[Pre-encoded] Record acknowledged with offset ID: 1

[High-throughput] Sending 10 records...
[High-throughput] All 10 records acknowledged (last offset: 11)

=== Future-based API (Deprecated) ===
[Auto-encoding] Record acknowledged with offset ID: 12
[Pre-encoded] Record acknowledged with offset ID: 13
Stream closed successfully
```

### Code Highlights

**Loading the descriptor:**

```typescript
import { loadDescriptorProto } from '../../utils/descriptor';
import * as airQuality from '../generated/air_quality';

const descriptorBase64 = loadDescriptorProto({
    descriptorPath: 'schemas/air_quality_descriptor.pb',
    protoFileName: 'air_quality.proto',
    messageName: 'AirQuality'
});

const tableProperties: TableProperties = {
    tableName: TABLE_NAME,
    descriptorProto: descriptorBase64  // Required for Protocol Buffers
};
```

**Offset-based API (Recommended):**

```typescript
const AirQuality = airQuality.examples.AirQuality;

const records = [
    AirQuality.create({ deviceName: 'sensor-001', temp: 22, humidity: 65 }),
    AirQuality.create({ deviceName: 'sensor-002', temp: 24, humidity: 70 })
];
for (const record of records) {
    await stream.ingestRecordOffset(record);
}
await stream.flush();
```

## Batch Example

### Running the Example

1. Generate proto files:
   ```bash
   npm run build:proto
   ```

2. Set environment variables (see [Prerequisites](../README.md#prerequisites))

3. Run the example:
   ```bash
   npm run example:proto:batch
   ```

### Code Highlights

**Offset-based API (Recommended):**

```typescript
// 1. Auto-encoding: array of messages
const messageBatch = [
    AirQuality.create({ deviceName: 'sensor-001', temp: 22, humidity: 65 }),
    AirQuality.create({ deviceName: 'sensor-002', temp: 23, humidity: 67 }),
    AirQuality.create({ deviceName: 'sensor-003', temp: 24, humidity: 69 })
];
const messageBatchOffset = await stream.ingestRecordsOffset(messageBatch);

// 2. Pre-encoded: array of Buffers
const bufferBatch = [
    AirQuality.create({ deviceName: 'sensor-004', temp: 25, humidity: 71 }),
    AirQuality.create({ deviceName: 'sensor-005', temp: 26, humidity: 73 }),
    AirQuality.create({ deviceName: 'sensor-006', temp: 27, humidity: 75 })
].map(record => Buffer.from(AirQuality.encode(record).finish()));
const bufferBatchOffset = await stream.ingestRecordsOffset(bufferBatch);

await stream.flush();
```

## Adapting for Your Custom Table

### 1. Create a Proto Schema

Create `schemas/your_table.proto`:

```protobuf
syntax = "proto2";

package examples;

message YourTable {
    optional string field1 = 1;
    optional int32 field2 = 2;
    optional int64 field3 = 3;
}
```

### 2. Generate Files

Update `package.json` build script or run manually:

```bash
mkdir -p examples/generated
pbjs -t static-module -w commonjs -o examples/generated/your_table.js schemas/your_table.proto
pbts -o examples/generated/your_table.d.ts examples/generated/your_table.js
protoc --descriptor_set_out=schemas/your_table_descriptor.pb --include_imports schemas/your_table.proto
```

### 3. Use in Your Code

```typescript
import * as yourTable from '../generated/your_table';
import { loadDescriptorProto } from '../../utils/descriptor';

const descriptorBase64 = loadDescriptorProto({
    descriptorPath: 'schemas/your_table_descriptor.pb',
    protoFileName: 'your_table.proto',
    messageName: 'YourTable'
});

const YourTable = yourTable.examples.YourTable;
const record = YourTable.create({
    field1: 'value',
    field2: 123,
    field3: 456789
});

await stream.ingestRecordOffset(record);
```
