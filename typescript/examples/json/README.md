# JSON Examples

This directory contains examples demonstrating JSON-based data ingestion into Databricks Delta tables using the Zerobus TypeScript SDK.

## Overview

JSON examples are recommended for getting started - they're simpler and don't require schema generation.

**Features:**
- No schema generation required
- Easy to understand and modify
- Great for quick prototyping

**Available examples:**
- **`single.ts`** - Ingest records one at a time using `ingestRecordOffset()` / `ingestRecord()`
- **`batch.ts`** - Ingest multiple records at once using `ingestRecordsOffset()` / `ingestRecords()`

## Two Ways to Pass Data

The SDK supports two approaches for passing JSON data:

| Approach | Type | Description |
|----------|------|-------------|
| **Auto-serializing** | Plain object | Pass JavaScript object; SDK calls `JSON.stringify()` |
| **Pre-serialized** | String | Pass pre-built JSON string directly |

**When to use each:**
- **Plain object** - Most convenient; SDK handles serialization
- **Pre-serialized string** - When you have JSON strings from external sources

## Single-Record Example

### Running the Example

1. Set environment variables (see [Prerequisites](../README.md#prerequisites))

2. Run the example:
   ```bash
   npm run example:json:single
   ```

**Expected output:**
```
JSON Single-Record Ingestion Example
============================================================
Stream created

=== Offset-based API (Recommended) ===
[Auto-serializing] Record sent with offset ID: 0
[Auto-serializing] Record acknowledged with offset ID: 0
[Pre-serialized] Record sent with offset ID: 1
[Pre-serialized] Record acknowledged with offset ID: 1

[High-throughput] Sending 10 records...
[High-throughput] All 10 records acknowledged (last offset: 11)

=== Future-based API (Deprecated) ===
[Auto-serializing] Record acknowledged with offset ID: 12
[Pre-serialized] Record acknowledged with offset ID: 13
Stream closed successfully
```

### Code Highlights

**Offset-based API (Recommended):**

```typescript
// Queue records, then wait once. Immediate waitForOffset after a single ingest
// is valid for low-volume strict confirmation, not the default bulk pattern.
const records = [
    { device_name: 'sensor-001', temp: 22, humidity: 65 },
    { device_name: 'sensor-002', temp: 24, humidity: 70 }
];
for (const record of records) {
    await stream.ingestRecordOffset(record);
}
await stream.flush();
```

**Future-based API (Deprecated):**

```typescript
// Blocks until acknowledged
const offset = await stream.ingestRecord(record);
```

**Key configuration for JSON:**
```typescript
const options: StreamConfigurationOptions = {
    recordType: RecordType.Json,  // Important!
    // ...
};
```

## Batch Example

### Running the Example

1. Set environment variables (see [Prerequisites](../README.md#prerequisites))

2. Run the example:
   ```bash
   npm run example:json:batch
   ```

**Expected output:**
```
JSON Batch Ingestion Example
============================================================
Stream created

=== Offset-based API (Recommended) ===
[Auto-serializing] Batch of 3 records sent with offset ID: 0
[Auto-serializing] Batch acknowledged with offset ID: 0
[Pre-serialized] Batch of 3 records sent with offset ID: 1
[Pre-serialized] Batch acknowledged with offset ID: 1

[Large batch] Sending batch of 100 records...
[Large batch] 100 records acknowledged with offset ID: 2
[Empty batch] Returns: null

=== Future-based API (Deprecated) ===
[Auto-serializing] Batch acknowledged with offset ID: 3
[Pre-serialized] Batch acknowledged with offset ID: 4
Stream closed successfully
```

### Code Highlights

**Offset-based API (Recommended):**

```typescript
// 1. Auto-serializing: array of objects
const objectBatch = [
    { device_name: 'sensor-001', temp: 22, humidity: 65 },
    { device_name: 'sensor-002', temp: 23, humidity: 67 },
    { device_name: 'sensor-003', temp: 24, humidity: 69 }
];
const objectBatchOffset = await stream.ingestRecordsOffset(objectBatch);
if (objectBatchOffset !== null) {
    await stream.waitForOffset(objectBatchOffset);
}

// 2. Pre-serialized: array of JSON strings
const stringBatch = [
    JSON.stringify({ device_name: 'sensor-004', temp: 25, humidity: 71 }),
    JSON.stringify({ device_name: 'sensor-005', temp: 26, humidity: 73 })
];
const stringBatchOffset = await stream.ingestRecordsOffset(stringBatch);
```

**Batch semantics:**
- **All-or-nothing**: The entire batch succeeds or fails as a unit
- **Single acknowledgment**: One offset ID for the whole batch
- **Empty batches**: Returns `null` (no-op)

## Adapting for Your Custom Table

JSON examples require no schema generation. Simply modify the object structure to match your table:

```typescript
interface YourData {
    your_field_1: string;
    your_field_2: number;
    your_field_3: boolean;
}

const record: YourData = {
    your_field_1: 'value',
    your_field_2: 123,
    your_field_3: true
};

await stream.ingestRecordOffset(record);
```
