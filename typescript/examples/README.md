# Zerobus TypeScript SDK Examples

This directory contains examples demonstrating how to use the Zerobus TypeScript SDK to ingest data into Databricks Delta tables.

## Table of Contents

- [Overview](#overview)
- [JSON Examples](json/README.md)
- [Protocol Buffers Examples](proto/README.md)
- [Arrow Flight Examples](arrow/README.md) (Beta)
- [Prerequisites](#prerequisites)
- [Common Code Patterns](#common-code-patterns)
- [API Styles](#api-styles)
- [Single-Record vs Batch Ingestion](#single-record-vs-batch-ingestion)
- [Choosing a Format](#choosing-a-format)

## Overview

The SDK supports three serialization formats and two ingestion methods:

**Serialization Formats:**
- **[JSON](json/README.md)** - Simpler, no schema generation required. Great for getting started.
- **[Protocol Buffers](proto/README.md)** - Type-safe with compile-time validation. Better for production.
- **[Arrow Flight](arrow/README.md)** - High-performance columnar format for analytics. **(Beta)** API is stabilising but may still change before reaching GA.

**Ingestion Methods:**
- **Single-record** (`ingestRecordOffset`) - Ingest records one at a time
- **Batch** (`ingestRecordsOffset`) - Ingest multiple records at once with all-or-nothing semantics

**Available Examples:**

| Example | Format | Method | Script |
|---------|--------|--------|--------|
| [JSON Single](json/README.md#single-record-example) | JSON | Single-record | `npm run example:json:single` |
| [JSON Batch](json/README.md#batch-example) | JSON | Batch | `npm run example:json:batch` |
| [Proto Single](proto/README.md#single-record-example) | Protocol Buffers | Single-record | `npm run example:proto:single` |
| [Proto Batch](proto/README.md#batch-example) | Protocol Buffers | Batch | `npm run example:proto:batch` |
| [Arrow](arrow/README.md) | Arrow Flight | Multi-row batches in a loop | `npm run example:arrow` |

## Prerequisites

### 1. Create a Databricks Table

Create a table in your Databricks workspace:

```sql
CREATE TABLE catalog.schema.air_quality (
  device_name STRING,
  temp INT,
  humidity BIGINT
);
```

### 2. Set Up OAuth Service Principal

1. In your Databricks workspace, go to **Settings** > **Identity and Access**
2. Create a service principal or use an existing one
3. Generate OAuth credentials (client ID and secret)
4. Grant the service principal these permissions on your table:
   - `SELECT` - Read table schema
   - `MODIFY` - Write data to the table
   - `USE CATALOG` and `USE SCHEMA` - Access catalog and schema

### 3. Configure Environment Variables

Set the following environment variables:

```bash
export ZEROBUS_SERVER_ENDPOINT="https://workspace-id.zerobus.region.cloud.databricks.com"
export DATABRICKS_WORKSPACE_URL="https://your-workspace.cloud.databricks.com"
export ZEROBUS_TABLE_NAME="catalog.schema.air_quality"
export DATABRICKS_CLIENT_ID="your-client-id"
export DATABRICKS_CLIENT_SECRET="your-client-secret"
```

## Common Code Patterns

All examples follow the same general flow:

### 1. Initialize SDK

```typescript
const sdk = new ZerobusSdk(SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL);
```

### 2. Configure Table Properties

**JSON:**
```typescript
const tableProperties: TableProperties = {
    tableName: TABLE_NAME
    // No descriptor needed for JSON
};
```

**Protocol Buffers:**
```typescript
const descriptorBase64 = loadDescriptorProto({ ... });
const tableProperties: TableProperties = {
    tableName: TABLE_NAME,
    descriptorProto: descriptorBase64
};
```

### 3. Configure Stream Options

```typescript
const options: StreamConfigurationOptions = {
    recordType: RecordType.Json,  // or RecordType.Proto
    maxInflightRequests: 100,
    recovery: true
};
```

### 4. Create Stream

```typescript
const stream = await sdk.createStream(
    tableProperties,
    CLIENT_ID,
    CLIENT_SECRET,
    options
);
```

### 5. Ingest and Acknowledge

Queue records, then flush once. Immediate `waitForOffset` after a single ingest is a low-volume confirmation pattern, not the default.

```typescript
for (const record of records) {
    await stream.ingestRecordOffset(record);
}
await stream.flush();
```

### 6. Close Stream

```typescript
await stream.close();
```

## API Styles

The SDK provides two API styles for ingestion:

| Style | Method | Returns | Promise resolves |
|-------|--------|---------|------------------|
| **Offset-based** (Recommended) | `ingestRecordOffset()` | `Promise<bigint>` | Immediately after queuing (before server ack) |
| **Future-based** (Deprecated) | `ingestRecord()` | `Promise<bigint>` | After server acknowledgment |

Both methods return `Promise<bigint>`, but the key difference is **when** the promise resolves:

**Offset-based (Recommended):**
```typescript
// Promise resolves immediately with offset (doesn't wait for server ack)
for (const record of records) {
    await stream.ingestRecordOffset(record);
}
await stream.flush();
```

**Future-based (Deprecated):**
```typescript
// Promise blocks until server acknowledges - slower for high-throughput
const offset = await stream.ingestRecord(data);
```

## Single-Record vs Batch Ingestion

| Aspect | Single-Record | Batch |
|--------|---------------|-------|
| **Method** | `ingestRecordOffset()` | `ingestRecordsOffset()` |
| **Use case** | Records arrive one at a time | Multiple records ready at once |
| **Semantics** | Each record independent | All-or-nothing (atomic) |
| **Acknowledgment** | Per record | Per batch |
| **Throughput** | Lower | Higher |

**Single-record:**
```typescript
for (const record of records) {
    const offset = await stream.ingestRecordOffset(record);
}
await stream.flush();
```

**Batch:**
```typescript
const offset = await stream.ingestRecordsOffset(records);
if (offset !== null) {
    await stream.waitForOffset(offset);
}
```

## Choosing a Format

| Feature | JSON | Protocol Buffers | Arrow Flight |
|---------|------|------------------|--------------|
| **Setup** | Simple - no schema files | Schema files required | Schema in code |
| **Type Safety** | Runtime validation | Compile-time validation | Runtime validation |
| **Performance** | Text-based | Efficient binary encoding | High-performance columnar |
| **Flexibility** | Easy to modify | Schema changes require regeneration | Dynamic schema |
| **Best For** | Prototyping, simple use cases | Production, high-throughput | Analytics, data science |
| **Status** | Stable | Stable | Beta |

**Recommendation:** Start with JSON for quick prototyping, then migrate to Protocol Buffers for production row-oriented workloads. Arrow Flight (Beta) is suitable for analytics and columnar workloads; the API is stabilising but may still change before reaching GA.
