# Zerobus Rust SDK Examples

This directory contains examples demonstrating how to use the Zerobus Rust SDK to ingest data into Databricks Delta tables.

## Table of Contents

- [Overview](#overview)
- [JSON Examples](json/README.md)
- [Protocol Buffers Examples](proto/README.md)
- [Arrow Flight Examples](arrow/README.md)
- [Prerequisites](#prerequisites)
  - [Create a Databricks Table](#1-create-a-databricks-table)
  - [Set Up OAuth Service Principal](#2-set-up-oauth-service-principal)
  - [Configure Credentials](#3-configure-credentials)
- [Common Code Patterns](#common-code-patterns)
- [API Styles](#api-styles)
- [Single-Record vs Batch Ingestion](#single-record-vs-batch-ingestion)
- [Choosing JSON vs Protocol Buffers](#choosing-json-vs-protocol-buffers)
- [Troubleshooting](#troubleshooting)

## Overview

The SDK supports three ingestion formats and two ingestion methods:

**Serialization Formats:**
- **[JSON](json/README.md)** - Simpler, no schema generation required. Great for getting started.
- **[Protocol Buffers](proto/README.md)** - Type-safe with compile-time validation. Better for production.
- **[Arrow Flight](arrow/README.md)** - Columnar Arrow `RecordBatch` ingestion over Arrow Flight. Behind the `arrow-flight` feature flag.

**Ingestion Methods:**
- **Single-record** (`ingest_record_offset`) - Ingest records one at a time (JSON / Protocol Buffers)
- **Batch** (`ingest_records_offset`) - Ingest multiple records at once with all-or-nothing semantics (JSON / Protocol Buffers)
- **Arrow batch** (`ingest_batch` / `ingest_ipc_batch`) - Ingest an Arrow `RecordBatch` (one or many rows) over Arrow Flight

**Available Examples:**

| Example | Format | Method | Run with |
|---------|--------|--------|----------|
| [JSON Single](json/README.md#single-record-example) | JSON | Single-record | `cargo run -p rust-examples-json --example json_single` |
| [JSON Batch](json/README.md#batch-example) | JSON | Batch | `cargo run -p rust-examples-json --example json_batch` |
| [Proto Compiled Single](proto/README.md#compiled-single-record-example) | Protocol Buffers | Single-record | `cargo run -p rust-examples-proto --example proto_compiled_single` |
| [Proto Compiled Batch](proto/README.md#compiled-batch-example) | Protocol Buffers | Batch | `cargo run -p rust-examples-proto --example proto_compiled_batch` |
| [Proto Multiplexed](proto/README.md#multiplexed-stream-example) | Protocol Buffers | Multiplexed | `cargo run -p rust-examples-proto --example proto_compiled_multiplexed` |
| [Proto Dynamic](proto/README.md#dynamic-schema-example) | Protocol Buffers | Single-record (runtime schema) | `cargo run -p rust-examples-proto --example proto_dynamic_single` |
| [Proto Dynamic Batch](proto/README.md#dynamic-batch) | Protocol Buffers | Batch (runtime schema) | `cargo run -p rust-examples-proto --example proto_dynamic_batch` |
| [Arrow](arrow/README.md) | Arrow Flight | `RecordBatch` | `cargo run -p example_arrow` |

## Prerequisites

### 1. Create a Databricks Table

Create a table in your Databricks workspace:

```sql
CREATE TABLE catalog.schema.orders (
  id INT,
  customer_name STRING,
  product_name STRING,
  quantity INT,
  price DOUBLE,
  status STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);
```

Replace `catalog.schema.orders` with your actual catalog, schema, and table name.

### 2. Set Up OAuth Service Principal

1. In your Databricks workspace, go to **Settings** > **Identity and Access**
2. Create a service principal or use an existing one
3. Generate OAuth credentials (client ID and secret)
4. Grant the service principal these permissions on your table:
   - `SELECT` - Read table schema
   - `MODIFY` - Write data to the table
   - `USE CATALOG` and `USE SCHEMA` - Access catalog and schema

### 3. Configure Credentials

Edit the source file (`batch.rs` or `single.rs`) for your chosen example and update these constants:

```rust
const DATABRICKS_WORKSPACE_URL: &str = "https://your-workspace.cloud.databricks.com";
const TABLE_NAME: &str = "catalog.schema.orders";
const DATABRICKS_CLIENT_ID: &str = "your-client-id";
const DATABRICKS_CLIENT_SECRET: &str = "your-client-secret";
const SERVER_ENDPOINT: &str = "https://workspace-id.zerobus.region.cloud.databricks.com";
```

**How to get these values:**
- **DATABRICKS_WORKSPACE_URL** - Your Databricks workspace URL (Unity Catalog endpoint)
- **TABLE_NAME** - Full table name in format `catalog.schema.table`
- **DATABRICKS_CLIENT_ID** - OAuth 2.0 client ID from your service principal
- **DATABRICKS_CLIENT_SECRET** - OAuth 2.0 client secret from your service principal
- **SERVER_ENDPOINT** - Zerobus ingestion endpoint (usually `https://<workspace-id>.zerobus.<region>.databricks.com`)

## Common Code Patterns

All examples follow the same general flow:

### 1. Initialize SDK

```rust
let sdk = ZerobusSdk::builder()
    .endpoint(SERVER_ENDPOINT)
    .unity_catalog_url(DATABRICKS_WORKSPACE_URL)
    .build()?;
```

### 2. Create Stream

**JSON:**
```rust
let mut stream = sdk
    .stream_builder()
    .table(TABLE_NAME)
    .oauth(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
    .json()
    .max_inflight_requests(100)
    .build()
    .await?;
```

**Protocol Buffers:**
```rust
let descriptor_proto = load_descriptor_proto(
    "output/orders.descriptor",
    "orders.proto",
    "table_Orders"
);

let mut stream = sdk
    .stream_builder()
    .table(TABLE_NAME)
    .oauth(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
    .compiled_proto(descriptor_proto)
    .max_inflight_requests(100)
    .build()
    .await?;
```

**Arrow Flight:**
```rust
use std::sync::Arc;
use databricks_zerobus_ingest_sdk::{ArrowSchema, DataType, Field};

let schema = Arc::new(ArrowSchema::new(vec![
    // Delta columns are nullable unless declared NOT NULL.
    Field::new("id", DataType::Int32, true),
    // ... other fields matching your table
]));

let mut stream = sdk
    .stream_builder()
    .table(TABLE_NAME)
    .oauth(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
    .arrow(schema)
    .max_inflight_batches(100)
    .build_arrow()
    .await?;
```

### 3. Ingest and Acknowledge

**JSON / Protocol Buffers:**

```rust
for record in records {
    // Returns once queued — do NOT wait on this offset inside the loop.
    let _offset = stream.ingest_record_offset(record).await?;
}
stream.flush().await?; // Confirm all pending records at once.
```

**Arrow Flight:**

```rust
for batch in record_batches {
    // Returns once the logical batch is queued.
    stream.ingest_batch(batch).await?;
}
stream.flush().await?; // Confirm every queued batch at once.
```

### 4. Close Stream

```rust
stream.close().await?;
```

## Ingestion API

> ⚡ **Do not call `wait_for_offset()` after every record.** `ingest_record_offset` queues the
> record and returns immediately; the round-trip happens in the background. Waiting per record
> serializes the pipeline into one round-trip per record and collapses throughput. Ingest in a
> loop, then call `flush()` once (or wait on only the last offset).

```rust
for record in records {
    let _offset = stream.ingest_record_offset(record).await?;
}
// Confirm everything at once.
stream.flush().await?;
```

`ingest_record_offset` returns the assigned `OffsetId` immediately after the record is queued. To confirm durability, call `flush()` after a run of records, or `wait_for_offset(offset)` on a single offset only when you must confirm that specific record before continuing (low volume).

## Single-Record vs Batch Ingestion

| Aspect | Single-Record | Batch |
|--------|---------------|-------|
| **Method** | `ingest_record_offset()` | `ingest_records_offset()` |
| **Use case** | Records arrive one at a time | Multiple records ready at once |
| **Semantics** | Each record independent | All-or-nothing (atomic) |
| **Acknowledgment** | Per record | Per batch |
| **Throughput** | Lower | Higher |

**Single-record:**
```rust
for record in records {
    stream.ingest_record_offset(record).await?;
}
stream.flush().await?;
```

**Batch:**
```rust
if let Some(offset) = stream.ingest_records_offset(records).await? {
    stream.wait_for_offset(offset).await?;
}
```

## Choosing JSON vs Protocol Buffers

| Feature | JSON | Protocol Buffers |
|---------|------|------------------|
| **Setup** | Simple - no schema files | Schema files included (or generate for custom tables) |
| **Type Safety** | Runtime validation | Compile-time validation |
| **Performance** | Text-based | Efficient binary encoding |
| **Flexibility** | Easy to modify on-the-fly | Schema changes require regeneration |
| **Best For** | Prototyping, simple use cases | Production, high-throughput |

**Recommendation:** Start with JSON for quick prototyping, then migrate to Protocol Buffers for production where type safety and performance matter.

## Troubleshooting

### Error: "Failed to create a stream"

**Possible causes:**
- Invalid credentials (client ID or secret)
- Service principal lacks permissions on the table
- Incorrect workspace URL or endpoint
- Table doesn't exist

**Solution:** Verify your credentials and table permissions.

### Error: "Failed to read proto descriptor file" (Protocol Buffers only)

**Possible causes:**
- Schema files not generated
- Wrong file paths in `load_descriptor_proto()`

**Solution:** Run the schema generation tool and verify the `output/` directory contains the generated files.

### Error: "Invalid token"

**Possible causes:**
- OAuth credentials expired or invalid
- Incorrect Unity Catalog endpoint

**Solution:** Regenerate your service principal credentials and verify the endpoint URL.

### Error: JSON parsing errors (JSON example only)

**Possible causes:**
- JSON string doesn't match table schema
- Invalid JSON syntax
- Type mismatches (e.g., passing string instead of number)

**Solution:** Verify your JSON structure matches the Databricks table schema exactly.

## Next Steps

- Try ingesting larger batches of records
- Experiment with different `StreamConfigurationOptions`
- Add error handling and retry logic
- Implement monitoring and metrics
- Use the SDK in a production application

## Additional Resources

- [Main SDK Documentation](../README.md)
- [Schema Generation Tool](../tools/generate_files/README.md)
- [Databricks Unity Catalog Documentation](https://docs.databricks.com/unity-catalog/index.html)
