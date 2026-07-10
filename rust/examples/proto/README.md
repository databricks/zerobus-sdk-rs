# Protocol Buffers Examples

This directory contains examples demonstrating Protocol Buffers-based data ingestion into Databricks Delta tables using the Zerobus Rust SDK.

## Table of Contents

- [Overview](#overview)
- [Compiled vs. Dynamic](#compiled-vs-dynamic)
- [Three Ways to Pass Data](#three-ways-to-pass-data)
- [Compiled: Single-Record Example](#compiled-single-record-example)
  - [Running the Example](#running-the-example)
  - [Code Highlights](#code-highlights)
- [Compiled: Batch Example](#compiled-batch-example)
  - [Running the Example](#running-the-example-1)
  - [Code Highlights](#code-highlights-1)
- [Dynamic Schema Example](#dynamic-schema-example)
  - [Running the Example](#running-the-example-2)
  - [Code Highlights](#code-highlights-2)
  - [Dynamic Batch](#dynamic-batch)
- [Adapting for Your Custom Table](#adapting-for-your-custom-table)
  - [Generate Schema Files](#generate-schema-files)
  - [Update main.rs](#update-mainrs)

## Overview

Protocol Buffers examples provide type safety and better performance.

**Features:**
- Type-safe record creation with compile-time validation
- Efficient binary encoding
- Better for production use cases

## Compiled vs. Dynamic

The examples are grouped by how the protobuf schema is obtained:

- **`compiled/`** — the schema is known ahead of time and compiled into Rust structs.
  **No schema generation needed to run these** — the files under `compiled/output/`
  are already included.
  - **`compiled/single.rs`** - Ingest records one at a time using `ingest_record_offset()` / `ingest_record()`
  - **`compiled/batch.rs`** - Ingest multiple records at once using `ingest_records_offset()` / `ingest_records()`
- **`dynamic/`** — the schema is known only at runtime (no compiled `.proto`), and records
  are built field-by-field with `DynamicRecord`.
  - **`dynamic/single.rs`** - Build the descriptor in code and ingest dynamic records one at a time
  - **`dynamic/batch.rs`** - Ingest multiple dynamic records at once using `ingest_records_offset()`

## Three Ways to Pass Data

The SDK supports three approaches for passing Protocol Buffers data:

| Approach | Type | Description |
|----------|------|-------------|
| **Auto-encoding** | `ProtoMessage(message)` | Pass protobuf messages directly; SDK handles encoding |
| **Pre-encoded** | `ProtoBytes(bytes)` | Pass pre-encoded bytes with explicit wrapper |
| **Backward-compatible** | `Vec<u8>` | Pass raw bytes directly (no wrapper needed) |

**When to use each:**
- **`ProtoMessage`** - When you have protobuf message structs and want the SDK to handle encoding
- **`ProtoBytes`** - When you have pre-encoded bytes and want explicit type clarity
- **Raw `Vec<u8>`** - For backward compatibility with existing code; works the same as `ProtoBytes`

## Compiled: Single-Record Example

### Running the Example

1. Configure credentials in `compiled/single.rs` (see [Prerequisites](../README.md#prerequisites))

2. Run the example:
   ```bash
   cargo run -p rust-examples-proto --example proto_compiled_single
   ```

**Expected output:**
```
[Auto-encoding] Record sent with offset ID: 0
[Auto-encoding] Record acknowledged with offset ID: 0
[Pre-encoded] Record sent with offset ID: 1
[Pre-encoded] Record acknowledged with offset ID: 1
[Backward-compatible] Record sent with offset ID: 2
[Backward-compatible] Record acknowledged with offset ID: 2
Stream closed successfully
```

### Code Highlights

The example demonstrates all three data-passing approaches. Each `ingest_record_offset()`
returns as soon as the record is queued; we ingest all of them and `flush()` once at the end
rather than waiting per record (waiting per record forces a server round-trip each time):

```rust
use databricks_zerobus_ingest_sdk::{ProtoMessage, ProtoBytes};
use prost::Message;

let order = TableOrders {
    id: Some(1),
    customer_name: Some("Alice".to_string()),
    // ... other fields
};

// 1. Auto-encoding: pass message directly
let _offset = stream.ingest_record_offset(ProtoMessage(order.clone())).await?;

// 2. Pre-encoded: pass bytes with wrapper
let bytes = order.encode_to_vec();
let _offset = stream.ingest_record_offset(ProtoBytes(bytes)).await?;

// 3. Backward-compatible: pass raw bytes (no wrapper)
let bytes = order.encode_to_vec();
let _offset = stream.ingest_record_offset(bytes).await?;

// Confirm all queued records at once.
stream.flush().await?;
```

**Building a Protocol Buffers stream:**
```rust
// Load descriptor from generated files
let descriptor_proto = load_descriptor_proto(
    "output/orders.descriptor",
    "orders.proto",
    "table_Orders"
);

let stream = sdk
    .stream_builder()
    .table(TABLE_NAME)
    .oauth(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
    .compiled_proto(descriptor_proto)
    .build()
    .await?;
```

## Compiled: Batch Example

### Running the Example

1. Configure credentials in `compiled/batch.rs` (see [Prerequisites](../README.md#prerequisites))

2. Run the example:
   ```bash
   cargo run -p rust-examples-proto --example proto_compiled_batch
   ```

**Expected output:**
```
[Auto-encoding] Batch of 3 records sent with offset ID: 0
[Auto-encoding] Batch acknowledged with offset ID: 0
[Pre-encoded] Batch of 3 records sent with offset ID: 1
[Pre-encoded] Batch acknowledged with offset ID: 1
[Backward-compatible] Batch of 3 records sent with offset ID: 2
[Backward-compatible] Batch acknowledged with offset ID: 2
Stream closed successfully
```

### Code Highlights

```rust
use databricks_zerobus_ingest_sdk::{ProtoMessage, ProtoBytes};
use prost::Message;

// 1. Auto-encoding: Vec of wrapped messages
let batch: Vec<ProtoMessage<TableOrders>> = vec![
    ProtoMessage(TableOrders { id: Some(1), /* ... */ }),
    ProtoMessage(TableOrders { id: Some(2), /* ... */ }),
    ProtoMessage(TableOrders { id: Some(3), /* ... */ }),
];
if let Some(offset) = stream.ingest_records_offset(batch).await? {
    stream.wait_for_offset(offset).await?;
}

// 2. Pre-encoded: Vec of wrapped bytes
let batch: Vec<ProtoBytes> = vec![
    ProtoBytes(TableOrders { id: Some(4), /* ... */ }.encode_to_vec()),
    ProtoBytes(TableOrders { id: Some(5), /* ... */ }.encode_to_vec()),
    ProtoBytes(TableOrders { id: Some(6), /* ... */ }.encode_to_vec()),
];
if let Some(offset) = stream.ingest_records_offset(batch).await? {
    stream.wait_for_offset(offset).await?;
}

// 3. Backward-compatible: Vec of raw bytes
let batch: Vec<Vec<u8>> = vec![
    TableOrders { id: Some(7), /* ... */ }.encode_to_vec(),
    TableOrders { id: Some(8), /* ... */ }.encode_to_vec(),
    TableOrders { id: Some(9), /* ... */ }.encode_to_vec(),
];
if let Some(offset) = stream.ingest_records_offset(batch).await? {
    stream.wait_for_offset(offset).await?;
}
```

**Batch semantics:**
- **All-or-nothing**: The entire batch succeeds or fails as a unit
- **Single acknowledgment**: One offset ID for the whole batch
- **Empty batches**: Returns `None` (no-op)

## Dynamic Schema Example

`dynamic/single.rs` covers the case where the table's schema is known only at runtime
and there is no compiled `prost::Message` type. It builds a `DescriptorProto` in code
with `schema::descriptor_from_uc_columns` (you could equally fetch one from Unity
Catalog), selects it with `.dynamic_proto(...)`, and fills each record
field-by-field with `DynamicRecord`.

### Running the Example

1. Configure credentials in `dynamic/single.rs` (see [Prerequisites](../README.md#prerequisites))

2. Run the example:
   ```bash
   cargo run -p rust-examples-proto --example proto_dynamic_single
   ```

### Code Highlights

```rust
use databricks_zerobus_ingest_sdk::message_descriptor;
use databricks_zerobus_ingest_sdk::schema::{descriptor_from_uc_columns, UcColumn};

// Build the descriptor at runtime — no `.proto` file, no generated structs.
// A column's protobuf field number is its `position + 1`.
let columns = vec![
    col("id", "BIGINT", 0),
    col("customer_name", "STRING", 1),
    col("quantity", "INT", 2),
    col("price", "DOUBLE", 3),
];
let descriptor_proto = descriptor_from_uc_columns(&columns, "table_Orders")?;
let descriptor = message_descriptor(&descriptor_proto)?;

let mut stream = sdk
    .stream_builder()
    .table(TABLE_NAME)
    .oauth(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
    .dynamic_proto(descriptor)
    .build()
    .await?;

// Fill records field-by-field. The value passed to `set()` must match the
// field's proto type (a BIGINT column takes an i64, an INT column an i32).
// `encode()` checks proto2 required fields before producing the bytes.
for i in 0..1_000i64 {
    let mut record = stream.new_record()?; // bound to the stream's schema
    record
        .set("id", i)?
        .set("customer_name", "Alice Smith")?
        .set("quantity", 2i32)?
        .set("price", 25.99f64)?;
    stream.ingest_record_offset(ProtoBytes(record.encode()?)).await?; // queue only — do NOT wait here
}
stream.flush().await?; // wait once for all pending acks
```

### Dynamic Batch

`dynamic/batch.rs` ingests many dynamic records in a single all-or-nothing call.
Each record is encoded up front (so `encode()`'s required-field check runs before
anything is sent), then the bytes are passed to `ingest_records_offset()`:

```bash
cargo run -p rust-examples-proto --example proto_dynamic_batch
```

```rust
// Build and encode each record; collecting into a ZerobusResult surfaces the
// first record missing a required field as an error.
let batch: Vec<ProtoBytes> = orders
    .iter()
    .map(|order| {
        let mut record = stream.new_record()?;
        record.set("id", order.id)?.set("customer_name", order.name)?;
        Ok(ProtoBytes(record.encode()?))
    })
    .collect::<ZerobusResult<_>>()?;

// The whole batch is queued in one call; a single offset covers it.
if let Some(offset) = stream.ingest_records_offset(batch).await? {
    println!("Batch queued with offset ID: {offset}");
}
stream.flush().await?;
```

## Adapting for Your Custom Table

To use your own table, you need to generate schema files and update the example code.

### Generate Schema Files

Run this from the repository root directory:

```bash
cd tools/generate_files

cargo run -- \
  --uc-endpoint "https://<your-workspace>.cloud.databricks.com" \
  --client-id "<your-client-id>" \
  --client-secret "<your-client-secret>" \
  --table "<catalog.schema.your_table>" \
  --output-dir "../../examples/proto/compiled/output"
```

Both `compiled/single.rs` and `compiled/batch.rs` share the same `compiled/output/` directory, so the generated schema files only need to be produced once. (The dynamic example needs no generated files.)

This generates:
- `output/<your_table>.proto` - Protocol Buffer schema definition
- `output/<your_table>.rs` - Rust structs with serialization code
- `output/<your_table>.descriptor` - Binary descriptor for runtime validation

### Update main.rs

**1. Update the module and use statements:**

Change `orders` to match your generated file name:

```rust
// Before:
pub mod orders {
    include!("output/orders.rs");
}
use crate::orders::TableOrders;

// After (for a table named `inventory`):
pub mod inventory {
    include!("output/inventory.rs");
}
use crate::inventory::TableInventory;
```

**2. Update the descriptor loading:**

```rust
// Before:
let descriptor_proto = load_descriptor_proto(
    "output/orders.descriptor",
    "orders.proto",
    "table_Orders"
);

// After:
let descriptor_proto = load_descriptor_proto(
    "output/inventory.descriptor",
    "inventory.proto",
    "table_Inventory"
);
```

**3. Update record creation:**

```rust
// Before:
ProtoMessage(TableOrders {
    id: Some(1),
    customer_name: Some("Alice".to_string()),
    // ...
})

// After:
ProtoMessage(TableInventory {
    item_id: Some(123),
    sku: Some("SKU-XYZ".to_string()),
    // ... your fields
})
```

**4. Update table name and credentials** in the constants at the top of `main.rs`.
