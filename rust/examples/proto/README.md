# Protocol Buffers Examples

This directory contains examples demonstrating Protocol Buffers-based data ingestion into Databricks Delta tables using the Zerobus Rust SDK.

## Table of Contents

- [Overview](#overview)
- [Three Ways to Pass Data](#three-ways-to-pass-data)
- [Single-Record Example](#single-record-example)
  - [Running the Example](#running-the-example)
  - [Code Highlights](#code-highlights)
- [Batch Example](#batch-example)
  - [Running the Example](#running-the-example-1)
  - [Code Highlights](#code-highlights-1)
- [Adapting for Your Custom Table](#adapting-for-your-custom-table)
  - [Generate Schema Files](#generate-schema-files)
  - [Update main.rs](#update-mainrs)

## Overview

Protocol Buffers examples provide type safety and better performance. **No schema generation needed to run these examples** - schema files are already included in the `output/` folders.

**Features:**
- Type-safe record creation with compile-time validation
- Efficient binary encoding
- Better for production use cases

**Available examples:**
- **`single.rs`** - Ingest records one at a time using `ingest_record_offset()` / `ingest_record()`
- **`batch.rs`** - Ingest multiple records at once using `ingest_records_offset()` / `ingest_records()`
- **`dynamic.rs`** - Ingest on the proto path with **no compiled `.proto`**: the descriptor is fetched from Unity Catalog (or built in code)

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

## Single-Record Example

### Running the Example

1. Configure credentials in `single.rs` (see [Prerequisites](../README.md#prerequisites))

2. Run the example:
   ```bash
   cargo run -p rust-examples-proto --example proto_single
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

The example demonstrates all three data-passing approaches:

```rust
use databricks_zerobus_ingest_sdk::{ProtoMessage, ProtoBytes};
use prost::Message;

let order = TableOrders {
    id: Some(1),
    customer_name: Some("Alice".to_string()),
    // ... other fields
};

// 1. Auto-encoding: pass message directly
let offset = stream.ingest_record_offset(ProtoMessage(order.clone())).await?;
stream.wait_for_offset(offset).await?;

// 2. Pre-encoded: pass bytes with wrapper
let bytes = order.encode_to_vec();
let offset = stream.ingest_record_offset(ProtoBytes(bytes)).await?;
stream.wait_for_offset(offset).await?;

// 3. Backward-compatible: pass raw bytes (no wrapper)
let bytes = order.encode_to_vec();
let offset = stream.ingest_record_offset(bytes).await?;
stream.wait_for_offset(offset).await?;
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

## Batch Example

### Running the Example

1. Configure credentials in `batch.rs` (see [Prerequisites](../README.md#prerequisites))

2. Run the example:
   ```bash
   cargo run -p rust-examples-proto --example proto_batch
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

## Dynamic Example (no compiled `.proto`)

`dynamic.rs` ingests on the efficient proto path without authoring a `.proto`
file or generating Rust types. The descriptor is obtained at runtime and records
are supplied as JSON, encoded to protobuf bytes client-side by the stream's
`encoder()`.

### Running the Example

1. Configure credentials in `dynamic.rs` (see [Prerequisites](../README.md#prerequisites))

2. Run the example:
   ```bash
   cargo run -p rust-examples-proto --example proto_dynamic
   ```

### Code Highlights

```rust
// Fetch the descriptor from Unity Catalog at stream creation — no .proto needed.
let mut stream = sdk
    .stream_builder()
    .table(TABLE_NAME)
    .oauth(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
    .proto_from_uc()
    .build()
    .await?;

// Build the encoder once, then encode JSON records against the descriptor.
let encoder = stream.encoder()?;
let offset = stream
    .ingest_record_offset(encoder.encode(r#"{"id": 1, "customer_name": "Alice"}"#)?)
    .await?;
stream.wait_for_offset(offset).await?;
```

When there is no Unity Catalog metadata, build the descriptor in code with
`schema::TableDescriptorBuilder` and pass it to `.compiled_proto(...)` instead;
the encoding step is identical. A few Databricks column types need shaping in the
JSON you supply (`DATE`/`TIMESTAMP` as integers, `BINARY` as base64, `DECIMAL` as
a string) — see the SDK's `dynamic` module docs.

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
  --output-dir "../../examples/proto/output"
```

Both `single.rs` and `batch.rs` share the same `output/` directory, so the generated schema files only need to be produced once.

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
