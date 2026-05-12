# Arrow Flight Example

This directory contains an example demonstrating Arrow Flight-based data ingestion into Databricks Delta tables using the Zerobus Rust SDK.

> **Beta**: Arrow Flight ingestion is in Beta. The API is stabilising but may still change before reaching GA. The `arrow-flight` feature flag must be enabled.

## Table of Contents

- [Overview](#overview)
- [Running the Example](#running-the-example)
- [Code Highlights](#code-highlights)
- [Adapting for Your Custom Table](#adapting-for-your-custom-table)

## Overview

Arrow Flight provides a columnar, high-throughput path for ingesting Apache Arrow `RecordBatch` data. Compared to JSON or Protocol Buffers, Arrow keeps data in its native columnar layout end-to-end, which is efficient when your data already lives in Arrow form (e.g., from DataFusion, Polars, or a Parquet reader).

**Features:**
- Columnar Arrow data sent over the Arrow Flight protocol
- Per-batch acknowledgments with the same recovery semantics as the standard streams

> **Feature flag.** The Arrow Flight API is behind the `arrow-flight` Cargo feature. The example's `Cargo.toml` enables it for you.

Arrow Flight is inherently batch-oriented — a `RecordBatch` carries one or more rows. To ingest one row at a time, just pass a single-row `RecordBatch`. `ingest_batch` returns an `OffsetId` directly; there is no future-based variant for the Arrow Flight API.

## Running the Example

1. Configure credentials in `src/main.rs` (see [Prerequisites](../README.md#prerequisites))

2. Run the example:
   ```bash
   cargo run -p example_arrow
   ```

The example ingests 100 `RecordBatch`es (3 rows each), waits for an acknowledgment every 10th batch, then calls `flush()` and `close()` at the end.

**Expected output:**
```
Acknowledged through batch 10 (offset ID 9)
Acknowledged through batch 20 (offset ID 19)
Acknowledged through batch 30 (offset ID 29)
...
Acknowledged through batch 100 (offset ID 99)
Flushed all in-flight batches
Stream closed successfully
```

## Code Highlights

**Building an Arrow Flight stream:**

```rust
use std::sync::Arc;
use databricks_zerobus_ingest_sdk::{ArrowSchema, DataType, Field, ZerobusSdk};

let schema = Arc::new(ArrowSchema::new(vec![
    Field::new("id", DataType::Int32, false),
    Field::new("customer_name", DataType::Utf8, false),
    // ... other fields
]));

let stream = sdk
    .stream_builder()
    .table(TABLE_NAME)
    .oauth(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
    .arrow(schema.clone())
    .build_arrow()
    .await?;
```

**Ingest many `RecordBatch`es and wait periodically:**

```rust
use arrow_array::{Int32Array, RecordBatch, StringArray};

for i in 0..100 {
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![/* ... */])),
            Arc::new(StringArray::from(vec![/* ... */])),
            // ... other columns
        ],
    )?;
    let offset = stream.ingest_batch(batch).await?;

    // Apply backpressure every 10 batches by waiting for an ack.
    if (i + 1) % 10 == 0 {
        stream.wait_for_offset(offset).await?;
    }
}

// Drain any in-flight batches before closing.
stream.flush().await?;
stream.close().await?;
```

**Batch semantics:**
- **All-or-nothing per `RecordBatch`**: A batch is acknowledged as a unit
- **Single acknowledgment**: One offset ID for the whole `RecordBatch`
- **Schema validation**: The `RecordBatch` schema must exactly match the schema configured on the stream

## Adapting for Your Custom Table

To ingest into your own table, change the Arrow schema and the array values to match its columns.

**1. Update the Arrow schema** (must match the Delta table column types exactly):

```rust
let schema = Arc::new(ArrowSchema::new(vec![
    Field::new("your_field_1", DataType::Utf8, false),
    Field::new("your_field_2", DataType::Int32, false),
    Field::new("your_field_3", DataType::Boolean, true),
]));
```

**2. Update the arrays in `RecordBatch::try_new`** to match the schema:

```rust
let batch = RecordBatch::try_new(
    schema.clone(),
    vec![
        Arc::new(StringArray::from(vec!["a", "b", "c"])),
        Arc::new(Int32Array::from(vec![1, 2, 3])),
        Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
    ],
)?;
```

**3. Update table name and credentials** in the constants at the top of `main.rs`.

> **Tip.** When in doubt about the Arrow type for a given Delta column type, the SDK validates the schema on the first batch — a mismatch fails fast with a descriptive error.
