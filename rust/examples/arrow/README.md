# Arrow Flight Example

This directory contains an example demonstrating Arrow Flight-based data ingestion into Databricks Delta tables using the Zerobus Rust SDK.

The `arrow-flight` feature flag must be enabled.

## Table of Contents

- [Overview](#overview)
- [Running the Example](#running-the-example)
- [Code Highlights](#code-highlights)
- [IPC compression](#ipc-compression)
- [Adapting for Your Custom Table](#adapting-for-your-custom-table)

## Overview

Arrow Flight is a third record format option alongside JSON and Protocol Buffers: it sends Apache Arrow `RecordBatch` data directly to Zerobus using Arrow Flight's gRPC transport. It is the best fit when your workload is naturally columnar or batched — analytics pipelines, gateways aggregating short windows of rows, wide/numeric schemas where row-by-row serialization adds noticeable CPU overhead — or when your application already produces Arrow data via pyarrow, the [arrow-rs](https://github.com/apache/arrow-rs) crates, DataFusion, Polars, or similar libraries.

**Features:**
- Columnar Arrow data sent over the Arrow Flight protocol
- Logical batch offsets, cumulative durability acknowledgments, and automatic recovery

> **Feature flag.** The Arrow Flight API is behind the `arrow-flight` Cargo feature. The example's `Cargo.toml` enables it for you.

Send multiple rows per `RecordBatch`. Start with natural application-sized batches; sending one row per call works but negates most of the performance advantage of using Arrow. For sparse, one-row-at-a-time traffic, the JSON or Protocol Buffers examples in `examples/json/` and `examples/proto/` are usually a better fit. `ingest_batch` returns an `OffsetId` directly; there is no future-based variant for the Arrow Flight API.

## Running the Example

1. Configure credentials in `src/main.rs` (see [Prerequisites](../README.md#prerequisites))

2. Run the example:
   ```bash
   cargo run -p example_arrow
   ```

The example queues 10 `RecordBatch`es of 10,000 rows each, calls `flush()` once
to confirm all pending data, then closes the stream.

**Expected output:**
```
Flushed all in-flight batches
Stream closed successfully
```

## Code Highlights

**Building an Arrow Flight stream:**

```rust
use std::sync::Arc;
use databricks_zerobus_ingest_sdk::{ArrowSchema, DataType, Field, ZerobusSdk};

let schema = Arc::new(ArrowSchema::new(vec![
    Field::new("id", DataType::Int32, true),
    Field::new("customer_name", DataType::LargeUtf8, true),
    // ... other fields
]));

let mut stream = sdk
    .stream_builder()
    .table(TABLE_NAME)
    .oauth(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
    .arrow(schema.clone())
    .build_arrow()
    .await?;
```

**Queue many `RecordBatch`es, then flush once:**

```rust
use arrow_array::{Int32Array, LargeStringArray, RecordBatch};

const ROWS_PER_BATCH: usize = 10_000;

for _ in 0..10 {
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![0; ROWS_PER_BATCH])),
            Arc::new(LargeStringArray::from(vec!["Customer"; ROWS_PER_BATCH])),
            // ... other columns
        ],
    )?;
    stream.ingest_batch(batch).await?;
}

// Drain any in-flight batches before closing.
stream.flush().await?;
stream.close().await?;
```

**Batch semantics:**
- **One logical offset per input batch**: `ingest_batch` returns one SDK offset even when
  Flight splits a large batch into multiple wire messages
- **Cumulative durability**: `wait_for_offset` completes after every record in that logical
  batch is durable; after a partial failure, recovery/retrieval keeps only the unacknowledged suffix
- **Non-empty batches**: zero-row batches are rejected with `InvalidArgument` because
  they produce no Flight data message to acknowledge
- **Schema validation**: Each `RecordBatch` must exactly match the client schema
  configured on the stream; the server validates that schema against the target

## IPC compression

Arrow IPC payloads can be compressed on the wire. Enable compression only when network bandwidth limits throughput — it reduces bytes on the wire at the cost of CPU on the client.

The `CompressionType` enum lives in the `arrow-ipc` crate, which is already added as a dependency for this example. The example's `main.rs` enables `ZSTD` compression on the stream builder:

```rust
use arrow_ipc::CompressionType;

let stream = sdk
    .stream_builder()
    .table(TABLE_NAME)
    .oauth(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
    .arrow(schema)
    .ipc_compression(Some(CompressionType::ZSTD))  // or LZ4_FRAME
    .build_arrow()
    .await?;
```

- `LZ4_FRAME` — fast, low CPU overhead, modest compression ratio.
- `ZSTD` — higher compression ratio, more CPU per batch.

To disable compression, drop the `.ipc_compression(...)` call (default is `None`).

## Adapting for Your Custom Table

To ingest into your own table, change the Arrow schema and the array values to match its columns.

**1. Update the Arrow schema** (use the target table's canonical Arrow types):

```rust
let schema = Arc::new(ArrowSchema::new(vec![
    Field::new("your_field_1", DataType::LargeUtf8, true),
    Field::new("your_field_2", DataType::Int32, true),
    Field::new("your_field_3", DataType::Boolean, true),
]));
```

The runnable example's `orders_schema()` shows the canonical mappings for strings
and timestamps. If you already have Unity Catalog columns, prefer
`arrow_schema_from_uc_columns()` or `arrow_schema_from_uc_schema()` to construct
the schema. Every `RecordBatch` must exactly match the schema passed to `.arrow(...)`.

**2. Update the arrays in `RecordBatch::try_new`** to match the schema:

```rust
let batch = RecordBatch::try_new(
    schema.clone(),
    vec![
        Arc::new(LargeStringArray::from(vec!["a", "b", "c"])),
        Arc::new(Int32Array::from(vec![1, 2, 3])),
        Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
    ],
)?;
```

**3. Update table name and credentials** in the constants at the top of `main.rs`.

> **Tip.** When in doubt about the Arrow type for a given Delta column type, the SDK
> validates the schema when the stream is created. A mismatch fails fast with a
> descriptive error.
