# JSON Examples

This directory contains examples demonstrating JSON-based data ingestion into Databricks Delta tables using the Zerobus Rust SDK.

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

## Overview

JSON examples are recommended for getting started - they're simpler and don't require schema generation.

**Features:**
- No schema generation required
- Easy to understand and modify
- Great for quick prototyping

**Available examples:**
- **`single.rs`** - Ingest records one at a time using `ingest_record_offset()`
- **`batch.rs`** - Ingest multiple records at once using `ingest_records_offset()`

## Three Ways to Pass Data

The SDK supports three approaches for passing JSON data:

| Approach | Type | Description |
|----------|------|-------------|
| **Auto-serializing** | `JsonValue(struct)` | Pass any serializable struct; SDK handles JSON conversion |
| **Pre-serialized** | `JsonString(string)` | Pass pre-built JSON strings with explicit wrapper |
| **Backward-compatible** | `String` | Pass raw strings directly (no wrapper needed) |

**When to use each:**
- **`JsonValue`** - When you have Rust structs and want compile-time type safety
- **`JsonString`** - When you have pre-built JSON strings and want explicit type clarity
- **Raw `String`** - For backward compatibility with existing code; works the same as `JsonString`

## Single-Record Example

### Running the Example

1. Configure credentials in `single.rs` (see [Prerequisites](../README.md#prerequisites))

2. Run the example:
   ```bash
   cargo run -p rust-examples-json --example json_single
   ```

**Expected output:**
```
[Auto-serializing] Record sent with offset ID: 0
[Auto-serializing] Record acknowledged with offset ID: 0
[Pre-serialized] Record sent with offset ID: 1
[Pre-serialized] Record acknowledged with offset ID: 1
[Backward-compatible] Record sent with offset ID: 2
[Backward-compatible] Record acknowledged with offset ID: 2
Stream closed successfully
```

### Code Highlights

The example demonstrates all three data-passing approaches. Each `ingest_record_offset()`
returns as soon as the record is queued; we ingest all of them and `flush()` once at the end
rather than waiting per record (waiting per record forces a server round-trip each time):

```rust
use databricks_zerobus_ingest_sdk::{JsonValue, JsonString};

// 1. Auto-serializing: pass struct directly
let order = Order { id: 1, customer_name: "Alice".to_string(), /* ... */ };
let _offset = stream.ingest_record_offset(JsonValue(order)).await?;

// 2. Pre-serialized: pass JSON string with wrapper
let json = r#"{
    "id": 2,
    "customer_name": "Bob"
}"#.to_string();
let _offset = stream.ingest_record_offset(JsonString(json)).await?;

// 3. Backward-compatible: pass raw string (no wrapper)
let json = r#"{
    "id": 3,
    "customer_name": "Carol"
}"#.to_string();
let _offset = stream.ingest_record_offset(json).await?;

// Confirm all queued records at once.
stream.flush().await?;
```

**Building a JSON stream:**
```rust
let stream = sdk
    .stream_builder()
    .table(TABLE_NAME)
    .oauth(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
    .json()
    .build()
    .await?;
```

## Batch Example

### Running the Example

1. Configure credentials in `batch.rs` (see [Prerequisites](../README.md#prerequisites))

2. Run the example:
   ```bash
   cargo run -p rust-examples-json --example json_batch
   ```

**Expected output:**
```
[Auto-serializing] Batch of 3 records sent with offset ID: 0
[Auto-serializing] Batch acknowledged with offset ID: 0
[Pre-serialized] Batch of 3 records sent with offset ID: 1
[Pre-serialized] Batch acknowledged with offset ID: 1
[Backward-compatible] Batch of 3 records sent with offset ID: 2
[Backward-compatible] Batch acknowledged with offset ID: 2
Stream closed successfully
```

### Code Highlights

```rust
use databricks_zerobus_ingest_sdk::{JsonValue, JsonString};

let batch1: Vec<JsonValue<Order>> = vec![
    JsonValue(Order { id: 1, /* ... */ }),
    JsonValue(Order { id: 2, /* ... */ }),
    JsonValue(Order { id: 3, /* ... */ }),
];
stream.ingest_records_offset(batch1).await?;

let batch2: Vec<JsonString> = vec![
    JsonString(r#"{ "id": 4 }"#.to_string()),
    JsonString(r#"{ "id": 5 }"#.to_string()),
    JsonString(r#"{ "id": 6 }"#.to_string()),
];
stream.ingest_records_offset(batch2).await?;

let batch3: Vec<String> = vec![
    r#"{ "id": 7 }"#.to_string(),
    r#"{ "id": 8 }"#.to_string(),
    r#"{ "id": 9 }"#.to_string(),
];
stream.ingest_records_offset(batch3).await?;

stream.flush().await?;
```

**Batch semantics:**
- **All-or-nothing**: The entire batch succeeds or fails as a unit
- **Single acknowledgment**: One offset ID for the whole batch
- **Empty batches**: Returns `None` (no-op)

## Adapting for Your Custom Table

JSON examples require no schema generation. Simply modify the struct and JSON to match your table:

**1. Update the struct (for `JsonValue` approach):**

```rust
#[derive(Serialize)]
struct YourData {
    your_field_1: String,
    your_field_2: i32,
    your_field_3: bool,
}
```

**2. Update the JSON strings (for `JsonString` or raw approach):**

```rust
let json = r#"{
    "your_field_1": "value",
    "your_field_2": 123,
    "your_field_3": true
}"#.to_string();
```

**3. Update table name and credentials** in the constants at the top of `main.rs`.
