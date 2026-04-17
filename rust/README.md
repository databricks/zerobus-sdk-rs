# Zerobus Rust SDK

A high-performance Rust client for streaming data ingestion into Databricks Delta tables using the Zerobus service.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Repository Structure](#repository-structure)
- [How It Works](#how-it-works)
- [Usage Guide](#usage-guide)
  - [1. Generate Protocol Buffer Schema](#1-generate-protocol-buffer-schema)
  - [2. Initialize the SDK](#2-initialize-the-sdk)
  - [3. Configure Authentication](#3-configure-authentication)
  - [4. Create a Stream](#4-create-a-stream)
  - [5. Ingest Data](#5-ingest-data)
  - [6. Handle Acknowledgments](#6-handle-acknowledgments)
  - [7. Close the Stream](#7-close-the-stream)
- [Configuration Options](#configuration-options)
- [Error Handling](#error-handling)
- [Examples](#examples)
- [Best Practices](#best-practices)
- [API Reference](#api-reference)
- [Language Bindings](#language-bindings)
- [Building from Source](#building-from-source)
- [Community and Contributing](#community-and-contributing)
- [License](#license)

## Overview

The Zerobus Rust SDK provides a robust, async-first interface for ingesting large volumes of data into Databricks Delta tables. It abstracts the complexity of the Zerobus service and handles authentication, retries, stream recovery, and acknowledgment tracking automatically.

**What is Zerobus?** See the [project overview](https://github.com/databricks/zerobus-sdk/blob/main/README.md#what-is-zerobus) for details on the Zerobus service.

## Features

- **Async/Await Support** - Built on Tokio for efficient concurrent I/O operations
- **Automatic OAuth 2.0 Authentication** - Seamless token management with Unity Catalog
- **Built-in Recovery** - Automatic retry and reconnection for transient failures
- **High Throughput** - Configurable inflight record limits for optimal performance
- **Batch Ingestion** - Ingest multiple records at once with all-or-nothing semantics for maximum throughput
- **Flexible Serialization** - Support for both JSON (simple) and Protocol Buffers (type-safe) data formats
- **Type Safety** - Protocol Buffers ensure schema validation at compile time
- **Schema Generation** - CLI tool to generate protobuf schemas from Unity Catalog tables
- **Flexible Configuration** - Fine-tune timeouts, retries, and recovery behavior
- **Graceful Stream Management** - Proper flushing and acknowledgment tracking
- **Acknowledgment Callbacks** - Receive notifications when records are acknowledged or encounter errors

## Installation

Add the SDK to your `Cargo.toml`:

```bash
cargo add databricks-zerobus-ingest-sdk
cargo add prost prost-types
cargo add tokio --features macros,rt-multi-thread
```
**Why these dependencies?**
- **`databricks-zerobus-ingest-sdk`** - The SDK itself
- **`prost`** and **`prost-types`** - Required for encoding your data to Protocol Buffers and loading schema descriptors
- **`tokio`** - Async runtime required for running async functions (the SDK is fully async)

> **What's in the crates.io package?** The published crate contains only the core Zerobus ingestion SDK. Tools for schema generation (`tools/generate_files`) and working examples (`examples/`) are only available in the [GitHub repository](https://github.com/databricks/zerobus-sdk). You'll need to clone the repo to generate protobuf schemas from your Unity Catalog tables.

### For Local Development

Clone the repository and use a path dependency:

```bash
git clone https://github.com/databricks/zerobus-sdk.git
cd your_project
```

Then in your `Cargo.toml`:

```toml
[dependencies]
databricks-zerobus-ingest-sdk = { path = "../zerobus-sdk/rust/sdk" }
prost = "0.13.3"
prost-types = "0.13.3"
tokio = { version = "1.42.0", features = ["macros", "rt-multi-thread"] }
```

## Quick Start

The SDK supports two serialization formats and two ingestion methods:

**Serialization:**
- **JSON** (Recommended for getting started): Simpler approach using JSON strings, no schema generation required
- **Protocol Buffers** (Recommended for production): Type-safe approach with schema validation at compile time

**Ingestion Methods:**
- **Single-record** (`ingest_record_offset`): Ingest records one at a time with per-record acknowledgment
- **Batch** (`ingest_records_offset`): Ingest multiple records at once with all-or-nothing semantics for higher throughput

> **Note:** The older `ingest_record()` and `ingest_records()` methods are deprecated as of v0.4.0. Use the `_offset` variants instead.

See [`examples/README.md`](https://github.com/databricks/zerobus-sdk/blob/main/rust/examples/README.md) for detailed setup instructions and examples for all combinations.

## Repository Structure

```
zerobus_rust_sdk/
├── sdk/                                # Core SDK library
│   ├── src/
│   │   ├── lib.rs                      # Main SDK and stream implementation
│   │   ├── default_token_factory.rs    # OAuth 2.0 token handling
│   │   ├── errors.rs                   # Error types and retryable logic
│   │   ├── headers_provider.rs         # Trait for custom authentication headers
│   │   ├── builder/                    # Builder pattern for SDK initialization
│   │   ├── tls_config.rs              # TLS configuration strategies
│   │   ├── stream_configuration.rs     # Stream options
│   │   ├── landing_zone.rs             # Inflight record buffer
│   │   └── offset_generator.rs         # Logical offset tracking
│   ├── zerobus_service.proto           # gRPC protocol definition
│   ├── build.rs                        # Build script for protobuf compilation
│   └── Cargo.toml
│
├── ffi/                                # C FFI bindings for other languages
│   ├── src/
│   │   ├── lib.rs                      # FFI implementation
│   │   └── tests.rs                    # FFI unit tests
│   ├── zerobus.h                       # Generated C header
│   ├── cbindgen.toml                   # Header generation config
│   ├── build.rs                        # Build script for header generation
│   └── Cargo.toml
│
├── jni/                                # JNI bindings for Java SDK
│   ├── src/
│   │   └── lib.rs                      # JNI implementation
│   └── Cargo.toml
│
├── tools/
│   └── generate_files/                 # Schema generation CLI tool
│       ├── src/
│       │   ├── main.rs                 # CLI entry point
│       │   └── generate.rs             # Unity Catalog -> Proto conversion
│       ├── README.md                   # Tool documentation
│       └── Cargo.toml
│
├── examples/
│   ├── README.md                       # Examples documentation
│   ├── json/
│   │   ├── README.md                   # JSON examples documentation
│   │   ├── single/                     # JSON single-record example
│   │   │   ├── src/main.rs
│   │   │   └── Cargo.toml
│   │   └── batch/                      # JSON batch ingestion example
│   │       ├── src/main.rs
│   │       └── Cargo.toml
│   └── proto/
│       ├── README.md                   # Protocol Buffers examples documentation
│       ├── single/                     # Protocol Buffers single-record example
│       │   ├── src/main.rs
│       │   ├── output/                 # Generated schema files
│       │   └── Cargo.toml
│       └── batch/                      # Protocol Buffers batch ingestion example
│           ├── src/main.rs
│           ├── output/                 # Generated schema files
│           └── Cargo.toml
│
├── tests/                              # Integration tests crate
│   ├── src/
│   │   ├── mock_grpc.rs                # Mock Zerobus gRPC server
│   │   └── rust_tests.rs               # Test suite
│   ├── build.rs
│   └── Cargo.toml
│
├── Cargo.toml                          # Workspace configuration
└── README.md                           # This file
```

### Key Components

- **`sdk/`** - The main library crate containing all SDK functionality
- **`ffi/`** - C FFI bindings for building language wrappers (Go, C#, C++, etc.)
- **`jni/`** - JNI bindings for the Java SDK
- **`tools/`** - CLI tool for generating Protocol Buffer schemas from Unity Catalog tables
- **`examples/`** - Complete working examples demonstrating SDK usage
- **Workspace** - Root `Cargo.toml` defines a Cargo workspace for unified builds

## How It Works

### Architecture Overview

```
+-----------------+
|    Your App     |
+-----------------+
        | 1. create_stream()
        v
+-----------------+
|   ZerobusSdk    |
| - Manages TLS   |
| - Creates       |
|   channels      |
+-----------------+
        | 2. Opens bidirectional gRPC stream
        v
+--------------------------------------+
|            ZerobusStream             |
| +----------------------------------+ |
| |           Supervisor             | | Manages lifecycle, recovery
| +----------------------------------+ |
|                  |                   |
|      +-----------+-----------+       |
|      v                       v       |
| +----------+          +----------+   | 
| |  Sender  |          | Receiver |   | Parallel tasks
| |  Task    |          |  Task    |   |
| +----------+          +----------+   |
|      ^                       |       |
|      |                       v       |
| +----------------------------------+ |
| |          Landing Zone            | | Inflight buffer
| +----------------------------------+ |
+--------------------------------------+
            | 3. gRPC stream
            v
+-----------------------+
|      Databricks       |
|    Zerobus Service    |
+-----------------------+
```

### Data Flow

1. **Ingestion** - Your app calls `stream.ingest_record(data)` or `stream.ingest_records(batch)`
2. **Buffering** - Records are placed in the landing zone with logical offsets
3. **Sending** - Sender task sends records over gRPC with physical offsets
4. **Acknowledgment** - Receiver task gets server ack and resolves the future
5. **Recovery** - If connection fails, supervisor reconnects and resends unacked records

### Authentication Flow

The SDK uses OAuth 2.0 client credentials flow:

1. SDK constructs authorization request with Unity Catalog privileges
2. Sends request to `{uc_endpoint}/oidc/v1/token` with client credentials
3. Token includes scoped permissions for the specific table
4. Token is attached to gRPC metadata as Bearer token
5. Fresh tokens are fetched automatically on each connection

### Custom Authentication

For advanced use cases, you can implement the `HeadersProvider` trait to supply your own authentication headers. This is useful for integrating with a different OAuth provider, using a centralized token caching service, or implementing alternative authentication mechanisms.

> **Note:** The headers you provide must still conform to the authentication protocol expected by the Zerobus service. The default implementation, `OAuthHeadersProvider`, serves as the reference for the required headers (`authorization` and `x-databricks-zerobus-table-name`). This feature provides flexibility in *how* you source your credentials, not in changing the authentication protocol itself.

**Example:**

```rust
use databricks_zerobus_ingest_sdk::*;
use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;

struct MyCustomAuthProvider;

#[async_trait]
impl HeadersProvider for MyCustomAuthProvider {
    async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
        let mut headers = HashMap::new();
        // Custom logic to fetch and cache a token would go here.
        headers.insert("authorization", "Bearer <your-token>".to_string());
        headers.insert("x-databricks-zerobus-table-name", "<your-table-name>".to_string());
        Ok(headers)
    }
}

async fn example(sdk: ZerobusSdk, table_properties: TableProperties) -> ZerobusResult<()> {
    let custom_provider = Arc::new(MyCustomAuthProvider {});
    let stream = sdk.create_stream_with_headers_provider(
        table_properties,
        custom_provider,
        None,
    ).await?;
    Ok(())
}
```

## Usage Guide

The SDK supports two approaches for data serialization:

1. **JSON** - Simpler approach that uses JSON strings. No schema generation required, making it ideal for quick prototyping. See [`examples/README.md`](https://github.com/databricks/zerobus-sdk/blob/main/rust/examples/README.md) for a complete example.
2. **Protocol Buffers** - Type-safe approach with schema validation at compile time. Recommended for production use cases. This guide focuses on the Protocol Buffers approach.

For JSON-based ingestion, you can skip the schema generation step and directly pass JSON strings to `ingest_record()`.

### 1. Generate Protocol Buffer Schema (Protocol Buffers approach only)

> **Important Note**: The schema generation tool and examples are **only available in the GitHub repository**. The crate published on [crates.io](https://crates.io/crates/databricks-zerobus-ingest-sdk) contains only the core Zerobus ingestion SDK logic. To generate protobuf schemas or see working examples, clone the repository:
> 
> ```bash
> git clone https://github.com/databricks/zerobus-sdk.git
> cd zerobus-sdk/rust
> ```

Use the included tool to generate schema files from your Unity Catalog table:

```bash
cd tools/generate_files

# For AWS
cargo run -- \
  --uc-endpoint "https://<your-workspace>.cloud.databricks.com" \
  --client-id "your-client-id" \
  --client-secret "your-client-secret" \
  --table "catalog.schema.table" \
  --output-dir "../../output"

# For Azure
cargo run -- \
  --uc-endpoint "https://<your-workspace>.azuredatabricks.net" \
  --client-id "your-client-id" \
  --client-secret "your-client-secret" \
  --table "catalog.schema.table" \
  --output-dir "../../output"
```

This generates three files:
- `{table}.proto` - Protocol Buffer schema definition
- `{table}.rs` - Rust structs with serialization code
- `{table}.descriptor` - Binary descriptor for runtime validation

See [`tools/generate_files/README.md`](https://github.com/databricks/zerobus-sdk/blob/main/rust/tools/generate_files/README.md) for supported data types and limitations.

See [`examples/README.md`](https://github.com/databricks/zerobus-sdk/blob/main/rust/examples/README.md) for more information on how to get OAuth credentials.

### 2. Initialize the SDK

Create an SDK instance using the builder pattern:

```rust
// For AWS
let sdk = ZerobusSdk::builder()
    .endpoint("https://<your-shard-id>.zerobus.<region>.cloud.databricks.com")
    .unity_catalog_url("https://<your-workspace>.cloud.databricks.com")
    .build()?;

// For Azure
let sdk = ZerobusSdk::builder()
    .endpoint("https://<your-shard-id>.zerobus.<region>.azuredatabricks.net")
    .unity_catalog_url("https://<your-workspace>.azuredatabricks.net")
    .build()?;
```

**Note:** The workspace ID is automatically extracted from the Zerobus endpoint. The `https://` scheme is optional — if omitted, the SDK automatically prepends `https://`.

#### TLS Configuration

By default, the SDK uses `SecureTlsConfig` which enables TLS with the operating system's trusted CA certificates. For testing against a local `http://` server, use `NoTlsConfig` (requires the `testing` feature):

```rust
use databricks_zerobus_ingest_sdk::{ZerobusSdk, NoTlsConfig};
use std::sync::Arc;

let sdk = ZerobusSdk::builder()
    .endpoint("http://localhost:50051")
    .tls_config(Arc::new(NoTlsConfig))
    .build()?;
```

For custom certificate handling, implement the `TlsConfig` trait:

```rust
use databricks_zerobus_ingest_sdk::{TlsConfig, ZerobusResult};
use tonic::transport::Endpoint;

struct MyTlsConfig { /* ... */ }

impl TlsConfig for MyTlsConfig {
    fn configure_endpoint(&self, endpoint: Endpoint) -> ZerobusResult<Endpoint> {
        // Custom TLS configuration logic
        Ok(endpoint)
    }
}
```

### 3. Configure Authentication

The SDK handles authentication automatically. You just need to provide:
- **Client ID** - Your OAuth client ID
- **Client Secret** - Your OAuth client secret
- **Unity Catalog Endpoint** - Passed to SDK constructor
- **Table Name** - Included in table properties

```rust
let client_id = "your-client-id".to_string();
let client_secret = "your-client-secret".to_string();
```

See [`examples/README.md`](https://github.com/databricks/zerobus-sdk/blob/main/rust/examples/README.md) for more information on how to get these credentials.

### 4. Create a Stream

Configure table properties and stream options:

```rust
use std::fs;
use prost::Message;
use prost_types::{FileDescriptorSet, DescriptorProto};

// Load descriptor from generated files
fn load_descriptor(path: &str, file: &str, msg: &str) -> DescriptorProto {
    let bytes = fs::read(path).expect("Failed to read descriptor");
    let file_set = FileDescriptorSet::decode(bytes.as_ref()).unwrap();

    let file_desc = file_set.file.into_iter()
        .find(|f| f.name.as_deref() == Some(file))
        .unwrap();

    file_desc.message_type.into_iter()
        .find(|m| m.name.as_deref() == Some(msg))
        .unwrap()
}

let descriptor_proto = load_descriptor(
    "output/orders.descriptor",
    "orders.proto",
    "table_Orders",
);

let table_properties = TableProperties {
    table_name: "catalog.schema.orders".to_string(),
    descriptor_proto,
};

let options = StreamConfigurationOptions {
    max_inflight_requests: 10000,
    recovery: true,
    recovery_timeout_ms: 15000,
    recovery_backoff_ms: 2000,
    recovery_retries: 4,
    ..Default::default()
};

let mut stream = sdk.create_stream(
    table_properties,
    client_id,
    client_secret,
    Some(options),
).await?;
```

### 5. Ingest Data

The SDK provides flexible ways to ingest data with different levels of abstraction:

| Wrapper | Format | Description |
|---------|--------|-------------|
| `ProtoMessage<T>` | Proto | Auto-encoding: pass structs, SDK handles encoding |
| `ProtoBytes` | Proto | Pre-encoded: pass bytes with explicit wrapper |
| `Vec<u8>` | Proto | Backward-compatible: raw bytes without wrapper |
| `JsonValue<T>` | JSON | Auto-serializing: pass structs, SDK handles JSON conversion |
| `JsonString` | JSON | Pre-serialized: pass JSON strings with explicit wrapper |
| `String` | JSON | Backward-compatible: raw strings without wrapper |

#### Single Record Ingestion

```rust
use databricks_zerobus_ingest_sdk::ProtoMessage;

let record = YourMessage { id: Some(1), name: Some("Alice".to_string()) };

// Ingest and get offset (after queuing)
let offset = stream.ingest_record_offset(ProtoMessage(record)).await?;

// Wait for server acknowledgment
stream.wait_for_offset(offset).await?;
```

#### Batch Ingestion

Ingest multiple records at once for higher throughput with all-or-nothing semantics.

```rust
use databricks_zerobus_ingest_sdk::ProtoMessage;

let records: Vec<ProtoMessage<YourMessage>> = vec![
    ProtoMessage(YourMessage { id: Some(1), /* ... */ }),
    ProtoMessage(YourMessage { id: Some(2), /* ... */ }),
    ProtoMessage(YourMessage { id: Some(3), /* ... */ }),
];

// Returns Some(offset) for non-empty batches, None for empty batches
if let Some(offset) = stream.ingest_records_offset(records).await? {
    stream.wait_for_offset(offset).await?;
}
```

#### High Throughput Pattern

Ingest many records without waiting for each acknowledgment, then flush periodically:

```rust
for i in 0..100_000 {
    let record = YourMessage { id: Some(i), /* ... */ };
    let _offset = stream.ingest_record_offset(ProtoMessage(record)).await?;

    // Periodically flush to avoid unbounded memory growth
    if (i + 1) % 10_000 == 0 {
        stream.flush().await?;
    }
}
stream.flush().await?;
```

See [`examples/`](https://github.com/databricks/zerobus-sdk/tree/main/rust/examples) for complete working examples with all wrapper types, serialization formats, and ingestion patterns.

### 6. Handle Acknowledgments

The recommended `ingest_record_offset()` and `ingest_records_offset()` methods return offsets directly (after queuing):
- `ingest_record_offset()` returns `OffsetId` (the logical offset)
- `ingest_records_offset()` returns `Option<OffsetId>` (None if the batch is empty)

```rust
// Ingest and get offset, after queuing the record.
let offset_id = stream.ingest_record_offset(data).await?;
println!("Record sent with offset Id: {}", offset_id);

// Wait for acknowledgment when needed.
stream.wait_for_offset(offset_id).await?;
println!("Record committed at offset: {}", offset_id);

// For batches, the method returns Option<OffsetId>.
// None if the batch is empty.
let batch = vec![data1, data2, data3];
if let Some(offset_id) = stream.ingest_records_offset(batch).await? {
    println!("Batch sent with last offset: {}", offset_id);
    stream.wait_for_offset(offset_id).await?;
    println!("Batch committed");
} else {
    println!("Empty batch, no records ingested");
}

// High-throughput: collect offsets and wait selectively.
let mut offsets = Vec::new();
for i in 0..1000 {
    let offset = stream.ingest_record_offset(record).await?;
    offsets.push(offset);
}
// Wait for specific offsets as needed.
for offset in offsets {
    stream.wait_for_offset(offset).await?;
}

// Or use flush() to wait for all pending acknowledgments at once.
stream.flush().await?;
```

#### Using Acknowledgment Callbacks

For scenarios where you need to track acknowledgments without explicitly waiting (e.g., for metrics or logging), you can use callbacks:

```rust
use databricks_zerobus_ingest_sdk::{AckCallback, OffsetId};
use std::sync::Arc;

// Define a callback that implements the AckCallback trait
struct MyCallback;

impl AckCallback for MyCallback {
    fn on_ack(&self, offset_id: OffsetId) {
        // Called when a record is acknowledged
        println!("✓ Acknowledged offset: {}", offset_id);
    }

    fn on_error(&self, offset_id: OffsetId, error_message: &str) {
        // Called when a record encounters an error
        eprintln!("✗ Error for offset {}: {}", offset_id, error_message);
    }
}

// Configure stream with callback
let options = StreamConfigurationOptions {
    max_inflight_requests: 10000,
    ack_callback: Some(Arc::new(MyCallback)),
    ..Default::default()
};

let mut stream = sdk.create_stream(
    table_properties,
    client_id,
    client_secret,
    Some(options),
).await?;

for i in 0..1000 {
    let record = YourMessage { id: Some(i), /* ... */ };
    stream.ingest_record_offset(record.encode_to_vec()).await?;
    // Callback fires when this record is acknowledged
}

stream.flush().await?;
```

**Important:** Callbacks run synchronously in a dedicated callback handler task. Keep them lightweight (simple logging, metrics increment) to avoid callback backlog. For heavy work like database writes or network calls, send data to a channel for processing in a separate task:

```rust
use tokio::sync::mpsc;

struct ChannelCallback {
    tx: mpsc::UnboundedSender<OffsetId>,
}

impl AckCallback for ChannelCallback {
    fn on_ack(&self, offset_id: OffsetId) {
        // Lightweight: just send to channel
        let _ = self.tx.send(offset_id);
    }

    fn on_error(&self, offset_id: OffsetId, error_message: &str) {
        eprintln!("Error: {}", error_message);
    }
}

let (tx, mut rx) = mpsc::unbounded_channel();
let callback = Arc::new(ChannelCallback { tx });

// Heavy processing in separate task
tokio::spawn(async move {
    while let Some(offset) = rx.recv().await {
        // Heavy work here (database writes, API calls, etc.)
        write_to_database(offset).await;
    }
});
```

### 7. Close the Stream

Always close streams to ensure data is flushed:

```rust
// Close gracefully (flushes automatically)
stream.close().await?;
```

If the stream fails, retrieve unacknowledged records:

```rust
match stream.close().await {
    Err(_) => {
        // Option 1: Get individual records (flattened)
        let unacked = stream.get_unacked_records().await?;
        let total_records = unacked.count();
        println!("Failed to ack {} records", total_records);
        
        // Option 2: Get records grouped by batch (preserves batch structure)
        let unacked_batches = stream.get_unacked_batches().await?;
        let total_records: usize = unacked_batches.iter().map(|batch| batch.get_record_count()).sum();
        println!("Failed to ack {} records in {} batches", total_records, unacked_batches.len());
        
        // Retry with a new stream
    }
    Ok(_) => println!("Stream closed successfully"),
}
```

## Configuration Options

### StreamConfigurationOptions

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_inflight_requests` | `usize` | 1,000,000 | Maximum unacknowledged requests in flight |
| `recovery` | `bool` | true | Enable automatic stream recovery on failure |
| `recovery_timeout_ms` | `u64` | 15,000 | Timeout for recovery operations (ms) |
| `recovery_backoff_ms` | `u64` | 2,000 | Delay between recovery retry attempts (ms) |
| `recovery_retries` | `u32` | 4 | Maximum number of recovery attempts |
| `server_lack_of_ack_timeout_ms` | `u64` | 60,000 | Timeout waiting for server acks (ms) |
| `flush_timeout_ms` | `u64` | 300,000 | Timeout for flush operations (ms) |
| `record_type` | `RecordType` | `RecordType::Proto` | Record serialization format (Proto or Json) |
| `stream_paused_max_wait_time_ms` | `Option<u64>` | `None` | Max time to wait during graceful close (`None` = full server duration, `Some(0)` = immediate, `Some(x)` = min(x, server_duration)) |
| `ack_callback` | `Option<Arc<dyn AckCallback>>` | `None` | Optional callback for acknowledgment notifications |
| `callback_max_wait_time_ms` | `Option<u64>` | `None` | Maximum time to wait for callback processing to complete after closing the stream (`None` = wait indefinitely, `Some(x)` = wait up to `x` ms) |


**Example:**

```rust
let options = StreamConfigurationOptions {
    max_inflight_requests: 50000,
    recovery: true,
    recovery_timeout_ms: 20000,
    recovery_retries: 5,
    flush_timeout_ms: 600000,
    ..Default::default()
};
```

## Error Handling

The SDK categorizes errors as **retryable** or **non-retryable**:

### Retryable Errors
Auto-recovered if `recovery` is enabled:
- Network failures
- Connection timeouts
- Temporary server errors
- Stream closed by server

### Non-Retryable Errors
Require manual intervention:
- `InvalidUCTokenError` - Invalid OAuth credentials
- `InvalidTableName` - Table doesn't exist or invalid format
- `InvalidArgument` - Invalid parameters or schema mismatch
- `Code::Unauthenticated` - Authentication failure
- `Code::PermissionDenied` - Insufficient table permissions
- `ChannelCreationError` - Failed to establish TLS connection

**Check if an error is retryable:**

```rust
match stream.ingest_record(payload).await {
    Ok(ack) => {
        let offset = ack.await?;
    }
    Err(e) if e.is_retryable() => {
        eprintln!("Retryable error, SDK will auto-recover: {}", e);
    }
    Err(e) => {
        eprintln!("Fatal error, manual intervention needed: {}", e);
        return Err(e.into());
    }
}
```

## Examples

### Complete Working Examples

The `examples/` directory contains four working examples covering different serialization formats and ingestion patterns:

| Example | Serialization | Ingestion | Package |
|---------|--------------|-----------|---------|
| `json/single/` | JSON | Single-record | `example_json_single` |
| `json/batch/` | JSON | Batch | `example_json_batch` |
| `proto/single/` | Protocol Buffers | Single-record | `example_proto_single` |
| `proto/batch/` | Protocol Buffers | Batch | `example_proto_batch` |


Check [`examples/README.md`](https://github.com/databricks/zerobus-sdk/blob/main/rust/examples/README.md) for setup instructions and detailed comparisons.

### Stream Recovery

```rust
let sdk = ZerobusSdk::builder()
    .endpoint(endpoint)
    .unity_catalog_url(uc_endpoint)
    .build()?;

let mut stream = sdk.create_stream(
    table_properties.clone(),
    client_id.clone(),
    client_secret.clone(),
    Some(options),
).await?;

// Ingest data...
match stream.close().await {
    Err(_) => {
        // Stream failed, recreate with unacked records
        stream = sdk.recreate_stream(stream).await?;
    }
    Ok(_) => println!("Closed successfully"),
}
```

## Tests

Integration tests live in the `tests/` crate and run against a lightweight mock Zerobus gRPC server.

- Mock server: `tests/src/mock_grpc.rs`
- Test suite: `tests/src/rust_tests.rs`

Run tests with logs:

```bash
cargo test -p tests -- --nocapture
```

## Best Practices

1. **Reuse SDK Instances** - Create one `ZerobusSdk` per application and reuse for multiple streams
2. **Always Close Streams** - Use `stream.close().await?` to ensure all data is flushed
3. **Choose the Right Ingestion Method**:
   - Use `ingest_records_offset()` for high throughput batch ingestion
   - Use `ingest_record_offset()` when processing records individually
   - Both return offsets directly; use `wait_for_offset()` to explicitly wait for acknowledgments
   - The older `ingest_record()` and `ingest_records()` methods are deprecated
4. **Tune Inflight Limits** - Adjust `max_inflight_requests` based on memory and throughput needs
5. **Enable Recovery** - Always set `recovery: true` in production environments
6. **Handle Ack Futures** - Use `tokio::spawn` for fire-and-forget or batch-wait for verification
7. **Monitor Errors** - Log and alert on non-retryable errors
8. **Validate Schemas** - Use the schema generation tool to ensure type safety (for Protocol Buffers)
9. **Secure Credentials** - Never hardcode secrets; use environment variables or secret managers
10. **Test Recovery** - Simulate failures to verify your error handling logic

## API Reference

### `ZerobusSdk`

Main entry point for the SDK.

**Builder:**
```rust
let sdk = ZerobusSdk::builder()
    .endpoint("https://workspace.zerobus.databricks.com")  // Required
    .unity_catalog_url("https://workspace.cloud.databricks.com")  // Optional with custom headers
    .tls_config(Arc::new(SecureTlsConfig::new()))  // Optional, defaults to SecureTlsConfig
    .build()?;
```

**Methods:**
```rust
pub async fn create_stream(
    &self,
    table_properties: TableProperties,
    client_id: String,
    client_secret: String,
    options: Option<StreamConfigurationOptions>,
) -> ZerobusResult<ZerobusStream>
```

```rust
pub async fn recreate_stream(
    &self,
    stream: ZerobusStream
) -> ZerobusResult<ZerobusStream>
```
Recreates a failed stream, preserving and re-ingesting unacknowledged records.

```rust
pub async fn create_stream_with_headers_provider(
    &self,
    table_properties: TableProperties,
    headers_provider: Arc<dyn HeadersProvider>,
    options: Option<StreamConfigurationOptions>,
) -> ZerobusResult<ZerobusStream>
```
Creates a stream with a custom headers provider for advanced authentication.

### `ZerobusStream`

Represents an active ingestion stream.

**Methods:**
```rust
pub async fn ingest_record_offset(
    &self,
    payload: impl Into<EncodedRecord>
) -> ZerobusResult<OffsetId>
```
Ingests a single encoded record (Protocol Buffers or JSON). The await queues the record for sending and returns the logical offset ID directly. Use `wait_for_offset()` to explicitly wait for server acknowledgment of this offset.

```rust
pub async fn ingest_records_offset(
    &self,
    payloads: Vec<impl Into<EncodedRecord>>
) -> ZerobusResult<Option<OffsetId>>
```
Ingests multiple encoded records as a batch with all-or-nothing semantics. The entire batch either succeeds or fails as a unit. The await queues the batch for sending and returns the logical offset ID directly (or `None` for empty batches). Use `wait_for_offset()` to explicitly wait for server acknowledgment.

```rust
pub async fn ingest_record(
    &self,
    payload: Vec<u8>
) -> ZerobusResult<impl Future<Output = ZerobusResult<OffsetId>>>>
```
**Deprecated:** Use `ingest_record_offset()` instead. Returns a future that resolves to the offset ID.

```rust
pub async fn ingest_records(
    &self,
    payloads: Vec<Vec<u8>>
) -> ZerobusResult<impl Future<Output = ZerobusResult<Option<OffsetId>>>>
```
**Deprecated:** Use `ingest_records_offset()` instead. Returns a future that resolves to `Some(offset_id)` for non-empty batches, or `None` if the batch is empty.

```rust
pub async fn wait_for_offset(&self, offset_id: OffsetId) -> ZerobusResult<()>
```
Waits for acknowledgment of a specific logical offset. Use this method with offsets returned from `ingest_record_offset()` or `ingest_records_offset()` to explicitly wait for server acknowledgment.

```rust
pub async fn flush(&self) -> ZerobusResult<()>
```
Flushes all pending records and waits for acknowledgment.

```rust
pub async fn close(&mut self) -> ZerobusResult<()>
```
Flushes and closes the stream gracefully.

```rust
pub async fn get_unacked_records(&self) -> ZerobusResult<impl Iterator<Item = EncodedRecord>>
```
Returns an iterator over all unacknowledged records as individual `EncodedRecord` items. This flattens batches into individual records. Only call after stream failure.

```rust
pub async fn get_unacked_batches(&self) -> ZerobusResult<Vec<EncodedBatch>>
```
Returns unacknowledged records grouped by batch, preserving the original batch structure. Records ingested together remain grouped:
- Each `ingest_record()` call creates a batch containing one record
- Each `ingest_records()` call creates a batch containing multiple records

Only call after stream failure.

### `TableProperties`

Configuration for the target table.

**Fields:**
```rust
pub struct TableProperties {
    pub table_name: String,
    pub descriptor_proto: Option<prost_types::DescriptorProto>,
}
```

- `table_name` - Full table name (e.g., "catalog.schema.table")
- `descriptor_proto` - Optional Protocol buffer descriptor loaded from generated files (required for Proto record type, None for JSON)

### `StreamConfigurationOptions`

Stream behavior configuration.

**Fields:**
```rust
pub struct StreamConfigurationOptions {
    pub max_inflight_requests: usize,
    pub recovery: bool,
    pub recovery_timeout_ms: u64,
    pub recovery_backoff_ms: u64,
    pub recovery_retries: u32,
    pub server_lack_of_ack_timeout_ms: u64,
    pub flush_timeout_ms: u64,
    pub record_type: RecordType,
    pub stream_paused_max_wait_time_ms: Option<u64>,
    pub ack_callback: Option<Arc<dyn AckCallback>>,
    pub callback_max_wait_time_ms: Option<u64>
}
```

See [Configuration Options](#configuration-options) for details.

### `AckCallback`

Trait for receiving acknowledgment notifications.

**Methods:**
```rust
pub trait AckCallback: Send + Sync {
    fn on_ack(&self, offset_id: OffsetId);
    fn on_error(&self, offset_id: OffsetId, error_message: &str);
}
```

- `on_ack()` - Called when a record/batch is successfully acknowledged
- `on_error()` - Called when a record/batch encounters an error

### `HeadersProvider`

Trait for custom authentication header providers.

**Methods:**
```rust
#[async_trait]
pub trait HeadersProvider: Send + Sync {
    async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>>;
}
```

Implement this trait to provide custom authentication headers. The default implementation (`OAuthHeadersProvider`) handles OAuth 2.0 token management. Use this for:
- Custom token caching strategies
- Alternative authentication mechanisms
- Integration with centralized credential services

See [Custom Authentication](#custom-authentication) section for usage examples.

### `TlsConfig`

Trait for TLS configuration strategies.

**Implementations:**
- `SecureTlsConfig` (default) - Production TLS with system CA certificates
- `NoTlsConfig` - No-op TLS for testing with `http://` endpoints (requires `testing` feature)

```rust
pub trait TlsConfig: Send + Sync {
    fn configure_endpoint(&self, endpoint: Endpoint) -> ZerobusResult<Endpoint>;
}
```

### `ZerobusError`

Error type for all SDK operations.

**Methods:**
```rust
pub fn is_retryable(&self) -> bool
```
Returns `true` if the error can be automatically recovered by the SDK.

## Language Bindings

This repository includes bindings for building SDKs in other languages.

### C FFI (`ffi/`)

C Foreign Function Interface for languages that can call C functions (Go, C#, C++, etc.).

```bash
# Build static and dynamic libraries
cargo build -p zerobus-ffi --release

# Output:
# target/release/libzerobus_ffi.a      (static library for Go, C++)
# target/release/libzerobus_ffi.so     (Linux dynamic library for C#)
# target/release/libzerobus_ffi.dylib  (macOS dynamic library for C#)
# target/release/zerobus_ffi.dll       (Windows dynamic library for C#)
# ffi/zerobus.h                        (C header file)
```

Pre-built binaries for all platforms are available in [GitHub Releases](https://github.com/databricks/zerobus-sdk/releases) with tags `ffi-vX.X.X`.

### JNI (`jni/`)

Java Native Interface bindings for the [Zerobus Java SDK](https://github.com/databricks/zerobus-sdk/tree/main/java).

```bash
# Build JNI library
cargo build -p zerobus-jni --release

# Output:
# target/release/libzerobus_jni.so     (Linux)
# target/release/libzerobus_jni.dylib  (macOS)
# target/release/zerobus_jni.dll       (Windows)
```

Pre-built binaries are available in [GitHub Releases](https://github.com/databricks/zerobus-sdk/releases) with tags `jni-vX.X.X`.

## Building from Source

For contributors or those who want to build and test the SDK:

```bash
git clone https://github.com/databricks/zerobus-sdk.git
cd zerobus-sdk/rust
cargo build --workspace
```

**Build specific components:**

```bash
# Build only SDK
cargo build -p databricks-zerobus-ingest-sdk

# Build only schema tool
cargo build -p generate_files

# Build and run JSON single-record example
cargo run -p example_json_single

# Build and run JSON batch example
cargo run -p example_json_batch

# Build and run Protocol Buffers single-record example
cargo run -p example_proto_single

# Build and run Protocol Buffers batch example
cargo run -p example_proto_batch
```

## Community and Contributing

This is an open source project. We welcome contributions, feedback, and bug reports.

- **[Contributing Guide](https://github.com/databricks/zerobus-sdk/blob/main/rust/CONTRIBUTING.md)**: Rust-specific development setup and workflow.
- **[General Contributing Guide](https://github.com/databricks/zerobus-sdk/blob/main/CONTRIBUTING.md)**: Pull request process, commit requirements, and policies.
- **[Changelog](https://github.com/databricks/zerobus-sdk/blob/main/rust/CHANGELOG.md)**: See the history of changes in the SDK.
- **[Security Policy](https://github.com/databricks/zerobus-sdk/blob/main/SECURITY.md)**: Read about our security process and how to report vulnerabilities.
- **[Developer Certificate of Origin (DCO)](https://github.com/databricks/zerobus-sdk/blob/main/DCO)**: Understand the agreement for contributions.
- **[Open Source Attributions](https://github.com/databricks/zerobus-sdk/blob/main/rust/NOTICE)**: See a list of the open source libraries we use.

## License

This SDK is licensed under the Apache License 2.0. See the [LICENSE](https://github.com/databricks/zerobus-sdk/blob/main/LICENSE) file for the full license text.

## Requirements

- **Rust** 1.70 or higher (2021 edition)
- **TLS** - Uses native OS certificate store by default (configurable via `TlsConfig` trait)
- See [prerequisites](https://github.com/databricks/zerobus-sdk/blob/main/README.md#prerequisites) for Databricks workspace and credential requirements


---

For issues, questions, or contributions, please visit the [GitHub repository](https://github.com/databricks/zerobus-sdk). See the [monorepo README](https://github.com/databricks/zerobus-sdk/blob/main/README.md) for an overview of all available SDKs.
