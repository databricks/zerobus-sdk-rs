# Zerobus Rust SDK

A high-performance Rust client for streaming data ingestion into Databricks Delta tables using the Zerobus protocol.

## Disclaimer

[Public Preview](https://docs.databricks.com/release-notes/release-types.html): This SDK is supported for production use cases and is available to all customers. Databricks is actively working on stabilizing the Zerobus Ingest SDK for Rust. Minor version updates may include backwards-incompatible changes.

We are keen to hear feedback from you on this SDK. Please [file issues](https://github.com/databricks/zerobus-sdk-rs/issues), and we will address them.

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
- [Building from Source](#building-from-source)
- [License](#license)

## Overview

The Zerobus Rust SDK provides a robust, async-first interface for ingesting large volumes of data into Databricks Delta tables. It abstracts the complexity of the Zerobus streaming protocol and handles authentication, retries, stream recovery, and acknowledgment tracking automatically.

**What is Zerobus?** Zerobus is a high-throughput streaming protocol for direct data ingestion into Databricks Delta tables, optimized for real-time data pipelines and high-volume workloads.

## Features

- **Async/Await Support** - Built on Tokio for efficient concurrent I/O operations
- **Automatic OAuth 2.0 Authentication** - Seamless token management with Unity Catalog
- **Built-in Recovery** - Automatic retry and reconnection for transient failures
- **High Throughput** - Configurable inflight record limits for optimal performance
- **Type Safety** - Protocol Buffers ensure schema validation at compile time
- **Schema Generation** - CLI tool to generate protobuf schemas from Unity Catalog tables
- **Flexible Configuration** - Fine-tune timeouts, retries, and recovery behavior
- **Graceful Stream Management** - Proper flushing and acknowledgment tracking

## Installation

Add the SDK to your `Cargo.toml`:

```bash
cargo add --git https://github.com/databricks/zerobus-sdk-rs zerobus
cargo add prost prost-types
cargo add tokio --features macros,rt-multi-thread
```
**Why these dependencies?**
- **`zerobus`** - The SDK itself
- **`prost`** and **`prost-types`** - Required for encoding your data to Protocol Buffers and loading schema descriptors
- **`tokio`** - Async runtime required for running async functions (the SDK is fully async)

### For Local Development

Clone the repository and use a path dependency:

```bash
git clone https://github.com/databricks/zerobus-sdk-rs.git
cd your_project
```

Then in your `Cargo.toml`:

```toml
[dependencies]
zerobus = { path = "../zerobus-sdk-rs/sdk" }
prost = "0.13.3"
prost-types = "0.13.3"
tokio = { version = "1.42.0", features = ["macros", "rt-multi-thread"] }
```

## Quick Start

```rust
use prost::Message;
use zerobus::{ZerobusSdk, TableProperties, StreamConfigurationOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize SDK
    let sdk = ZerobusSdk::new(
        "https://your-workspace-id.cloud.databricks.com".to_string(),
        "https://your-workspace.cloud.databricks.com".to_string(),
    )?;

    // Configure table properties
    let table_properties = TableProperties {
        table_name: "catalog.schema.table".to_string(),
        descriptor_proto: your_descriptor_proto, // loaded from generated files
    };

    // Create stream with authentication
    let mut stream = sdk.create_stream(
        table_properties,
        "your-client-id".to_string(),
        "your-client-secret".to_string(),
        Some(StreamConfigurationOptions {
            max_inflight_records: 1000,
            ..Default::default()
        }),
    ).await?;

    // Ingest a record
    let record = YourMessage { /* your data */ };
    let ack_future = stream.ingest_record(record.encode_to_vec()).await?;

    // Wait for acknowledgment
    let offset = ack_future.await?;
    println!("Record ingested at offset: {}", offset);

    // Close stream gracefully
    stream.close().await?;
    Ok(())
}
```

## Repository Structure

```
zerobus_rust_sdk/
├── sdk/                                # Core SDK library
│   ├── src/
│   │   ├── lib.rs                      # Main SDK and stream implementation
│   │   ├── default_token_factory.rs    # OAuth 2.0 token handling
│   │   ├── errors.rs                   # Error types and retryable logic
│   │   ├── stream_configuration.rs     # Stream options
│   │   ├── landing_zone.rs             # Inflight record buffer
│   │   └── offset_generator.rs         # Logical offset tracking
│   ├── zerobus_service.proto           # gRPC protocol definition
│   ├── build.rs                        # Build script for protobuf compilation
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
│   └── basic_example/                  # Working example application
│       ├── README.md                   # Example documentation
│       ├── src/main.rs                 # Example usage code
│       ├── output/                     # Generated schema files
│       │   ├── orders.proto
│       │   ├── orders.rs
│       │   └── orders.descriptor
│       └── Cargo.toml
│
├── Cargo.toml                          # Workspace configuration
└── README.md                           # This file
```

### Key Components

- **`sdk/`** - The main library crate containing all SDK functionality
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

1. **Ingestion** - Your app calls `stream.ingest_record(data)`
2. **Buffering** - Record is placed in the landing zone with a logical offset
3. **Sending** - Sender task sends record over gRPC with physical offset
4. **Acknowledgment** - Receiver task gets server ack and resolves the future
5. **Recovery** - If connection fails, supervisor reconnects and resends unacked records

### Authentication Flow

The SDK uses OAuth 2.0 client credentials flow:

1. SDK constructs authorization request with Unity Catalog privileges
2. Sends request to `{uc_endpoint}/oidc/v1/token` with client credentials
3. Token includes scoped permissions for the specific table
4. Token is attached to gRPC metadata as Bearer token
5. Fresh tokens are fetched automatically on each connection

## Usage Guide

### 1. Generate Protocol Buffer Schema

Use the included tool to generate schema files from your Unity Catalog table:

```bash
cd tools/generate_files

cargo run -- \
  --uc-endpoint "https://your-workspace.cloud.databricks.com" \
  --uc-token "dapi123..." \
  --table "catalog.schema.table" \
  --output-dir "../../output"
```

This generates three files:
- `{table}.proto` - Protocol Buffer schema definition
- `{table}.rs` - Rust structs with serialization code
- `{table}.descriptor` - Binary descriptor for runtime validation

See [`tools/generate_files/readme.md`](tools/generate_files/readme.md) for supported data types and limitations.

### 2. Initialize the SDK

Create an SDK instance with your Databricks workspace endpoints:

```rust
let sdk = ZerobusSdk::new(
    "https://workspace-id.cloud.databricks.com".to_string(),  // Zerobus endpoint
    "https://workspace.cloud.databricks.com".to_string(),     // Unity Catalog endpoint
)?;
```

**Note:** The workspace ID is automatically extracted from the Zerobus endpoint when `ZerobusSdk::new()` is called.

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

See [`examples/basic_example/README.md`](examples/basic_example/README.md) for more information on how to get these credentials.

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
        .find(|f| f.name.as_ref().map(|n| n.as_str()) == Some(file))
        .unwrap();

    file_desc.message_type.into_iter()
        .find(|m| m.name.as_ref().map(|n| n.as_str()) == Some(msg))
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
    max_inflight_records: 10000,
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

Ingest records by encoding them with Protocol Buffers:

```rust
use prost::Message;

// Single record
let record = YourMessage {
    field1: Some("value".to_string()),
    field2: Some(42),
};

let ack_future = stream.ingest_record(record.encode_to_vec()).await?;
```

**Batch ingestion** for high throughput:

```rust
use futures::future::join_all;

let mut ack_futures = Vec::new();

for i in 0..100_000 {
    let record = YourMessage {
        id: Some(i),
        timestamp: Some(chrono::Utc::now().timestamp()),
        data: Some(format!("record-{}", i)),
    };

    let ack = stream.ingest_record(record.encode_to_vec()).await?;
    ack_futures.push(ack);
}

// Flush all pending records
stream.flush().await?;

// Wait for all acknowledgments
let results = join_all(ack_futures).await;
```

### 6. Handle Acknowledgments

Each `ingest_record()` returns a future that resolves to the record's offset:

```rust
// Fire-and-forget (not recommended for production)
let ack = stream.ingest_record(data).await?;
tokio::spawn(ack);

// Wait for specific record
let ack = stream.ingest_record(data).await?;
let offset = ack.await?;
println!("Record committed at offset: {}", offset);

// Batch wait with error handling
for ack in ack_futures {
    match ack.await {
        Ok(offset) => println!("Success: {}", offset),
        Err(e) => eprintln!("Failed: {}", e),
    }
}
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
        let unacked = stream.get_unacked_records().await?;
        println!("Failed to ack {} records", unacked.len());
        // Retry with a new stream
    }
    Ok(_) => println!("Stream closed successfully"),
}
```

## Configuration Options

### StreamConfigurationOptions

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_inflight_records` | `usize` | 1,000,000 | Maximum unacknowledged records in flight |
| `recovery` | `bool` | true | Enable automatic stream recovery on failure |
| `recovery_timeout_ms` | `u64` | 15,000 | Timeout for recovery operations (ms) |
| `recovery_backoff_ms` | `u64` | 2,000 | Delay between recovery retry attempts (ms) |
| `recovery_retries` | `u32` | 4 | Maximum number of recovery attempts |
| `flush_timeout_ms` | `u64` | 300,000 | Timeout for flush operations (ms) |
| `server_lack_of_ack_timeout_ms` | `u64` | 60,000 | Timeout waiting for server acks (ms) |

**Example:**

```rust
let options = StreamConfigurationOptions {
    max_inflight_records: 50000,
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

### Complete Working Example

See [`examples/basic_example/`](examples/basic_example/) for a fully functional example.

**Run the example:**

```bash
cd examples/basic_example

# Edit src/main.rs with your credentials:
# - DATABRICKS_WORKSPACE_URL
# - TABLE_NAME
# - DATABRICKS_CLIENT_ID
# - DATABRICKS_CLIENT_SECRET
# - SERVER_ENDPOINT

cargo run
```

### High-Throughput Ingestion

```rust
use futures::future::join_all;

let mut ack_futures = Vec::with_capacity(100_000);

for i in 0..100_000 {
    let record = MyRecord {
        id: Some(i),
        value: Some(rand::random()),
    };

    let ack = stream.ingest_record(record.encode_to_vec()).await?;
    ack_futures.push(ack);
}

stream.flush().await?;
let results = join_all(ack_futures).await;

println!("Ingested {} records", results.len());
```

### Stream Recovery

```rust
let sdk = ZerobusSdk::new(endpoint, uc_endpoint);

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

## Best Practices

1. **Reuse SDK Instances** - Create one `ZerobusSdk` per application and reuse for multiple streams
2. **Always Close Streams** - Use `stream.close().await?` to ensure all data is flushed
3. **Tune Inflight Limits** - Adjust `max_inflight_records` based on memory and throughput needs
4. **Enable Recovery** - Always set `recovery: true` in production environments
5. **Handle Ack Futures** - Use `tokio::spawn` for fire-and-forget or batch-wait for verification
6. **Monitor Errors** - Log and alert on non-retryable errors
7. **Use Batch Ingestion** - For high throughput, ingest many records before waiting for acks
8. **Validate Schemas** - Use the schema generation tool to ensure type safety
9. **Secure Credentials** - Never hardcode secrets; use environment variables or secret managers
10. **Test Recovery** - Simulate failures to verify your error handling logic

## API Reference

### `ZerobusSdk`

Main entry point for the SDK.

**Constructor:**
```rust
pub fn new(zerobus_endpoint: String, unity_catalog_url: String) -> ZerobusResult<Self>
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

### `ZerobusStream`

Represents an active ingestion stream.

**Methods:**
```rust
pub async fn ingest_record(
    &self,
    payload: Vec<u8>
) -> ZerobusResult<impl Future<Output = ZerobusResult<i64>>>
```
Ingests a protobuf-encoded record. Returns a future that resolves to the offset ID.

```rust
pub async fn flush(&self) -> ZerobusResult<()>
```
Flushes all pending records and waits for acknowledgment.

```rust
pub async fn close(&mut self) -> ZerobusResult<()>
```
Flushes and closes the stream gracefully.

```rust
pub async fn get_unacked_records(&self) -> ZerobusResult<Vec<Vec<u8>>>
```
Returns unacknowledged record payloads. Only call after stream failure.

### `TableProperties`

Configuration for the target table.

**Fields:**
```rust
pub struct TableProperties {
    pub table_name: String,
    pub descriptor_proto: prost_types::DescriptorProto,
}
```

- `table_name` - Full table name (e.g., "catalog.schema.table")
- `descriptor_proto` - Protocol buffer descriptor loaded from generated files

### `StreamConfigurationOptions`

Stream behavior configuration.

**Fields:**
```rust
pub struct StreamConfigurationOptions {
    pub max_inflight_records: usize,
    pub recovery: bool,
    pub recovery_timeout_ms: u64,
    pub recovery_backoff_ms: u64,
    pub recovery_retries: u32,
    pub flush_timeout_ms: u64,
    pub server_lack_of_ack_timeout_ms: u64,
}
```

See [Configuration Options](#configuration-options) for details.

### `ZerobusError`

Error type for all SDK operations.

**Methods:**
```rust
pub fn is_retryable(&self) -> bool
```
Returns `true` if the error can be automatically recovered by the SDK.

## Building from Source

For contributors or those who want to build and test the SDK:

```bash
git clone https://github.com/YOUR_USERNAME/zerobus_rust_sdk.git
cd zerobus_rust_sdk
cargo build --workspace
```

**Build specific components:**

```bash
# Build only SDK
cargo build -p zerobus

# Build only schema tool
cargo build -p generate_files

# Build and run example
cargo run -p basic_example
```

## Requirements

- **Rust** 1.70 or higher (2021 edition)
- **Databricks** workspace with Zerobus access enabled
- **OAuth 2.0** client credentials (client ID and secret)
- **Unity Catalog** endpoint access
- **TLS Certificates** - `/etc/ssl/certs/ca-certificates.crt` (Linux) or `/etc/ssl/cert.pem` (macOS)


---

For issues, questions, or contributions, please visit the [GitHub repository](https://github.com/databricks/zerobus-sdk-rs).
