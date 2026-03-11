# Zerobus Go SDK

A high-performance Go client for streaming data ingestion into Databricks Delta tables using the Zerobus service.

## Disclaimer

[Public Preview](https://docs.databricks.com/release-notes/release-types.html): This SDK is supported for production use cases and is available to all customers. Databricks is actively working on stabilizing the Zerobus Ingest SDK for Go. Minor version updates may include backwards-incompatible changes.

We are keen to hear feedback from you on this SDK. Please [file issues](https://github.com/databricks/zerobus-sdk/issues), and we will address them.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Getting Started](#getting-started)
  - [For SDK Users (Install from pkg.go.dev)](#for-sdk-users-install-from-pkggodev)
  - [For Contributors (Build from Source)](#for-contributors-build-from-source)
- [Quick Start](#quick-start)
- [Repository Structure](#repository-structure)
- [How It Works](#how-it-works)
  - [Architecture Overview](#architecture-overview)
  - [Data Flow](#data-flow)
  - [Authentication Flow](#authentication-flow)
  - [Custom Authentication](#custom-authentication)
- [Usage Guide](#usage-guide)
  - [1. Initialize the SDK](#1-initialize-the-sdk)
  - [2. Configure Authentication](#2-configure-authentication)
  - [3. Create a Stream](#3-create-a-stream)
  - [4. Ingest Data](#4-ingest-data)
  - [5. Handle Acknowledgments](#5-handle-acknowledgments)
  - [6. Error Handling](#6-error-handling)
  - [7. Close the Stream](#7-close-the-stream)
- [Configuration Options](#configuration-options)
- [Error Handling](#error-handling)
- [Examples](#examples)
- [Arrow Flight Ingestion (Experimental)](#arrow-flight-ingestion-experimental)
- [Tests](#tests)
- [Performance Benchmarks](#performance-benchmarks)
- [Best Practices](#best-practices)
- [Migration Guide](#migration-guide)
- [API Reference](#api-reference)
- [Building from Source](#building-from-source)
- [Community and Contributing](#community-and-contributing)
- [License](#license)
- [Requirements](#requirements)

## Overview

The Zerobus Go SDK provides a robust, CGO-based wrapper around the high-performance Rust implementation for ingesting large volumes of data into Databricks Delta tables. It abstracts the complexity of the Zerobus service and handles authentication, retries, stream recovery, and acknowledgment tracking automatically.

**What is Zerobus?** Zerobus is a high-throughput streaming service for direct data ingestion into Databricks Delta tables, optimized for real-time data pipelines and high-volume workloads.

This SDK wraps the [Rust SDK](https://github.com/databricks/zerobus-sdk/tree/main/rust) using CGO and FFI (Foreign Function Interface), providing an idiomatic Go API while leveraging the performance and reliability of the underlying Rust implementation.

## Features

- **Static Linking** - Self-contained binaries with no runtime dependencies or LD_LIBRARY_PATH configuration
- **High-throughput streaming ingestion** into Databricks Delta tables
- **Automatic OAuth 2.0 authentication** with Unity Catalog
- **Simple JSON ingestion** - No code generation required for basic use cases
- **Protocol Buffers support** for type-safe, efficient data encoding
- **Batch ingestion** - Ingest multiple records at once for maximum throughput
- **Backpressure control** to manage memory usage
- **Automatic retry and recovery** for transient failures
- **Configurable timeouts and retry policies**
- **Immediate offset returns** for ingested records
- **Graceful stream management** - Proper flushing and resource cleanup
- **[Experimental] Arrow Flight ingestion** - High-throughput Apache Arrow RecordBatch ingestion via the Arrow Flight protocol
## Getting Started

Choose your installation path:

| Path | When to Use |
|------|-------------|
| **[Standard Installation](#installation)** | You want to use the SDK in your project (via `go get`) |
| **[Development Setup](#development-setup)** | You want to contribute or build from source (via `git clone`) |

### Installation

The Zerobus Go SDK is a CGO-based wrapper around a high-performance Rust core. For the best experience, use a tagged release which includes pre-built binaries.

**Prerequisites:**

Before using the SDK, you need a Databricks workspace URL, a Delta table, and a service principal. See the [monorepo prerequisites](https://github.com/databricks/zerobus-sdk/blob/main/README.md#prerequisites) for detailed setup instructions.

- **Go 1.21+**
- **CGO enabled** (enabled by default)
- **C compiler** (gcc or clang)

**Installation Steps:**

```bash
# Add the SDK to your project (use a tagged version for pre-built binaries)
go get github.com/databricks/zerobus-sdk/go@latest
```

> **Note:** Tagged releases (e.g., `v1.0.0`) come with pre-built Rust libraries for Linux, macOS, and Windows. If you use `@main` or a commit hash, you will need to have Rust installed and run `go generate` to build the library yourself.

**In your code:**

```go
import zerobus "github.com/databricks/zerobus-sdk/go"

func main() {
    sdk, err := zerobus.NewZerobusSdk(
        "https://your-shard.zerobus.region.cloud.databricks.com",
        "https://your-workspace.cloud.databricks.com",
    )
    if err != nil {
        log.Fatal(err)
    }
    defer sdk.Free()
    // ... create streams and ingest data
}
```

> **Note:** After the initial `go generate` step, regular `go build` works normally. The Rust library is statically linked into your binary.

### Development Setup

**Prerequisites:**
- Same as above (Go, CGO, Rust, C compiler, Databricks workspace)
- **Git**

**Setup:**

```bash
# Clone the repository
git clone https://github.com/databricks/zerobus-sdk.git
cd zerobus-sdk/go
go generate  # Builds Rust FFI
make build   # Builds everything
```

See [Building from Source](#building-from-source) for more build options and [Community and Contributing](#community-and-contributing) for contribution guidelines.

## Quick Start

The SDK supports two serialization formats and two ingestion methods:

**Serialization:**
- **JSON** (Recommended for getting started): Simpler approach using JSON strings, no schema generation required
- **Protocol Buffers** (Recommended for production): Type-safe approach with schema validation at compile time

**Ingestion Methods:**
- **Single-record** (`IngestRecordOffset`): Ingest records one at a time with per-record acknowledgment
- **Batch** (`IngestRecordsOffset`): Ingest multiple records at once with all-or-nothing semantics for higher throughput

See [`examples/README.md`](examples/README.md) for detailed setup instructions and examples for all combinations.

### JSON Ingestion (Simplest Approach)

Perfect for getting started quickly without code generation:

```go
package main

import (
    "log"
    zerobus "github.com/databricks/zerobus-sdk/go"
)

func main() {
    // 1. Initialize SDK
    sdk, err := zerobus.NewZerobusSdk(
        "https://your-shard-id.zerobus.region.cloud.databricks.com",
        "https://your-workspace.cloud.databricks.com",
    )
    if err != nil {
        log.Fatal(err)
    }
    defer sdk.Free()

    // 2. Configure for JSON records
    options := zerobus.DefaultStreamConfigurationOptions()
    options.RecordType = zerobus.RecordTypeJson

    // 3. Create stream
    stream, err := sdk.CreateStream(
        zerobus.TableProperties{
            TableName: "catalog.schema.table",
        },
        "your-client-id",
        "your-client-secret",
        options,
    )
    if err != nil {
        log.Fatal(err)
    }
    defer stream.Close()

    // 4. Send record to server and get offset
    // The offset is a logical sequence number assigned to this record
    offset, err := stream.IngestRecordOffset(`{"id": 1, "message": "Hello"}`)
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Record queued for ingestion with offset %d", offset)

    // 5. Wait for server to acknowledge the record is durably written
    if err := stream.WaitForOffset(offset); err != nil {
        log.Fatal(err)
    }
    log.Println("Record confirmed by server")
}
```

### Protocol Buffer Ingestion (Production Recommended)

For type-safe, efficient ingestion in production:

```go
import (
    "google.golang.org/protobuf/proto"
    "google.golang.org/protobuf/types/descriptorpb"
)

// 1. Load descriptor from generated files
descriptorBytes, err := os.ReadFile("path/to/schema.descriptor")
if err != nil {
    log.Fatal(err)
}

descriptor := &descriptorpb.DescriptorProto{}
if err := proto.Unmarshal(descriptorBytes, descriptor); err != nil {
    log.Fatal(err)
}

// 2. Create stream for Proto records
options := zerobus.DefaultStreamConfigurationOptions()
options.RecordType = zerobus.RecordTypeProto

stream, err := sdk.CreateStream(
    zerobus.TableProperties{
        TableName:       "catalog.schema.table",
        DescriptorProto: descriptorBytes,
    },
    clientID,
    clientSecret,
    options,
)

// 3. Send proto-encoded record to server
// Returns an offset that identifies this record in the ingestion sequence
offset, err := stream.IngestRecordOffset(protoBytes)
if err != nil {
    log.Fatal(err)
}
log.Printf("Record queued with offset %d", offset)

// 4. Optionally wait for server acknowledgment
if err := stream.WaitForOffset(offset); err != nil {
    log.Fatal(err)
}
log.Println("Record confirmed by server")
```

### Next Steps

- See [Usage Guide](#usage-guide) for detailed step-by-step documentation
- See [Examples](#examples) for complete working examples you can run
- See [Configuration Options](#configuration-options) to tune performance

## Repository Structure

```
go/
├── zerobus.go                      # Main SDK and stream implementation
├── ffi.go                          # CGO bindings to Rust FFI
├── ffi_test.go                     # CGO bindings tests
├── errors.go                       # Error types
├── types.go                        # Type definitions
├── ack.go                          # Acknowledgment handling
├── build.go                        # Build utilities and go:generate
├── build_rust.sh                   # Rust build script
├── go.mod                          # Go module definition
│
├── examples/                       # Working examples
│   ├── json/                       # JSON ingestion examples
│   │   ├── single/                 # Single record ingestion
│   │   │   ├── main.go
│   │   │   └── go.mod
│   │   └── batch/                  # Batch ingestion
│   │       ├── main.go
│   │       └── go.mod
│   └── proto/                      # Protocol Buffer examples
│       ├── single/                 # Single record ingestion
│       │   ├── main.go
│       │   └── go.mod
│       ├── batch/                  # Batch ingestion
│       │   ├── main.go
│       │   └── go.mod
│       ├── air_quality.proto       # Example proto schema
│       ├── pb/                     # Generated proto code
│       └── go.mod                  # Shared proto module
│
├── Makefile                        # Build automation
├── README.md                       # This file
├── CHANGELOG.md                    # Version history
├── CONTRIBUTING.md                 # Contribution guidelines
└── NOTICE                          # Third-party attribution
```

### Key Components

- **Root directory** - The main Go SDK library
- **`zerobus-ffi/`** - Rust FFI wrapper for high-performance ingestion
- **`examples/`** - Complete working examples demonstrating SDK usage
- **`Makefile`** - Standard make targets for building, testing, and linting

## How It Works

### Architecture Overview

```
┌─────────────────┐
│   Go SDK        │  ← Idiomatic Go API (this package)
│   (zerobus)     │
└────────┬────────┘
         │ CGO (Static Linking)
┌────────▼────────┐
│  Rust FFI Crate │  ← C-compatible wrapper
│ (zerobus-ffi)   │
└────────┬────────┘
         │
┌────────▼────────┐
│  Rust SDK       │  ← Core async implementation (Tokio)
│  (rust/)        │     - gRPC bidirectional streaming
└────────┬────────┘     - OAuth 2.0 authentication
         │              - Automatic recovery
         ▼
   Databricks
 Zerobus Service
```

The Go SDK consists of three layers:

1. **Go SDK Layer** - Provides idiomatic Go APIs, manages object lifecycles, and handles Go-specific error handling
2. **FFI Layer** - C-compatible interface that bridges Go and Rust, handling memory management across the boundary
3. **Rust Core** - High-performance async implementation with gRPC streaming, OAuth, and automatic recovery

### Data Flow

1. **Ingestion** - Your app calls `stream.IngestRecordOffset(data)` or `stream.IngestRecordsOffset(batch)`
2. **Buffering** - Records are placed in the landing zone with logical offsets
3. **Sending** - Sender task sends records over gRPC with physical offsets
4. **Acknowledgment** - Receiver task gets server ack and resolves the offset
5. **Recovery** - If connection fails, supervisor reconnects and resends unacked records

### Authentication Flow

The SDK uses OAuth 2.0 client credentials flow:

1. SDK constructs authorization request with Unity Catalog privileges
2. Sends request to `{uc_endpoint}/oidc/v1/token` with client credentials
3. Token includes scoped permissions for the specific table
4. Token is attached to gRPC metadata as Bearer token
5. Fresh tokens are fetched automatically on each connection

### Custom Authentication

For advanced use cases, you can implement the `HeadersProvider` interface to supply your own authentication headers. This is useful for integrating with a different OAuth provider, using a centralized token caching service, or implementing alternative authentication mechanisms.

> **Note:** The headers you provide must still conform to the authentication protocol expected by the Zerobus service. The default OAuth implementation serves as the reference for the required headers (`authorization` and `x-databricks-zerobus-table-name`). This feature provides flexibility in *how* you source your credentials, not in changing the authentication protocol itself.

**Example:**

```go
import zerobus "github.com/databricks/zerobus-sdk/go"

// Implement the HeadersProvider interface.
type MyCustomAuthProvider struct {
    tableName string
}

func (p *MyCustomAuthProvider) GetHeaders() (map[string]string, error) {
    // Custom logic to fetch and cache a token would go here.
    return map[string]string{
        "authorization":                    "Bearer <your-token>",
        "x-databricks-zerobus-table-name": p.tableName,
    }, nil
}

func example(sdk *zerobus.ZerobusSdk, tableProps zerobus.TableProperties) error {
    customProvider := &MyCustomAuthProvider{tableName: "catalog.schema.table"}

    stream, err := sdk.CreateStreamWithHeadersProvider(
        tableProps,
        customProvider,
        nil,
    )
    if err != nil {
        return err
    }
    defer stream.Close()

    offset, _ := stream.IngestRecordOffset(`{"data": "value"}`)
    log.Printf("Ingested at offset: %d", offset)
    return nil
}
```

**Common use cases:**

- **Token caching**: Implement custom token refresh logic
- **Alternative auth mechanisms**: Use different authentication providers
- **Dynamic credentials**: Fetch credentials on-demand from secret managers

## Usage Guide

This section provides detailed step-by-step documentation. For a quick start, see [Quick Start](#quick-start). For working examples, see [Examples](#examples).

The SDK supports two approaches for data serialization:

1. **JSON** - Simpler approach that uses JSON strings. No schema generation required, making it ideal for quick prototyping. See [`examples/README.md`](examples/README.md) for a complete example.
2. **Protocol Buffers** - Type-safe approach with schema validation at compile time. Recommended for production use cases. This guide focuses on the Protocol Buffers approach.

For JSON-based ingestion, you can skip the schema generation step and directly pass JSON strings to `IngestRecordOffset()`.

### 1. Initialize the SDK

Create an SDK instance with your Databricks workspace endpoints:

```go
// For AWS
sdk, err := zerobus.NewZerobusSdk(
    "https://your-shard-id.zerobus.region.cloud.databricks.com",
    "https://your-workspace.cloud.databricks.com",
)

// For Azure
sdk, err := zerobus.NewZerobusSdk(
    "https://your-shard-id.zerobus.region.azuredatabricks.net",
    "https://your-workspace.azuredatabricks.net",
)

if err != nil {
    log.Fatal(err)
}
defer sdk.Free()
```

### 2. Configure Authentication

The SDK handles authentication automatically. You just need to provide your OAuth credentials:

```go
clientID := os.Getenv("DATABRICKS_CLIENT_ID")
clientSecret := os.Getenv("DATABRICKS_CLIENT_SECRET")
```

See the examples directory for how to obtain OAuth credentials.

### 3. Create a Stream

Configure table properties and stream options:

```go
options := zerobus.DefaultStreamConfigurationOptions()
options.MaxInflightRequests = 10000
options.Recovery = true
options.RecoveryRetries = 4
options.RecordType = zerobus.RecordTypeJson

stream, err := sdk.CreateStream(
    zerobus.TableProperties{
        TableName: "catalog.schema.table",
    },
    clientID,
    clientSecret,
    options,
)
if err != nil {
    log.Fatal(err)
}
defer stream.Close()
```

### 4. Ingest Data

**Single record:**

```go
// JSON (string) - queues record and returns offset
offset, err := stream.IngestRecordOffset(`{"id": 1, "value": "hello"}`)
if err != nil {
    log.Fatal(err)
}
log.Printf("Record queued at offset: %d", offset)
```

**Batch ingestion for high throughput:**

```go
// Ingest multiple records at once
records := []interface{}{
    `{"id": 1, "value": "first"}`,
    `{"id": 2, "value": "second"}`,
    `{"id": 3, "value": "third"}`,
}
batchOffset, err := stream.IngestRecordsOffset(records)
if err != nil {
    log.Fatal(err)
}
log.Printf("Batch queued with offset: %d", batchOffset)
```

**High throughput pattern:**

```go
// Ingest many records without waiting
for i := 0; i < 100000; i++ {
    jsonData := fmt.Sprintf(`{"id": %d, "timestamp": %d}`, i, time.Now().Unix())
    offset, err := stream.IngestRecordOffset(jsonData)
    if err != nil {
        log.Printf("Record %d failed: %v", i, err)
        continue
    }

    // Optional: log progress
    if i%10000 == 0 {
        log.Printf("Ingested %d records, latest offset: %d", i, offset)
    }
}

// Wait for all records to be acknowledged
stream.Flush()
```

**Concurrent ingestion with goroutines:**

```go
var wg sync.WaitGroup
errCh := make(chan error, 100)

for i := 0; i < 100; i++ {
    wg.Add(1)
    go func(id int) {
        defer wg.Done()

        data := fmt.Sprintf(`{"id": %d}`, id)
        offset, err := stream.IngestRecordOffset(data)
        if err != nil {
            errCh <- err
            return
        }
        log.Printf("Record %d queued at offset %d", id, offset)
    }(id)
}

wg.Wait()
close(errCh)

// Check for errors
for err := range errCh {
    log.Printf("Ingestion error: %v", err)
}
```

**Concurrent ingestion with multiple streams:**

```go
var wg sync.WaitGroup
for partition := 0; partition < 4; partition++ {
    wg.Add(1)
    go func(p int) {
        defer wg.Done()

        // Each goroutine gets its own stream
        stream, err := sdk.CreateStream(tableProps, clientID, clientSecret, options)
        if err != nil {
            log.Fatal(err)
        }
        defer stream.Close()

        for i := p * 25000; i < (p+1)*25000; i++ {
            data := fmt.Sprintf(`{"id": %d}`, i)
            offset, err := stream.IngestRecordOffset(data)
            if err != nil {
                log.Printf("Failed to ingest: %v", err)
                continue
            }
            // Note: stream.Close() will flush all pending records
        }
    }(p)
}
wg.Wait()
```

### 5. Handle Acknowledgments

The recommended `IngestRecordOffset()` and `IngestRecordsOffset()` methods return offsets directly (after queuing):
- `IngestRecordOffset()` returns `int64`
- `IngestRecordsOffset()` returns `int64` (one offset for the entire batch)

```go
// Ingest and get offset, after queuing the record
offset, err := stream.IngestRecordOffset(data)
if err != nil {
    log.Fatal(err)
}
log.Printf("Record queued with offset: %d", offset)

// Wait for acknowledgment when needed
if err := stream.WaitForOffset(offset); err != nil {
    log.Fatal(err)
}
log.Printf("Record confirmed at offset: %d", offset)

// For batches, the method returns one offset for the entire batch
batch := []interface{}{data1, data2, data3}
batchOffset, err := stream.IngestRecordsOffset(batch)
if err != nil {
    log.Fatal(err)
}
log.Printf("Batch queued with offset: %d", batchOffset)

// Wait for the entire batch to be acknowledged
if err := stream.WaitForOffset(batchOffset); err != nil {
    log.Fatal(err)
}
log.Println("Batch confirmed")

// High-throughput:
for i := 0; i < 1000; i++ {
    _, _ := stream.IngestRecordOffset(record)
}

// Use Flush() to wait for all pending acknowledgments at once
stream.Flush()
```

### 6. Error Handling

Handle ingestion errors appropriately:

```go
offset, err := stream.IngestRecordOffset(data)
if err != nil {
    // Check if error is retryable
    if zerobusErr, ok := err.(*zerobus.ZerobusError); ok {
        if zerobusErr.Retryable() {
            // Transient error, SDK will auto-recover if Recovery is enabled
            log.Printf("Retryable error: %v", err)
        } else {
            // Fatal error, requires manual intervention
            log.Fatalf("Fatal error: %v", err)
        }
    }
}

log.Printf("Record queued at offset: %d", offset)
```

### 7. Close the Stream

Always close streams to ensure data is flushed:

```go
// Close gracefully (flushes automatically)
if err := stream.Close(); err != nil {
    log.Fatal(err)
}
```

If the stream fails, retrieve unacknowledged records:

```go
if err := stream.Close(); err != nil {
    // Stream failed, get unacked records
    unacked, err := stream.GetUnackedRecords()
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Failed to ack %d records", len(unacked))
    // Retry with a new stream
}
```

## Configuration Options

### StreamConfigurationOptions

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `MaxInflightRequests` | `uint64` | 1,000,000 | Maximum number of in-flight requests |
| `Recovery` | `bool` | true | Enable automatic stream recovery on failure |
| `RecoveryTimeoutMs` | `uint64` | 15,000 | Timeout for recovery operations (ms) |
| `RecoveryBackoffMs` | `uint64` | 2,000 | Delay between recovery retry attempts (ms) |
| `RecoveryRetries` | `uint32` | 4 | Maximum number of recovery attempts |
| `FlushTimeoutMs` | `uint64` | 300,000 | Timeout for flush operations (ms) |
| `ServerLackOfAckTimeoutMs` | `uint64` | 60,000 | Timeout waiting for server acks (ms) |
| `RecordType` | `int` | Proto | Record type: `RecordTypeProto` or `RecordTypeJson` |
| `StreamPausedMaxWaitTimeMs` | `*uint64` | nil | Max time to wait during graceful close when server sends pause signal (`nil` = wait full server duration, `0` = immediate, `x` = min(x, server_duration)) |

**Example:**

```go
options := zerobus.DefaultStreamConfigurationOptions()
options.MaxInflightRequests = 50000
options.RecoveryRetries = 10
options.FlushTimeoutMs = 600000
options.RecordType = zerobus.RecordTypeJson
```

## Error Handling

The SDK categorizes errors as **retryable** or **non-retryable**:

### Retryable Errors
Auto-recovered if `Recovery` is enabled:
- Network failures
- Connection timeouts
- Temporary server errors
- Stream closed by server

### Non-Retryable Errors
Require manual intervention:
- Invalid OAuth credentials
- Invalid table name
- Schema mismatch
- Authentication failure
- Permission denied

**Check if an error is retryable:**

```go
offset, err := stream.IngestRecordOffset(data)
if err != nil {
    if zerobusErr, ok := err.(*zerobus.ZerobusError); ok {
        if zerobusErr.Retryable() {
            log.Printf("Retryable error, SDK will auto-recover: %v", err)
            // Optionally implement custom retry logic
        } else {
            log.Fatalf("Fatal error, manual intervention needed: %v", err)
        }
    }
}
```

## Examples

The `examples/` directory contains complete, runnable examples organized by format and ingestion pattern:

**JSON Examples:**
- **`examples/json/single/`** - Single record ingestion with JSON
- **`examples/json/batch/`** - Batch ingestion with JSON

**Protocol Buffer Examples:**
- **`examples/proto/single/`** - Single record ingestion with protobuf
- **`examples/proto/batch/`** - Batch ingestion with protobuf

**Arrow Flight Examples (Experimental):**
- **`examples/arrow/`** - Arrow RecordBatch ingestion via Arrow Flight protocol

**To run an example:**

```bash
# Navigate to an example directory
cd examples/json/single

# Set your credentials
export ZEROBUS_SERVER_ENDPOINT="https://your-zerobus-endpoint.databricks.com"
export DATABRICKS_WORKSPACE_URL="https://your-workspace.databricks.com"
export DATABRICKS_CLIENT_ID="your-client-id"
export DATABRICKS_CLIENT_SECRET="your-client-secret"
export ZEROBUS_TABLE_NAME="catalog.schema.table"

# Run the example
go run main.go
```

Each example includes detailed comments and demonstrates best practices for production use. See [`examples/README.md`](examples/README.md) for complete setup instructions, prerequisites, and detailed comparisons between examples.

## Arrow Flight Ingestion (Experimental)

> **Experimental/Unsupported**: Arrow Flight ingestion is experimental and not yet supported for production use. The API may change in future releases.

The SDK supports high-throughput ingestion of Apache Arrow RecordBatches via the Arrow Flight protocol. This is useful for pipelines that already work with columnar Arrow data.

See [`examples/arrow/`](examples/arrow/) for a complete working example and setup instructions.

## Tests

Tests are located in the repository and can be run using:

```bash
# Run all tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Run tests with coverage
go test -cover ./...

# Run specific test file
go test -v ./ffi_test.go
```

The test suite includes:
- **Unit tests** - Test individual functions and components
- **FFI tests** - Test the CGO bindings to Rust
- **Integration tests** - Test end-to-end functionality

## Performance Benchmarks

The following results were obtained running against a local server with a local table. These numbers should not be expected in production environments as they don't account for network latency. All benchmarks used a single stream without parallelism.

### Go SDK

```
Record Size | Throughput
------------|------------
     20 B   |     9.59 MB/s
    220 B   |    98.39 MB/s
    750 B   |   265.54 MB/s
  10000 B   |   463.98 MB/s
```

### Rust SDK (for comparison)

```
Record Size | Throughput
------------|------------
     20 B   |     9.29 MB/s
    220 B   |   103.11 MB/s
    750 B   |   282.10 MB/s
  10000 B   |   465.47 MB/s
```

**Note:** These benchmarks demonstrate comparable performance between the Go and Rust SDKs, which is expected since both use the same Rust core via FFI. The variations are within expected margins for local testing. Production throughput will vary significantly based on network conditions, server load, and system configuration.

## Best Practices

1. **Reuse SDK Instances** - Create one `ZerobusSdk` per application and reuse for multiple streams
2. **Always Close Streams** - Use `defer stream.Close()` to ensure all data is flushed
3. **Choose the Right Ingestion Method**:
   - Use `IngestRecordsOffset()` for high throughput batch ingestion
   - Use `IngestRecordOffset()` when processing records individually
   - Both return offsets directly; use `WaitForOffset()` to explicitly wait for acknowledgments
   - The older `IngestRecord()` method is deprecated
4. **Tune Inflight Limits** - Adjust `MaxInflightRequests` based on memory and throughput needs
5. **Enable Recovery** - Always set `Recovery: true` in production environments
6. **Use Batch Ingestion** - For high throughput, ingest many records before calling `Flush()`
7. **Monitor Errors** - Log and alert on non-retryable errors
8. **Use Protocol Buffers for Production** - More efficient than JSON for high-volume scenarios
9. **Secure Credentials** - Never hardcode secrets; use environment variables or secret managers
10. **Test Recovery** - Simulate failures to verify your error handling logic
11. **One Stream Per Goroutine** - Don't share streams across goroutines; create separate streams for concurrent ingestion

## Migration Guide

### Migrating from IngestRecord (Deprecated) to IngestRecordOffset

As of v0.2.0, the `IngestRecord()` method is deprecated in favor of the simpler `IngestRecordOffset()` method. Both APIs continue to work, but we recommend migrating to the new API.

**Old API (Deprecated but still works):**
```go
// IngestRecord queues the record and returns a RecordAck wrapper
ack, err := stream.IngestRecord(`{"id": 1, "message": "Hello"}`)
if err != nil {
    log.Fatal(err)
}

// Await() blocks until the server acknowledges the record
offset, err := ack.Await()
if err != nil {
    log.Fatal(err)
}
log.Printf("Ingested and acknowledged at offset %d", offset)
```

**New API (Recommended):**
```go
// Ingest and get offset directly
offset, err := stream.IngestRecordOffset(`{"id": 1, "message": "Hello"}`)
if err != nil {
    log.Fatal(err)
}
log.Printf("Ingested at offset %d", offset)

// Optionally wait for server acknowledgment
if err := stream.WaitForOffset(offset); err != nil {
    log.Fatal(err)
}
log.Printf("Record acknowledged at offset %d", offset)
```

**Why migrate?**
- Simpler API with fewer lines of code
- More idiomatic Go (direct return values)
- Explicit control over when to wait for acknowledgments
- No wrapper types to understand
- Better performance (don't wait unless you need to)

**Note:** `IngestRecordOffset()` returns immediately after queuing the record. Use `WaitForOffset()` to explicitly wait for server acknowledgment when needed. For concurrent ingestion, use goroutines:

```go
var wg sync.WaitGroup
for i := 0; i < 100; i++ {
    wg.Add(1)
    go func(record string) {
        defer wg.Done()
        offset, err := stream.IngestRecordOffset(record)
        if err != nil {
            log.Printf("Failed: %v", err)
            return
        }
        log.Printf("Ingested at offset %d", offset)
    }(myRecord)
}
wg.Wait()
```

## API Reference

### `ZerobusSdk`

Main entry point for the SDK.

**Constructor:**
```go
func NewZerobusSdk(zerobusEndpoint, unityCatalogURL string) (*ZerobusSdk, error)
```

Creates a new SDK instance.

- `zerobusEndpoint` - Zerobus gRPC service endpoint
- `unityCatalogURL` - Unity Catalog URL for OAuth token acquisition

**Methods:**

```go
func (sdk *ZerobusSdk) CreateStream(
    tableProps TableProperties,
    clientID, clientSecret string,
    options *StreamConfigurationOptions,
) (*ZerobusStream, error)
```

Creates a new ingestion stream with OAuth authentication.

```go
func (sdk *ZerobusSdk) CreateStreamWithHeadersProvider(
    tableProps TableProperties,
    headersProvider HeadersProvider,
    options *StreamConfigurationOptions,
) (*ZerobusStream, error)
```

Creates a new ingestion stream with a custom headers provider for advanced authentication. Use this when you need custom authentication logic (e.g., custom token caching, or alternative auth providers).

**Example:**
```go
provider := &MyCustomAuthProvider{}
stream, err := sdk.CreateStreamWithHeadersProvider(
    tableProps,
    provider,
    options,
)
```

```go
func (sdk *ZerobusSdk) Free()
```

Explicitly releases SDK resources. Called automatically by finalizer.

### `ZerobusStream`

Represents an active bidirectional streaming connection.

**Methods:**

```go
func (st *ZerobusStream) IngestRecordOffset(payload interface{}) (int64, error)
```

Sends a record to the Zerobus server for ingestion. This method queues the record and returns an offset that identifies the record's position in the ingestion sequence. The offset can later be used to wait for server acknowledgment that the record has been durably written.

**What happens:**
1. Your record is queued for transmission to the server
2. An offset (logical sequence number) is assigned to the record
3. The offset is returned immediately - you don't need to wait for server acknowledgment
4. Use `WaitForOffset()` when you need to confirm the server has durably stored the record

Accepts either:
- `string` for JSON-encoded records
- `[]byte` for Protocol Buffer-encoded records

**Example:**
```go
// Send record and get its offset
offset, err := stream.IngestRecordOffset(`{"id": 1}`)
if err != nil {
    log.Fatal(err)
}
log.Printf("Record queued with offset %d", offset)

// Later, wait for server confirmation
if err := stream.WaitForOffset(offset); err != nil {
    log.Fatal(err)
}
log.Println("Server confirmed record is durable")
```

```go
func (st *ZerobusStream) IngestRecordsOffset(records []interface{}) (int64, error)
```

Sends multiple records to the server in a single batch operation. This is more efficient than sending records individually when you have many records to ingest. Returns one offset for the whole batch.

**What happens:**
1. All records in the batch are queued for transmission as a single unit
2. The entire batch is assigned one offset
3. That single offset is returned
4. Use `WaitForOffset()` with that offset to confirm the entire batch is durable

Accepts a slice where each element is either:
- `string` for JSON-encoded records
- `[]byte` for Protocol Buffer-encoded records

All records in the batch must be of the same type. Returns one offset for the batch, or `-1` if the batch is empty.

**Example:**
```go
// Send batch of records - the entire batch gets ONE offset
records := []interface{}{
    `{"id": 1, "value": "first"}`,
    `{"id": 2, "value": "second"}`,
    `{"id": 3, "value": "third"}`,
}
batchOffset, err := stream.IngestRecordsOffset(records)
if err != nil {
    log.Fatal(err)
}
log.Printf("Batch queued with offset %d", batchOffset)

// Wait for server to acknowledge the entire batch
if err := stream.WaitForOffset(batchOffset); err != nil {
    log.Fatal(err)
}
log.Println("Entire batch confirmed by server")
```

```go
func (st *ZerobusStream) WaitForOffset(offset int64) error
```

Waits for the server to acknowledge that a specific record has been durably written. This method blocks until the server confirms the record at the given offset is persisted, or returns an error if the record fails.

**When to use:**
- After sending critical records that must be confirmed before proceeding
- To implement checkpoint-based processing (wait for batches at certain intervals)
- When you need explicit confirmation before marking work as complete

Unlike `Flush()` which waits for all pending records, this waits only for a specific offset, allowing more granular control.

**Example:**
```go
// Send multiple records
offset1, _ := stream.IngestRecordOffset(`{"id": 1}`)
offset2, _ := stream.IngestRecordOffset(`{"id": 2}`)
offset3, _ := stream.IngestRecordOffset(`{"id": 3}`)

// Only wait for confirmation that record 3 is durable
// (records 1 and 2 will also be durable since offsets are sequential)
if err := stream.WaitForOffset(offset3); err != nil {
    log.Printf("Record at offset %d failed: %v", offset3, err)
}
log.Println("All three records confirmed by server")
```

```go
func (st *ZerobusStream) GetUnackedRecords() ([]interface{}, error)
```

Returns a snapshot of all records that have been sent to the server but not yet confirmed as durably written.

**IMPORTANT:** This method should **only be called after the stream has closed or failed**. Calling it on an active stream will return an error.

**What you get:**
- A copy of all pending records still waiting for server acknowledgment
- Empty slice if all records have been acknowledged
- Each element is either `string` (JSON) or `[]byte` (protobuf)

**When to use:**
- After stream failure to retrieve unacknowledged records for retry
- After `Close()` fails to see which records weren't durably written
- For implementing custom retry logic after stream errors

**Note:** This creates a memory snapshot of pending data. For large numbers of unacked records, this can temporarily increase memory usage.

**Example:**
```go
// Try to close the stream
if err := stream.Close(); err != nil {
    // Stream failed to close, check for unacked records
    unacked, err := stream.GetUnackedRecords()
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("%d records failed to be acknowledged", len(unacked))

    // Retry with a new stream
    for _, record := range unacked {
        newStream.IngestRecordOffset(record)
    }
}
```

```go
func (st *ZerobusStream) IngestRecord(payload interface{}) (*RecordAck, error)
```

**Deprecated:** Use `IngestRecordOffset()` instead for a simpler API.

This method exists for backwards compatibility. It ingests a record and returns a `*RecordAck` that wraps the offset (available immediately).

**Example:**
```go
ack, err := stream.IngestRecord(`{"id": 1}`)
if err != nil {
    log.Fatal(err)
}
offset, err := ack.Await()  // Returns immediately with cached offset
```

```go
func (st *ZerobusStream) Flush() error
```

Waits for the server to acknowledge all records that have been sent. This ensures that all pending records are durably written before continuing. Blocks until all in-flight records receive confirmation from the server.

**When to use:**
- Before closing a stream to ensure no data is lost
- At checkpoints where you need to guarantee all previous records are durable
- When you want to wait for everything, not just specific offsets

**Example:**
```go
// Send many records
for i := 0; i < 1000; i++ {
    stream.IngestRecordOffset(data)
}

// Wait for all of them to be confirmed
if err := stream.Flush(); err != nil {
    log.Fatal(err)
}
log.Println("All 1000 records confirmed by server")
```

```go
func (st *ZerobusStream) Close() error
```

Gracefully shuts down the stream, first waiting for all pending records to be acknowledged, then releasing resources. Always call this when done with a stream to avoid data loss.

**What happens:**
1. Waits for all pending records to be acknowledged (like calling `Flush()`)
2. Closes the connection to the server
3. Frees SDK resources

Always use `defer stream.Close()` to ensure cleanup even if errors occur.

### `RecordAck` (Deprecated)

**Deprecated:** This type is maintained for backwards compatibility only. Use `IngestRecordOffset()` instead of `IngestRecord()` to get the offset directly.

Represents an acknowledgment for an ingested record. The offset is available immediately after ingestion.

**Methods:**

```go
func (ack *RecordAck) Await() (int64, error)
```

Returns the offset for the ingested record. Returns immediately since the offset is already available.

**Example:**
```go
ack, _ := stream.IngestRecord(data)  // Deprecated
offset, err := ack.Await()
if err != nil {
    log.Printf("Record failed: %v", err)
}
```

**Prefer the new API:**
```go
offset, err := stream.IngestRecordOffset(data)  // Recommended
```

### `HeadersProvider`

Interface for providing custom authentication headers.

```go
type HeadersProvider interface {
    // GetHeaders returns authentication headers.
    // Called by the SDK when authentication is needed.
    GetHeaders() (map[string]string, error)
}
```

**Example implementation:**
```go
type CustomProvider struct{}

func (p *CustomProvider) GetHeaders() (map[string]string, error) {
    return map[string]string{
        "authorization": "Bearer token",
        "x-databricks-zerobus-table-name": "catalog.schema.table",
    }, nil
}
```

### `TableProperties`

Configuration for the target table.

```go
type TableProperties struct {
    TableName       string
    DescriptorProto []byte
}
```

- `TableName` - Full table name (e.g., "catalog.schema.table")
- `DescriptorProto` - Optional Protocol buffer descriptor loaded from generated files (required for Proto record type, nil for JSON)

### `StreamConfigurationOptions`

Stream behavior configuration. See [Configuration Options](#configuration-options) for details.

### `ZerobusError`

Error type for all SDK operations.

```go
type ZerobusError struct {
    Message     string
    IsRetryable bool
}
```

**Methods:**

```go
func (e *ZerobusError) Error() string
```

Returns the error message.

```go
func (e *ZerobusError) Retryable() bool
```

Returns `true` if the error can be automatically recovered by the SDK.

### `ZerobusArrowStream` (Experimental)

Represents an active Arrow Flight ingestion stream.

**Methods:**

```go
func (s *ZerobusArrowStream) IngestBatch(ipcBytes []byte) (int64, error)
```

Ingests an Arrow RecordBatch serialized as Arrow IPC stream bytes. Returns the logical offset.

```go
func (s *ZerobusArrowStream) WaitForOffset(offset int64) error
```

Blocks until the server acknowledges the batch at the given offset.

```go
func (s *ZerobusArrowStream) Flush() error
```

Waits for all pending batches to be acknowledged.

```go
func (s *ZerobusArrowStream) Close() error
```

Flushes and closes the stream.

```go
func (s *ZerobusArrowStream) GetUnackedBatches() ([][]byte, error)
```

Returns unacknowledged batches as Arrow IPC bytes. Call only after stream failure.

### `ArrowStreamConfigurationOptions` (Experimental)

```go
type ArrowStreamConfigurationOptions struct {
    MaxInflightBatches uint64
    Recovery           bool
    RecoveryTimeoutMs  uint64
    RecoveryBackoffMs  uint64
    RecoveryRetries    uint32
    IpcCompression     IpcCompressionType  // None, LZ4Frame, or Zstd
}
```

Use `DefaultArrowStreamConfigurationOptions()` to get sensible defaults.

**`ZerobusSdk` Arrow methods:**

```go
func (s *ZerobusSdk) CreateArrowStream(
    tableName string,
    schemaIpcBytes []byte,
    clientID, clientSecret string,
    options *ArrowStreamConfigurationOptions,
) (*ZerobusArrowStream, error)
```

```go
func (s *ZerobusSdk) CreateArrowStreamWithHeadersProvider(
    tableName string,
    schemaIpcBytes []byte,
    headersProvider HeadersProvider,
    options *ArrowStreamConfigurationOptions,
) (*ZerobusArrowStream, error)
```

## Building from Source

This section is for contributors and those who need to build the SDK from source. If you just want to use the SDK, see [Installation](#installation) instead.

### Basic Build

```bash
git clone https://github.com/databricks/zerobus-sdk.git
cd zerobus-sdk/go
make build
```

### Build Specific Components

```bash
# Build only Rust FFI
make build-rust

# Build only Go SDK
make build-go

# Build examples
make examples

# Run tests
make test

# Format code
make fmt

# Run linters
make lint
```

### Platform-Specific Build Notes

#### Windows
The build system automatically compiles Rust with the GNU toolchain (`x86_64-pc-windows-gnu`) for compatibility with Go's MinGW-based CGO. This ensures the MSVC and GNU ABIs don't conflict.

#### Linux
Standard GCC toolchain is used. Install build tools with:
```bash
sudo apt-get install build-essential  # Ubuntu/Debian
sudo dnf install gcc                   # Fedora/RHEL
```

#### macOS
Uses the system Clang toolchain. Install with:
```bash
xcode-select --install
```

### Troubleshooting

**Installing Rust**

If you need to install Rust, use the official installer:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

After installation, restart your terminal or run:
```bash
source $HOME/.cargo/env
```

Verify installation:
```bash
rustc --version
cargo --version
```

**Enabling Rust SDK Logs**

The SDK includes detailed tracing from the underlying Rust core. You can enable these logs to debug issues or understand SDK behavior.

**Set the `RUST_LOG` environment variable before running your Go application:**

```bash
# Show all logs at info level and above
export RUST_LOG=info
go run your_app.go

# Show debug logs (more verbose)
export RUST_LOG=debug

# Show trace logs (very verbose, shows everything)
export RUST_LOG=trace

# Show only Zerobus SDK logs at debug level
export RUST_LOG=databricks_zerobus_ingest_sdk=debug

# Multiple targets with different levels
export RUST_LOG=databricks_zerobus_ingest_sdk=debug,tokio=warn
```

**Example output with `RUST_LOG=debug`:**

```
2026-01-27T13:45:23.456Z DEBUG databricks_zerobus_ingest_sdk: Creating stream for table catalog.schema.table
2026-01-27T13:45:23.567Z DEBUG databricks_zerobus_ingest_sdk: Ingesting record at offset_id=0 record_count=1
2026-01-27T13:45:23.678Z DEBUG databricks_zerobus_ingest_sdk: Record at offset 0 acknowledged
```

**Log Levels:**
- `error` - Only errors
- `warn` - Warnings and errors
- `info` - General information (recommended for production)
- `debug` - Detailed debugging information
- `trace` - Very verbose, includes all internal operations

**Note:** Logs are written to stderr and will appear in your application's error output.

**Common Build Issues**

**"cargo not found"**
- Ensure Rust is installed and `~/.cargo/bin` is in your PATH
- Restart your terminal after installing Rust

**"linker 'cc' not found"** (Linux)
- Install build tools: `sudo apt-get install build-essential`

**"xcrun: error: unable to find utility 'cc'"** (macOS)
- Install Xcode tools: `xcode-select --install`

**"undefined reference to __chkstk"** (Windows)
- Ensure you're using MinGW-w64, not just MinGW
- The SDK automatically builds for the GNU target

## Community and Contributing

This is an open source project. We welcome contributions, feedback, and bug reports.

- **[Contributing Guide](https://github.com/databricks/zerobus-sdk/blob/main/go/CONTRIBUTING.md)**: Go-specific development setup and workflow.
- **[General Contributing Guide](https://github.com/databricks/zerobus-sdk/blob/main/CONTRIBUTING.md)**: Pull request process, commit requirements, and policies.
- **[Changelog](https://github.com/databricks/zerobus-sdk/blob/main/go/CHANGELOG.md)**: See the history of changes in the SDK.
- **[Security Policy](https://github.com/databricks/zerobus-sdk/blob/main/SECURITY.md)**: Read about our security process and how to report vulnerabilities.
- **[Developer Certificate of Origin (DCO)](https://github.com/databricks/zerobus-sdk/blob/main/DCO)**: Understand the agreement for contributions.
- **[Open Source Attributions](https://github.com/databricks/zerobus-sdk/blob/main/go/NOTICE)**: See a list of the open source libraries we use.

## License

This SDK is licensed under the Databricks License. See the [LICENSE](https://github.com/databricks/zerobus-sdk/blob/main/LICENSE) file for the full license text. The license is also available online at [https://www.databricks.com/legal/db-license](https://www.databricks.com/legal/db-license).

## Requirements

- **Go 1.21+**
- **CGO enabled** (enabled by default)
- **C compiler** (gcc or clang)
- **Databricks** workspace with Zerobus access enabled
- **OAuth 2.0** client credentials (client ID and secret)
- **Unity Catalog** endpoint access
- **TLS** - Uses native OS certificate store

---

For issues, questions, or contributions, please visit the [GitHub repository](https://github.com/databricks/zerobus-sdk).
