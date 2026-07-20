# Databricks Zerobus Ingest SDK for .NET

High-throughput streaming ingestion SDK for Databricks Delta tables over gRPC.

## Features

- **High throughput** — Native Rust backend with P/Invoke bindings for maximum performance
- **Offset-based API** — Minimal overhead; no per-record object allocation
- **Built-in retry and recovery** — Configurable automatic recovery from stream failures
- **Flexible configuration** — Fine-grained control over batching, timeouts, and callbacks
- **Protocol Buffers support** — Type-safe, compact wire format with schema compilation
- **JSON ingestion** — Schema-free JSON ingestion without protobuf schemas
- **Arrow Flight (Beta)** — Columnar, batched ingestion for analytics workloads
- **OAuth 2.0 authentication** — Service principal support via Databricks OAuth
- **Cross-platform** — Windows (x64), Linux (glibc x64, arm64), macOS (x64, Apple Silicon)

## Architecture

```
┌──────────────────────────────────────────┐
│              .NET Application            │
│  ZerobusSdk / ZerobusProtoStream<T> /    │
│  ZerobusJsonStream / ZerobusArrowStream  │
├──────────────────────────────────────────┤
│          BaseZerobusStream (P/Invoke)    │
├──────────────────────────────────────────┤
│     Native Rust SDK (zerobus_ffi)        │
│  Tokio runtime · gRPC · HTTP/2 · Arrow   │
└──────────────────────────────────────────┘
```

Direct native calls avoid .NET gRPC overhead. The offset-based API eliminates per-record `Task<T>` allocation.

## Requirements

**Runtime:** .NET 8.0+ or .NET Standard 2.0 compatible runtime. A Databricks workspace with Zerobus access enabled.

**Supported Platforms:**

| Platform | Architecture | Notes |
|---|---|---|
| Windows | x86_64 | |
| Linux | x86_64, aarch64 | glibc 2.26+ (Amazon Linux 2 compatible) |
| macOS | x86_64, Apple Silicon | |

**Dependencies:**
- `Google.Protobuf` 3.28.2 (for protobuf streams)
- Apache Arrow library (optional — only needed for Arrow Flight ingestion)

**Build requirements:** .NET 8.0 SDK or later.

## Quick Start User Guide

### Prerequisites

You need:
- A [Databricks workspace](https://databricks.com) URL and workspace ID
- A Delta table created with `USING DELTA`
- A service principal with OAuth client ID, client secret, and SQL-level permissions (`USE CATALOG`, `USE SCHEMA`, `SELECT`, `MODIFY`) on the target table

### Installation

**Option 1: NuGet (Recommended)**

```bash
dotnet add package Databricks.Zerobus.Ingest
```

**Option 2: Build from Source**

```bash
git clone https://github.com/databricks/zerobus-sdk.git
cd zerobus-sdk/dotnet
dotnet restore --configfile NuGet.Config
dotnet build
```

Place the native libraries in `src/Databricks.Zerobus/runtimes/{rid}/native/` or set the `ZEROBUS_NATIVE_LIB_PATH` environment variable.

### Write Client Code

The idiomatic flow is ingest in a loop, then `Flush()` once at the end.

```csharp
using Databricks.Zerobus;

const string serverEndpoint = "https://my-workspace.databricks.com";
const string unityCatalogUrl = "https://my-workspace.databricks.com/api/2.1/unity-catalog";
const string tableName = "my_catalog.my_schema.my_table";

using var sdk = new ZerobusSdk(serverEndpoint, unityCatalogUrl);

await using var stream = await sdk.StreamBuilder()
    .Table(tableName)
    .OAuth("client-id", "client-secret")
    .Json()
    .BuildAsync();

// Ingest records — returns as soon as the record is queued
for (int i = 0; i < 100; i++)
{
    stream.IngestRecord($"{{\"id\": {i}, \"name\": \"item-{i}\"}}");
}

// Confirm durability — acks are ordered, so the last offset confirms all
stream.Flush();

Console.WriteLine("Successfully ingested 100 records!");
```

### Acknowledgments and Throughput

`IngestRecord()` returns immediately after queuing; the SDK handles sending and acknowledgment tracking in the background. To confirm durability, call `Flush()`, which returns once everything queued so far is acknowledged.

The idiomatic pattern is ingest in a loop, then `Flush()` for bounded batches (or periodically for long-running streams). Alternatively, register ack callbacks via `AckCallback(onAck, onError)`.

Each `IngestRecord()` returns the record's offset. `WaitForOffset(offset)` blocks until that offset is acknowledged. Because acks are ordered, waiting on the last offset confirms the whole run.

> **Avoid calling `WaitForOffset()` after every record in a tight loop** — that limits throughput to one record per round-trip.

## Usage Examples

The `examples/` directory is organized by stream type: `JsonIngestion/`, `ProtoIngestion/`, `ArrowIngestion/` (Beta).

### Creating Streams (Stream Builder)

`ZerobusSdk.StreamBuilder()` is the recommended way to create a stream, exposing a single fluent API for all stream types.

**Proto:**

```csharp
byte[] descriptorBytes = ProtoSchema.FromUnityCatalogJson(ucTableJson).GetDescriptorBytes();

ZerobusProtoStream<MyMessage> protoStream = await sdk.StreamBuilder()
    .Table("catalog.schema.table")
    .Oauth(clientId, clientSecret)
    .CompiledProto(descriptorBytes)
    .BuildAsync<MyMessage>();
```

**JSON:**

```csharp
ZerobusJsonStream jsonStream = await sdk.StreamBuilder()
    .Table("catalog.schema.table")
    .Oauth(clientId, clientSecret)
    .Json()
    .BuildAsync();
```

**Arrow (Beta):**

```csharp
byte[] schemaIpcBytes = GetArrowSchemaIpcBytes(); // Use Apache.Arrow or any Arrow library

ZerobusArrowStream arrowStream = await sdk.StreamBuilder()
    .Table("catalog.schema.table")
    .Oauth(clientId, clientSecret)
    .Arrow(schemaIpcBytes)
    .IpcCompression(IPCCompressionType.Zstd)
    .BuildAsync();
```

Configuration is set directly on the builder (e.g., `.MaxInflightRecords(50000)`, `.Recovery(true)`). Arrow-specific options like `.MaxInflightBatches(...)` and `.IpcCompression(...)` are available after calling `.Arrow(...)`.

### Protocol Buffers Examples

Proto is best for production systems — type-safe, compact, fast.

```csharp
// Generate the proto descriptor from Unity Catalog
using var protoSchema = ProtoSchema.FromUnityCatalogJson(ucTableJson);
byte[] descriptorBytes = protoSchema.GetDescriptorBytes();

var stream = await sdk.StreamBuilder()
    .Table(tableName)
    .Oauth(clientId, clientSecret)
    .CompiledProto(descriptorBytes)
    .BuildAsync<MyProtoMessage>();

// Single record
var record = new MyProtoMessage { Id = 1, Name = "test" };
long offset = stream.IngestRecord(record);

// Batch
var records = new List<MyProtoMessage> { /* ... */ };
long? lastOffset = stream.IngestRecords(records);
stream.Flush();
```

### JSON Examples

JSON is best for rapid prototyping and flexible schemas. No protobuf types required.

```csharp
var stream = await sdk.StreamBuilder()
    .Table(tableName)
    .Oauth(clientId, clientSecret)
    .Json()
    .BuildAsync();

// Single record
stream.IngestRecord("{\"id\": 1, \"name\": \"test\"}");

// Batch
var jsonRecords = new[] { "{\"a\": 1}", "{\"a\": 2}" };
stream.IngestRecords(jsonRecords);
stream.Flush();
```

### Arrow Flight Examples (Beta)

Arrow is best for columnar data, wide/numeric schemas, or applications that already produce Apache Arrow RecordBatches.

> **Beta disclaimer:** Arrow Flight ingestion is in Beta. The API is stabilizing but may still change before reaching GA.

```csharp
// Serialize schema via Apache.Arrow
byte[] schemaIpcBytes = SerializeSchema(schema);

var stream = await sdk.StreamBuilder()
    .Table(tableName)
    .Oauth(clientId, clientSecret)
    .Arrow(schemaIpcBytes)
    .MaxInflightBatches(10_000)
    .IpcCompression(IPCCompressionType.Zstd)
    .BuildAsync();

long offset = stream.IngestBatch(recordBatchIpcBytes);
stream.WaitForOffset(offset);
```

## API Style

The SDK uses an **offset-based API** — every `IngestRecord()` / `IngestRecords()` returns a `long` offset immediately after queuing. The offset is a lightweight handle you can wait on later via `WaitForOffset()` or `Flush()`. No per-record object allocation.

```csharp
long lastOffset = 0;
for (int i = 0; i < 1_000_000; i++)
{
    lastOffset = stream.IngestRecord($"{{}}");
}
stream.WaitForOffset(lastOffset); // Ingest in a loop, then confirm durability once
```

## Choose Your Serialization Format

| Format | Best For | Pros | Cons |
|---|---|---|---|
| Protocol Buffers | Production systems | Type-safe, compact, fast | Requires schema compilation |
| JSON | Prototyping, flexible schemas | Human-readable, no compilation | Larger payload, slower |
| Arrow Flight (Beta) | Columnar/analytics, wide/numeric schemas, Arrow-native apps | High throughput, native Arrow types, optional IPC compression | Extra deps, API may change |

## Configuration

### Stream Configuration Options

| Option | Default | Description |
|---|---|---|
| `MaxInflightRecords` | 1000000 | Max unacknowledged records |
| `Recovery` | true | Enable auto recovery |
| `RecoveryTimeoutMs` | 15000 | Recovery timeout (ms) |
| `RecoveryBackoffMs` | 2000 | Delay between recovery attempts (ms) |
| `RecoveryRetries` | 4 | Max recovery attempts |
| `FlushTimeoutMs` | 300000 | Flush timeout (ms) |
| `ServerLackOfAckTimeoutMs` | 60000 | Server ack timeout (ms) |
| `OnAck` / `OnError` | None | Callbacks for record acknowledgment |

### Arrow Stream Configuration Options (Beta)

| Option | Default | Description |
|---|---|---|
| `MaxInflightBatches` | 10000 | Max unacknowledged Arrow batches |
| `Recovery` | true | Enable auto recovery |
| `RecoveryTimeoutMs` | 15000 | Recovery timeout (ms) |
| `RecoveryBackoffMs` | 2000 | Delay between recovery attempts (ms) |
| `RecoveryRetries` | 4 | Max recovery attempts |
| `ServerLackOfAckTimeoutMs` | 60000 | Server ack timeout (ms) |
| `FlushTimeoutMs` | 300000 | Flush timeout (ms) |
| `ConnectionTimeoutMs` | 30000 | gRPC connection timeout (ms) |
| `IpcCompression` | None | IPC compression codec |
| `StreamPausedMaxWaitTimeMs` | 0 | Max wait during paused state on graceful close. Negative = wait full server duration |

## Logging

The .NET SDK has minimal managed-side logging. Most detailed logging (token generation, gRPC, retries) is handled internally by the native Rust SDK. Control Rust-side logging via the `RUST_LOG` environment variable:

```bash
export RUST_LOG=debug
```

## Error Handling

Two exception types:

- **`ZerobusException`** — Base exception. May be retryable (`IsRetryable = true`) for network issues or temporary server errors.
- **`NonRetriableException`** — Fatal errors (invalid credentials, missing table). No retry.

```csharp
try
{
    stream.IngestRecord(json);
}
catch (NonRetriableException ex)
{
    // Fatal — don't retry
    Console.Error.WriteLine($"Non-retriable error: {ex.Message}");
}
catch (ZerobusException ex) when (ex.IsRetryable)
{
    // Retry with backoff
}
```

## API Reference

### ZerobusSdk

**Constructors:**
- `ZerobusSdk(string serverEndpoint, string unityCatalogEndpoint)`
- `ZerobusSdk(string serverEndpoint, string unityCatalogEndpoint, string applicationName)` — applicationName is appended to the HTTP user-agent header. Conventionally `"<product>/<version>"`.

**Static factories:**
- `ZerobusSdk.CreateBuilder(string serverEndpoint, string unityCatalogEndpoint)` — returns `SdkBuilder` for advanced configuration

**Methods:**
- `StreamBuilder()` — Returns `StreamBuilder`. The recommended way to create streams.
- `Dispose()` — Closes the SDK and frees native resources.

### StreamBuilder

Fluent builder returned by `ZerobusSdk.StreamBuilder()`. Shared methods:

| Method | Description |
|---|---|
| `.Table(name)` | Full UC table name (catalog.schema.table) |
| `.Oauth(clientId, secret)` | Service principal credentials |
| `.MaxInflightRecords(n)` | Buffer size (default: 1M) |
| `.Recovery(bool)` | Auto-recovery on failure (default: true) |
| `.RecoveryTimeoutMs(n)` | Recovery timeout (default: 15s) |
| `.RecoveryBackoffMs(n)` | Recovery backoff (default: 2s) |
| `.RecoveryRetries(n)` | Max recovery attempts (default: 4) |
| `.ServerLackOfAckTimeoutMs(n)` | Server ack timeout (default: 60s) |
| `.FlushTimeoutMs(n)` | Flush timeout (default: 5 min) |
| `.AckCallback(onAck, onError)` | Acknowledgment callbacks |

Format selection methods that return typed sub-builders:

- `.Json()` → `JsonStreamBuilder` with `BuildAsync()` returning `Task<ZerobusJsonStream>`
- `.CompiledProto(byte[])` → `ProtoStreamBuilder` with `BuildAsync<T>()` returning `Task<ZerobusProtoStream<T>>`
- `.Arrow(byte[])` → `ArrowStreamBuilder` (Beta) with `BuildAsync()` returning `Task<ZerobusArrowStream>`, plus additional methods: `MaxInflightBatches()`, `ConnectionTimeoutMs()`, `IpcCompression()`, `StreamPausedMaxWaitTimeMs()`

### ZerobusProtoStream\<T\>

**Single Record:**
- `long IngestRecord(T record)` — ingests protobuf message, returns offset immediately
- `long IngestRecord(byte[] encodedBytes)` — ingests pre-encoded bytes

**Batch:**
- `long? IngestRecords(IEnumerable<T> records)`
- `long? IngestRecords(IReadOnlyList<byte[]> encodedRecords)`

**Recovery:**
- `IReadOnlyList<byte[]> GetUnackedRecords()`
- `IReadOnlyList<T> GetUnackedRecords(MessageParser<T> parser)`

**Lifecycle:** `WaitForOffset()`, `Flush()`, `Close()`, `IsClosed`, `Dispose()`, `TableName`, `Options`, `ClientId`, `ClientSecret`

### ZerobusJsonStream

**Single Record:**
- `long IngestRecord(string json)`

**Batch:**
- `long? IngestRecords(IReadOnlyList<string> jsonRecords)`
- `long? IngestRecords(IEnumerable<string> jsonRecords)`

**Recovery:**
- `IReadOnlyList<string> GetUnackedRecords()`
- `IReadOnlyList<byte[]> GetUnackedRecordBytes()`

**Lifecycle:** Same as ZerobusProtoStream.

### ZerobusArrowStream (Beta)

- `long IngestBatch(byte[] ipcBytes)` — serializes to Arrow IPC, queues it, returns offset
- `IReadOnlyList<EncodedBatch> GetUnackedBatches()`
- Same lifecycle methods: `WaitForOffset()`, `Flush()`, `Close()`, `IsClosed`, `Dispose()`

### StreamConfigurationOptions

Built via `StreamConfigurationOptions.NewBuilder()`. Chainable setters: `SetMaxInflightRecords`, `SetRecovery`, `SetRecoveryTimeoutMs`, `SetRecoveryBackoffMs`, `SetRecoveryRetries`, `SetFlushTimeoutMs`, `SetServerLackOfAckTimeoutMs`, `SetAckCallback(onAck, onError)`, `SetStreamPausedMaxWaitTimeMs`, `SetCallbackMaxWaitTimeMs`. Static `Default` and `NewBuilder()` methods.

### ArrowStreamConfigurationOptions (Beta)

Built via `ArrowStreamConfigurationOptions.NewBuilder()`. Chainable setters: `SetMaxInflightBatches`, `SetRecovery`, `SetRecoveryTimeoutMs`, `SetRecoveryBackoffMs`, `SetRecoveryRetries`, `SetServerLackOfAckTimeoutMs`, `SetFlushTimeoutMs`, `SetConnectionTimeoutMs`, `SetIpcCompression`, `SetStreamPausedMaxWaitTimeMs`. Static `Default` and `NewBuilder()` methods.

### IPCCompressionType

Three values: `None` (default, -1), `Lz4Frame` (fast, modest ratio, 0), `Zstd` (higher ratio at higher CPU cost, 1). Enable compression only when network bandwidth limits throughput.

### ZerobusException

Base exception. Constructors: `(string message)`, `(string message, bool isRetryable)`, `(string message, bool isRetryable, Exception? innerException)`. Property: `bool IsRetryable`.

### NonRetriableException

Extends `ZerobusException`. Always `IsRetryable = false`.

### AckOnAckDelegate / AckOnErrorDelegate

```csharp
delegate void AckOnAckDelegate(long offsetId);
delegate void AckOnErrorDelegate(long offsetId, string errorMessage);
```

Observe acknowledgments as they arrive on a background thread while you keep ingesting. Callbacks must be thread-safe and lightweight.

## Best Practices

1. **Reuse one `ZerobusSdk` per application**
2. **Always close streams** in `using` statements or `finally` blocks
3. **Use offset-based API for high throughput** — avoids `Task<T>` overhead
4. **Ingest in a loop then flush** — confirm durability once. Per-record waits only when a specific record must be confirmed before continuing
5. **Batch records** with `IngestRecords()`
6. **Configure `maxInflightRecords`** based on throughput/memory needs
7. **Distinguish retryable vs non-retryable** errors via `ZerobusException.IsRetryable`
8. **Use `AckCallback`** for non-blocking durability monitoring
9. **Use `ProtoSchema.FromUnityCatalogJson()`** to generate proto descriptors from table schemas
10. **Choose the right API**: `IngestRecord()` + final `Flush()` for high throughput; `IngestRecord()` + per-record `WaitForOffset()` for specific record confirmation
11. **Recovery pattern**: use `sdk.RecreateStreamAsync(closedStream)` for automatic re-ingestion of unacknowledged records, or manually use `GetUnackedRecords()` after close

## Community and Contributing

- [Contributing Guide](CONTRIBUTING.md) — .NET-specific development setup and workflow
- [General Contributing Guide](https://github.com/databricks/zerobus-sdk/blob/main/CONTRIBUTING.md) — PR process, commit requirements, policies
- [Changelog](CHANGELOG.md)
- [Security Policy](https://github.com/databricks/zerobus-sdk/blob/main/SECURITY.md)
- Developer Certificate of Origin (DCO) — all commits must be signed off (`git commit -s`)
- Open Source Attributions — NOTICE file

## License

Apache License 2.0 — see [LICENSE](LICENSE).
