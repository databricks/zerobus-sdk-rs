# Databricks.Zerobus — .NET SDK

High-performance .NET SDK for streaming data ingestion into Databricks Delta tables using the Zerobus service. Built on the same Rust core as the Go SDK, exposed via P/Invoke (C FFI bindings).

## Requirements

- **.NET 8** or **.NET 10**
- **Rust toolchain** (for building the native `zerobus_ffi` library from source)

## Quick Start

### Fluent StreamBuilder (recommended)

```csharp
using Databricks.Zerobus;

// 1. Create SDK instance.
using var sdk = ZerobusSdk.CreateBuilder()
    .Endpoint("https://your-shard.zerobus.databricks.com")
    .UnityCatalogUrl("https://your-workspace.databricks.com")
    .Build();

// 2. Create stream (fluent builder).
using var stream = sdk.StreamBuilder()
    .Table("catalog.schema.table")
    .OAuth(clientId, clientSecret)
    .MaxInflightRequests(50_000)
    .Json()
    .Build();

// 3. Ingest records.
long offset = stream.IngestRecord("""{"id": 1, "message": "Hello"}""");

// 4. Wait for acknowledgment.
stream.WaitForOffset(offset);
```

### Factory methods (also supported)

```csharp
// 2. Configure stream options.
var options = StreamConfigurationOptions.Default with
{
    MaxInflightRequests = 50_000,
};

// 3. Create stream (factory method).
using var stream = sdk.CreateJsonStream(
    "catalog.schema.table",
    clientId,
    clientSecret,
    options);

// 4. Ingest records.
long offset = stream.IngestRecord("""{"id": 1, "message": "Hello"}""");

// 5. Wait for acknowledgment.
stream.WaitForOffset(offset);
```

## Installation

### NuGet (when published)

```bash
dotnet add package Databricks.Zerobus
```

### From Source

```bash
cd dotnet
dotnet build
```

The build automatically invokes `build_native.sh` to compile the Rust FFI shared library and place it in the correct `runtimes/<RID>/native/` directory. You need `cargo` on your `PATH` (or in `~/.cargo/bin/`).

To skip the automatic native build (e.g. when the library is pre-built):

```bash
dotnet build -p:SkipNativeBuild=true
```

## API Reference

### `ZerobusSdk`

The main entry point. Manages the connection to Zerobus and Unity Catalog.

```csharp
using var sdk = ZerobusSdk.CreateBuilder()
    .Endpoint(zerobusEndpoint)
    .UnityCatalogUrl(unityCatalogUrl)
    .Build();
```

#### `CreateJsonStream`

Creates a JSON-only stream with OAuth 2.0 client credentials authentication.

```csharp
using var stream = sdk.CreateJsonStream(
    "catalog.schema.table",
    clientId,
    clientSecret,
    options);  // optional, defaults if null
```

#### `CreateProtoStream`

Creates a protobuf-only stream with OAuth 2.0 client credentials authentication.

```csharp
using var stream = sdk.CreateProtoStream(
    "catalog.schema.table",
    descriptorProto,
    clientId,
    clientSecret,
    options);  // optional, defaults if null
```

#### `CreateStream`

Creates the legacy untyped stream with OAuth 2.0 client credentials authentication.
Use this only when you intentionally need access to both JSON and protobuf overloads.
The SDK now validates the `RecordType`/`DescriptorProto` combination before calling Rust.

```csharp
using var stream = sdk.CreateStream(
    new TableProperties("catalog.schema.table"),
    clientId,
    clientSecret,
    options);  // optional, defaults if null
```

#### `CreateJsonStreamWithHeadersProvider`

Creates a JSON-only stream with custom authentication headers.

```csharp
using var stream = sdk.CreateJsonStreamWithHeadersProvider(
    "catalog.schema.table",
    new MyHeadersProvider(),
    options);  // optional
```

#### `CreateProtoStreamWithHeadersProvider`

Creates a protobuf-only stream with custom authentication headers.

```csharp
using var stream = sdk.CreateProtoStreamWithHeadersProvider(
    "catalog.schema.table",
    descriptorProto,
    new MyHeadersProvider(),
    options);  // optional
```

#### `CreateStreamWithHeadersProvider`

Creates a stream with custom authentication headers.

```csharp
using var stream = sdk.CreateStreamWithHeadersProvider(
    new TableProperties("catalog.schema.table"),
    new MyHeadersProvider(),
    options);  // optional
```

#### `RecreateStream`

Recreates a stream for recovery after the original stream has failed or closed.

Important: `RecreateStream(stream)` transfers ownership from `stream` to the returned
stream. The input `stream` is disposed during recreation and must not be used afterward.
A later `Dispose()` on the original wrapper (for example at the end of a `using` scope)
is a no-op.

#### `CreateArrowStream`

Creates an Arrow Flight ingestion stream with OAuth 2.0 credentials. **(Beta)**

```csharp
using var stream = sdk.CreateArrowStream(
    "catalog.schema.table",
    schemaIpcBytes,
    clientId,
    clientSecret,
    options);  // ArrowStreamConfigurationOptions, optional

stream.IngestBatch(recordBatchIpcBytes);
stream.Flush();
```

Also available: `CreateArrowStreamAsync`, `CreateArrowStreamWithHeadersProvider`, `CreateArrowStreamWithHeadersProviderAsync`.

### `ProtoSchema`

Generates protobuf schemas from Unity Catalog table metadata. Useful for obtaining
descriptor bytes for `CreateProtoStream` without compiling `.proto` files manually.

```csharp
using var schema = ProtoSchema.FromUnityCatalogJson(ucTableJson);
byte[] descriptor = schema.GetDescriptorBytes();
byte[] protoBytes = schema.EncodeJson("""{"id": 1, "name": "test"}""");
```

### `StreamBuilder` (fluent API)

Alternative to factory methods — chain configuration then select the stream type.

```csharp
// JSON
using var stream = sdk.StreamBuilder()
    .Table("catalog.schema.table")
    .OAuth(clientId, clientSecret)
    .MaxInflightRequests(50_000)
    .Json()
    .Build();

// Protobuf
using var stream = sdk.StreamBuilder()
    .Table("catalog.schema.table")
    .OAuth(clientId, clientSecret)
    .CompiledProto(descriptorBytes)
    .Build();

// Arrow Flight
using var stream = sdk.StreamBuilder()
    .Table("catalog.schema.table")
    .OAuth(clientId, clientSecret)
    .Arrow(schemaIpcBytes)
    .IpcCompression(IPCCompressionType.Lz4Frame)
    .Build();
```

All sub-builders support `Build()` (sync) and `BuildAsync()` (async).

### `ZerobusArrowStream`

Arrow Flight ingestion stream for columnar data. Thread-safe, implements `IDisposable` and `IAsyncDisposable`.

```csharp
long offset = stream.IngestBatch(ipcBytes);
stream.WaitForOffset(offset);
stream.Flush();

// Async variants available
await stream.IngestBatchAsync(ipcBytes);
await stream.FlushAsync();

// Retrieve unacknowledged batches after close/failure
ArrowBatchInfo[] unacked = stream.GetUnackedBatches();
```

### `JsonZerobusStream` and `ProtoZerobusStream`

Typed stream wrappers expose only the matching ingest overloads while preserving the
same lifecycle APIs (`Flush`, `WaitForOffset`, `Close`, `Dispose`, `GetUnackedRecords`).

#### `JsonZerobusStream.IngestRecord`

```csharp
stream.IngestRecord("""{"field": "value"}""");
stream.Flush();
```

#### `ProtoZerobusStream.IngestRecord`

```csharp
byte[] protoBytes = myMessage.ToByteArray();
stream.IngestRecord(protoBytes);
stream.Flush();
```

### `ZerobusStream`

The untyped stream remains available for advanced callers. Thread-safe.

#### `IngestRecord`

Ingests a single record and returns its offset immediately (acknowledgment happens in background).
If you use this untyped API, JSON streams must set `RecordType.Json` and use the string overloads.
Proto streams must provide `DescriptorProto` and use the byte-oriented overloads.

```csharp
// JSON
long offset = stream.IngestRecord("""{"field": "value"}""");

// Protobuf
byte[] protoBytes = myMessage.ToByteArray();
long offset = stream.IngestRecord(protoBytes);
stream.WaitForOffset(offset);
```

#### `IngestRecords`

Ingests a batch of records and returns one offset for the whole batch.

```csharp
string[] records = [
    """{"device": "sensor-001", "temp": 20}""",
    """{"device": "sensor-002", "temp": 21}""",
];
long lastOffset = stream.IngestRecords(records);
stream.WaitForOffset(lastOffset);
```

#### `WaitForOffset` (sync)

Blocks until a specific offset is acknowledged by the server.

```csharp
stream.WaitForOffset(offset);
```

#### `Flush` (sync)

Blocks until all pending records are acknowledged.

```csharp
stream.Flush();
```

#### `GetUnackedRecords`

Retrieves unacknowledged records after stream failure (call after close/failure only).

```csharp
ReadOnlyMemory<byte>[] unacked = stream.GetUnackedRecords();
foreach (var payload in unacked)
{
    // Decode as UTF-8 if you know this stream ingests JSON.
    Console.WriteLine($"record bytes: {payload.Length}");
}
```

#### `Close` / `Dispose` / `DisposeAsync`

`Close()` gracefully closes the stream (flushes first) but keeps the stream
readable for recovery (`GetUnackedRecords`, `RecreateStream`).
If you call `RecreateStream(stream)`, the original stream object is disposed and
no longer usable.
`Dispose()` frees native resources and should be called when recovery work is done.
`using` calls `Dispose()` automatically.

```csharp
stream.Close();
// or let `using` / `await using` handle cleanup
```

### `IHeadersProvider`

Interface for custom authentication.

```csharp
public class CustomHeadersProvider : IHeadersProvider
{
    public IDictionary<string, string> GetHeaders()
    {
        return new Dictionary<string, string>
        {
            ["authorization"] = "Bearer " + GetToken(),
            ["x-databricks-zerobus-table-name"] = "catalog.schema.table",
        };
    }
}
```

### `StreamConfigurationOptions`

Use C# record `with` expressions to customise:

```csharp
var options = StreamConfigurationOptions.Default with
{
    MaxInflightRequests = 50_000,
    RecoveryRetries = 10,
};
```

| Property                    | Default   | Description                  |
| --------------------------- | --------- | ---------------------------- |
| `MaxInflightRequests`       | 1,000,000 | Backpressure control         |
| `Recovery`                  | `true`    | Auto-recovery on failures    |
| `RecoveryTimeoutMs`         | 15,000    | Timeout per recovery attempt |
| `RecoveryBackoffMs`         | 2,000     | Delay between retries        |
| `RecoveryRetries`           | 4         | Max recovery attempts        |
| `ServerLackOfAckTimeoutMs`  | 60,000    | Server ack timeout           |
| `FlushTimeoutMs`            | 300,000   | Flush timeout (5 min)        |
| `RecordType`                | `Proto`   | Proto / Json / Unspecified   |
| `StreamPausedMaxWaitTimeMs` | `null`    | Graceful close wait time     |

Typed factories set `RecordType` automatically. You only need to set it manually when using
the untyped `CreateStream` or `CreateStreamWithHeadersProvider` APIs.

### `ArrowStreamConfigurationOptions`

Use C# record `with` expressions to configure Arrow Flight streams:

```csharp
var options = ArrowStreamConfigurationOptions.Default with
{
    MaxInflightBatches = 5_000,
    IpcCompression = IPCCompressionType.Zstd,
};
```

| Property                     | Default | Description                            |
| ---------------------------- | ------- | -------------------------------------- |
| `MaxInflightBatches`         | 1,000   | Backpressure control for batches       |
| `Recovery`                   | `true`  | Auto-recovery on failures              |
| `RecoveryTimeoutMs`          | 15,000  | Timeout per recovery attempt           |
| `RecoveryBackoffMs`          | 2,000   | Delay between retries                  |
| `RecoveryRetries`            | 4       | Max recovery attempts                  |
| `ServerLackOfAckTimeoutMs`   | 60,000  | Server ack timeout                     |
| `FlushTimeoutMs`             | 300,000 | Flush timeout (5 min)                  |
| `ConnectionTimeoutMs`        | 30,000  | Connection timeout                     |
| `IpcCompression`             | `None`  | IPC compression (`None`, `Lz4Frame`, `Zstd`) |
| `StreamPausedMaxWaitTimeMs`  | -1      | Graceful close wait (-1 = full server duration) |

### Error Handling

Errors throw `ZerobusException` with an `IsRetryable` property:

```csharp
try
{
    long offset = stream.IngestRecord(data);
}
catch (ZerobusException ex) when (ex.IsRetryable)
{
    // Transient error — SDK auto-recovers when Recovery is enabled.
    Console.WriteLine($"Retryable error: {ex.RawMessage}");
}
catch (ZerobusException ex)
{
    // Fatal error — manual intervention needed.
    Console.WriteLine($"Fatal error: {ex.RawMessage}");
}
```

## Native Library Setup

The native `zerobus_ffi` shared library is built automatically when you run `dotnet build`. The MSBuild target invokes `build_native.sh`, which:

1. Detects your OS and architecture
2. Runs `cargo build --release` in the `zerobus-ffi` crate
3. Copies the shared library (`.dylib` / `.so` / `.dll`) to `src/Zerobus/runtimes/<RID>/native/`
4. Skips the rebuild if the library is already up to date

### Manual Build

You can also run the script directly:

```bash
cd dotnet
./build_native.sh           # Build for current platform
./build_native.sh --force   # Force rebuild
```

### Runtime Directories

The native library is placed in the standard .NET runtime identifier layout:

| Platform    | Path                                             |
| ----------- | ------------------------------------------------ |
| Linux x64   | `runtimes/linux-x64/native/libzerobus_ffi.so`    |
| Linux arm64 | `runtimes/linux-arm64/native/libzerobus_ffi.so`  |
| macOS x64   | `runtimes/osx-x64/native/libzerobus_ffi.dylib`   |
| macOS arm64 | `runtimes/osx-arm64/native/libzerobus_ffi.dylib` |
| Windows x64 | `runtimes/win-x64/native/zerobus_ffi.dll`        |

## Testing

### Unit Tests

Unit tests are isolated and do not require the native library:

```bash
dotnet test tests/Zerobus.Tests
```

### Integration Tests

Integration tests spin up a mock gRPC server (per test) and exercise the full SDK through the native FFI layer. They require the Rust toolchain to build the native library:

```bash
dotnet test tests/Zerobus.IntegrationTests
```

The integration tests cover:

| Test                                                | Scenario                                   |
| --------------------------------------------------- | ------------------------------------------ |
| `SuccessfulStreamCreation`                          | Stream creation succeeds                   |
| `TimeoutedStreamCreation`                           | Timeout when server responds slowly        |
| `NonRetriableErrorDuringStreamCreation`             | Non-retriable error (e.g. Unauthenticated) |
| `RetriableErrorWithoutRecoveryDuringStreamCreation` | Retriable error with recovery disabled     |
| `GracefulClose`                                     | Ingest record then close gracefully        |
| `IdempotentClose`                                   | Multiple `Close()` calls succeed           |
| `IngestAfterClose`                                  | Ingest after close throws                  |
| `IngestSingleRecord`                                | Single record ingest and ack               |
| `IngestMultipleRecords`                             | Multiple sequential records with ack       |
| `IngestBatchRecords`                                | Batch ingest of 5 records                  |
| `IngestRecordsAfterClose`                           | Batch ingest after close throws            |
| `ArrowStream_CreateAndDispose_DoesNotLeak`          | Arrow stream creation and disposal          |
| `ArrowStream_Close_AndDispose_NoError`              | Arrow stream close then dispose             |
| `ArrowStream_MultipleBatches_FlushThenClose`        | Arrow batch ingest, flush, and close        |
| `StreamBuilder_Arrow_BuildsWithSchema`              | Fluent Arrow stream via StreamBuilder       |

Each test gets its own mock gRPC server on a unique port, so all tests run in parallel.

### Running All Tests

```bash
dotnet test
```

## Project Structure

```
dotnet/
├── Zerobus.slnx                              # Solution file
├── Directory.Build.props                      # Shared build settings
├── build_native.sh                            # Rust FFI build script
├── README.md
├── src/
│   └── Zerobus/                               # Main SDK library
│       ├── Zerobus.csproj
│       ├── ZerobusSdk.cs                      # SDK entry point + Arrow stream factories
│       ├── ZerobusStream.cs                   # Streaming for JSON/Proto records
│       ├── ZerobusArrowStream.cs              # Arrow Flight ingestion stream (Beta)
│       ├── ZerobusException.cs                # Error type with IsRetryable
│       ├── IHeadersProvider.cs                # Custom auth interface
│       ├── RecordType.cs                      # Proto / Json / Unspecified enum
│       ├── StreamConfigurationOptions.cs      # Config record for JSON/Proto streams
│       ├── ArrowStreamConfigurationOptions.cs # Config record for Arrow Flight streams
│       ├── IPCCompressionType.cs              # Arrow IPC compression enum
│       ├── ArrowBatchInfo.cs                  # Unacked Arrow batch record
│       ├── ProtoSchema.cs                     # Protobuf schema from Unity Catalog
│       ├── StreamBuilder.cs                   # Fluent builder API
│       ├── TableProperties.cs                 # Table name + optional descriptor
│       ├── Properties/
│       │   └── AssemblyInfo.cs
│       └── Native/                            # P/Invoke layer (internal)
│           ├── NativeBindings.cs              # Raw DllImport declarations
│           ├── NativeInterop.cs               # Safe wrappers + marshalling
│           └── HeadersProviderBridge.cs       # Managed→native callback bridge
├── tests/
│   ├── Zerobus.Tests/                         # Unit tests (NUnit)
│   │   ├── StreamBuilderTests.cs              # Fluent builder validation
│   │   ├── ArrowStreamConfigurationOptionsTests.cs
│   │   └── ProtoSchemaTests.cs
│   └── Zerobus.IntegrationTests/              # Integration tests (NUnit + gRPC mock)
│       ├── Zerobus.IntegrationTests.csproj
│       ├── StreamCreationIntegrationTests.cs  # Stream creation scenarios
│       ├── IngestionIntegrationTests.cs       # Ingestion scenarios
│       ├── ArrowIntegrationTests.cs           # Arrow Flight integration tests
│       ├── LifecycleRecoveryIntegrationTests.cs
│       ├── MockZerobusServer.cs               # Mock gRPC server
│       ├── TestHelpers.cs                     # Fixtures, response builders, interceptor
│       └── Protos/
│           └── zerobus_service.proto          # Proto definition for gRPC stubs
└── examples/
    ├── JsonSingle/                            # Single JSON record ingestion
    ├── JsonBatch/                             # Batch JSON record ingestion
    ├── ProtoSingle/                           # Single protobuf record ingestion
    └── ProtoSchema/                           # ProtoSchema: UC → descriptor + JSON→proto
```

## Architecture

```
.NET SDK (Databricks.Zerobus)
    ↓ P/Invoke
Rust FFI (zerobus-ffi / libzerobus_ffi)
    ↓
Rust Core (databricks-zerobus-ingest-sdk)
    ↓ gRPC
Zerobus Service
```
