# Databricks.Zerobus — .NET SDK

High-performance .NET SDK for streaming data ingestion into Databricks Delta tables using the Zerobus service. Built on the same Rust core as the Go SDK, exposed via P/Invoke (C FFI bindings).

## Requirements

- **.NET 8** or **.NET 10**
- **Rust toolchain** (for building the native `zerobus_ffi` library from source)

## Quick Start

```csharp
using Databricks.Zerobus;

// 1. Create SDK instance.
using var sdk = ZerobusSdk.CreateBuilder()
    .Endpoint("https://your-shard.zerobus.databricks.com")
    .UnityCatalogUrl("https://your-workspace.databricks.com")
    .Build();

// 2. Configure stream options.
var options = StreamConfigurationOptions.Default with
{
    RecordType = RecordType.Json,
};

// 3. Create stream.
using var stream = sdk.CreateStream(
    new TableProperties("catalog.schema.table"),
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

#### `CreateStream`

Creates a stream with OAuth 2.0 client credentials authentication.

```csharp
using var stream = sdk.CreateStream(
    new TableProperties("catalog.schema.table"),
    clientId,
    clientSecret,
    options);  // optional, defaults if null
```

#### `CreateStreamWithHeadersProvider`

Creates a stream with custom authentication headers.

```csharp
using var stream = sdk.CreateStreamWithHeadersProvider(
    new TableProperties("catalog.schema.table"),
    new MyHeadersProvider(),
    options);  // optional
```

### `ZerobusStream`

An active bidirectional gRPC stream for record ingestion. Thread-safe.

#### `IngestRecord`

Ingests a single record and returns its offset immediately (acknowledgment happens in background).

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
object[] unacked = stream.GetUnackedRecords();
```

#### `Close` / `Dispose`

`Close()` gracefully closes the stream (flushes first) but keeps the stream
readable for recovery (`GetUnackedRecords`, `RecreateStream`).
`Dispose()` frees native resources and should be called when recovery work is done.
`using` calls `Dispose()` automatically.

```csharp
stream.Close();
// or simply let `using` handle it
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
    RecordType = RecordType.Json,
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
│       ├── ZerobusSdk.cs                      # SDK entry point (IDisposable)
│       ├── ZerobusStream.cs                   # Stream for record ingestion (IDisposable)
│       ├── ZerobusException.cs                # Error type with IsRetryable
│       ├── IHeadersProvider.cs                # Custom auth interface
│       ├── RecordType.cs                      # Proto / Json / Unspecified enum
│       ├── StreamConfigurationOptions.cs      # Config record with defaults
│       ├── TableProperties.cs                 # Table name + optional descriptor
│       ├── Properties/
│       │   └── AssemblyInfo.cs
│       └── Native/                            # P/Invoke layer (internal)
│           ├── NativeBindings.cs              # Raw DllImport declarations
│           ├── NativeInterop.cs               # Safe wrappers + marshalling
│           └── HeadersProviderBridge.cs       # Managed→native callback bridge
├── tests/
│   ├── Zerobus.Tests/                         # Unit tests (NUnit)
│   └── Zerobus.IntegrationTests/              # Integration tests (NUnit + gRPC mock)
│       ├── Zerobus.IntegrationTests.csproj
│       ├── IntegrationTests.cs                # 11 integration tests
│       ├── MockZerobusServer.cs               # Mock gRPC server
│       ├── TestHelpers.cs                     # Fixtures, response builders, interceptor
│       └── Protos/
│           └── zerobus_service.proto          # Proto definition for gRPC stubs
└── examples/
    ├── JsonSingle/                            # Single JSON record ingestion
    ├── JsonBatch/                             # Batch JSON record ingestion
    ├── ProtoSingle/                           # Single protobuf record ingestion
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
