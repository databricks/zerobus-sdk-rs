# Databricks.Zerobus.Ingest.Sdk — .NET SDK

High-performance .NET SDK for streaming data ingestion into Databricks Delta tables using the Zerobus service. Built on the same Rust core as the Go SDK, exposed via P/Invoke (C FFI bindings).

## Requirements

- Consumers: .NET 8 or .NET 10
- Building from source: .NET 10 SDK and a Rust toolchain

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
    MaxInflightRequests = 50_000,
};

// 3. Create stream.
using var stream = sdk.CreateJsonStream(
    "catalog.schema.table",
    clientId,
    clientSecret,
    options);

// 4. Ingest records, then call Flush() once.
for (int id = 1; id <= 100; id++)
{
    stream.IngestRecord($$"""{"id": {{id}}, "message": "Hello"}""");
}
stream.Flush();
```

### Acknowledgments and throughput

Ingestion is asynchronous. `IngestRecord()` and `IngestRecords()` return as soon as the
record or batch is queued; the SDK sends it and tracks its acknowledgment in the background.
Call `Flush()` to wait until everything queued so far is acknowledged—once after a bounded
run, or periodically for a long-running stream. Each ingest also returns an offset.
`WaitForOffset(offset)` waits for that offset and every earlier one to be acknowledged.
Use it for targeted waits, but avoid waiting after every record: doing so limits throughput
to one record per round-trip.

## Installation

### NuGet (when published)

```bash
dotnet add package Databricks.Zerobus.Ingest.Sdk
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

### `JsonZerobusStream` and `ProtoZerobusStream`

Typed stream wrappers expose only the matching ingest overloads while preserving the
same lifecycle APIs (`Flush`, `WaitForOffset`, `Close`, `Dispose`, `GetUnackedRecords`).

#### `JsonZerobusStream.IngestRecord`

```csharp
for (int id = 1; id <= 100; id++)
{
    stream.IngestRecord($$"""{"id": {{id}}, "field": "value"}""");
}
stream.Flush();
```

#### `ProtoZerobusStream.IngestRecord`

```csharp
for (int id = 1; id <= 100; id++)
{
    byte[] protoBytes = myMessage.ToByteArray();
    stream.IngestRecord(protoBytes);
}
stream.Flush();
```

### `ZerobusStream`

The untyped stream remains available for advanced callers. Thread-safe.

#### `IngestRecord`

Ingests a single record and returns its assigned offset. The record is queued for background
sending and acknowledgment. With this untyped API, JSON streams must set
`RecordType.Json` and use the string overloads; protobuf streams must provide
`DescriptorProto` and use the byte-oriented overloads.

```csharp
var jsonOptions = options with { RecordType = RecordType.Json };
using var jsonStream = sdk.CreateStream(
    new TableProperties("catalog.schema.json_table"),
    clientId,
    clientSecret,
    jsonOptions);

for (int id = 1; id <= 100_000; id++)
{
    jsonStream.IngestRecord($$"""{"id": {{id}}, "field": "value"}""");

    // Flush periodically to bound memory on long-running streams.
    if (id % 10_000 == 0)
        jsonStream.Flush();
}
jsonStream.Flush();
```

The protobuf stream works the same way, using the byte-oriented overloads:

```csharp
var protoOptions = options with { RecordType = RecordType.Proto };
using var protoStream = sdk.CreateStream(
    new TableProperties("catalog.schema.proto_table", descriptorProto),
    clientId,
    clientSecret,
    protoOptions);

for (int id = 1; id <= 100; id++)
{
    protoStream.IngestRecord(myMessage.ToByteArray());
}
protoStream.Flush();
```

#### `IngestRecords`

Ingests multiple records as one batch and returns its assigned offset. The batch succeeds
or fails as a unit. Prefer this API in hot paths to amortize the per-call P/Invoke overhead.

```csharp
string[] records = [
    """{"device": "sensor-001", "temp": 20}""",
    """{"device": "sensor-002", "temp": 21}""",
];

stream.IngestRecords(records);
stream.Flush();
```

#### `WaitForOffset` (sync)

Waits for the server to acknowledge all records through the specified offset.
Acknowledgments are ordered, so waiting for the last offset also confirms every earlier
record. Use this for targeted waits; for a bulk run, `Flush()` is simpler. Avoid waiting
after every record, which limits throughput to one record per round-trip.

```csharp
long offset = stream.IngestRecord("""{"type": "control"}""");
stream.WaitForOffset(offset);
```

#### `Flush` (sync)

Waits for all records queued before the call to be acknowledged. Records queued while the
flush is in progress are not included.

```csharp
stream.Flush();
```

#### `GetUnackedRecords`

Retrieves unacknowledged records after stream failure (call after close/failure only). A flush timeout can leave the stream active, in which case `GetUnackedRecords()` throws until the stream closes.

```csharp
try
{
    stream.Flush();
}
catch (ZerobusException)
{
    // A flush timeout can leave the stream active. GetUnackedRecords
    // requires a closed or failed stream.
    try
    {
        var unacked = stream.GetUnackedRecords();
        Console.WriteLine($"Failed to acknowledge {unacked.Length} records");
    }
    catch (ZerobusException retrieval)
    {
        Console.WriteLine($"Could not inspect unacked records (stream may still be active): {retrieval.Message}");
    }
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

## Best Practices

1. Reuse one `ZerobusSdk` instance across multiple streams.
2. Use `using` / `await using`, or call `Close()` explicitly, so pending records are flushed.
3. Follow the [acknowledgment and throughput](#acknowledgments-and-throughput) guidance.
4. Prefer `IngestRecords()` in high-throughput paths to reduce P/Invoke overhead.
5. Tune `MaxInflightRequests` for your memory and throughput requirements.
6. Keep recovery enabled in production.
7. Log and alert on non-retryable errors.

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

| Platform               | RID                 | Path                                                | NuGet package payload today |
| ---------------------- | ------------------- | --------------------------------------------------- | --------------------------- |
| Linux x64 (glibc)      | `linux-x64`         | `runtimes/linux-x64/native/libzerobus_ffi.so`       | Included                    |
| Linux arm64 (glibc)    | `linux-arm64`       | `runtimes/linux-arm64/native/libzerobus_ffi.so`     | Included                    |
| Linux x64 (musl/Alpine) | `linux-musl-x64`    | `runtimes/linux-musl-x64/native/libzerobus_ffi.so`  | Included                    |
| Linux arm64 (musl)      | `linux-musl-arm64`  | `runtimes/linux-musl-arm64/native/libzerobus_ffi.so` | Included                    |
| Windows x64            | `win-x64`           | `runtimes/win-x64/native/zerobus_ffi.dll`           | Included                    |
| macOS x64              | `osx-x64`           | `runtimes/osx-x64/native/libzerobus_ffi.dylib`      | Source build only           |
| macOS arm64            | `osx-arm64`         | `runtimes/osx-arm64/native/libzerobus_ffi.dylib`    | Source build only           |

Published packages currently omit macOS native binaries. Source builds via
`build_native.sh` still produce and place the macOS `.dylib` in the runtime
layout above.

## Testing

### Unit Tests

Unit tests are isolated and do not require the native library:

```bash
dotnet test tests/Zerobus.Tests
```

### Integration Tests

Integration tests spin up a mock gRPC server per test. SDK tests exercise the native FFI layer, while mock-server tests use generated test-only gRPC types. The suite requires the Rust toolchain to build the native library:

```bash
dotnet test tests/Zerobus.IntegrationTests
```

The mock server and generated test client use the canonical schema at `../rust/sdk/zerobus_service.proto`.

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
| `AvroBatchUpdatesWriteCount`                        | Avro batch updates mock write count        |

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
├── Directory.Packages.props                   # Central NuGet version pins
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
│       ├── *IntegrationTests.cs               # SDK and mock integration tests
│       ├── MockZerobusServer.cs               # Mock gRPC server
│       └── TestHelpers.cs                     # Fixtures, response builders, interceptor
└── examples/
    ├── JsonSingle/                            # Single JSON record ingestion
    ├── JsonBatch/                             # Batch JSON record ingestion
    ├── ProtoSingle/                           # Single protobuf record ingestion
```

## Architecture

```
.NET SDK (Databricks.Zerobus.Ingest.Sdk)
    ↓ P/Invoke
Rust FFI (zerobus-ffi / libzerobus_ffi)
    ↓
Rust Core (databricks-zerobus-ingest-sdk)
    ↓ gRPC
Zerobus Service
```
