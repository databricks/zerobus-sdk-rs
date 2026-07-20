# .NET SDK

A .NET wrapper around the Rust core via P/Invoke (C FFI).

## Client Code Patterns (Read Before Writing or Reviewing Examples/Docs)

Ingestion is asynchronous. The methods `IngestRecord()` and `IngestRecords()` return control the moment a record or batch enters a queue; the SDK handles transmission and acknowledgment tracking in the background.

Four distinct patterns:

1. **Idiomatic flow** — Ingest repeatedly in a loop, then issue a single `Flush()` call after a bounded batch (or periodically for long-running streams) to confirm durability. Alternatively, call `WaitForOffset()` on the last received offset, since acknowledgments are ordered and confirming the last one confirms everything prior.

2. **Async monitoring** — Register an ack callback (`AckOnAckDelegate` / `AckOnErrorDelegate`) to observe progress without blocking the ingest loop.

3. **Per-record `WaitForOffset()`** — Use this when a specific record must be acknowledged before proceeding. Avoid calling it after every record in a tight loop, as that limits throughput to one record per round-trip.

4. **Stream class preference** — Use `ZerobusProtoStream<T>` for protobuf ingestion or `ZerobusJsonStream` for JSON ingestion.

## Structure

```
dotnet/
├── src/Databricks.Zerobus/           # Main library
│   ├── Native/                       # P/Invoke layer
│   │   ├── NativeMethods.cs          # [DllImport] declarations
│   │   ├── NativeLibraryResolver.cs  # Platform lib loading
│   │   ├── InteropStructs.cs         # C struct marshaling
│   │   └── SafeHandles/              # SafeHandle subclasses
│   ├── ZerobusSdk.cs                 # Main SDK class

│   ├── ZerobusProtoStream.cs         # Proto ingestion stream
│   ├── ZerobusJsonStream.cs          # JSON ingestion stream
│   ├── ZerobusArrowStream.cs         # Arrow Flight stream (Beta)
│   ├── StreamBuilder.cs              # Fluent builder API
│   ├── StreamConfigurationOptions.cs # gRPC stream config
│   ├── ArrowStreamConfigurationOptions.cs # Arrow stream config
│   ├── ProtoSchema.cs                # UC schema → proto
│   └── ...                           # Exceptions, callbacks, enums
├── tests/                            # xUnit test suite
├── examples/                         # Runnable examples (JsonIngestion, ProtoIngestion, ArrowIngestion)
├── tools/                            # generate-proto CLI
└── Zerobus-DotNet.sln                # Solution file
```

The native library is built from `rust/ffi/` and bundled in the NuGet package for all supported platforms under `runtimes/{rid}/native/`.

## Build Commands

All commands are run from the `dotnet/` directory:

- `dotnet build` — Compile
- `dotnet test` — Run all tests
- `dotnet test --filter "FullyQualifiedName~ClassName"` — Run specific tests
- `dotnet format` — Auto-format code (whitespace + style)
- `dotnet format --verify-no-changes` — Check formatting (CI)
- `dotnet pack src/Databricks.Zerobus -c Release` — Create NuGet package
- `dotnet restore --configfile NuGet.Config` — Restore with clean package sources

## FFI Boundary: P/Invoke

The .NET SDK uses the C FFI from `rust/ffi/`:

1. **Handle pattern** — .NET holds an `IntPtr` wrapped in `SafeHandle` subclasses. Every native method receives this handle. If the handle is invalid after close, Rust panics or returns an error.

2. **No finalizers** — Unlike Go, Python, or TypeScript, .NET streams do not have GC-triggered cleanup. Users must call `Dispose()` explicitly or use `using` statements. Forgetting this causes native memory leakage. `SafeHandle` provides critical-finalization as a backstop.

3. **IDisposable** — Both SDK and stream classes implement `IDisposable`. Always prefer `using` statements.

4. **Async bridge** — `Task<T>` bridges Rust futures to .NET. Stream creation returns `Task<ZerobusJsonStream>`, `Task<ZerobusProtoStream<T>>`, or `Task<ZerobusArrowStream>`. `BuildAsync()` wraps the synchronous native stream creation in `Task.Run()` for non-blocking behavior.

5. **Thread safety** — The SDK is not thread-safe. Do not share SDK or stream instances across threads without external synchronization.

6. **Native library loading** — `NativeLibraryResolver.cs` detects the OS and architecture, resolves the correct `.so`, `.dylib`, or `.dll` from NuGet `runtimes/{rid}/native/`, development paths, or the `ZEROBUS_NATIVE_LIB_PATH` environment variable. On .NET 8+, it uses `NativeLibrary.SetDllImportResolver`. On netstandard2.0, it falls back to `LoadLibrary` / `dlopen`.

7. **Native method stability** — `[DllImport]` declarations in `NativeMethods.cs` are internal but must stay in sync with `rust/ffi/zerobus.h`. Changing one without the other causes `DllNotFoundException` or `EntryPointNotFoundException` at runtime.

## Breaking Change Rules

The public API is everything in the `Databricks.Zerobus` namespace with `public` visibility:

- Removing or renaming public classes, methods, or properties is breaking
- Changing method signatures (parameter types, return types) is breaking
- Deprecation requires `[Obsolete("message")]` with XML doc explaining the replacement
- Native method signatures (`internal static extern`) must stay synchronized with `rust/ffi/` — modifying one without the other causes runtime errors

## Performance Notes

- P/Invoke calls have non-trivial overhead (~50-100ns per crossing). Batch APIs reduce this cost.
- Proto descriptors are transmitted as `byte[]` with a copy at the boundary, a one-time cost at stream creation.
- Record payloads (`byte[]` for proto, strings for JSON) are copied across P/Invoke. For high throughput, prefer proto with batch ingestion.
- `GCHandle.Alloc` is used to pin managed buffers during native calls — buffers are unpinned immediately after each call.
- Native library resolution runs once at class initialization time.

## Changelog and Documentation

- Every PR that changes user-facing behavior must update `dotnet/NEXT_CHANGELOG.md` under the appropriate section
- Update `dotnet/README.md` if the change impacts usage, setup, or API surface
- Add or update examples in `dotnet/examples/` for new or modified APIs
- Add XML doc comments for all new public classes and methods

## Release

- The version source is `dotnet/src/Databricks.Zerobus/Databricks.Zerobus.csproj` (`<Version>x.y.z</Version>`)
- Tag format: `dotnet/v<semver>` — triggers `release-dotnet.yml` which downloads FFI native libs from `release-ffi.yml`, places them into `runtimes/{rid}/native/`, builds the NuGet package, and optionally publishes to NuGet.org
- The NuGet package bundles native libraries for all 5 platforms. The release depends on `rust/ffi/` — if Rust FFI code changed, both sides must be coordinated
- On a version bump PR: update the version in `Databricks.Zerobus.csproj`, move `NEXT_CHANGELOG.md` contents into `CHANGELOG.md`, and reset `NEXT_CHANGELOG.md`

## Config

- .NET 8.0+ and .NET Standard 2.0 compatibility
- Implicit usings + nullable reference types enabled
- EditorConfig-based formatting (`dotnet format`)
- Dependency: `Google.Protobuf` 3.28.2 (for protobuf streams)
- Apache.Arrow is optional — Arrow schema IPC bytes are passed as `byte[]`, so any Arrow library can be used
