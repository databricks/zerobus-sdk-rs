# Contributing to the Zerobus .NET SDK

See the top-level [CONTRIBUTING.md](https://github.com/databricks/zerobus-sdk/blob/main/CONTRIBUTING.md) for general contribution guidelines, pull request process, and commit requirements. This document covers .NET-specific development setup and workflow.

## Development Setup

### Prerequisites

- [Git](https://git-scm.com/downloads)
- [.NET 8.0 SDK or later](https://dotnet.microsoft.com/download/dotnet/8.0)

### Setting Up Your Development Environment

1. **Clone the repository**

   ```bash
   git clone https://github.com/databricks/zerobus-sdk.git
   cd zerobus-sdk/dotnet
   ```

2. **Build**

   ```bash
   dotnet restore --configfile NuGet.Config
   dotnet build
   ```

3. **Run tests**

   ```bash
   dotnet test
   ```

> **Note:** The `NuGet.Config` file in `dotnet/` clears external package sources and uses only nuget.org. If you have corporate feeds configured in a parent `NuGet.Config`, adjust accordingly.

## Coding Style

Style is enforced by EditorConfig (`.editorconfig`). Use `dotnet format` for code formatting.

### Running the Formatter

```bash
dotnet format
```

This formats:
- C# code (whitespace, style, and analyzers)
- Imports organization and unused import removal
- XML doc comment formatting

### Checking Formatting

```bash
dotnet format --verify-no-changes
```

### Running Tests

```bash
dotnet test                                                # All tests
dotnet test --filter "FullyQualifiedName~StreamBuilderTests" # Specific test class
dotnet test --filter "FullyQualifiedName~Default_ReturnsOptions" # Specific test method
```

## Continuous Integration

All pull requests must pass CI checks:

- **fmt** — `dotnet format --verify-no-changes`
- **test** — `dotnet build` + `dotnet test` across Ubuntu, Windows, and macOS runners

Check the GitHub Actions tab of the pull request for results.

## Build Commands

| Command | Description |
|---|---|
| `dotnet clean` | Clean build outputs |
| `dotnet build` | Compile code |
| `dotnet test` | Run tests |
| `dotnet format` | Format code |
| `dotnet format --verify-no-changes` | Check formatting |
| `dotnet pack src/Databricks.Zerobus -c Release` | Create NuGet package |
| `dotnet restore --configfile NuGet.Config` | Restore with clean package sources |

## Project Structure

```
dotnet/
├── src/Databricks.Zerobus/     # Main library
│   ├── Native/                 # P/Invoke declarations and interop
│   │   ├── NativeMethods.cs    # [DllImport] extern methods
│   │   ├── NativeLibraryResolver.cs  # Cross-platform lib loading
│   │   ├── InteropStructs.cs   # C struct marshaling
│   │   └── SafeHandles/        # SafeHandle subclasses
│   ├── ZerobusSdk.cs           # Main SDK class
│   ├── StreamBuilder.cs        # Fluent builder API
│   ├── BaseZerobusStream.cs    # Abstract stream base class
│   ├── ZerobusProtoStream.cs   # Protobuf ingestion stream
│   ├── ZerobusJsonStream.cs    # JSON ingestion stream
│   ├── ZerobusArrowStream.cs   # Arrow Flight stream (Beta)

│   ├── StreamConfigurationOptions.cs      # gRPC stream config
│   ├── ArrowStreamConfigurationOptions.cs # Arrow stream config
│   ├── ProtoSchema.cs          # Unity Catalog → proto schema
│   ├── ZerobusException.cs     # Base exception type
│   ├── NonRetriableException.cs
│   ├── AckCallback.cs          # Delegate types
│   ├── EncodedBatch.cs         # Arrow batch wrapper
│   ├── HeadersProvider.cs      # Custom auth headers delegate
│   ├── IPCCompressionType.cs   # Compression enum
│   └── TableProperties.cs      # Table metadata
├── tests/Databricks.Zerobus.Tests/  # xUnit test suite
├── examples/                        # Runnable examples
│   ├── JsonIngestion/          # JSON single + batch
│   ├── ProtoIngestion/         # Protobuf single + batch
│   └── ArrowIngestion/         # Arrow Flight (Beta)
├── tools/GenerateProto/        # Schema generation CLI
├── .github/workflows/          # CI/CD
│   ├── ci-dotnet.yml           # Push/PR: build, test, pack
│   └── release-dotnet.yml      # Manual release to NuGet.org
└── Zerobus-DotNet.sln          # Solution file
```
