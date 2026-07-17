# Changelog

All notable changes to the Databricks Zerobus Ingest SDK for .NET.

## [0.1.0] - 2026-07-17

### Added
- Initial release of the .NET SDK
- JSON ingestion stream (`ZerobusJsonStream`) with single and batch ingestion
- Protocol Buffers ingestion stream (`ZerobusProtoStream<T>`) with typed and pre-serialized ingestion
- Arrow Flight ingestion stream (`ZerobusArrowStream`) — Beta
- `ZerobusSdk` with `CreateBuilder()` for advanced configuration
- `StreamBuilder` fluent API with JsonStreamBuilder, ProtoStreamBuilder, ArrowStreamBuilder
- `ProtoSchema` for generating protobuf descriptors from Unity Catalog table JSON
- `StreamConfigurationOptions` and `ArrowStreamConfigurationOptions` with builder pattern
- Acknowledgment callback support via `AckOnAckDelegate` and `AckOnErrorDelegate`
- Native library auto-resolution via `DllImportResolver` (.NET 8+) and `LoadLibrary`/`dlopen` (netstandard2.0)
- NuGet package with bundled native libraries for win-x64, linux-x64, linux-arm64, osx-x64, osx-arm64
- Exception hierarchy: `ZerobusException`, `NonRetriableException`
- Support for .NET 8.0 and .NET Standard 2.0
- Examples for JSON, protobuf, and Arrow ingestion
- `generate-proto` CLI tool for schema generation
