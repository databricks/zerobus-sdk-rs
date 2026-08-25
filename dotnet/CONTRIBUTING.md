# Contributing to the Zerobus SDK for .NET

Please read the [top-level CONTRIBUTING.md](https://github.com/databricks/zerobus-sdk/blob/main/CONTRIBUTING.md) first for general contribution guidelines, pull request process, commit requirements, and DCO/sign-off requirements.

This document covers .NET-specific development setup and workflow.

## Development Setup

### Prerequisites

- Git
- .NET SDK 10.0 or higher (the projects target both .NET 8 and .NET 10)
- Rust toolchain (`cargo`) - [Install Rust](https://rustup.rs/)
- Bash shell (used by `build_native.sh`)

### Setting Up Your Development Environment

1. **Clone the repository:**
	```bash
	git clone https://github.com/databricks/zerobus-sdk.git
	cd zerobus-sdk/dotnet
	```

2. **Build the project:**
	```bash
	make build
	```

	This will:
	- Build the Rust FFI layer (`zerobus_ffi`)
	- Build the .NET SDK
	- Place the native library in `src/Zerobus/runtimes/<RID>/native/`

3. **Run tests:**
	```bash
	make test
	```

## Coding Style

Code style is enforced by formatters in pull requests. We use `dotnet format` and `rustfmt`.

### Running the Formatter

Format your code before committing:

```bash
make fmt
```

This runs:
- `dotnet format` for .NET code
- `cargo fmt --all` for Rust FFI code

### Running Linters

Check your code for issues:

```bash
make lint
```

This runs:
- Rust linting via `cargo clippy --all -- -D warnings`
- .NET lint target currently prints a message and does not run standalone analyzers

## Testing

Run the full test suite:

```bash
make test
```

This runs:
- Rust FFI tests via `cargo test`
- .NET tests via `dotnet test -p:SkipNativeBuild=true`

### Running Tests Individually

- Unit tests:
  ```bash
  dotnet test tests/Zerobus.Tests
  ```

- Integration tests:
  ```bash
  dotnet test tests/Zerobus.IntegrationTests
  ```

Integration tests use a mock gRPC server and exercise the SDK through the native FFI layer.

## Working with the Native FFI Layer

The .NET SDK relies on the Rust FFI library in `../rust/ffi`.

When making FFI-related changes:

1. Update Rust code in `../rust/ffi/src/`
2. Update exported C API in `../rust/ffi/zerobus.h` if needed
3. Update .NET interop bindings in `src/Zerobus/Native/`
4. Rebuild native artifacts:
	```bash
	./build_native.sh
	```
5. Run tests:
	```bash
	make test
	```

### Native Build Notes

- `dotnet build` automatically invokes `build_native.sh` from MSBuild.
- To skip automatic native build (for example, when a prebuilt library is already present):
  ```bash
  dotnet build -p:SkipNativeBuild=true
  ```
- To force a native rebuild:
  ```bash
  ./build_native.sh --force
  ```

## NuGet lock files

Direct package versions live in `Directory.Packages.props`. The full restore graph (including transitives) is pinned in `packages.lock.json` next to each project that restores NuGet packages (the SDK, tests, and the Protobuf example). JSON examples have no package references and do not use lock files. CI restore runs in locked mode and fails if those files are stale.

After changing a version in `Directory.Packages.props`, regenerate the lock files:

```bash
dotnet restore
```

Commit the updated `packages.lock.json` files with the version change.

## Continuous Integration

All pull requests must pass CI checks.

Typical checks include:

- **formatting**: `dotnet format` and `cargo fmt`
- **lint**: Rust lint (`cargo clippy`)
- **test**: .NET and Rust test suites

You can view CI results in the GitHub Actions tab of your pull request.

## Makefile Targets

Available make targets:

- `make build` - Build both Rust FFI and .NET SDK
- `make build-rust` - Build only the Rust FFI layer
- `make build-rust-force` - Force rebuild Rust FFI artifacts
- `make build-dotnet` - Build only the .NET SDK
- `make clean` - Remove build artifacts
- `make fmt` - Format all code (Rust and .NET)
- `make fmt-dotnet` - Format .NET code
- `make fmt-rust` - Format Rust code
- `make lint` - Run linters on all code
- `make lint-dotnet` - Run .NET lint target (informational currently)
- `make lint-rust` - Run Rust clippy
- `make check` - Run formatting and lint checks
- `make test` - Run all tests
- `make test-rust` - Run Rust tests
- `make test-dotnet` - Run .NET tests
- `make examples` - Build all .NET examples
- `make help` - Show available targets
