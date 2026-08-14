# Contributing to the Zerobus SDK for Go

Please read the [top-level CONTRIBUTING.md](https://github.com/databricks/zerobus-sdk/blob/main/CONTRIBUTING.md) first for general contribution guidelines, pull request process, and commit requirements.

This document covers Go-specific development setup and workflow.

## Development Setup

### Prerequisites

- Git
- Go 1.21 or higher
- Rust toolchain (cargo) - [Install Rust](https://rustup.rs/)
- CGO enabled (default on most systems)

### Setting Up Your Development Environment

1. **Clone the repository:**
   ```bash
   git clone https://github.com/databricks/zerobus-sdk.git
   cd zerobus-sdk/go
   ```

2. **Build the project:**
   ```bash
   make build
   ```

   This will:
   - Build the Rust FFI layer
   - Compile the Go SDK
   - Create a self-contained static library

3. **Run tests:**
   ```bash
   make test
   ```

## Coding Style

Code style is enforced by formatters in your pull request. We use `gofmt` and `rustfmt` to format our code.

### Running the Formatter

Format your code before committing:

```bash
make fmt
```

This runs:
- `go fmt ./...` for Go code
- `cargo fmt --all` for Rust FFI code

### Running Linters

Check your code for issues:

```bash
make lint
```

This runs:
- `go vet ./...` to catch common Go mistakes
- `cargo clippy` to catch common Rust issues

## Testing

Run the test suite to ensure your changes don't break existing functionality:

```bash
make test
```

## Continuous Integration

All pull requests must pass CI checks:

- **fmt**: Runs formatting checks (`go fmt`, `cargo fmt`)
- **lint**: Runs linting checks (`go vet`, `cargo clippy`)
- **test**: Runs unit tests for both Go and Rust components on Linux and Windows

You can view CI results in the GitHub Actions tab of the pull request.

## Makefile Targets

Available make targets:

- `make build` - Build both Rust FFI and Go SDK
- `make build-rust` - Build only the Rust FFI layer
- `make build-go` - Build the Go SDK. This target depends on `build-rust`.
- `make clean` - Remove build artifacts
- `make fmt` - Format all code (Go and Rust)
- `make lint` - Run linters on all code
- `make check` - Run all checks (fmt and lint)
- `make test` - Run all tests
- `make examples` - Build all examples
- `make help` - Show available targets

## Working with CGO

When making changes to the FFI layer:

1. Update the Rust code in `../rust/ffi/src/`
2. Update the C header if needed in `../rust/ffi/zerobus.h`
3. Update the Go bindings in `ffi.go`
4. Rebuild with `make build`
5. Test the changes with `make test`

### CGO Best Practices

- Always use `C.CString()` for string conversion and free with `C.free()`
- Use `defer C.free()` immediately after allocating C memory
- Handle panics gracefully in exported functions
- Document memory ownership clearly
- Test for memory leaks
