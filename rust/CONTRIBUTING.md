# Contributing to the Zerobus Rust SDK

Please read the [top-level CONTRIBUTING.md](../CONTRIBUTING.md) first for general contribution guidelines, pull request process, and commit requirements.

This document covers Rust-specific development setup and workflow.

## Development Setup

### Prerequisites

- Git
- Rust 1.70+ (stable toolchain)
- Pinned formatter: `rustup toolchain install nightly-2025-08-07 --profile minimal --component rustfmt`
- Protocol Buffers compiler (`protoc`)

### Setting Up Your Development Environment

1. **Clone the repository:**
   ```bash
   git clone https://github.com/databricks/zerobus-sdk.git
   cd zerobus-sdk/rust
   ```

2. **Build the project:**
   ```bash
   make build
   ```

   This will download all dependencies and build the project in debug mode.

## Coding Style

Code style is enforced by a formatter check in your pull request. We use `rustfmt` to format our code. Run `make fmt` to ensure your code is properly formatted prior to raising a pull request.

### Running the Formatter

```bash
make fmt
```

This runs `cargo fmt --all` with the same pinned rustfmt used by CI. Compilation, linting, and tests continue to use stable Rust.

### Running Linters

```bash
make lint
```

This runs `cargo clippy` to catch common mistakes and improve your code.

### Running Tests

```bash
make test
```

This runs `cargo test` for all crates in the workspace.

## Continuous Integration

All pull requests must pass CI checks:

- **fmt**: Runs formatting checks (`cargo fmt`)
- **lint**: Runs linting checks (`cargo clippy`)
- **tests**: Runs tests on Ubuntu and Windows for the stable Rust toolchain.

You can view CI results in the GitHub Actions tab of the pull request.

## Makefile Targets

- `make build` - Build the project for debugging
- `make build-release` - Build the project for release
- `make build-jni` - Build JNI library for Java SDK
- `make build-ffi` - Build C FFI library for language bindings
- `make fmt` - Format code with `rustfmt`
- `make lint` - Run linting with `clippy`
- `make check` - Run all checks (fmt and lint)
- `make test` - Run tests
- `make clean` - Remove build artifacts
- `make help` - Show available targets
