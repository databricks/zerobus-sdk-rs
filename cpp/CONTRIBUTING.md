# Contributing to the Zerobus SDK for C++

Please read the [top-level CONTRIBUTING.md](https://github.com/databricks/zerobus-sdk/blob/main/CONTRIBUTING.md) first for general contribution guidelines, pull request process, and commit requirements.

This document covers C++-specific development setup and workflow.

## Development Setup

### Prerequisites

- Git
- A C++17 compiler (GCC, Clang, or MSVC)
- CMake 3.16 or higher
- Rust toolchain (cargo) — [Install Rust](https://rustup.rs/), required to build the C FFI library from local source (the default)

### Setting Up Your Development Environment

1. **Clone the repository:**
   ```bash
   git clone https://github.com/databricks/zerobus-sdk.git
   cd zerobus-sdk/cpp
   ```

2. **Build the project:**
   ```bash
   make build
   ```

   This will:
   - Build the Rust C FFI static library (`cargo build --release` in `../rust/ffi`)
   - Configure and build the C++ SDK and its tests

3. **Run tests:**
   ```bash
   make test
   ```

To link a prebuilt/vendored FFI library instead of building it from source,
configure CMake with `-DZEROBUS_FFI_LIBRARY=<path-to-.a>` and
`-DZEROBUS_FFI_HEADER_DIR=<dir containing zerobus.h>`.

## Coding Style

Code style is enforced by `clang-format` (Google style; see `.clang-format`).

### Running the Formatter

Format your code before committing:

```bash
make fmt
```

Check formatting without modifying files:

```bash
make fmt-check
```

### Running Linters

Without `clang-tidy` available everywhere, `lint` combines a formatting check
with the compiler's own `-Wall -Wextra` warnings (enabled on the library
target):

```bash
make lint
```

## Testing

Run the test suite to ensure your changes don't break existing functionality:

```bash
make test
```

Tests are **dependency-free**: each is a plain executable registered with CTest,
so the build is hermetic (no vendored framework, no package manager, no
network). To catch the memory/lifetime bugs an FFI wrapper is prone to
(use-after-free, double-free), build and run the suite under a sanitizer:

```bash
make test SANITIZE=address   # or thread, undefined
```

## Continuous Integration

All pull requests must pass CI checks:

- **fmt**: Runs the formatting check (`clang-format`)
- **lint**: Runs the formatting check plus a warnings-as-errors build
- **test**: Builds and runs the CTest suite

You can view CI results in the GitHub Actions tab of the pull request.

## Makefile Targets

Available make targets:

- `make build` — Configure (CMake) and build the SDK, tests, and examples
- `make build-ffi` — Build only the underlying Rust C FFI library
- `make test` — Build and run the test suite (`ctest`)
- `make examples` — Build the example binaries
- `make fmt` — Format all C++ sources with clang-format
- `make fmt-check` — Check formatting without modifying files
- `make lint` — Run lint checks (formatting + compiler warnings)
- `make check` — Run `fmt-check` and `lint`
- `make clean` — Remove the build directory
- `make help` — Show available targets

Add `SANITIZE=address` (or `thread`/`undefined`) to build and test under a
sanitizer, e.g. `make test SANITIZE=address`.

## Working with the FFI Layer

The public headers under `include/zerobus/` never include the generated C header
(`../rust/ffi/zerobus.h`); they hold opaque, forward-declared FFI handles. Only
`src/` includes `zerobus.h`, keeping the C FFI an implementation detail.

When making changes that touch the FFI boundary:

1. Update the Rust code in `../rust/ffi/src/` (and regenerate `zerobus.h` if signatures change)
2. Update the C++ wrapper in `src/` and, if the public API changes, the headers under `include/zerobus/`
3. Rebuild with `make build`
4. Test the changes with `make test` (and `make test SANITIZE=address` for lifetime-sensitive changes)

### FFI Best Practices

- **Rust owns the memory** for opaque handles; the wrapper holds a raw pointer and frees it via the matching FFI function in the destructor. Wrapper objects are move-only so each handle is freed exactly once.
- Route every `CResult` through the helper in `detail/ffi_util.hpp`, which converts failure into a `ZerobusException` and always frees `CResult.error_message` via `zerobus_free_error_message`.
- Free returned arrays and buffers with their matching FFI free function after copying the payload out.
- Never let a C++ exception cross the FFI boundary — exceptions from `HeadersProvider::get_headers()` are caught and surfaced as a headers-provider error.

See [`CLAUDE.md`](CLAUDE.md) for the full memory-ownership, threading, and
breaking-change contract.
