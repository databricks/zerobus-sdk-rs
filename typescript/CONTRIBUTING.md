# Contributing to the Zerobus TypeScript SDK

Please read the [top-level CONTRIBUTING.md](https://github.com/databricks/zerobus-sdk/blob/main/CONTRIBUTING.md) first for general contribution guidelines, pull request process, and commit requirements.

This document covers TypeScript-specific development setup and workflow.

## Development Setup

### Prerequisites

- Git
- Node.js >= 16 - [Download Node.js](https://nodejs.org/)
- Rust toolchain (1.70+) - [Install Rust](https://rustup.rs/)

### Setting Up Your Development Environment

1. **Clone the repository:**
   ```bash
   git clone https://github.com/databricks/zerobus-sdk.git
   cd zerobus-sdk/typescript
   ```

2. **Install dependencies:**
   ```bash
   npm install
   ```

3. **Build the project:**
   ```bash
   npm run build:debug
   ```

   This will compile the Rust code into a native Node.js addon (`.node` file) for your platform.

   Arrow Flight is opt-in, same as the Rust SDK. Use `npm run build:debug:arrow` (or
   `npm run build:arrow` for a release build) when you need `createArrowStream`.
   Published npm binaries are built with Arrow Flight enabled.

4. **Run tests:**
   ```bash
   npm run build:debug:arrow
   npm test
   ```

   Unit tests import the Arrow API, so they need an Arrow-enabled build.

## Building

### Debug Build

Faster compilation, includes debug symbols:

```bash
npm run build:debug
```

### Release Build

Optimized for production:

```bash
npm run build
```

### Arrow Flight Build

Arrow Flight is behind the `arrow-flight` Cargo feature (off by default). Enable it with:

```bash
npm run build:debug:arrow
npm run build:arrow
```

`npm test` expects an Arrow-enabled debug build (`npm run build:debug:arrow`).

### Cross-Platform Builds

Build for specific targets:

```bash
npm run build -- --target x86_64-apple-darwin
npm run build -- --target x86_64-unknown-linux-gnu
npm run build -- --target x86_64-pc-windows-msvc
```

For Arrow-enabled artifacts (including darwin binaries added to a TypeScript
release), pass the same `--target` to `npm run build:arrow`.

## Testing

```bash
# Run all tests
npm test

# Unit tests only
npm run test:unit

# Integration tests
npm run test:integration
```

## Code Style

### Rust Code

Follow standard Rust formatting:

```bash
cargo fmt --all
cargo clippy --all-targets --all-features
```

### TypeScript/JavaScript

Follow standard TypeScript conventions:
- Use camelCase for variables and functions
- Use PascalCase for types and classes
- Prefer `const` over `let`
- Use async/await over Promise chains

## Continuous Integration

All pull requests must pass CI checks:

- **build**: Cross-platform builds for Linux (x86_64, aarch64) and Windows (x64)
- **test**: Unit tests on Node.js 16, 18, and 20 across Linux and Windows runners
