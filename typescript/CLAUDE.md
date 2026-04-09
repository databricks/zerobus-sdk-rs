# TypeScript SDK

Node.js wrapper around the Rust core via NAPI-RS.

## Structure

```
typescript/
├── src/lib.rs        # NAPI-RS bindings (Rust ↔ Node.js bridge)
├── Cargo.toml        # Rust crate config for NAPI-RS
├── package.json      # npm package config
├── test/             # Node.js test runner tests
├── examples/         # Usage examples
├── schemas/          # Proto schema files for examples
└── utils/            # Helper utilities
```

The native addon compiles to a platform-specific `.node` binary. NAPI-RS generates TypeScript type definitions automatically.

## Build commands

Run from `typescript/`:

- `npm install` — Install dependencies
- `npm run build` — Release build
- `npm run build:debug` — Debug build (faster iteration)
- `npm run build:arrow` — Build with Arrow Flight support
- `npm test` — Run all tests
- `npm run test:unit` / `npm run test:integration` — Targeted test runs
- `cargo fmt --all` and `cargo clippy --all-targets --all-features` — Lint/format Rust code

## FFI boundary: NAPI-RS

NAPI-RS bridges Rust types to JavaScript with automatic type conversions. Key considerations:

- **Memory**: NAPI external objects are ref-counted by Node.js GC. Cleanup triggers Rust `Drop`. No explicit free needed, but `close()` should be called to flush.
- **Async**: Rust futures become JavaScript Promises. The tokio runtime runs on a separate thread pool — Node.js event loop is never blocked.
- **Payload conversion**: The binding auto-detects payload types:
  - `Buffer` → proto bytes
  - `string` → JSON
  - Objects with `.encode()` → protobuf message (calls encode, passes bytes)
  - Plain objects → JSON.stringify'd
- **Error mapping**: `ZerobusError` struct with `message` and `isRetryable` fields, thrown as JS exceptions.

## Breaking change rules

The public API is what TypeScript consumers see — the generated `.d.ts` types and exported classes:

- Removing or renaming exported classes/methods is breaking.
- Changing a method signature (parameter types, return type) is breaking.
- The NAPI-RS binding layer (`src/lib.rs`) is internal; refactoring is safe as long as the JS-facing API is preserved.
- Deprecate by adding JSDoc `@deprecated` annotations and console warnings before removal.

## Performance notes

- NAPI-RS has low overhead for Buffer payloads (shared memory, no copy).
- String payloads (JSON) require UTF-8 validation at the boundary.
- The automatic `JSON.stringify` path for plain objects adds serialization cost — encourage users to pass pre-serialized strings or Buffers for hot paths.
- Batch ingestion amortizes the JS→Rust crossing overhead.

## Changelog and documentation

- Every PR must update `typescript/NEXT_CHANGELOG.md` under the appropriate section if it changes user-facing behavior.
- Update `typescript/README.md` if the change affects usage, setup, or API surface.
- Add or update examples in `typescript/examples/` for new or modified APIs.
- NAPI-RS auto-generates `.d.ts` type definitions — verify the generated types after API changes.

## Release

- Version source: `typescript/package.json` (`"version": "x.y.z"`).
- Tag: `typescript/v<semver>` → triggers `release-typescript.yml` → builds native `.node` binaries for all platforms → publishes to npm.
- On version bump PR: update version in `package.json`, move `NEXT_CHANGELOG.md` contents to `CHANGELOG.md`, reset `NEXT_CHANGELOG.md`.

## Platform targets

Pre-built binaries for: Linux x86_64, Linux aarch64, macOS x86_64, macOS arm64, Windows x86_64.
Node.js >= 16 required.
