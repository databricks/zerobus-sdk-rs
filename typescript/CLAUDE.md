# TypeScript SDK

A hand-written TypeScript facade on top of a NAPI-RS layer that wraps the Rust core.

## Structure

```
typescript/
├── src/
│   ├── lib.rs       # NAPI-RS bindings (Rust ↔ Node.js bridge). Internal.
│   └── index.ts     # Public TypeScript API (options-bag style). Compiled to dist/.
├── dist/            # tsc output. `main` / `types` point here.
├── index.js, index.d.ts, *.node   # napi-rs build outputs at repo root.
├── Cargo.toml       # Rust crate config for NAPI-RS
├── tsconfig.json    # Type-checks examples/
├── tsconfig.build.json  # Compiles src/index.ts → dist/
├── package.json     # npm package config
├── test/            # node:test runner
├── examples/        # JSON / proto / Arrow examples + _config.ts helper
├── schemas/         # Proto schema files for examples
└── utils/           # Public helpers (loadDescriptorProto)
```

The TypeScript facade is the **public** surface. The napi-rs bindings under
the repo root are internal — they can be reshaped freely as long as
`src/index.ts` stays stable.

## Build commands

Run from `typescript/`:

- `npm install` — Install dependencies
- `npm run build` — Release: `napi build --features arrow-flight` + `tsc`
- `npm run build:debug` — Debug variant of the above
- `npm run build:napi` / `npm run build:ts` — Run one phase at a time. In CI, use `build:napi -- --target <triple>` so the flag reaches napi-cli (it would otherwise be appended to tsc through the chained `build` script).
- `npm run build:proto` — Compile `schemas/air_quality.proto` for the proto example
- `npm test` — All tests; `npm run test:unit` / `npm run test:integration`
- `cargo fmt --all` and `cargo clippy --all-targets --all-features` — Lint/format Rust

Arrow Flight is compiled into the default build now (it's Beta, not experimental). The `arrow-flight` Cargo feature is still opt-in at the Cargo level, but every npm script passes it.

## API shape (v2.0)

Options-bag API mirroring Python's `**kwargs` idiom; chosen because it
extends gracefully — adding optional fields or new discriminated-union arms
is non-breaking.

```typescript
new ZerobusSdk({ endpoint, unityCatalogUrl?, tls?, applicationName?, sdkIdentifier? })

sdk.createStream({
  table, auth, format,                       // required
  recovery?, maxInflightRequests?, ...       // optional
});

sdk.createArrowStream({
  table, auth, schema,
  compression?: 'none' | 'lz4_frame' | 'zstd',
  maxInflightBatches?, ...
});
```

`auth` and `format` are discriminated unions
(`'oauth' | 'headersProvider' | 'noAuth'` and `'json' | 'proto'`).

## FFI boundary: NAPI-RS

- **SDK identity**: set on the Rust SDK via `sdk_identifier`
  (`zerobus-sdk-ts/<version>`) at construction. Sent as the HTTP
  `user-agent` header. The deprecated `x-zerobus-sdk` gRPC header is no
  longer emitted.
- **TLS**: `tls: 'secure'` (default, system CAs) or `tls: 'none'` for
  plaintext local endpoints. `NoTlsConfig` is exposed via the Rust SDK's
  `testing` feature, which we enable as a dependency feature.
- **Arrow zero-copy**: when `compression: 'none'`, `ingestBatch` forwards
  IPC bytes via the Rust SDK's `ingest_ipc_batch(Bytes)` — no parse, no
  re-encode. With compression set, the SDK parses + re-encodes with the
  configured codec.
- **Async headers callbacks**: napi-rs 2.x does not auto-await JS promises
  returned from threadsafe functions. The adapter uses
  `Promise<Vec<Vec<String>>>` and an explicit `.await` to keep async
  callbacks working.
- **Memory**: NAPI external objects are ref-counted by Node.js GC. Cleanup
  triggers Rust `Drop`. No explicit free needed, but `close()` should be
  called to flush.

## Breaking-change rules

The public surface is `src/index.ts` and what it exposes in `dist/`:

- Add to options interfaces freely — new optional fields are non-breaking.
- Add new arms to `Auth` / `GrpcFormat` — non-breaking for existing callers.
- Renaming or reshaping existing fields is breaking.
- The NAPI binding layer (`src/lib.rs`) is internal; refactor freely.

Mark deprecations with `@deprecated` JSDoc + optional `console.warn` before
deletion in the next major.

## Changelog and documentation

- Every PR that touches user-facing behavior must update
  `typescript/NEXT_CHANGELOG.md`.
- Update `typescript/README.md` if the change affects usage, setup, or API
  surface.
- Add or update examples in `typescript/examples/` for new or modified APIs.

## Release

- Version source: `typescript/package.json` (`"version": "x.y.z"`).
- Tag: `typescript/v<semver>` → triggers `release-typescript.yml` → builds
  native `.node` binaries for all platforms → publishes to npm.
- On version bump PR: update version in `package.json` and
  `typescript/Cargo.toml`, move `NEXT_CHANGELOG.md` contents into
  `CHANGELOG.md`, reset `NEXT_CHANGELOG.md`.

## Platform targets

Pre-built binaries for: Linux x86_64, Linux aarch64, macOS x86_64, macOS
arm64, Windows x86_64. Node.js >= 20 required.

Linux + Windows are built in CI (`release-typescript.yml`). macOS prebuilds
are produced manually on-laptop until a darwin CI runner exists — see
`scripts/build-macos.md` (or this section's instructions) for the steps.

### Releasing macOS prebuilts manually

On an Intel Mac (or with `--target x86_64-apple-darwin` on Apple Silicon
with the Rosetta SDK installed):

```bash
cd typescript
npm install
npm run build:napi -- --target x86_64-apple-darwin
# produces zerobus-ingest-sdk.darwin-x64.node
```

On an Apple Silicon Mac:

```bash
cd typescript
npm install
npm run build:napi -- --target aarch64-apple-darwin
# produces zerobus-ingest-sdk.darwin-arm64.node
```

Collect both `.node` files alongside the Linux + Windows artifacts from CI,
run `npx napi artifacts` to stage them, then `npx napi prepublish -t npm`
to generate the per-platform `npm/<triple>/` package directories. Publish
each with `npm publish` from inside the generated directory. The published
package names are:

- `@databricks/zerobus-ingest-sdk-darwin-x64`
- `@databricks/zerobus-ingest-sdk-darwin-arm64`
