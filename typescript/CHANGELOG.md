# Version changelog

## Release v2.0.0

### Major Changes

- **License**: Migrated from the Databricks License to the Apache License 2.0.
- **Native Rust SDK bumped to 2.0.0.** Picks up Arrow Flight Beta and the
  deletion of the deprecated `ingest_record` / `ingest_records` APIs.
- **New options-bag public API.** Modeled on Python's `**kwargs` idiom — every
  entry point takes a single plain object. Designed to evolve gracefully:
  adding optional fields or new discriminated-union arms is non-breaking.
- **New TypeScript facade** (`dist/index.{js,d.ts}`) on top of the napi-rs
  bindings. The native bindings under the package root are now treated as
  internal — published for low-level use only.

### Breaking Changes

- **`ZerobusSdk` constructor**: now takes `{endpoint, unityCatalogUrl?, tls?,
  applicationName?, sdkIdentifier?}`. The positional `(endpoint, workspaceUrl)`
  form is gone.
- **`createStream` signature**: `{table, auth, format, ...}` options bag.
  `auth` is a discriminated union (`oauth | headersProvider | noAuth`); same
  for `format` (`json | proto`).
- **`createArrowStream` signature**: similar options bag with `schema`
  (`ArrowField[]`) and `compression: 'none' | 'lz4_frame' | 'zstd'`.
- **`ZerobusStream.ingestRecord` / `ingestRecords`**: now resolve at queue
  time (formerly the deprecated blocking-on-ack variants from v1.x). Use
  `waitForOffset` to await server acknowledgment.
- **`x-zerobus-sdk` gRPC metadata header is gone.** SDK identity is now
  emitted exclusively in the `user-agent` HTTP header
  (`zerobus-sdk-ts/<version>`). Override the prefix with `sdkIdentifier`;
  append your own product identifier with `applicationName`.
- **`RecordType` / `ArrowDataType`**: plain `as const` objects (not
  `const enum`s) — survive `--isolatedModules`.
- **`IpcCompressionType` numeric enum removed** from the public surface;
  use the `'none' | 'lz4_frame' | 'zstd'` string union.
- **`apache-arrow` peer dependency**: `^21.0.0` (was the bogus `^56.0.0`
  copied from a Rust crate version in earlier v1.x).
- **Minimum Node.js**: 20 (was 16). Node 18 went EOL April 2025.

### New Features and Improvements

- **macOS prebuilt binaries.** v2.0 publishes
  `@databricks/zerobus-ingest-sdk-darwin-x64` (Intel) and
  `@databricks/zerobus-ingest-sdk-darwin-arm64` (Apple Silicon) as npm
  optional dependencies, so `npm install` resolves them automatically on
  Mac and no longer falls back to building the native binary from source.
- **Arrow Flight ingestion is now Beta** (mirrors the Rust SDK 2.0 promotion)
  and **compiled into the default `npm install` prebuilds**. v1.x required
  rebuilding the native binary from source with `--features arrow-flight`;
  v2.0 ships it everywhere. The opt-in `build:arrow` / `build:debug:arrow`
  scripts are gone — use `build` / `build:debug`.
- **Zero-copy Arrow IPC path.** When `compression: 'none'` (default), JS IPC
  bytes are forwarded straight to the Rust SDK's `ingest_ipc_batch` —
  no parse-then-re-encode round trip. With `'lz4_frame'` or `'zstd'`, the SDK
  parses the batch and re-encodes with the requested codec.
- **TLS configuration**: `tls: 'secure'` (default, system CA certificates) or
  `tls: 'none'` for plaintext local endpoints / sidecar-proxy deployments.
- **`auth: { type: 'noAuth' }`** for local servers that don't enforce auth.
- **`auth: { type: 'headersProvider', getHeaders }`** for PATs, M2M tokens, or
  any custom auth. The callback must return a Promise, and is invoked once at
  stream open (its result is cached for the lifetime of the stream — call
  `sdk.createStream(...)` again to pick up a fresh token).
- **`bearerTokenProvider(table, getToken)` helper** for the common
  refreshable-bearer-token case.
- **`applicationName`** (appended) and **`sdkIdentifier`** (overrides default
  prefix) on `ZerobusSdk` for telemetry attribution.
- **Arrow examples** added: `examples/arrow/single.ts` and
  `examples/arrow/batch.ts`, both demonstrating an explicit-schema
  `RecordBatch` (nullable fields matching the Delta target).

### Bug Fixes

- **`waitForOffset` precision loss.** The napi binding routed `bigint`
  offsets through `Number()` before extracting an `i64`, silently truncating
  values above `2^53 − 1`. The SDK can hand out offsets that exceed that
  range (`bigint` is the public type), so high-throughput / long-lived
  streams could wait on the wrong offset. Both `ZerobusStream.waitForOffset`
  and `ZerobusArrowStream.waitForOffset` now take `BigInt` directly and use
  napi-rs's lossless `BigInt::get_i64()`, erroring on out-of-range values.
- **Arrow compressed path silently dropped batches and decode errors.**
  `parse_arrow_ipc_to_batch` (used when `compression` is enabled) did
  `filter_map(.ok())` + `.into_iter().next().unwrap()`, so any IPC stream
  with more than one batch lost everything after the first and per-batch
  decode errors were swallowed. Now requires exactly one batch and
  propagates decode errors.
- **`ingestRecord` rejected `Uint8Array`.** The napi binding only
  recognized Node `Buffer`; a `Uint8Array` fell through to the
  `JSON.stringify` arm and produced a corrupt `'{"0":...}'` payload with no
  error. Now accepted as raw bytes (Proto path). Other typed arrays
  (`Int8Array`, `Int16Array`, …) are explicitly rejected with a clear
  message rather than silently reinterpreted.
- **Header callback case-sensitivity.** A user returning `[['Authorization',
  ...]]` (capital A) was rejected with "must include 'authorization'
  header". Header names are now lowercased before validation, matching
  HTTP/gRPC's case-insensitive semantics.
- **Header pair validation silently dropped malformed entries.** A pair
  with wrong arity (`['authorization']` missing the value, or
  `['k', 'v', 'extra']`) used to be dropped via `filter_map`, surfacing as
  a confusing "missing required header" downstream. Now errors with the
  pair's actual shape.
- **`Box::leak` per stream creation in `StaticHeadersProvider`.** The
  required `'static` key lifetime meant each `createStream` permanently
  leaked one heap allocation per header. The two canonical headers
  (`authorization`, `x-databricks-zerobus-table-name`) are now interned as
  `&'static str` constants, so the common case doesn't leak. Custom
  headers still leak (rare path; sized per-stream).
- **Async headers callbacks now work end-to-end.** Previously the napi
  threadsafe-function adapter received the JS `Promise` instead of its
  resolved value, crashing with `InvalidArg, Given napi value is not an
  array`. The adapter now explicitly awaits the JS-returned promise.

### Packaging Fixes

- **npm tarball no longer ships Rust source.** `Cargo.toml`, `Cargo.lock`,
  `build.rs`, and `src/` were in the published tarball even though the
  `Cargo.toml` referenced `path = "../rust/sdk"`, which doesn't exist
  inside an installed npm package. Anyone hitting `npm install
  --build-from-source` (e.g. unsupported triple) got a Cargo error. The
  tarball now ships only the prebuilt `.node` (via platform packages), the
  napi loader, and the TS facade.
- **CI `[patch.crates-io]` preserves the `testing` feature.** The line
  appended by `use_local_sdk=true` in `ci-typescript.yml` previously
  dropped `features = ["testing"]`, which would silently break
  `tls: 'none'` once we flip back to a crates.io dep. Fixed for both Unix
  and Windows variants.

### Documentation Fixes

- **`recreateStream` recovery flow.** README previously instructed
  `await stream.close()` *before* `sdk.recreateStream(stream)`. That always
  errored because `close()` empties the underlying handle. Corrected to
  recreate first, then close.
- **`getUnacked*` after `close()` semantics.** Same root cause —
  documented as "call on closed streams"; actually errors. Now correctly
  documented as "call on a failed-but-not-yet-closed stream".
- **`recreateStream` does NOT re-fetch headers.** The old release note
  claimed it did. Corrected: the `StaticHeadersProvider` is reused via
  `Arc::clone`. Call `sdk.createStream(...)` from scratch for a fresh
  token; per-attempt callback is tracked as a v2.x follow-up.
- **Node.js minimum is 20, not 16.** README + CONTRIBUTING + CLAUDE.md
  prerequisite sections aligned with `engines.node`.
- **`npm run build:arrow`** references removed from `examples/arrow/`
  comments and `Cargo.toml` (Arrow is included in the default build now).

### Internal Changes

- TS SDK depends on the in-tree Rust SDK via a `path` dependency in
  `Cargo.toml`. The `testing` Rust feature is enabled so `NoTlsConfig` is
  available for `tls: 'none'`.
- Bumped Rust deps to match the Rust 2.0 workspace: `prost` 0.13 → 0.14,
  `tokio` 1.42 → 1.52, `thiserror` 1 → 2, `base64` 0.21 → 0.22, `bytes` 1 →
  1.11, arrow crates 56.2 → 58.2.
- Bumped TypeScript deps: `@napi-rs/cli` to 2.18.4, `@types/node` to ^22,
  `dotenv` to 17.4.2, `typescript` to ^5.6.
- CI: switched from `npm ci` to `npm install --no-audit --no-fund`. v2.0
  bumps the 5 platform-sub-package `optionalDependencies` to `2.0.0` — a
  version that doesn't exist on the registry until publish. `npm ci`'s
  pre-flight lockfile validation rejects that gap; `npm install`
  tolerates it. Standard napi-rs publish chicken-and-egg. CI builds its
  own `.node` and doesn't need the prebuilts anyway.
- **Security**: bumped vulnerable transitive and dev deps to close every
  open dependabot alert against the TS SDK:
  - `protobufjs` 7.5.4 → 8.0.3 (closes 7 advisories, one critical:
    GHSA-xq3m-2v4x-88gg arbitrary code execution).
  - `protobufjs-cli` 2.0.0 → 2.0.3 (closes GHSA-6r35-46g8-jcw9 +
    GHSA-f84p-cvgm-xgjj — pbjs/CLI code-injection paths). The bump cascades
    to `protobufjs ^8` for the dev dep; the published `peerDependencies`
    accept `^7 || ^8` so existing consumers are unaffected.
  - `lodash` 4.17.21 → 4.18.1 (closes GHSA-r5fr-rjxr-66jc + GHSA-xxjr-mmjv-4gpg
    + GHSA-f23m-r3pf-42rh), `minimatch` 9.0.5 → 9.0.9 (ReDoS),
    `brace-expansion`, `markdown-it`, `underscore` to their patched versions —
    all via npm `overrides` so the fixes survive a transitive resolution.
  - Cargo: `rustls-webpki` 0.103.9 → 0.103.13 (closes GHSA-82j2-j2ch-gfr8
    + GHSA-xgp8-3hg3-c2mh + GHSA-965h-392x-2mh5 + GHSA-pwjx-qhcg-rvj4),
    `rand` 0.8.5 → 0.8.6 and 0.9.2 → 0.9.4 (closes GHSA-cq8v-f236-94qc).
  - All TS dependabot alerts (#18-#43) are closed by these bumps.
- Added `tsconfig.build.json` and `dist/` build output; `npm run build`
  chains `napi build` + `tsc`.
- Examples now share a `_config.ts` helper that reads
  `ZEROBUS_ENDPOINT` / `ZEROBUS_TLS` / `ZEROBUS_NO_AUTH` / `ZEROBUS_TABLE_NAME`
  env vars so the same example can target a local server or a real workspace.

### Documentation

- README rewritten for the v2.0 options-bag API, with a new Arrow Flight
  (Beta) section and a v2.0 migration guide.

## Release v1.0.2

### Bug Fixes

- Split platform-specific native binaries into separate npm packages (`@databricks/zerobus-ingest-sdk-linux-x64-gnu`, `-linux-arm64-gnu`, `-win32-x64-msvc`). npm now auto-installs only the binary matching the user's OS/arch via `optionalDependencies`, reducing download size from ~15MB to ~5MB.

## Release v1.0.1

### Bug Fixes

- Fixed npm packaging: v1.0.0 was published without the napi-rs generated `index.js` loader and `index.d.ts` type declarations, causing `MODULE_NOT_FOUND` on `require('@databricks/zerobus-ingest-sdk')`. The platform-specific native binary packages (e.g. `@databricks/zerobus-ingest-sdk-linux-x64-gnu`) were also missing from npm. This release includes all generated files and platform packages.

## Release v1.0.0

GA release of the Databricks Zerobus Ingest SDK for TypeScript.

### New Features and Improvements
- Added HTTP proxy support via standard environment variables (`grpc_proxy`, `https_proxy`, `http_proxy`), following gRPC core conventions. Proxied connections use HTTP CONNECT tunneling with end-to-end TLS. Supports `no_grpc_proxy` / `no_proxy` for bypass rules.

## Release v0.3.0

### Native Library Update

- Updated native Rust backend to v0.6.0
- Schemeless server endpoints now automatically get `https://` prepended
- All documentation and examples updated to explicitly use `https://` prefixed endpoints

## Release v0.2.0

### New Features and Improvements

- Upgraded to Rust SDK v0.4.0
- Added new offset-based ingestion APIs for better high-throughput patterns:
  - `ingestRecordOffset()` - Returns offset immediately after queuing
  - `ingestRecordsOffset()` - Batch version, returns offset immediately
  - `waitForOffset()` - Wait for specific offset to be acknowledged
- Added experimental Arrow Flight support (behind feature flag)
- Added `streamPausedMaxWaitTimeMs` configuration option
- Set user agent to identify as `zerobus-sdk-ts/0.2.0`
- Reorganized examples into `json/`, `proto/`, `arrow/` directories

### API Changes

- **New (Recommended):** `ingestRecordOffset()`, `ingestRecordsOffset()`, `waitForOffset()`
- **Deprecated:** `ingestRecord()`, `ingestRecords()` - still work but return Promise that blocks until ack
- Added `streamPausedMaxWaitTimeMs` to `StreamConfigurationOptions`
- Custom `headers_provider` now automatically includes TS SDK user agent if not specified

### Documentation

- Updated README with new APIs and deprecation notices
- Reorganized examples with separate directories for each format
- Added Arrow Flight examples (experimental)

---

## Release v0.1.0

Initial release of the Databricks Zerobus Ingest SDK for TypeScript.

### New Features and Improvements

- High-throughput data ingestion into Databricks Delta tables using native Rust implementation
- Support for JSON and Protocol Buffers serialization formats
- OAuth 2.0 client credentials authentication
- Batch ingestion API with `ingestRecords()` for higher throughput
- Type widening support for flexible record input:
  - JSON mode: Accept objects (auto-stringify) or strings (pre-serialized)
  - Protocol Buffers mode: Accept Message objects (auto-serialize) or Buffers (pre-serialized)
- Stream recovery mechanisms with `getUnackedRecords()` and `getUnackedBatches()`
- Automatic retry and recovery for transient failures
- Protocol Buffer descriptor utilities with `loadDescriptorProto()`
- Cross-platform support (Linux, macOS, Windows)

### API Changes

- Added `ZerobusSdk` class for creating ingestion streams
- Added `ZerobusStream` class for managing stateful gRPC streams
- Added `createStream()` method with optional `headers_provider` parameter
- Added `ingestRecord()` method accepting Buffer, string, or object types
- Added `ingestRecords()` method for batch ingestion
- Added `getUnackedRecords()` and `getUnackedBatches()` for recovery
- Added `TableProperties` interface for table configuration
- Added `StreamConfigurationOptions` interface with `recordType` parameter
- Added `RecordType` enum with `Json` and `Proto` values
- Added `HeadersProvider` interface for custom authentication
- Support for Node.js >= 16

### Documentation

- Comprehensive README with quick start guide
- Protocol Buffer setup instructions
- Type mapping guide (Delta ↔ Proto)
- API reference documentation
- Examples for JSON and Protocol Buffers ingestion
