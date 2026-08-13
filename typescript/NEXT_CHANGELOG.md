# NEXT CHANGELOG

## Release v1.2.0

### Major Changes

### New Features and Improvements

- `ZerobusSdk` constructor now accepts an optional `options` object
  (`ZerobusSdkOptions`) as its third argument. Its `applicationName` field is
  appended to the HTTP `user-agent` header sent on every request
  (e.g. `zerobus-sdk-ts/1.2.0 my-app/1.0`), enabling server-side attribution.
  The SDK now also correctly identifies itself as `zerobus-sdk-ts` rather than
  falling back to the underlying `zerobus-sdk-rs` identifier.

### Bug Fixes

- Added published CommonJS and type declaration files for
  `@databricks/zerobus-ingest-sdk/utils/descriptor.js`, so consumers can import
  `loadDescriptorProto()` from the npm package without running TypeScript
  source files directly. The package now installs the helper's `protobufjs`
  runtime dependency automatically, and the `.js` subpath supports CommonJS,
  native Node.js ESM, and NodeNext type resolution.
- Fixed descriptor file lookup so similarly suffixed names such as
  `not_air_quality.proto` cannot be selected for `air_quality.proto`.

### Documentation

- Simplified the README quick start to install the published npm package first
  and moved clone/build instructions into a source-development path.
- Corrected README, example, and JSDoc snippets for CommonJS async entry points,
  generated Protobuf field names, variable declarations, stream recovery, and
  custom-header callbacks.
- Documented the `HeadersProvider` shape that `createStream()` actually accepts
  (`getHeadersCallback` returning header tuples synchronously).
- Documented that omitted `descriptorProto` does not select JSON, that the
  inherited inflight default is 1,000,000, and that `close()` is still required
  to flush. README `main().catch` handlers now set a non-zero exit code.

- Clarified the high-throughput ingestion pattern across the README, API reference, JSDoc
  doc comments (`ingestRecordOffset`, `ingestRecordsOffset`, `waitForOffset`, `flush`), and
  examples: ingest in a loop without waiting, then wait once on the last offset (the ack
  watermark is monotonic) or `flush()` once at the end. Added explicit warnings that calling
  `waitForOffset()` after every record collapses throughput and should be reserved for
  low-volume cases.

### Internal Changes

- Updated TypeScript SDK development dependencies and the NAPI Cargo lockfile to
  resolve Dependabot security alerts without changing the public SDK API.
- Updated the wrapped Rust SDK dependency from v2.0.1 to v2.6.0 and aligned
  Arrow dependencies with the Rust SDK 2.6 workspace.

### Breaking Changes

### Deprecations

### API Changes
