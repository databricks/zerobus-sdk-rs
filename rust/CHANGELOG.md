# Version changelog

## Release v2.8.0

### Major Changes

### New Features and Improvements

### Bug Fixes

- Fixed multiplexed-stream admission during terminal sub-stream failure so
  accepted records are retained for recovery, capacity waiters keep their FIFO
  position, and waits fail after 30 seconds with actionable timeout context.
- The OAuth `expires_in` field is now parsed from a quoted integer (`"3600"`) in
  addition to a plain JSON integer. A value that is missing or does not represent
  a positive integer still yields no token lifetime, as before.
- OAuth token lifetime is now anchored to when the token request starts rather
  than when the response arrives, so a slow response no longer makes the SDK treat
  a token as valid longer than the issuer does. A token already past its
  (start-anchored) expiry is dead on arrival and surfaces a retryable
  `TokenFetchError` when there is no still-valid cached token to serve instead,
  including when the token cache is disabled.
- A stalled proactive token refresh no longer hangs stream creation. For streams
  built with `.oauth(...)`, the refresh is capped at half the stream's configured
  setup budget (`recovery_timeout_ms`) — but only when the cached token has more
  life left than that cap, so a hung endpoint becomes a prompt failure and the
  still-valid cached token is served in time. When too little of the token's life
  remains for that fallback to help, the refresh runs unbounded (like a cold miss)
  so a slow-but-working mint isn't cut off early.
- When a proactive token refresh fails — for any error, including a non-retryable
  one such as revoked credentials — the still-valid cached token is served rather
  than failing the caller. The token was validly issued and hasn't expired, and the
  server re-validates it on every connection, so the error surfaces only when there
  is no still-valid token to fall back to. A refresh that returns a dead-on-arrival
  token falls back the same way.
- After a proactive refresh falls back to the cached token, a short backoff paces
  further attempts so a burst of stream creations cannot turn one token-endpoint
  failure into a mint stampede. The interval scales with the configured token
  refresh buffer and shrinks as the token nears expiry, down to a floor, and never
  extends past expiry.
- OAuth credential invalidation after an auth rejection is more precise and never
  waits on an in-flight mint. Each cached token carries a monotonic generation, and
  invalidation records the rejected generation on the cache entry with a lock-free
  atomic, so it never takes the per-token lock or detaches a mint. A token at or
  below the recorded generation is dropped before its next use and is never refreshed
  from, so the next fetch re-mints; a newer token installed by a concurrent refresh
  has a higher generation and is kept and reused.

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

- Added the in-development persistent-stream protobuf contract for creating,
  resuming, ingesting into, and retiring durable streams.

## Release v2.7.1

### Major Changes

### New Features and Improvements

### Bug Fixes

- Arrow Flight now rolls back logical offsets and record ranges when an enqueue
  fails with recovery disabled. `ingest_batch()` waits for terminal finalization
  and returns the request-stream error; `flush()` and `close()` no longer wait on
  the withdrawn offset. An already-acknowledged flush target still succeeds, while
  `close()` preserves the terminal error and retained batches are immediately available.

### Documentation

- Corrected README and rustdoc examples so their dependencies, feature flags,
  imports, and mutable stream bindings compile as shown.
- Batch examples and primary rustdoc now queue all records and wait once with
  `flush()` or the last offset, and no longer refer to removed `ingest_record()`
  / `ingest_records()` methods.
- Example READMEs and `get_unacked_*` rustdoc now name `ingest_record_offset()` /
  `ingest_records_offset()`. The generate-files tool README quoting is valid shell.
  Arrow example docs place schema validation at stream creation, not the first batch.
- Updated the Arrow example to use application-sized batches and queue them before
  one `flush()`, clarified logical versus wire offsets and partial acknowledgments,
  and added an Arrow Flight architecture guide covering lifecycle, recovery, close,
  and concurrency invariants.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

## Release v2.7.0

### Major Changes

### New Features and Improvements

### Bug Fixes

- Arrow builders now reject unsupported ACK callbacks instead of silently
  discarding them. Remove `.ack_callback(...)` before calling `build_arrow()`;
  otherwise it returns `InvalidArgument`.
- Arrow Flight acknowledgment deadlines are pending-relative: no timer runs
  while a stream is idle. During normal stream operation, each batch receives
  an absolute deadline when it becomes pending; responses and partial
  acknowledgments do not extend it. Recovery refreshes the deadline when
  the full replay completes and ACK processing can resume on the replacement
  connection.
- Arrow Flight rejects unrepresentable timeout values: stream creation returns
  `InvalidArgument` when ACK, recovery, or flush deadlines exceed the platform
  monotonic-clock range. Server-advertised graceful-rotation periods are capped
  at one year.
- Arrow Flight close is cancellation-safe and half-closes the active request before
  bounded response draining. ACK success is decided when the durable watermark is
  applied relative to the original flush deadline. Close during recovery cancels the
  attempt, retains the unacknowledged suffix, and returns the error that triggered the
  current attempt. Close during an existing recovery or server-requested rotation keeps
  that trigger even if every record is durable, so an error can coexist with an empty
  unacknowledged-batch set. After a request-send failure, one ready response may still
  be applied; later stream items are not discarded in order to start recovery.
- Fixed Arrow Flight recovery sender lifetime: replacement senders are now published
  only after pending replay succeeds, while initial supervisor handoff and failed or
  cancelled replay promptly drop redundant senders instead of retaining incomplete
  `DoPut` request channels until later teardown.

### Documentation

### Internal Changes

- Updated Arrow crates and the vendored `arrow-flight` fork from `59.1` to
  `59.2` while retaining the slice-aware batch-splitting patch.
- Added Arrow C Data `RecordBatch` conversion behind a disabled-by-default
  wrapper-only SDK feature so current and future native bindings can share one
  ownership implementation. No supported Rust SDK or Flight behavior changed.
- Reorganized Arrow Flight under `stream/arrow/` with focused API, connection,
  ACK, supervisor, and batch modules and no public API changes. Its tracing
  target now follows the module path:
  `databricks_zerobus_ingest_sdk::stream::arrow`.

### Breaking Changes

### Deprecations

### API Changes

## Release v2.6.0

### Major Changes

### New Features and Improvements

- Arrow Flight: new `arrow_schema_from_uc_columns_with_options` /
  `arrow_schema_from_uc_schema_with_options` accept an `ArrowSchemaOptions`. When
  `annotate_variant_extension` is set, `VARIANT` fields (top-level and nested
  inside structs, arrays, and maps) are annotated with the canonical
  `arrow.parquet.variant` Arrow extension marker (`ARROW:extension:name` + empty
  `ARROW:extension:metadata`), so downstream consumers can recover which fields
  are variants — previously lost when a UC schema was converted to Arrow. The
  physical `Struct<metadata, value>` shape is unchanged. The option defaults to
  off (and `arrow_schema_from_uc_columns` / `arrow_schema_from_uc_schema` are
  unchanged) because the Arrow Flight server's target schema is unmarked, so a
  marked schema forces a per-batch server-side cast; enable it only when a
  consumer needs the annotation and that cost is acceptable.

### Bug Fixes

- **Arrow Flight — invalid acknowledgment watermarks are rejected** (Beta): ack progress is now monotonic, so delayed or duplicate responses cannot move the durable watermark backward. A response claiming more records than were actually submitted on the active connection is rejected without making buffered, unsent records appear durable.
- Fixed server-initiated Arrow Flight rotation to wait only for records submitted on the active connection, half-close its request, and drain late acknowledgments or peer status before reconnecting. `stream_paused_max_wait_time_ms = Some(0)` skips the ACK wait but still performs bounded transport cleanup. Explicit close and teardown of incomplete replacement connections retain their existing best-effort behavior.

### Documentation

- README: document the opt-in variant extension annotation
  (`ArrowSchemaOptions`) for the Arrow Flight UC→Arrow schema conversion.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

## Release v2.5.0

### Major Changes

### New Features and Improvements

- Arrow Flight: schema-validation rejections now surface as the new
  `ZerobusError::InvalidSchema` variant (carrying the server-reported `causes` as
  typed `SchemaValidationCause` values) instead of a generic `CreateStreamError`.
  This lets callers detect a table/stream schema mismatch — e.g. a column added
  to or dropped from the target table — and re-resolve their schema rather than
  treating it as an opaque invalid-argument failure. The variant is not
  SDK-retryable. This applies both to initial stream setup and to mid-stream
  reconnects: on a reconnect, the typed error flows through the terminal
  recovery path (a non-retriable failure ends recovery and is reported as-is),
  so a schema change detected during recovery is surfaced to a blocked
  `wait_for_offset` / `flush` as `InvalidSchema` — letting callers rebuild the
  stream without downtime — rather than being retried until the recovery budget
  drains and reported as a generic failure.
- Added dynamic protobuf support: build and ingest records against a descriptor known only at runtime (for example one fetched from Unity Catalog or built with `schema::descriptor_from_uc_columns`), with no compiled `prost::Message` type. Resolve the descriptor with `message_descriptor()`, pass it to `StreamBuilder::dynamic_proto()` (also available from `ZerobusStream::message_descriptor()`), fill records field-by-field with `DynamicRecord`, and `encode()` them (which enforces proto2 `required` fields) for ingestion. See the new `proto_dynamic_single` example.

### Bug Fixes

- **Proxy target TLS is now applied exactly once**: Standard and Arrow Flight streams now keep the CONNECT tunnel raw after establishing an HTTP or HTTPS proxy connection, allowing tonic to apply endpoint TLS once instead of attempting a second TLS handshake for HTTPS targets.
- **Arrow Flight — proxy configuration now applies to all connections** (Beta): Arrow streams now honor the same `grpc_proxy`/`https_proxy`/`http_proxy`, `no_grpc_proxy`/`no_proxy`, and caller-supplied `connector_factory` policy as standard streams, including replacement channels created during recovery.
- **Initial setup refreshes one stale credential**: an `Unauthenticated` or `PermissionDenied` response during initial stream setup previously failed after invalidating the rejected credential. When recovery is enabled and a recovery retry remains, initial setup now spends at most one such retry so the headers provider can refresh the credential; a repeated auth rejection remains terminal, and auth errors remain globally non-retryable. Provider invalidation shares the setup deadline and preserves the auth rejection if it stalls instead of consuming the remaining budget as generic timeout retries. This applies to both standard (gRPC) and Arrow Flight streams; reconnect behavior is unchanged on both.
- **Arrow Flight — `close()` now propagates flush errors and survives cancellation** (Beta): `ZerobusArrowStream::close()` previously swallowed a failed final `flush()` and always returned `Ok(())`, contradicting its documentation and diverging from the proto stream's `close()`. It now returns the flush error after still tearing down the stream and moving pending batches to the failed set (retrievable via `get_unacked_batches()`). If the close future is cancelled after teardown starts, the stream enters a non-ingestable `Closing` state and a later `close()` resumes teardown without waiting for another flush.
- **Arrow Flight — `max_inflight_batches` now bounds batches awaiting acknowledgment** (Beta): it previously limited only the pre-encode channel, so pending batches could grow unbounded under a slow-acking server. `ingest_batch` now holds a permit until the batch is acked, applying backpressure (it blocks) at the configured limit. `max_inflight_batches = 0` is now rejected with `InvalidArgument` instead of panicking.
- **Arrow Flight — recovery replay is now failure-safe** (Beta): if a batch send failed while replaying after a reconnect, the pending set was drained and lost (unrecoverable via automatic replay or `get_unacked_batches()`). Pending batches (and their in-flight accounting) are now retained so the next recovery attempt replays them.
- **Arrow Flight — no spurious ingest error during recovery handoff** (Beta): a race between starting recovery and an in-flight `ingest_batch` could make ingest return a "stream sender is closed" error for a batch that was actually retained and replayed. The pause and sender-detach is now atomic with respect to ingest, so ingest either sends or buffers (returns `Ok`).
- **Arrow Flight — records ingested during recovery are always replayed** (Beta): `reconnect` reset the recovery counters before rebuilding the pending record ranges, so a record ingested in that window could be assigned a stale range and skipped by replay as already acknowledged. The counter reset and range rebuild are now applied atomically, so a record ingested during a recovery handoff is always replayed.
- **Arrow Flight — unacknowledged batches no longer duplicate durably-acked records** (Beta): after a terminal failure, a partially-acknowledged auto-chunked batch was retained whole, so retrying it via `get_unacked_batches()` re-sent the already-persisted prefix. Retained batches are now sliced to their un-acknowledged suffix, and `get_unacked_batches()` returns a consistent, idempotent snapshot: closure and the terminal drain are serialized with `ingest_batch`, so a batch accepted concurrently with recovery/close is included rather than omitted from the first snapshot and revealed by a later call.
- **Arrow Flight — `flush()`/`wait_for_offset()`/`close()` return the real terminal error** (Beta): on a terminal failure a blocked `flush()`/`wait_for_offset()` could return a generic "timed out" or "stream is closed" error instead of the actual cause. All terminal paths — mid-stream server error, server stream end, and ack timeout — now publish the error and wake waiters with it. `close()` likewise returns the terminal error when the stream was already closed by a background failure (rather than `Ok(())`), so the common ingest-then-`close()` pattern no longer hides failed batches. Additionally, an acknowledgment that lands just before the stream closes now resolves as `Ok(())` instead of a spurious closed error (which could otherwise trigger a duplicate retry of an already-durable batch).
- **Arrow Flight — empty (zero-row) batches are rejected** (Beta): `ingest_batch()`/`ingest_ipc_batch()` now return `InvalidArgument` for a zero-row `RecordBatch`. Previously it entered the pending set but the Flight encoder emits no data message for zero rows, so it was never sent or acknowledged and `flush()`/`wait_for_offset()` would hang until they timed out.
- **Arrow Flight — recovery surfaces the original reconnect failure** (Beta): after reconnect attempts were exhausted, the stream terminated with a synthetic "Reconnection failed" error, losing the underlying cause and its retry classification. The real reconnect error is now carried through: its message is surfaced, an auth rejection still invalidates cached credentials and retries (so a fresh token can be minted), and if retries are exhausted the original error is reported rather than a synthetic one. A single `recovery_timeout_ms` deadline bounds reconnect plus credential invalidation, and terminal cleanup is bounded separately; a stalled custom provider therefore cannot hang recovery or leave the supervisor alive indefinitely.
- **Arrow Flight — authorization metadata is now sensitive** (Beta): Bearer credentials are marked sensitive in tonic metadata, matching the standard gRPC stream and preventing token values from appearing in metadata debug output. Invalid authorization header values now return `InvalidUCTokenError` instead of `InvalidArgument`, also matching the standard gRPC stream.

### Documentation

### Internal Changes

- Added a test-only `test-hooks` Cargo feature that exposes deterministic synchronization seams in the Arrow stream for recovery-race tests. It has zero footprint in default and FFI builds.

### Breaking Changes

### Deprecations

### API Changes

- Added `StreamBuilder::dynamic_proto()`, `ZerobusStream::message_descriptor()`, and `ZerobusStream::new_record()`.
- Added dynamic-proto types at the crate root: `DynamicRecord` (with `set()` and a `required`-field-checking `encode()`), the `IntoDynamicValue` conversion trait, the `message_descriptor()` resolver, the `missing_required_fields()` helper, and re-exports of `prost_reflect::{DynamicMessage, MessageDescriptor, Value}`.


## Release v2.4.0

### Major Changes

### New Features and Improvements

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

- Bumped the Arrow dependencies (`arrow-array`, `arrow-schema`, `arrow-ipc`, and
  the vendored `arrow-flight` fork) from `58.3` to `59.1`. The vendored
  `arrow-flight` crate was re-synced to upstream `59.1.0` with the slice-aware
  batch-split fix (arrow-rs#9388 / #5352) re-applied. Because the SDK re-exports
  Arrow types through its public API, consumers using the Beta `arrow-flight`
  feature that exchange the re-exported Arrow types with the SDK **must upgrade
  their own Arrow dependency to `59.x`** — Arrow `58` and `59` types cannot be
  mixed in the same build. Consumers on the default build (without the
  `arrow-flight` feature) are unaffected.

### Deprecations

### API Changes


## Release v2.3.2

### Major Changes

### New Features and Improvements

- Added the callback bridge used by multiplexed streams to report `MessageId`
  values while preserving the existing `AckCallback` API. Each sub-stream
  callback converts its stream-local `OffsetId` into a message ID containing
  both the sub-stream index and offset.

### Bug Fixes

- Fixed `VARIANT` columns in Arrow Flight schemas generated from Unity Catalog
  metadata. `arrow_schema_from_uc_columns` / `arrow_schema_from_uc_schema` now
  project `VARIANT` as `Struct<metadata: LargeBinary not null, value:
  LargeBinary not null>` instead of `LargeUtf8`, matching the server's expected
  binary variant representation. Protobuf descriptor generation continues to
  expose `VARIANT` as `string`.

### Documentation

### Internal Changes

- Add a `testing`-feature-gated `CallbackHandlerHarness` that drives the real callback-handler task and reproduces `close()`'s teardown, and split the callback drain-then-abort / wait-indefinitely logic out of `shutdown_all_tasks_gracefully` into `ZerobusStream::shutdown_callback_task` so it can be exercised in isolation. Test-only; no change to shipped behavior or the default (non-`testing`) build.

### Breaking Changes

### Deprecations

### API Changes

- Generalized `AckCallback` over its identifier type while preserving
  `OffsetId` as the default for existing single-stream callbacks. Multiplexed
  callbacks use the same trait with `MessageId` as the identifier type.


## Release v2.3.1

### Major Changes

### New Features and Improvements

### Bug Fixes

- Fixed `VARIANT` columns nested inside a `STRUCT`, `ARRAY`, or `MAP` failing with `unknown primitive type 'variant'` when building a descriptor from a Unity Catalog schema. Nested `VARIANT` now maps to `string` (unshredded JSON-encoded text) at any depth, matching the top-level behavior. Applies to both the protobuf and Arrow Flight schema paths (`descriptor_from_uc_columns` / `arrow_schema_from_uc_columns`).

### Documentation

- Reworked ingestion docs to lead with the high-throughput pattern (ingest in a loop, then `flush()` once) and explicitly warn against calling `wait_for_offset()` after every record. Updated the README, crate- and method-level doc comments (`ingest_record_offset`, `ingest_records_offset`, `wait_for_offset`, `flush`), and the `json`/`proto` single-record examples accordingly.

### Internal Changes

- Established `rust/sdk/zerobus_service.proto` as the single canonical gRPC schema, now referenced directly by the cgo Go SDK tests and the Java SDK build instead of their own duplicated (and drifted) copies. No schema or behavior change for the Rust core — the file stays where it was.

### Breaking Changes

### Deprecations

### API Changes

## Release v2.3.0

### Major Changes

### New Features and Improvements

- Token caching for the default OAuth path. Tokens obtained via `.oauth(...)` are now cached per table on the `ZerobusSdk` instance and reused across stream creations and recoveries until they near expiry, instead of minting a fresh token on every stream. This reduces load on the Unity Catalog token endpoint for clients that create many short-lived streams. Caching is on by default and can be tuned via `ZerobusSdkBuilder::token_cache_enabled` and `ZerobusSdkBuilder::token_refresh_buffer`.
- On a server-side authentication rejection during stream creation, the cached token is invalidated so the next attempt re-mints (re-checking grants at Unity Catalog), rather than reusing a rejected token until the refresh window.
- `OAuthHeadersProvider::new` now caches tokens for the lifetime of the returned provider (previously it minted a fresh token on every call). Behavior is unchanged for the common path of constructing streams through `ZerobusSdk`, which already shares a cache.
- Add `StreamBuilder::no_auth()` and `NoAuthHeadersProvider` for local testing
  against Zerobus endpoints that do not enforce authentication. Both are gated
  behind the `testing` feature flag.
- Add `ZerobusSdkBuilder::no_tls()` convenience method as a shortcut for
  `.tls_config(Arc::new(NoTlsConfig))` when connecting to plaintext `http://`
  endpoints. Gated behind the `testing` feature flag.
- Added a configurable payload size limit per `ingest_record_offset` / `ingest_records_offset` call. Attempts to ingest more than the limit of encoded record data in a single call now return `ZerobusError::InvalidArgument` immediately, before any network I/O. The default is set slightly below the 10 MiB server limit to leave headroom for the request envelope (protobuf framing/stream metadata), so payloads accepted client-side are not later rejected by the server's transport layer. The limit is tunable per stream via `StreamBuilder::max_ingest_payload_bytes` (gRPC JSON/proto streams only; Arrow Flight streams do not enforce it and log a warning if it is set before `build_arrow()`).
- Added stream lifecycle logging to make recovery observable. The SDK now logs (at `info`) when recovery starts and how many records are pending, and when a recovered stream re-sends unacknowledged records and how many. Each failed stream-creation attempt is logged (at `warn`) with its attempt number and retryability, and a non-retryable failure logs (at `error`) how many records were left unacknowledged (these are retained for retrieval via `get_unacked_records`/`get_unacked_batches`). These counts now distinguish in-flight batches from the true record count they carry (a single `ingest_records` can be one batch but many records), and a terminal recovery failure now always emits a single `error` even when no records remain pending.
- `ZerobusSdkBuilder::application_name` is now normalized and validated in `build()`: the value is trimmed of surrounding whitespace, a blank value is ignored (the default `zerobus-sdk-rs/<version>` identifier is used), and a value that is not a valid `user-agent` header value (for example one containing a newline or other control byte) is rejected with `ZerobusError::InvalidArgument`. The validity rule mirrors `http::HeaderValue` exactly, so this rejects only values tonic would reject anyway — it surfaces the error early at `build()` as `InvalidArgument` instead of later as a channel-creation error on first connect. Centralizing this in the core means all wrapper SDKs inherit the same handling.

### Bug Fixes
- Redacted the OAuth authorization token from an error log and error message on the gRPC stream-setup path; a malformed token value is no longer written to logs.
- A UC token that cannot be encoded as an HTTP `authorization` header value is now rejected at mint time rather than cached, so it cannot poison the cache and fail every stream creation until its refresh window.
- Arrow Flight stream errors now preserve the server's gRPC status code instead of flattening it to `Unknown`. Previously a `FlightError` was wrapped via `tonic::Status::from_error`, which dropped the inner code, so non-retryable rejections (for example `PermissionDenied`) were misclassified as retryable and auth-rejection detection did not fire on the Arrow path.
- Fixed Arrow Flight streams over-splitting batches that were deserialized from Arrow IPC bytes. The zero-copy IPC reader makes every column buffer report its whole allocation size, so the Flight encoder's `split_batch_for_grpc_response` over-estimated batch size and split it into many small `FlightData` messages — inflating message counts and rendering IPC compression ineffective. The encoder now sizes batches with a slice-aware calculation (`ArrayData::get_slice_memory_size`) so already-sliced/IPC-decoded batches are measured accurately, with no extra data copy. Shipped via a vendored `arrow-flight` (`58.3.0`) referenced as a workspace `path` dependency (see `rust/third_party/arrow-flight`); fixes [arrow-rs#9388](https://github.com/apache/arrow-rs/issues/9388) / [#5352](https://github.com/apache/arrow-rs/issues/5352).

### Documentation

### Internal Changes

- Added `ZerobusStream::signal_shutdown` (crate-private), a `&self`-callable
  helper that flips `is_closed` and cancels the cancellation token. Lets
  `MultiplexedStream` tear down sub-stream background tasks from its poison
  path and `Drop` without needing `&mut`. JoinHandle reaping still happens in
  `close` or the existing `Drop` impl.
- Split `sdk/src/lib.rs` into per-concern modules (`sdk.rs`, `stream/grpc/`). No public API change — all `pub use` re-exports preserved. The new layout separates transport-agnostic logic (ingestion, ack tracking, teardown, callback dispatch) from gRPC-specific code (connection setup, sender/receiver tasks, supervisor) and places the gRPC transport under `stream/grpc/`, leaving room for `stream/arrow/` and a shared `stream/` core in follow-ups.

### Breaking Changes

### Deprecations

### API Changes

- Added `ZerobusSdkBuilder::token_cache_enabled(bool)` to enable or disable OAuth token caching (default enabled).
- Added `ZerobusSdkBuilder::token_refresh_buffer(Duration)` to configure how long before a cached token's expiry it is refreshed (default 5 minutes).
- Added `HeadersProvider::invalidate` with a default no-op implementation; the SDK calls it when the server rejects the supplied credentials so a provider can drop cached auth state. Existing trait implementations are unaffected.

## Release v2.2.2

### Major Changes

### New Features and Improvements

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

## Release v2.2.1

### Major Changes

### New Features and Improvements

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

## Release v2.2.0

### Major Changes

### New Features and Improvements

- Added a process-wide stream churn warning: logs a `WARN` when 100 or more streams for the same table are opened within a 60-second sliding window, which may indicate a "one stream per record" misuse pattern. Applies to `ZerobusStream` and `ZerobusArrowStream`. Set `ZEROBUS_SDK_WARNINGS_ENABLED=false` to suppress.

### Bug Fixes

### Documentation

### Internal Changes

- `DefaultTokenFactory` now requests OAuth tokens scoped to the `zerobuswrite` operation by including `"operations": ["zerobuswrite"]` in the token mint authorization details. Tightens token scope on the target table; transparent to callers using `ZerobusSdkBuilder::oauth(...)`.

### Breaking Changes

### Deprecations

### API Changes

## Release v2.1.1

### Major Changes

### New Features and Improvements

### Bug Fixes

- Fix the Arrow Flight example so it works against the prerequisite `orders`
  table — corrected the schema to `LargeUtf8` for `STRING`,
  `Timestamp(Microsecond, Some("UTC"))` for `TIMESTAMP`, and `nullable: true`.
  All four `examples/{json,proto}/{batch,single}.rs` now use
  `timestamp_micros()` so `created_at` / `updated_at` land at the current
  time instead of January 1970 (the server stores any int64 in a TIMESTAMP
  column without unit validation).

### Documentation

- Enable `all-features` on docs.rs so `arrow-flight` and `zeroparser` are
  visible. Re-export `TimeUnit` from the SDK root.
- Refresh `rust/README.md`: correct `prost` / `tokio` versions in the
  install snippet, fix the schema-tool build command, advertise the
  `arrow-flight` Beta feature, and update Repository Structure.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

## Release v2.1.0

### Major Changes

### New Features and Improvements

- **`zeroparser` (opt-in Cargo feature): zero-copy, descriptor-driven
  protobuf parser** for ingestion paths where the schema is only known at
  runtime. Exposes `databricks_zerobus_ingest_sdk::zeroparser`. Off by
  default; see `sdk/src/zeroparser/README.md`.

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

## Release v2.0.1

### Major Changes

### New Features and Improvements

### Bug Fixes

- **Arrow Flight: fix race condition causing stale wire offsets after non-close-signal
  recovery.** When a stream broke via a server error or ack timeout (rather than a graceful
  close signal), the supervisor did not set the ingest-pause gate before starting reconnect.
  A concurrent `ingest_batch` call could send a batch with a pre-recovery wire offset,
  which the server rejects with error code 4002 (`NonIncrementalOffset`), exhausting
  recovery retries and failing the entire stream. Fix: set `is_paused = true` immediately
  when entering the retriable-error retry branch, symmetric with the existing close-signal
  path.

- **Arrow Flight: restore automatic batch chunking at 2 MiB.** Reverted the manual
  zero-copy IPC encoding introduced in v2.0.0 back to `FlightDataEncoderBuilder`, which
  automatically chunks large `RecordBatch` values at 2 MiB. The zero-copy refactor had
  removed this chunking, causing large batches to exceed the server's message size limit of 10MB and be rejected. `ingest_ipc_batch` now deserialises IPC bytes into a `RecordBatch`
  before encoding, so it correctly benefits from the same chunking and supports streams
  with `ipc_compression` enabled.

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

## Release v2.0.0

### New Features and Improvements

- **Arrow Flight ingestion promoted to Beta**: The `arrow-flight` feature
  (`ZerobusArrowStream`, `ArrowStreamConfigurationOptions`, and related types)
  is no longer labelled experimental/unsupported. The API is stabilising but
  may still change before reaching GA.
- **Arrow schema from UC schema** (feature `arrow-flight`):
  `schema::arrow_schema_from_uc_columns` and `schema::arrow_schema_from_uc_schema`
  build an `arrow_schema::Schema` directly from Unity Catalog metadata, parallel
  to the existing `descriptor_from_uc_*` functions. Emits native Arrow types
  (`Date32`, `Timestamp(Microsecond, ..)`, `LargeUtf8`, `LargeBinary`,
  `Map("entries", Struct{keys,values})`) matching the canonical Arrow schema
  the Databricks Arrow Flight server builds from Delta.
- **`ZerobusSdkBuilder::application_name`**: Set a custom application identifier
  appended to the HTTP `user-agent` header (sent on the underlying tonic
  `Endpoint`) on every request. The default `zerobus-sdk-rs/<version>` prefix
  is preserved for server-side telemetry, so the wire value becomes
  `zerobus-sdk-rs/<version> <application_name>`. The previous `x-zerobus-sdk`
  gRPC metadata header is no longer emitted; downstream consumers that parsed
  it should switch to reading `user-agent`.
- **`ZerobusSdkBuilder::sdk_identifier`**: Override the SDK prefix of the
  HTTP `user-agent` header, replacing the default `zerobus-sdk-rs/<version>`.
  Intended for wrapper SDKs that need to replace the SDK identification; most
  callers should prefer `application_name`, which preserves the SDK version
  prefix. When both are set, `application_name` is still appended, so the wire value becomes `<sdk_identifier> <application_name>`.

### Bug Fixes

- Corrected the values returned by the C FFI `zerobus_get_default_config()`
  for `callback_max_wait_time_ms` / `has_callback_max_wait_time_ms`. The
  function previously reported `0 / false` (i.e., "no callback timeout"),
  while the actual Rust SDK default is `Some(5000ms)`. The C-side defaults
  now correctly mirror the Rust defaults (`5000 / true`).

### Documentation

- Updated `rust/README.md`, `rust/examples/README.md`,
  `rust/examples/json/README.md`, and `rust/examples/proto/README.md` to
  remove all references to the deleted future-based APIs. The
  "Future-based API (Deprecated)" example sections and the deprecated
  method entries in the API Reference were removed.
- Added an Arrow Flight example under `examples/arrow/` (`example_arrow`)
  demonstrating both `ingest_batch` (RecordBatch) and `ingest_ipc_batch`
  (Arrow IPC bytes).

### Internal Changes

- Consolidated Cargo workspace dependencies under `[workspace.dependencies]`
  in `rust/Cargo.toml`; member crates now use `dep.workspace = true` so
  versions are pinned in one place.
- Collapsed the four example packages (`example_json_{single,batch}`,
  `example_proto_{single,batch}`) into two packages,
  `rust-examples-json` and `rust-examples-proto`, each exposing two
  `[[example]]` targets. Examples are invoked as
  `cargo run -p rust-examples-json --example json_{single,batch}` and
  `cargo run -p rust-examples-proto --example proto_{single,batch}`.
- Bumped `prost` and `prost-types` from 0.13 to 0.14; `prost-reflect` from
  0.14 to 0.16. Public APIs that name `prost::Message` (e.g.
  `ProtoMessage<T: prost::Message>`) now require callers to use prost 0.14
  messages.
- Bumped `tonic` from 0.13 to 0.14. The 0.14 release splits code generation
  into separate crates: build-time codegen now uses `tonic-prost-build`
  (replacing `tonic-build`), and the runtime depends on the new
  `tonic-prost` crate for the prost codec. `sdk/build.rs`, `tests/build.rs`,
  and `tools/generate_files/src/generate.rs` were updated accordingly.
- Bumped Arrow crates (`arrow-flight`, `arrow-array`, `arrow-schema`,
  `arrow-ipc`) from 56.2.0 to 58.2. Switched `IpcDataGenerator::encoded_batch`
  to the non-deprecated `encode` API which takes an explicit
  `CompressionContext`.
- Raised minimum-version floors on several non-breaking dependencies to
  current latest minor: `tokio` 1.42 → 1.52, `tokio-stream` 0.1.16 →
  0.1.18, `tokio-util` 0.7.17 → 0.7.18, `once_cell` 1.19 → 1.21,
  `bytes` 1 → 1.11, `tempfile` 3.21 → 3.27, `clap` 4 → 4.6,
  `urlencoding` 2 → 2.1.
- Migrated the FFI and JNI crates off the deleted stream-creation methods.
  Both wrappers now build streams via `StreamBuilder`. Default config in
  `zerobus_get_default_config()` / `zerobus_arrow_get_default_config()`
  now reads `stream_options::defaults::*` constants directly instead of
  constructing `*ConfigurationOptions` (no longer needed at the FFI layer).
  No C ABI or JNI signature changes.
- FFI and JNI no longer construct `StreamConfigurationOptions` /
  `ArrowStreamConfigurationOptions`. They read C/Java struct fields
  directly and apply each via builder setters.

### Breaking Changes

- Removed `ZerobusSdk::create_stream()` (in deprecation since v1.3.0).
  Use `sdk.stream_builder().table(name).oauth(id, secret).json()` /
  `.compiled_proto(desc).build().await` instead. Removed from all
  examples, documentation, and tests.
- Removed `ZerobusSdk::create_stream_with_headers_provider()` (in
  deprecation since v1.3.0). Use
  `sdk.stream_builder().table(name).headers_provider(p).json()` /
  `.compiled_proto(desc).build().await` instead. Removed from all
  examples, documentation, and tests.
- Removed `ZerobusSdk::create_arrow_stream()` _(feature `arrow-flight`)_
  (in deprecation since v1.3.0). Use
  `sdk.stream_builder().table(name).oauth(id, secret).arrow(schema).build_arrow().await`
  instead. Removed from all examples, documentation, and tests.
- Removed `ZerobusSdk::create_arrow_stream_with_headers_provider()`
  _(feature `arrow-flight`)_ (in deprecation since v1.3.0). Use
  `sdk.stream_builder().table(name).headers_provider(p).arrow(schema).build_arrow().await`
  instead. Removed from all examples, documentation, and tests.
- Removed `ZerobusStream::ingest_record()` (in deprecation since v0.4.0).
  Use `stream.ingest_record_offset(payload).await?` followed by
  `stream.wait_for_offset(offset).await?` to wait for acknowledgment.
  Removed from all examples, documentation, and tests.
- Removed `ZerobusStream::ingest_records()` (in deprecation since v0.4.0).
  Use `stream.ingest_records_offset(payloads).await?` followed by
  `stream.wait_for_offset(offset).await?`. Removed from all examples,
  documentation, and tests.
- Removed `ZerobusSdk::new()` (in deprecation since v0.5.0). Use
  `ZerobusSdk::builder().endpoint(...).unity_catalog_url(...).build()?`
  instead.
- Removed the `ZerobusSdk::use_tls` field (in deprecation since v0.5.0).
  TLS is controlled via `ZerobusSdkBuilder::tls_config(...)`. The C FFI
  `zerobus_sdk_set_use_tls()` function is retained as a no-op for ABI
  compatibility.
- Removed the `test_proto_stream_creation_without_descriptor_fails` test
  — the typestate `StreamBuilder` makes that scenario impossible at
  compile time.
- Added `#[non_exhaustive]` to `StreamConfigurationOptions`. External
  crates can no longer construct the struct via struct-literal syntax;
  all configuration must go through `StreamBuilder` setters. Field reads
  via `stream.options.*` are unaffected. Adding new config fields in
  future releases is now non-breaking.
- Added `#[non_exhaustive]` to `ArrowStreamConfigurationOptions`. Same
  semantics as above; reads via `stream.options().*` are unaffected.
- Added `#[non_exhaustive]` to `ZerobusError`, `StreamType`, and
  `SchemaError` enums. External `match` expressions on these types now
  require a `_ =>` wildcard arm. Adding new variants is non-breaking.
- Added `#[non_exhaustive]` to `ZerobusSdk`, `ZerobusStream`, and
  `ZerobusArrowStream` structs. Adding new fields to these top-level
  handle types is non-breaking.
- `TableProperties` and `ArrowTableProperties` are now `pub(crate)` and
  no longer part of the public API. They are only used internally by
  `StreamBuilder`; after the deletion of the deprecated
  `create_*_stream()` methods there are no external constructors.
- Removed `ZerobusArrowStream::table_properties()` getter (returned the
  now-private `ArrowTableProperties`). Use the existing `table_name()`
  and `schema()` getters instead.
- Major-version bumps of `prost` (0.13 → 0.14), `tonic` (0.13 → 0.14),
  `prost-reflect` (0.14 → 0.16), and the Arrow crates (56 → 58). Downstream
  consumers that directly handle SDK-exported `prost::Message` or
  `arrow_array::RecordBatch` values must move to the matching major
  versions of those crates.

## Release v1.3.0

### New Features and Improvements

- **Arrow Flight — graceful stream close**: When the server signals that the stream will close, the SDK enters a paused state: it stops sending new batches, drains in-flight acknowledgments up to a configurable wait, then recovers.
- **`stream_paused_max_wait_time_ms`** on `ArrowStreamConfigurationOptions`: Optional cap (milliseconds) on how long to wait during that paused phase (`None` = use full server duration, `Some(0)` = recover immediately, `Some(x)` = wait up to `min(x, server_duration)`).
- Added `ZerobusSdkBuilder::connector_factory` for programmatic proxy
  configuration. Callers can install a `ConnectorFactory` (a
  `Fn(&str) -> Option<ProxyConnector>` closure) that fully overrides the
  default env-var proxy detection — useful for embedders that already model
  proxy config in their own configuration system (e.g. Vector's `ProxyConfig`).
  When no factory is installed, the existing `grpc_proxy` / `https_proxy` /
  `http_proxy` env-var behavior is unchanged.
- The env-var proxy path now supports `https://` proxy URLs. The client→proxy
  hop does a TLS handshake using the system trust store; the CONNECT tunnel
  still carries raw TCP so tonic applies end-to-end TLS to the target endpoint
  on top.
- **`StreamBuilder` API**: New fluent builder for creating ingestion streams.
  Setters can be called in any order; the builder validates at `build()` time
  that both authentication and format have been configured.

### Bug Fixes

- **gRPC / HTTP/2 teardown on close and recovery**: Receive and send tasks now shut down with a per-stream `CancellationToken`, bounded waits before `abort`, and a separate `recv_drain_token` on the receiver. This avoids racing **`RST_STREAM` / `CANCEL`** from the client against **`END_STREAM`** from the server—failure modes that could show up as HTTP/2 protocol errors or broken pipe on the server.
- After the inbound receive loop exits, the response-stream drain is now split by exit reason: the close path (`recv_drain_token`) drains **inline** so the server sees `END_STREAM` before the client process exits and the runtime tears down; the recovery / error paths drain in a **detached** task so `flush()` and stream recovery aren't delayed.
- **`StreamBuilder::stream_paused_max_wait_time_ms`**: Now updates Arrow stream settings (`arrow_config`) as well as JSON/proto gRPC settings, so `.build_arrow()` respects this option (previously only JSON/proto streams saw the value).

### Internal Changes

- Reduced log verbosity in `wait_for_offset` / `wait_for_acks` polling loops.
  Per-iteration progress logs are now emitted at `trace` level, and the
  one-shot "completed" log is now at `debug` level (previously `info`). This
  removes repeated `info`-level noise observed when callers wait for flushes
  or graceful close.

### Deprecations

- **`ZerobusSdk::create_stream()`**: Use `sdk.stream_builder(table).oauth(id, secret).json().build().await` instead
- **`ZerobusSdk::create_stream_with_headers_provider()`**: Use `sdk.stream_builder(table).headers_provider(p).json().build().await` instead
- **`ZerobusSdk::create_arrow_stream()`**: Use `sdk.stream_builder(table).oauth(id, secret).arrow(schema).build_arrow().await` instead
- **`ZerobusSdk::create_arrow_stream_with_headers_provider()`**: Use `sdk.stream_builder(table).headers_provider(p).arrow(schema).build_arrow().await` instead

### API Changes

- New public exports: `ProxyConnector`, `ConnectorFactory`, `StreamBuilder`.
- New builder method: `ZerobusSdkBuilder::connector_factory`.
- New entry point: `ZerobusSdk::stream_builder()`.
- Changed `ZerobusSdk` fields `workspace_id` and `tls_config` to `pub(crate)` visibility (no public API impact).

## Release v1.2.0

### Major Changes

- **License: Migrated from the Databricks License to the Apache License 2.0**

### New Features and Improvements

- Added the `schema` module with `descriptor_from_uc_columns` /
  `descriptor_from_uc_schema`, which convert a Unity Catalog table schema
  (including nested `STRUCT`, `ARRAY`, and `MAP` columns via `type_json`) into
  a `prost_types::DescriptorProto` that can be passed to
  `TableProperties::descriptor_proto`. Enables building descriptors at runtime
  without pre-generating `.proto` files.

### Internal Changes

- The `generate_files` CLI tool now delegates schema → descriptor conversion
  to the SDK's new `schema` module instead of its own hand-rolled DDL-string
  parser, and renders the resulting `DescriptorProto` back to proto2 text.

### Breaking Changes

- `generate_files`: the emitted `.proto` files have changed shape for
  non-trivial schemas. Consumers regenerating existing files should expect:
  - Field numbers now follow Unity Catalog's `position + 1` (so gaps from
    `DROP COLUMN` under Delta column-mapping are preserved) instead of the
    previous 1,2,3… sequential numbering with a 19000-range skip.
  - Nested struct messages use path-based names (e.g. `OuterInner` instead of
    `Inner`) and are emitted hierarchically inside their parent message.
  - Struct field nullability now honors Unity Catalog's `nullable` flag
    instead of being forced to `optional`.

## Release v1.1.0

### New Features and Improvements

- **[Experimental Arrow Flight] Zero-copy IPC ingestion via `ingest_ipc_batch`**: Added `ZerobusArrowStream::ingest_ipc_batch(Bytes)` for FFI callers (Go, Python, Java, TypeScript) that already hold Arrow IPC stream bytes. Raw bytes are forwarded directly to the Flight wire format without deserialising to a `RecordBatch` and re-serialising, eliminating one IPC round-trip per batch compared to `ingest_batch`. The existing `ingest_batch` API is unchanged.

### Bug Fixes

- Fixed proto generation tool to skip reserved field numbers 19000-19999 for tables with more than 19000 columns

## Release v1.0.1

### Bug Fixes

- Fixed TLS certificate validation failure when behind corporate VPN/proxy with MITM certificates (e.g., GlobalProtect). Changed `reqwest` TLS configuration from `rustls-tls` to `rustls-tls-native-roots` + `rustls-tls-webpki-roots`, so the SDK now loads CA certificates from the OS native trust store (respecting `SSL_CERT_FILE` and system certificate stores) while keeping bundled Mozilla roots as a fallback for minimal environments.

### New Features and Improvements

- Exported `OAuthHeadersProvider` in the public API, allowing clients to directly construct and use the built-in OAuth 2.0 headers provider.

## Release v1.0.0

GA release of the Databricks Zerobus Ingest SDK for Rust.

### New Features and Improvements

- Added HTTP proxy support via standard environment variables (`grpc_proxy`, `https_proxy`, `http_proxy`), following gRPC core conventions. Proxied connections use HTTP CONNECT tunneling with end-to-end TLS. Supports `no_grpc_proxy` / `no_proxy` for bypass rules.

### Deprecations

### Bug Fixes

- Fixed a rare race condition in `wait_for_offset_internal` where the actual server error (e.g., `InvalidArgument`) was lost and replaced by a generic `StreamClosedError`. This occurred when `error_rx.changed()` fired but `is_closed` had not yet been set by the supervisor, causing the error to be missed on the next loop iteration.

## Release v0.6.0

### New Features and Improvements

- **Automatic `https://` scheme prepending**: Endpoints without a scheme now automatically get `https://` prepended. Previously, schemeless endpoints would fail with `InvalidUri` (builder) or fail to extract the workspace ID (deprecated `new()` constructor).

### Documentation

- Updated all examples to consistently include `https://` in endpoint URLs

## Release v0.5.0

### New Features and Improvements

- **Builder Pattern for SDK Initialization**: Added `ZerobusSdk::builder()` for fluent SDK configuration
  - `.endpoint()` - Set the Zerobus endpoint (~~scheme is optional, defaults work with or without `https://`~~ `https://` is required; schemeless endpoints are auto-prepended since v0.6.0)
  - `.unity_catalog_url()` - Set the Unity Catalog URL (optional when using custom headers providers)
  - `.tls_config()` - Provide a custom `TlsConfig` implementation (defaults to `SecureTlsConfig`)
- **Configurable TLS via `TlsConfig` trait**: TLS is now configured through a strategy pattern
  - `SecureTlsConfig` (default) - Production TLS with system CA certificates
  - `NoTlsConfig` - No-op TLS for testing with plaintext `http://` endpoints (requires `testing` feature)
  - Implement `TlsConfig` trait for custom certificate handling
- **SDK Identifier Header**: Renamed `user-agent` header to `x-zerobus-sdk` for clearer SDK identification in gRPC metadata
- **Type Widening for Record Ingestion**: Added wrapper types for record ingestion
  - **`ProtoMessage<T>`**: SDK handles encoding - pass any `prost::Message` directly
  - **`JsonValue<T>`**: SDK handles serialization - pass any `serde::Serialize` type directly
  - **`ProtoBytes`**: Client handles encoding - explicit wrapper for pre-encoded protobuf bytes
  - **`JsonString`**: Client handles serialization - explicit wrapper for pre-serialized JSON strings
  - **Backward compatible**: existing code using `Vec<u8>` and `String` continues to work
  - Works with both single record and batch ingestion methods

### Deprecations

- **`ZerobusSdk::new()`**: Use `ZerobusSdk::builder()` instead
- **`ZerobusSdk.use_tls` field**: TLS is now controlled via the `TlsConfig` trait passed to the builder

### Bug Fixes

- **[Experimental] Record-based acknowledgment tracking for Arrow Flight streams**: Added cumulative record counting to support proper ack tracking and correct recovery when batches are auto-chunked.

### Documentation

- Reorganized examples directory structure: `json/single`, `json/batch`, `proto/single`, `proto/batch`
- Added separate README files for JSON and Protocol Buffers examples with comprehensive documentation
- Updated all examples to demonstrate three data-passing approaches: auto-encoding/serializing wrappers, pre-encoded/serialized wrappers, and backward-compatible raw types

### Internal Changes

### API Changes

- **Added `ZerobusSdkBuilder`** for fluent SDK configuration (replaces `ZerobusSdk::new()`)
- **Added `TlsConfig` trait** with `SecureTlsConfig` (default) and `NoTlsConfig` (behind `testing` feature)
- **Renamed header** from `user-agent` to `x-zerobus-sdk` in gRPC metadata
- **Added type widening wrapper types** (backward compatible):
  - Added `ProtoMessage<T: prost::Message>` - SDK handles encoding for protobuf messages
  - Added `JsonValue<T: serde::Serialize>` - SDK handles serialization for JSON objects
  - Added `ProtoBytes` - for pre-encoded protobuf bytes (client handles encoding)
  - Added `JsonString` - for pre-serialized JSON strings (client handles serialization)
  - All new types implement `Into<EncodedRecord>` for seamless integration
  - Existing `Vec<u8>` and `String` types continue to work (backward compatible)

## Release v0.4.0

### New Features and Improvements

- **Acknowledgment Callbacks**: Added callback support for receiving notifications when records are acknowledged
  - New `AckCallback` trait with `on_ack()` and `on_error()` methods
  - Configurable via `ack_callback` field in `StreamConfigurationOptions` and `ArrowStreamConfigurationOptions`

- Added support for `TINYINT/BYTE`, `TIMESTAMP_NTZ`, and `VARIANT` data types in the proto generation tool

- **Alternative Ingestion API with Direct Offset Return**: Added `ingest_record_offset()` and `ingest_records_offset()` methods
  - Return `OffsetId` (logical offset) directly as an integer (after queuing) instead of wrapping it in a Future
  - Can be used with new `wait_for_offset()` method to block on acknowledgment when needed
  - Allows decoupling record ingestion from acknowledgment tracking
  - Useful for scenarios where you want to collect offsets and wait on them selectively

### Deprecations

- **Deprecated `ingest_record()` and `ingest_records()` methods**: Use `ingest_record_offset()` and `ingest_records_offset()` instead
  - The new methods return offsets directly (after queuing) without Future wrapping for a cleaner API
  - Use with `wait_for_offset()` to explicitly wait for acknowledgments when needed
  - Old methods will continue to work but may be removed in a future version

### Bug Fixes

- Improved error propagation in `wait_for_offset()` and `flush()`: errors from the server are now detected and returned immediately instead of waiting for timeout, providing faster feedback and more accurate error messages

- Improved error classification in OAuth token retrieval: 5xx server errors and network failures are now retryable, while 4xx client errors (invalid credentials, etc.) are non-retryable

### Documentation

### Internal Changes

- Refactored `wait_for_offset_internal` to remove unnecessary double loop
- Optimized gRPC channel reuse: `ZerobusSdk` now reuses a single gRPC channel across multiple stream creations instead of creating a new channel for each stream, improving connection efficiency and reducing resource overhead
- Enhanced background tasks with `is_closed` checks and proper error broadcasting to the shared error channel, ensuring timely shutdown and accurate error reporting

- Added `user-agent` header to all gRPC requests for SDK version tracking

- Refactored `flush()` and `wait_for_offset()` to share common waiting logic via `wait_for_offset_internal()`, reducing code duplication and ensuring consistent behavior

- Improved graceful close mechanism: when server signals stream closure, SDK now continues processing acknowledgments for in-flight records while pausing new record transmission until timeout.

### API Changes

- [**BREAKING**] Added `callback_max_wait_time_ms` to `StreamConfigurationOptions` to limit how long callbacks may run after stream closure (`None` = infinite, `Some(x)` = `x` ms).
- Added `ack_callback: Option<Arc<dyn AckCallback>>` field to `StreamConfigurationOptions` and `ArrowStreamConfigurationOptions` for acknowledgment callbacks
- Added `AckCallback` trait with `on_ack(offset_id)` and `on_error(offset_id, error_message)` methods

- Added Arrow IPC compression support via `ipc_compression: Option<CompressionType>` in `ArrowStreamConfigurationOptions` (supports `LZ4_FRAME` and `ZSTD`, default: `None`)
- **[BREAKING]** Changed `ZerobusArrowStream::ingest_batch()` to return `OffsetId` directly instead of `Future<Output = OffsetId>`. Use `wait_for_offset(offset)` to explicitly wait for acknowledgment
- Added `ZerobusArrowStream::wait_for_offset()` method to wait for acknowledgment of a specific offset
- Added `is_closed` check at the beginning of `flush()` for both `ZerobusStream` and `ZerobusArrowStream`

- Added `ingest_record_offset()` method to `ZerobusStream` for direct offset return without Future wrapping
- Added `ingest_records_offset()` method to `ZerobusStream` for batch ingestion with direct offset return
- Added `wait_for_offset()` method to `ZerobusStream` to wait for acknowledgment of a specific offset

- [**BREAKING**] Added `stream_paused_max_wait_time_ms` to `StreamConfigurationOptions` to configure maximum wait time during graceful stream close (`None` = wait for full server duration, `Some(0)` = immediate recovery, `Some(x)` = wait up to min(x, server_duration) milliseconds)

## Release v0.3.0

### New Features and Improvements

- **Arrow Flight Ingestion (Experimental)**: Added experimental Arrow Flight support for high-throughput Apache Arrow record batch ingestion
  - Opt-in feature: enable with `features = ["arrow-flight"]` in Cargo.toml
  - Transmits Arrow RecordBatches in native IPC format (no format conversion required)
  - Same recovery and retry semantics as gRPC streams
  - **Note**: This feature is currently experimental and unsupported

## Release v0.2.0

### New Features and Improvements

- **Batch Ingestion API**: Added `ingest_records()` method for ingesting multiple records at once
  - All-or-nothing semantics: entire batch succeeds or fails as a unit
  - Ingesting an empty batch is a no-op.
- **JSON Serialization Support**: Added support for JSON record serialization alongside Protocol Buffers (default)
  - No protobuf schema compilation required
- Added `HeadersProvider`, a trait for flexible authentication strategies
- Implemented `OAuthHeadersProvider` for OAuth 2.0 Client Credentials flow (default authentication method used by `create_stream()`)

### Bug Fixes

### Documentation

- Added JSON and protobuf serialization examples for batch ingestion
- Enhanced API Reference with batch ingestion documentation
- Added JSON and protobuf serialization examples
- Updated README's.
- Enhanced API Reference with JSON mode documentation
- Added Azure workspace and endpoint URL examples

### Internal Changes

- [**BREAKING**] Changed backpressure mechanism to track in-flight requests instead of in-flight records

### API Changes

- [**BREAKING**] changed `max_inflight_records` to `max_inflight_requests` in `StreamConfigurationOptions` as we now track in-flight requests
- [**BREAKING**] `get_unacked_records()` method now returns `impl Iterator<Item = EncodedRecord>` instead of `Vec<Vec<u8>>` - flattens all batches into individual records
- Added `get_unacked_batches()` method to `ZerobusStream` that returns `Vec<EncodedBatch>` to preserve batch structure - records ingested together remain grouped
- Added `ingest_records()` method to `ZerobusStream` for bulk record ingestion
- `recreate_stream` method in `ZerobusSdk` now accepts a reference to a stream, instead of taking ownership of it.
- `TableProperties` struct now has `descriptor_proto` field as optional (**breaking change**).
- Added `HeadersProvider` trait for custom header strategies
- Added `OAuthHeadersProvider` struct for OAuth 2.0 authentication with Databricks OIDC endpoint
- Added `create_stream_with_headers_provider` method to `ZerobusSdk` for custom authentication header providers

## Release v0.1.1

- Added comprehensive API documentation and fixed Cargo.toml metadata for crates.io publication

## Release v0.1.0

Initial release of the Databricks Zerobus Ingest SDK for Rust.

### API Changes

- Added `ZerobusSdk` struct for creating ingestion streams.
- Added `ZerobusStream` struct for managing the stateful gRPC stream.
- The `ingest_record` method returns a future that resolves to the record's acknowledgment offset.
- Added `TableProperties` for configuring the target table schema and name.
- Added `StreamConfigurationOptions` for fine-tuning stream behavior like recovery and timeouts.
- Added `ZerobusError` enum for detailed error handling, including a `is_retryable()` method.
- The SDK is built on `tokio` and is fully asynchronous.
