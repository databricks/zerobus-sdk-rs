# PR #159 — "zero-copy IPC batch ingestion" — Architecture & Alignment Review

**PR**: `elenagaljak-db-arrow_ipc` → `main`  
**Issue**: #147 — "[Rust] Zero-copy ingestion pipeline"  
**Reviewer**: AI review (April 7 2026)

---

## 1. Scope Assessment

Issue #147 proposes four changes:

| # | Item | In this PR? |
|---|------|:-----------:|
| 1 | Use `bytes::Bytes` internally (LandingZone O(1) clone) | No |
| 2 | Configure prost to generate `Bytes` for proto `bytes` fields | No |
| 3 | New `ingest_ipc_batch(Bytes)` on `ZerobusArrowStream` | **Yes** |
| 4 | New `ingest_proto_bytes`/`ingest_json_bytes` on `ZerobusStream` | No |

**The PR correctly scopes to item #3 only.** The new `ingest_ipc_batch` method, the `ArrowPayload` enum, and all IPC-parsing helpers are exclusively related to the Arrow path.

### Items included beyond strict item-#3 scope

The PR also refactors the internal pipeline from `mpsc::Sender<Result<RecordBatch, FlightError>>` to `mpsc::Sender<Result<FlightData, FlightError>>`, removing the `FlightDataEncoderBuilder` entirely. This change affects all callers — including the existing `ingest_batch` path — and is not described as a separate item in issue #147. It is defensible as a prerequisite for item #3 (the channel must carry `FlightData` to support pre-encoded IPC payloads), but it is a bigger architectural change than the issue text anticipates for item #3 alone.

### Items that arguably should be included but aren't

- **`bytes::Bytes` re-export**: `ingest_ipc_batch` takes `Bytes` as a parameter, but `bytes::Bytes` is not re-exported from the crate's public API (`lib.rs:79-83`). Users of the public API must add a direct `bytes` dependency to call the new method. This is a minor ergonomic gap. The crate should either re-export `Bytes` or accept `impl Into<Bytes>` (see §2).

### Unrelated changes

- `python/rust/Cargo.lock`: A 3,174-line new lockfile is introduced. This is a generated file and appears to be an artifact from the author's local workspace — it adds no `bytes`-related changes to the Python bindings. Should be reviewed for whether it's intentional or accidental.
- `.github/workflows/*`: CI changes are out of scope per instructions and ignored.

**Verdict**: Scope is well-chosen with one caveat — the pipeline refactor from `RecordBatch` to `FlightData` on the channel is a significant architectural change that affects `ingest_batch` callers, not just `ingest_ipc_batch`, and deserves explicit call-out in the PR description.

---

## 2. API Surface Review

### 2.1 Parameter type: `Bytes` vs `impl Into<Bytes>`

The signature is:

```rust
pub async fn ingest_ipc_batch(&self, ipc_bytes: Bytes) -> ZerobusResult<OffsetId>
```

**`Bytes` is the right choice** for the primary use case (FFI callers that already hold `Bytes`). However, accepting `impl Into<Bytes>` would be strictly more ergonomic:

- `Vec<u8>` converts to `Bytes` via `Bytes::from(Vec<u8>)` with zero copy (pointer handoff).
- `&'static [u8]` converts via `Bytes::from_static`.
- `Bytes` itself implements `Into<Bytes>` trivially.

This would let Rust-native callers pass a `Vec<u8>` without an explicit conversion. The trade-off is a marginally more complex signature. Given that the primary audience is FFI and the method lives in an experimental module, the current `Bytes` parameter is acceptable, but `impl Into<Bytes>` would be better for Rust ergonomics.

**Recommendation**: Consider `impl Into<Bytes>` for the parameter. If not, re-export `bytes::Bytes` from `lib.rs` so callers don't need a separate dependency.

### 2.2 Schema validation

`ingest_batch` validates the schema at `arrow_stream.rs:1288-1295`:

```rust
if batch.schema() != self.table_properties.schema {
    return Err(ZerobusError::InvalidArgument(...));
}
```

`ingest_ipc_batch` does **not** validate that the IPC batch's schema matches the stream schema. It only validates structural correctness (valid IPC stream, exactly one batch).

This is a deliberate trade-off documented in the issue:
> For the `Ipc` variant, construct `FlightData` directly from the IPC bytes without reconstructing a `RecordBatch`.

Schema validation would require parsing the IPC schema message and comparing it against `self.table_properties.schema`. This is cheaper than full deserialization but not free. The IPC schema is already embedded in the `validate_ipc_stream_exactly_one_record_batch` call — the `StreamReader` parses it. A lightweight schema check could be done there.

**Recommendation**: At minimum, document the schema validation gap in the `ingest_ipc_batch` doc comment (e.g., "Callers are responsible for ensuring the IPC schema matches the stream schema; mismatches will be caught by the server."). Ideally, extract the IPC schema during validation and compare it, since the `StreamReader` already parses it — this would be nearly free.

### 2.3 Visibility

The method is `pub` on `ZerobusArrowStream`, which itself lives in a module gated behind `#[cfg(feature = "arrow-flight")]` and documented as **experimental/unsupported** (`arrow_stream.rs:1-4`). The `pub` visibility is appropriate — the feature gate is the access control mechanism.

---

## 3. Pipeline Architecture Change

### 3.1 What changed

| Aspect | Before | After |
|--------|--------|-------|
| Channel type | `mpsc::Sender<Result<RecordBatch, FlightError>>` | `mpsc::Sender<Result<FlightData, FlightError>>` |
| Encoding point | `FlightDataEncoderBuilder` in the stream pipeline (lazy, on-demand) | At call site in `ingest_batch` / `ingest_ipc_batch` (eager, at ingestion time) |
| Encoder | `FlightDataEncoderBuilder` (arrow-flight crate) | `IpcDataGenerator::encoded_batch` + `SchemaAsIpc` (arrow-ipc crate) |

### 3.2 Backpressure semantics

The `mpsc::channel` is bounded by `options.max_inflight_batches`. The change does **not** alter the channel capacity, but it changes **what** is buffered:

- **Before**: `RecordBatch` objects sit in the channel. Encoding to `FlightData` happens lazily as the `FlightDataEncoderBuilder` stream is polled by the gRPC transport layer.
- **After**: `FlightData` (already-encoded IPC bytes) sits in the channel. Encoding happens eagerly in the `ingest_batch`/`ingest_ipc_batch` call.

This means:
- **Backpressure signal is unchanged**: The channel still blocks when `max_inflight_batches` items are queued. The semantic is identical.
- **Work distribution shifts**: Encoding CPU work moves from the transport task to the caller task. This is fine — it means `ingest_batch().await` does more work before returning, but that work is O(batch_size) regardless of when it happens.
- **For `ingest_ipc_batch`**: The encoding is essentially a metadata parse + `Bytes::slice`, which is O(1). This is strictly better than the old pipeline which would have had to deserialize IPC → `RecordBatch` → re-encode.

**Verdict**: No regression in backpressure semantics.

### 3.3 Memory usage patterns

- **Before**: Channel holds `RecordBatch` (Arc-backed column arrays). `FlightDataEncoderBuilder` produces `FlightData` bytes on demand; the `RecordBatch` is dropped after encoding.
- **After**: Channel holds `FlightData` (encoded IPC bytes). The `RecordBatch` (or `Bytes`) is also held in `pending_batches` for recovery.

This means:
- For `ingest_batch`: Memory holds **both** the `RecordBatch` (in `PendingBatch.payload`) and its encoded `FlightData` (in the channel) simultaneously until the `FlightData` is consumed by gRPC. This is a ~2x memory spike per in-flight batch compared to before, where only the `RecordBatch` was held.
- For `ingest_ipc_batch`: The `Bytes` in `PendingBatch.payload` and the `FlightData` in the channel share the same underlying allocation via `Bytes::slice`, so memory is ~1x (plus small metadata overhead). This is the optimal case.

**Recommendation**: Document this memory trade-off. For the `ingest_batch` path, consider encoding lazily or not storing both forms simultaneously. However, the double-storage only exists for the duration the `FlightData` is in the channel (bounded by `max_inflight_batches`), so in practice the impact is limited.

### 3.4 Architectural coupling

The change introduces cleaner separation: the channel now carries the wire-format type (`FlightData`) rather than a domain type (`RecordBatch`). The `start_stream_connection` method no longer needs to know about Arrow encoding — it just chains a metadata-adding map over the stream. This is a **net improvement** in coupling.

The encoding logic in `record_batch_to_flight_data` and `ipc_bytes_to_flight_data` is cleanly factored into standalone functions.

---

## 4. Removal of `FlightDataEncoderBuilder`

### 4.1 What was lost

`FlightDataEncoderBuilder` (from the `arrow-flight` crate) provides several features:

| Feature | Was it used? | Handled in PR? |
|---------|:------------:|:--------------:|
| Schema message emission | Yes | Yes — `schema_to_flight_data()` via `SchemaAsIpc` |
| RecordBatch → FlightData encoding | Yes | Yes — `record_batch_to_flight_data()` via `IpcDataGenerator` |
| Dictionary encoding/hydration | No (tracker set to `false`) | Yes — `DictionaryTracker::new(false)` matches old behavior |
| `max_flight_data_size` chunking | **No** | N/A — never used, old code also didn't set it |
| IPC compression | Yes (via `with_options`) | Yes — `make_ipc_write_options` passes through `ipc_compression` |
| `FlightDescriptor` attachment | No | N/A |
| `app_metadata` per-batch | Handled externally | Still handled externally via `.enumerate().map()` |

**The PR correctly replicates all features that were actually in use.** The `DictionaryTracker::new(false)` at `arrow_stream.rs:308` matches the behavior of `FlightDataEncoderBuilder`'s default (no dictionary deduplication).

### 4.2 `ipc_bytes_to_flight_data` manual IPC parsing

The function at `arrow_stream.rs:211-279` manually parses the Arrow IPC wire format to extract `data_header` and `data_body` ranges. This is the most sensitive code in the PR.

**Concerns**:
1. **IPC alignment padding**: The Arrow IPC spec requires 8-byte alignment for message bodies. The `read_meta_range` function handles the continuation marker (`0xFFFFFFFF`) but does not account for padding bytes between the metadata and the body. In practice, Arrow IPC stream writers (as opposed to file writers) may or may not add padding. The `validate_ipc_stream_exactly_one_record_batch` call (which uses the canonical `StreamReader`) runs first and will catch malformed streams, so this is a defense-in-depth situation.
2. **Double validation**: `ipc_bytes_to_flight_data` calls `validate_ipc_stream_exactly_one_record_batch` (full StreamReader parse) and then manually re-parses the same bytes to extract ranges. This means every IPC batch is parsed twice. The validation could be combined with the range extraction to halve the parsing cost.
3. **Flatbuffer dependency**: The function uses `arrow_ipc::root_as_message`, which is a low-level flatbuffer accessor. This ties the code to the arrow-ipc crate's internal flatbuffer layout, which, while stable, is less insulated from upstream changes than using the `StreamReader` API.

**Recommendation**: Consider combining the validation and range-extraction into a single pass. Alternatively, use the `StreamReader` to parse and extract both the schema and batch, then construct `FlightData` from the reader's output — this would be more robust and only marginally slower (the `StreamReader` already does flatbuffer parsing).

---

## 5. Breaking Change Analysis

### 5.1 Public API changes

| Change | Breaking? |
|--------|:---------:|
| New `ingest_ipc_batch` method | No — additive |
| `bytes = "1"` added to `Cargo.toml` | No — transitive dependency already present |
| `ArrowPayload` enum | No — `pub(crate)` only |
| `BatchSender` type alias changed | No — private |
| `PendingBatch.batch` → `PendingBatch.payload` | No — private |
| `FlightData` on the channel | No — private |

**No public API breakage.**

### 5.2 Behavioral changes for `ingest_batch` callers

- **Encoding happens eagerly** in `ingest_batch` rather than lazily in the stream pipeline. This changes the timing of encoding errors: they now surface as `ingest_batch` return errors rather than as stream errors. This is arguably better (fail-fast).
- **Schema message** is now sent via `SchemaAsIpc` rather than `FlightDataEncoderBuilder`. The wire format should be identical, but this is a subtle change that should be verified with integration tests.

### 5.3 Recovery semantics changes

- `slice_batch_for_recovery` now returns `ZerobusResult<Option<ArrowPayload>>` instead of `Option<RecordBatch>`. For the `ArrowPayload::Batch` variant, behavior is identical. For the `ArrowPayload::Ipc` variant, partial recovery requires deserialization (documented at `arrow_stream.rs:125-137`). This is a new error path that didn't exist before.
- The reconnect method (`arrow_stream.rs:863-1053`) now encodes replayed batches to `FlightData` before sending, matching the new channel type. The old code sent `RecordBatch` and let `FlightDataEncoderBuilder` handle encoding. Functionally equivalent.

### 5.4 `get_unacked_batches` behavior changes

- Return type is unchanged: `Vec<RecordBatch>`.
- **New error path**: For IPC-backed payloads, `get_unacked_batches` now calls `materialize()` which can fail if the IPC bytes are corrupt (`arrow_stream.rs:1650-1656`). Previously, `get_unacked_batches` was infallible (it just cloned `RecordBatch`es). The doc comment is updated to reflect this (`arrow_stream.rs:1610`).
- This is a **subtle behavioral change**: callers that previously relied on `get_unacked_batches` never failing (after close) may now see `InvalidArgument` errors. In practice, corrupt IPC bytes would have been caught at ingestion time, so this path should be unreachable under normal conditions.

**Verdict**: The PR is non-breaking at the public API level. There are subtle behavioral changes in error paths for `get_unacked_batches`, but these are edge cases that are documented and reasonable.

---

## 6. Forward Compatibility

### 6.1 Item #1 — `Bytes` in LandingZone

This PR does not touch the LandingZone or proto/JSON path. The `ArrowPayload::Ipc(Bytes)` variant demonstrates the pattern that item #1 would generalize. **No obstacles created.**

### 6.2 Item #2 — prost `Bytes` generation

This PR does not touch `build.rs` or proto generation. **No obstacles created.**

### 6.3 Item #4 — `ingest_proto_bytes`/`ingest_json_bytes`

This PR does not touch `ZerobusStream`. The `ArrowPayload` pattern (an enum with a `Bytes` variant and a materialised variant) could serve as a template for a similar `ProtoPayload` or `JsonPayload` enum. **No obstacles created; pattern is reusable.**

### 6.4 Potential friction

- The PR introduces `bytes = "1"` as a direct dependency in `sdk/Cargo.toml`. Items #1 and #2 also need this, so this is forward-compatible and avoids duplicate work.
- The pipeline refactor (channel carrying `FlightData` instead of `RecordBatch`) is specific to the Arrow path and doesn't affect the proto/JSON pipeline architecture. Items #1 and #4 will need their own pipeline changes.

**Verdict**: This PR creates no obstacles and sets useful precedent for the remaining items.

---

## 7. Changelog and Documentation

### 7.1 Changelog entry (`rust/NEXT_CHANGELOG.md`)

The entry at line 9 is well-written:

> **[Experimental Arrow Flight] Zero-copy IPC ingestion via `ingest_ipc_batch`**: Added `ZerobusArrowStream::ingest_ipc_batch(Bytes)` for FFI callers (Go, Python, Java, TypeScript) that already hold Arrow IPC stream bytes. Raw bytes are forwarded directly to the Flight wire format without deserialising to a `RecordBatch` and re-serialising, eliminating one IPC round-trip per batch compared to `ingest_batch`. The existing `ingest_batch` API is unchanged.

**Missing from changelog**: The internal pipeline refactor from `RecordBatch` to `FlightData` on the channel. While this is an internal change, it affects encoding timing for `ingest_batch` callers and the fact that `FlightDataEncoderBuilder` is no longer used. A line under "Internal Changes" would be appropriate:

> Replaced `FlightDataEncoderBuilder` with direct `IpcDataGenerator` encoding; the internal channel now carries pre-encoded `FlightData` messages.

### 7.2 Doc comments

- `ingest_ipc_batch` doc comment (`arrow_stream.rs:1322-1332`) is clear and specifies the IPC contract (single RecordBatch, trailing metadata OK). Cross-references to language-specific serialization functions are helpful.
- **Missing**: No mention that schema validation is not performed (unlike `ingest_batch`). Callers should be informed that schema mismatches will be caught by the server, not the client.
- `ArrowPayload` doc comment is clear and internal-only.
- `ipc_bytes_to_flight_data` doc comment is accurate.

---

## 8. Test Coverage

The PR introduces a new `rust/tests/src/arrow_tests.rs` file (1,532 lines) that consolidates all Arrow tests previously in `rust_tests.rs` and adds new IPC-specific tests.

### New IPC tests

| Test | What it covers |
|------|---------------|
| `test_ingest_ipc_batch_basic` | Happy path: single IPC batch, ack, offset |
| `test_ingest_ipc_batch_multiple` | Multiple IPC batches, sequential acks |
| `test_ingest_ipc_batch_invalid_bytes` | Garbage bytes rejected |
| `test_ingest_ipc_batch_after_close_rejected` | Closed stream rejects ingestion |
| `test_ingest_ipc_batch_recovery` | Supervisor recovery replays IPC batch |
| `test_ingest_ipc_batch_unacked_returns_record_batch` | `get_unacked_batches` materializes IPC → RecordBatch |

### Missing test coverage

- **Mixed ingestion**: No test that interleaves `ingest_batch` and `ingest_ipc_batch` on the same stream. This is a likely real-world pattern.
- **Schema mismatch via IPC**: No test that sends IPC bytes with a different schema than the stream's declared schema. This would exercise the gap noted in §2.2.
- **Partial IPC batch recovery**: The `test_partial_batch_recovery` test uses `ingest_batch`, not `ingest_ipc_batch`. There's no test for partial ack of an IPC batch followed by recovery (which triggers the `materialize_ipc` → slice path at `arrow_stream.rs:125-137`).
- **IPC compression**: No test for IPC payloads with LZ4/ZSTD compression enabled.

---

## 9. Minor Issues

1. **`ingest_ipc_batch` validation before mutex** (`arrow_stream.rs:1341-1344`): The IPC validation and `FlightData` construction happen *before* acquiring `ingest_mutex`. This is different from `ingest_batch` where schema validation happens before the mutex but encoding happens after. The inconsistency is harmless (validation is read-only) but worth noting: if two concurrent calls to `ingest_ipc_batch` both pass validation, they will serialize on the mutex for offset generation. The `Bytes::clone` at line 1341 is O(1) so this is fine.

2. **Double parse in `ipc_bytes_to_flight_data`**: As noted in §4.2, the function calls `validate_ipc_stream_exactly_one_record_batch` (which fully parses the IPC stream via `StreamReader`) and then manually re-parses the binary layout. This is functionally correct but does redundant work. For small batches this is negligible; for large batches the `StreamReader` validation dominates.

3. **`python/rust/Cargo.lock`**: A new 3,174-line Cargo.lock file is generated. This appears unrelated to the PR's purpose. If it was generated by a local build, it should either be committed intentionally (with a note) or excluded.

4. **`DictionaryTracker::new(false)`** at `arrow_stream.rs:308`: This disables cross-batch dictionary deduplication. This matches the old `FlightDataEncoderBuilder` default, but it means dictionary-encoded columns are re-sent in full for every batch. Worth a comment explaining the choice.

---

## Summary Verdict

**This is a well-scoped, well-implemented PR that correctly delivers item #3 of issue #147.** The core `ingest_ipc_batch` API is sound, the `ArrowPayload` abstraction is clean, and the test coverage for the new path is solid.

### Must-fix before merge

None — no correctness issues identified.

### Should-fix (strongly recommended)

1. **Document the schema validation gap** in `ingest_ipc_batch`'s doc comment.
2. **Add a changelog entry under "Internal Changes"** for the pipeline refactor.
3. **Consider re-exporting `bytes::Bytes`** from `lib.rs`, or accept `impl Into<Bytes>`.

### Nice-to-have

4. Add tests for mixed `ingest_batch`/`ingest_ipc_batch` usage and partial IPC batch recovery.
5. Combine the double IPC parse in `ipc_bytes_to_flight_data` into a single pass.
6. Verify `python/rust/Cargo.lock` is intentionally included.

### Architecture assessment: **Clean**

The pipeline refactor (channel carrying `FlightData` instead of `RecordBatch`) is a justified prerequisite for the IPC path and results in better separation of concerns. The memory trade-off (double storage for `ingest_batch` in-flight batches) is bounded and acceptable. Forward compatibility with the remaining issue #147 items is excellent.
