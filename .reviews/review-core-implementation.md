# Code Review: `rust/sdk/src/arrow_stream.rs` — Zero-Copy IPC Batch Ingestion

**PR:** #159 "zero-copy IPC batch ingestion"
**Issue:** #147 "[Rust] Zero-copy ingestion pipeline"
**Reviewer scope:** Core implementation in `arrow_stream.rs` only (CI/workflow changes excluded)

---

## 1. IPC Binary Parsing — `ipc_bytes_to_flight_data()` and `read_meta_range()`

**Severity: Major**

The hand-rolled IPC parser at lines 211–279 is the heart of the zero-copy path and has
several edge-case concerns:

### 1a. Metadata padding between flatbuffer and body is not accounted for

The Arrow IPC encapsulated message format writes the flatbuffer metadata, then **pads it
to the alignment boundary** (8-byte default for stream, up to 64-byte for custom
`IpcWriteOptions`), and only then writes the body.  The `meta_len` field returned by
`read_meta_range` (line 225) is the *padded* length that `write_continuation` wrote —
i.e. it already includes the padding after the flatbuffer. So `meta_end` (line 232) is
actually the end of the padded metadata region, and the body starts there.

However, this only works because the Arrow IPC stream writer writes the **padded**
metadata length into the 4-byte length prefix (see `write_message` in
`arrow-ipc/src/writer.rs` line 1495: `(aligned_size - prefix_size) as i32`). So
`meta_len` *is* the padded length, and `meta_end` correctly points past the padding.
This is subtle but correct.

**The real issue:** `data_header` is sliced as `ipc_bytes.slice(ms..me)` (line 274).
This includes the **padding bytes** after the actual flatbuffer data. The `FlightData`
receiver on the server side must call `root_as_message(&data_header)` on these bytes.
Flatbuffers tolerates trailing bytes, so this works in practice — but it transmits
unnecessary padding bytes over the wire. When alignment is 64 (the default for
`IpcWriteOptions`), this could be up to 63 extra bytes per message. Minor waste, but
worth documenting.

### 1b. `bodyLength` can be 0 for schema messages — `.max(0) as usize` is correct

Line 244: `schema_msg.bodyLength().max(0) as usize` — this correctly handles schema
messages where `bodyLength` is 0. Good.

### 1c. Negative `meta_len` check (line 226–230) rejects `meta_len == 0`

An IPC end-of-stream marker is written as `[0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00,
0x00]` — a continuation marker followed by length 0. The `meta_len <= 0` check at
line 226 correctly rejects this as an error, which is correct since
`validate_ipc_stream_exactly_one_record_batch` has already verified there is exactly one
RecordBatch, so hitting an EOS marker during the manual parse would indicate corruption.
This is fine.

### 1d. `i32` to `usize` cast for `meta_len` (line 232)

`meta_len as usize` — since we've verified `meta_len > 0` on line 226, this cast is
safe on all platforms (32-bit and 64-bit). Correct.

### 1e. No verification that the second message is actually a RecordBatch *before* slicing

Line 261–264 calls `header_as_record_batch()` which returns `Option`, and the code
correctly handles the `None` case by returning an error. Good.

**Overall verdict on parsing:** The parsing logic is correct for well-formed Arrow IPC
streams produced by the canonical `StreamWriter`. It relies on the validation pass
(`validate_ipc_stream_exactly_one_record_batch`) to reject malformed inputs before
the manual parse. This is a valid defense-in-depth approach.

---

## 2. Double Validation Concern

**Severity: Major (performance)**

`ipc_bytes_to_flight_data` (line 213) calls `validate_ipc_stream_exactly_one_record_batch`
which creates a `StreamReader`, fully deserializes the schema and RecordBatch (allocating
Arrow arrays), advances past any trailing data, and drops everything. Then lines 241–278
manually parse the same bytes again to extract metadata offsets and row count.

This means every IPC batch is **fully decoded once and then parsed again** — negating a
significant portion of the "zero-copy" benefit. The first pass allocates Arrow arrays,
validates field types, and builds column data — which is the most expensive part of IPC
decoding.

**Recommendation:** Replace `validate_ipc_stream_exactly_one_record_batch` with a
lightweight validation that does NOT materialise arrays. Two options:

1. **Parse-only validation:** Walk the IPC framing (continuation marker, length, flatbuf
   parse) to verify message types without calling `StreamReader`. The manual parser
   already does half of this — extend it to check for trailing batches.

2. **Validate during the manual parse:** The manual parse at lines 241–278 already
   reads both the schema and RecordBatch flatbuf headers. Add a check that the byte
   range after `be` contains only the EOS marker (8 bytes: `0xFFFFFFFF` + 4 zero bytes)
   or nothing, rather than another RecordBatch.

Either option would reduce the cost from O(data_size) to O(1) for valid inputs.

---

## 3. `ArrowPayload` Enum Design — Clone Semantics

**Severity: Minor (documentation)**

```rust
#[derive(Clone)]
enum ArrowPayload {
    Ipc(Bytes),
    Batch(RecordBatch),
}
```

- **`Bytes::clone()` is O(1):** `bytes::Bytes` is reference-counted internally (an `Arc`
  over the backing allocation). Clone increments the refcount. Correct.

- **`RecordBatch::clone()` is O(n_columns), not O(data_size):** `RecordBatch` is
  `#[derive(Clone)]` with `schema: SchemaRef` (`Arc<Schema>`) and
  `columns: Vec<Arc<dyn Array>>`. Cloning copies the `Vec` (allocating a new backing
  buffer for pointers) and increments one `Arc` per column. The actual array buffer data
  is not copied. For a typical batch with 2–20 columns this is effectively O(1).

- **Correctness concern with `Ipc` variant during recovery slicing (line 125–137):**
  When a partially-acked IPC batch is sliced, it is materialised first (`materialize_ipc`),
  then `RecordBatch::slice()` is called. The result is stored as
  `ArrowPayload::Batch(sliced)`, which is correct — it upgrades the payload type. However,
  if recovery is attempted **again** on the same pending entry, the second time it will
  hit the `ArrowPayload::Batch` arm (line 122) and `slice` it correctly. No issue.

**Verdict:** The `Clone` derive is correct and cheap for both variants.

---

## 4. `send_flight_data_internal` Factoring

**Severity: Minor**

Lines 1182–1244 extract the shared send path used by both `ingest_batch` (line 1312) and
`ingest_ipc_batch` (line 1353).

### Error handling comparison with the original `ingest_batch`:

| Concern | `ingest_batch` (before refactor) | `send_flight_data_internal` |
|---|---|---|
| Pending batch tracking | Push before send | Same (line 1191–1198) |
| Sender acquisition | Lock + clone | Same (line 1200–1203) |
| No sender available | Return `StreamClosedError` | Same, plus checks `server_error_rx` first (line 1208) — **improvement** |
| Send failure w/ recovery | Leave batch in pending | Same (line 1219–1223) |
| Send failure w/o recovery | Remove from pending, check server error | Same (lines 1225–1240) |

The factoring preserves all original semantics. The only difference is that
`send_flight_data_internal` now also checks `server_error_rx` when the sender is `None`
(line 1208), which is a small improvement — it returns the real server error instead of a
generic "Stream sender is closed" message.

**Verdict:** Clean factoring, no regressions.

---

## 5. Recovery Path — Slicing Logic Safety

**Severity: Minor**

`slice_batch_for_recovery` (lines 95–140) materialises IPC batches and slices them.

### Schema mismatch risk

When an `ArrowPayload::Ipc` batch is materialised at line 127, `materialize_ipc` uses
the schema embedded in the IPC bytes themselves (the `StreamReader` reads the schema from
the IPC stream header). This schema is **not** compared against
`self.table_properties.schema`.

In normal operation this is safe because:
1. The IPC bytes were originally accepted by `ingest_ipc_batch`, which (as noted in
   finding #7 below) does not validate the schema either.
2. The server validates the schema on initial connection and would reject mismatches.

However, if the server silently accepted a schema-mismatched batch before the disconnect
(a server bug), the materialised RecordBatch would have the wrong schema, and replaying
it on a new connection (which sends the stream's `table_properties.schema` as the Flight
schema header) could cause a mismatch on the server side.

**Risk:** Low. This is a defense-in-depth concern, not a practical bug.

### Arithmetic safety

- `records_already_acked` is clamped with `.min(total_rows)` (line 104). Good.
- `remaining_rows` uses `.saturating_sub` (line 105). Good.
- The `usize` cast at line 123 (`records_already_acked as usize`) is safe because
  `records_already_acked <= total_rows` and `total_rows = pb.end_record - pb.start_record`
  which was originally derived from `RecordBatch::num_rows()` (a `usize`).

**Verdict:** Safe. The materialised schema concern is theoretical.

---

## 6. `make_ipc_write_options` Called Per-Batch

**Severity: Minor (performance nit)**

In `ingest_batch` at line 1307–1308:

```rust
let flight_data = record_batch_to_flight_data(
    &batch,
    &make_ipc_write_options(self.options.ipc_compression)?,
)?;
```

`make_ipc_write_options` is called for every batch. The function constructs an
`IpcWriteOptions` and optionally enables compression. While the construction itself is
cheap (just struct init + a `try_with_compression` call), it's unnecessary to repeat it
when `self.options.ipc_compression` never changes during the stream's lifetime.

The same pattern appears in `reconnect` (line 892) and `start_stream_connection`
(line 620), but those are one-time calls, so it's fine there.

**Recommendation:** Cache `IpcWriteOptions` on the `ZerobusArrowStream` struct. Construct
it once in `new()` and reuse it in `ingest_batch`, `reconnect`, etc. This saves one
allocation + one fallible call per batch.

```rust
pub struct ZerobusArrowStream {
    // ...
    ipc_write_options: IpcWriteOptions,
}
```

---

## 7. Schema Validation Gap in `ingest_ipc_batch`

**Severity: Major (correctness)**

`ingest_batch` validates the schema at lines 1289–1295:

```rust
if batch.schema() != self.table_properties.schema {
    return Err(ZerobusError::InvalidArgument(...));
}
```

`ingest_ipc_batch` (lines 1334–1361) does **no** schema validation. The IPC bytes
contain an embedded schema, but it is never compared against
`self.table_properties.schema`.

This means:

1. **FFI callers can send schema-mismatched data** that will be forwarded to the server
   as raw `FlightData`. The server may reject it, but the error will surface as a
   stream-level error (potentially triggering recovery loops) rather than an immediate
   `InvalidArgument` error at the call site.

2. **The `data_header` sent in `FlightData` is the IPC RecordBatch message from the
   caller's bytes** — not one produced from `self.table_properties.schema`. If the
   caller's schema differs (e.g., different field names, nullability, or metadata), the
   server receives inconsistent schema information: the schema message from
   `start_stream_connection` says one thing, but the RecordBatch metadata says another.

**Recommendation:** Either:
- (a) Validate the IPC-embedded schema during `ipc_bytes_to_flight_data` by parsing the
  schema flatbuffer and comparing it, or
- (b) Accept this as intentional (documented as "server validates schema") and add a doc
  comment explaining why client-side validation is skipped for the IPC path.

If (a), the schema can be extracted from the first flatbuf message that
`ipc_bytes_to_flight_data` already parses (line 242–243) without materialising arrays.

---

## 8. Thread Safety / Ordering — Mutex Acquisition After Parsing

**Severity: Minor (not a TOCTOU bug)**

`ingest_ipc_batch` acquires `ingest_mutex` at line 1344, **after**
`ipc_bytes_to_flight_data` at line 1341.

```rust
let (record_count, flight_data) = ipc_bytes_to_flight_data(ipc_bytes.clone())?;
let _guard = self.ingest_mutex.lock().await;
// offset_id, cumulative_records_sent, send_flight_data_internal...
```

Compare with `ingest_batch` which acquires the mutex at line 1297, **before** encoding:

```rust
let _guard = self.ingest_mutex.lock().await;
// record_count, offset_id, cumulative_records_sent, encode, send...
```

### Is this a TOCTOU issue?

No. The purpose of `ingest_mutex` is to serialise:
1. The `offset_generator.next()` call (ensuring monotonic offsets)
2. The `cumulative_records_sent.fetch_add()` (ensuring correct record ranges)
3. The `send_flight_data_internal` call (ensuring ordering on the channel)

`ipc_bytes_to_flight_data` is a pure function of its input bytes — it reads no shared
state and writes no shared state. Moving it outside the mutex is safe and actually
**beneficial** because it allows parsing to happen concurrently with other ingestion
operations, reducing lock contention.

The same argument applies to `ingest_batch`: `record_batch_to_flight_data` (line 1306)
could also be moved before the mutex acquisition for better concurrency. This would be a
good follow-up optimization.

**Verdict:** The ordering is correct and intentionally optimized. No TOCTOU.

---

## 9. `FlightData` Construction — Field Correctness

**Severity: Minor (correct but subtle)**

Lines 271–278:

```rust
FlightData {
    data_header: ipc_bytes.slice(ms..me),
    data_body: ipc_bytes.slice(me..be),
    ..Default::default()
}
```

### Are `data_header` and `data_body` the correct fields?

Yes. Confirmed by examining the `arrow-flight` crate's `From<EncodedData> for FlightData`
implementation:

```rust
impl From<EncodedData> for FlightData {
    fn from(data: EncodedData) -> Self {
        FlightData {
            data_header: data.ipc_message.into(),  // flatbuf metadata
            data_body: data.arrow_data.into(),      // buffer data
            ..Default::default()
        }
    }
}
```

The mapping is:
- `data_header` = IPC message flatbuffer (including padding) ← `ipc_bytes.slice(ms..me)`
- `data_body` = Arrow buffer data ← `ipc_bytes.slice(me..be)`

This matches exactly.

### Does omitting `flight_descriptor` cause issues?

No. The `FlightData` protobuf spec says `flight_descriptor` is "only relevant when a
client is starting a new DoPut stream." The schema message (sent by
`schema_to_flight_data` in `start_stream_connection`) also omits it (via
`..Default::default()`), and `SchemaAsIpc::into() -> FlightData` in the arrow-flight
crate does the same. For data messages, `flight_descriptor` is not expected.

### Zero-copy via `Bytes::slice`

`ipc_bytes.slice(ms..me)` creates a new `Bytes` that shares the same backing allocation
via an `Arc` reference. No data is copied. This is the core zero-copy benefit of this PR.
Correct and efficient.

**Verdict:** FlightData construction is correct and truly zero-copy.

---

## Additional Observations

### A. `app_metadata` is set downstream, not in `ipc_bytes_to_flight_data`

The `FlightData` returned by `ipc_bytes_to_flight_data` has empty `app_metadata`. The
offset metadata is added later in the `flight_data_stream` mapping (lines 627–637). This
is consistent with how `record_batch_to_flight_data` works — the metadata addition is
centralised in the stream adapter. Good design.

### B. `DictionaryTracker` in `record_batch_to_flight_data` (line 308)

`DictionaryTracker::new(false)` is created per-call. For dictionary-encoded columns, the
first batch sends the dictionary, and subsequent batches should ideally not re-send it.
But since a new tracker is created each time, every batch will include its dictionary
data. This is wasteful for dictionary-heavy schemas but not incorrect. This is a
pre-existing concern (not introduced by this PR) and out of scope.

### C. Recovery replays IPC batches through `ipc_bytes_to_flight_data` again (line 1021)

During recovery, `ArrowPayload::Ipc` batches are re-parsed by `ipc_bytes_to_flight_data`,
which calls `validate_ipc_stream_exactly_one_record_batch` again. Since these bytes
were already validated on first ingestion, the re-validation is redundant (and triggers
the double-decode issue from finding #2 a second time). Consider a flag or separate
function for trusted replays.

---

## Summary Verdict

| # | Finding | Severity | Actionable |
|---|---------|----------|------------|
| 1 | IPC parsing correctness | — | Correct for canonical streams; padding bytes transmitted over wire (nit) |
| 2 | Double validation: full StreamReader pass + manual parse | **Major** | Replace validation with lightweight framing check |
| 3 | `ArrowPayload` Clone semantics | — | Correct. Both variants are O(1) or near-O(1) to clone |
| 4 | `send_flight_data_internal` factoring | — | Clean. Preserves all error handling semantics |
| 5 | Recovery slicing safety | Minor | Safe. Schema mismatch is theoretical only |
| 6 | `make_ipc_write_options` per-batch | Minor | Cache on struct for marginal perf improvement |
| 7 | Schema validation gap in `ingest_ipc_batch` | **Major** | Add schema validation or document intentional omission |
| 8 | Mutex ordering (TOCTOU) | — | Correct. No TOCTOU. Current ordering is optimal |
| 9 | `FlightData` field correctness | — | Correct. Zero-copy via `Bytes::slice` confirmed |

**Overall: Approve with requested changes.**

The zero-copy architecture is sound and the `Bytes::slice` approach for
`FlightData` construction is clean and correct. The two major findings
that should be addressed before merge are:

1. **Double validation (#2):** The full `StreamReader` deserialization pass in
   `validate_ipc_stream_exactly_one_record_batch` negates much of the zero-copy
   benefit. Replace with a lightweight framing-only check.

2. **Schema validation gap (#7):** `ingest_ipc_batch` should either validate the
   IPC-embedded schema against `self.table_properties.schema` or explicitly document
   why this is deferred to the server. This is a correctness gap that could cause
   confusing error behavior for FFI callers.

The remaining findings are minor or nits and can be addressed in follow-up PRs.
