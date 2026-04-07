# PR #159 — Test Coverage & Correctness Review

**PR**: "zero-copy IPC batch ingestion"
**Issue**: #147 "[Rust] Zero-copy ingestion pipeline"
**Reviewer scope**: `rust/tests/src/arrow_tests.rs`, `rust/tests/src/utils.rs`, `rust/tests/src/mock_arrow_flight.rs`

---

## 1. `ingest_ipc_batch` Test Completeness (`ipc_ingestion_tests` module)

### 1.1 Basic single IPC batch ingestion — COVERED

**Test**: `test_ingest_ipc_batch_basic` (line 1181)

Creates a RecordBatch with 3 rows, serialises it via `record_batch_to_ipc_bytes`, ingests
with `ingest_ipc_batch`, asserts offset is 0, waits for ack, and verifies mock server
received exactly 1 batch. Solid happy-path coverage.

**Rating**: Pass

### 1.2 Multiple IPC batch ingestion — COVERED

**Test**: `test_ingest_ipc_batch_multiple` (line 1235)

Ingests 3 IPC batches in a loop, asserts monotonically increasing offsets (0, 1, 2),
waits for all acks, and verifies `batch_count == 3` and `max_offset_received == 2`.
Good sequential multi-batch verification.

**Rating**: Pass

### 1.3 Invalid IPC bytes rejection — COVERED

**Test**: `test_ingest_ipc_batch_invalid_bytes` (line 1308)

Passes `b"not valid arrow ipc"` as IPC bytes and asserts the result is `Err`. This
exercises the `validate_ipc_stream_exactly_one_record_batch` path which checks that
the StreamReader can parse the input.

**Rating**: Pass — but see finding 5.1 for a subtlety about error message assertion.

### 1.4 Ingestion after stream close — COVERED

**Test**: `test_ingest_ipc_batch_after_close_rejected` (line 1347)

Closes the stream, then attempts `ingest_ipc_batch` and asserts `Err`. Mirrors the
existing `test_ingest_after_close_rejected` (line 412) for `ingest_batch`.

**Rating**: Pass

### 1.5 Recovery of IPC batches after disconnect — COVERED

**Test**: `test_ingest_ipc_batch_recovery` (line 1385)

Injects ack → unavailable error → ack sequence. First IPC batch is acked, second
triggers a retriable error, the supervisor reconnects and the replayed batch gets
acked. Asserts `wait_for_offset(offset2).is_ok()`.

**Rating**: Pass

### 1.6 `get_unacked_batches()` round-trip for IPC payloads — COVERED

**Test**: `test_ingest_ipc_batch_unacked_returns_record_batch` (line 1467)

Ingests an IPC batch that the server permanently rejects (non-retriable
`INVALID_ARGUMENT`). Closes the stream, calls `get_unacked_batches()`, verifies the
returned Vec has length 1, then checks `num_rows`, `schema`, and column-level data
equality via an IPC round-trip through `record_batch_to_ipc_bytes` →
`ipc_bytes_to_record_batch`.

This verifies the `ArrowPayload::Ipc → materialize()` path works end-to-end.

**Rating**: Pass

### 1.7 Mixed IPC + RecordBatch ingestion on same stream — NOT COVERED

There is no test that calls both `ingest_batch(RecordBatch)` and
`ingest_ipc_batch(Bytes)` on the same stream. The SDK supports both via the
`ArrowPayload` enum (`Batch` vs `Ipc`), and both paths share offset generation and
pending-batch tracking. A test should verify that:

- Offsets remain monotonically increasing across mixed calls.
- `get_unacked_batches()` returns the correct materialised `RecordBatch` for both payload types.
- The mock server's `batch_count` reflects the total.

**Rating**: Gap — **medium severity**. This is a realistic usage pattern especially
for FFI callers that might transition between batch modes.

### 1.8 IPC with multiple batches (should be rejected) — NOT COVERED

The SDK's `validate_ipc_stream_exactly_one_record_batch` (arrow_stream.rs:178) explicitly
rejects IPC streams containing more than one RecordBatch with the error message *"IPC
stream must contain exactly one RecordBatch (found extra batch)"*. There is no integration
test that constructs valid IPC bytes containing two RecordBatches and asserts the specific
rejection.

The `test_ingest_ipc_batch_invalid_bytes` test only covers garbage bytes, not
structurally valid IPC with the wrong batch count.

**Rating**: Gap — **medium severity**. This is a documented invariant of the API and
should have explicit test coverage.

### 1.9 IPC with zero batches (should be rejected) — NOT COVERED

The validation function also rejects IPC streams with zero RecordBatches (*"IPC stream
contains no RecordBatch"*). No test constructs an IPC stream containing only a schema
message and the EOS marker (zero batches) to verify this rejection.

**Rating**: Gap — **medium severity**. Same reasoning as 1.8.

### 1.10 Large batch IPC — NOT COVERED

While `rust_tests.rs` has `test_large_batch_ingestion` (line 1648) for the protobuf
path, there is no equivalent for the IPC path. A large IPC batch (e.g. 10,000+ rows)
would stress the `ipc_bytes_to_flight_data` parser's byte-slicing logic and the mock
server's handling of large `data_body` payloads.

**Rating**: Gap — **low severity**. The byte-slicing code is simple arithmetic, but a
smoke test for large payloads is good practice to catch off-by-one issues.

### 1.11 IPC bytes with trailing EOS marker — NOT DIRECTLY TESTED (but handled)

The SDK code at arrow_stream.rs:267–269 has a comment:

> `bytes.len()` may exceed `be`: Arrow IPC stream writers typically append an
> end-of-stream marker after the RecordBatch.

The `record_batch_to_ipc_bytes` utility in utils.rs calls `writer.finish()` which does
append an EOS marker, so **all IPC tests implicitly exercise this path**. However,
there is no test that constructs IPC bytes *without* the EOS marker (i.e., truncated
after the RecordBatch body) to verify the parser tolerates that variant.

**Rating**: Implicit coverage — **low severity**. The happy path is covered; the edge
case of missing EOS is a defensive concern.

---

## 2. Test File Reorganisation

### 2.1 No tests lost in the move — VERIFIED

`rust_tests.rs` contains zero references to `arrow`, `Arrow`, `ArrowStream`,
`ArrowTable`, `ingest_batch`, `ingest_ipc`, `record_batch`, or `RecordBatch` (confirmed
by grep). It imports only `ZerobusError`, `ZerobusSdk`, `StreamConfigurationOptions`,
`TableProperties`, `RecordType`, `StreamType`, and `NoTlsConfig` — all protobuf/gRPC
pipeline types.

`arrow_tests.rs` covers: stream creation, single/multi-batch ingestion, flush/close,
error handling (server error, schema mismatch, ingest-after-close), ack timeout, flush
timeout, idempotent close, empty flush, concurrent ingestion, unacked batch tracking
(all-acked and after-failure), recovery (retriable error, recreate stream,
record-based ack, partial batch recovery), and the full IPC ingestion suite.

**Rating**: Pass — clean separation.

### 2.2 Module structure — CLEAN

`arrow_tests.rs` declares `mod mock_arrow_flight; mod utils;` and contains a single
top-level `mod arrow_flight_tests` with well-organised sub-modules:

| Sub-module | # tests | Purpose |
|---|---|---|
| `stream_creation_tests` | 1 | Stream creation happy path |
| `ingestion_tests` | 2 | Single and multiple RecordBatch ingestion |
| `flush_and_close_tests` | 2 | Flush waits for acks; close flushes and closes |
| `error_handling_tests` | 3 | Server error, schema mismatch, ingest-after-close |
| `timeout_tests` | 2 | Ack timeout, flush timeout |
| `lifecycle_tests` | 2 | Idempotent close, empty flush |
| `concurrency_tests` | 1 | 5 concurrent tasks ingesting batches |
| `unacked_tests` | 2 | Empty unacked after full ack; unacked after failure |
| `recovery_tests` | 4 | Retriable error recovery, recreate stream, record-based ack, partial batch |
| `ipc_ingestion_tests` | 6 | Full IPC-specific test suite |

Total: **25 tests**. Naming is consistent. Module boundaries mirror functional concerns.

**Rating**: Pass

### 2.3 `rust_tests.rs` no longer has arrow-related tests — VERIFIED

`rust_tests.rs` is a separate `[[test]]` binary (Cargo.toml line 7–9) that uses
`mod mock_grpc; mod utils;` (the protobuf mock) and contains ~76 test functions, all
focused on the protobuf/gRPC pipeline. No arrow-flight imports or references remain.

**Rating**: Pass

---

## 3. Mock Server Adequacy

### 3.1 `MockFlightServer` implements the FlightData-based `do_put` pipeline

`mock_arrow_flight.rs` implements `FlightService::do_put` (line 182) which:

1. Extracts table name from `x-databricks-zerobus-table-name` header.
2. Handles the first `FlightData` message as a schema message (empty `app_metadata`), responding with a `PutResult` containing a `FlightAckMetadata { ack_up_to_offset: -1, ack_up_to_records: 0 }` "ready" signal.
3. Subsequent messages parse `FlightBatchMetadata { offset_id }` from `app_metadata`.
4. Validates strictly sequential offsets and responds with `PutResult` containing `FlightAckMetadata`.
5. Supports `BatchAck`, `Error`, and `CloseStream` mock response types.
6. Tracks `max_offset_received`, `batch_count`, `row_count`, and persists
   `response_indices` across reconnections for recovery testing.

**Rating**: Pass — the mock accurately models the `DoPut`-based Flight protocol
including the schema-first handshake, JSON-serialised metadata, and cross-connection
response indexing.

### 3.2 Mock does NOT decode Arrow data — NOTED

The mock server checks `!flight_data.data_body.is_empty()` (line 302) but does not
deserialise the IPC payload to verify row counts or schema. This means the mock cannot
detect if the client sends corrupted or schema-mismatched Arrow data in the `FlightData`.

**Rating**: Acceptable — the mock's purpose is protocol-level testing (offset tracking,
ack semantics, error injection), not data fidelity. The SDK's own
`validate_ipc_stream_exactly_one_record_batch` handles client-side validation. However,
a single test that decodes the `data_body` on the server side would increase confidence
that the zero-copy slicing in `ipc_bytes_to_flight_data` produces valid Flight payloads.

### 3.3 Separation from protobuf mock — CORRECT

`mock_arrow_flight.rs` implements `arrow_flight::FlightService` (Arrow Flight protocol),
while `mock_grpc.rs` implements `databricks::zerobus::Zerobus` (custom gRPC protocol).
They serve different `[[test]]` binaries and do not conflict.

**Rating**: Pass

---

## 4. Test Utility Functions

### 4.1 `record_batch_to_ipc_bytes` (utils.rs:73–80)

```rust
pub fn record_batch_to_ipc_bytes(batch: &RecordBatch) -> bytes::Bytes {
    let mut buf = Vec::new();
    let mut writer =
        arrow_ipc::writer::StreamWriter::try_new(&mut buf, batch.schema_ref()).unwrap();
    writer.write(batch).unwrap();
    writer.finish().unwrap();
    bytes::Bytes::from(buf)
}
```

**Analysis**: Correct. Uses `StreamWriter` (not `FileWriter`), which produces the IPC
stream format that `ingest_ipc_batch` expects. Calls `finish()` to append the EOS
marker. The `unwrap()` calls are acceptable in test code — a malformed test batch would
panic with a clear message at the call site.

**Rating**: Pass

### 4.2 `ipc_bytes_to_record_batch` (utils.rs:83–87)

```rust
pub fn ipc_bytes_to_record_batch(bytes: &bytes::Bytes) -> RecordBatch {
    let mut reader = arrow_ipc::reader::StreamReader::try_new(Cursor::new(bytes.as_ref()), None)
        .expect("valid IPC stream");
    reader.next().expect("one batch").expect("no error")
}
```

**Analysis**: Correct inverse of `record_batch_to_ipc_bytes`. Uses `Cursor` to wrap
the byte slice, creates a `StreamReader`, reads exactly one batch. Only used in
`test_ingest_ipc_batch_unacked_returns_record_batch` for verifying the round-trip.

**Minor concern**: If passed bytes with multiple batches, this would silently return
only the first. Acceptable for current usage since it's only called on output from
`record_batch_to_ipc_bytes` (which always produces exactly one batch).

**Rating**: Pass

### 4.3 `create_test_record_batch` (utils.rs:60–70)

Correctly constructs a `RecordBatch` from `Int64Array` and `StringArray` columns. The
`expect("Failed to create RecordBatch")` panic message is clear.

**Rating**: Pass

---

## 5. Missing Test Scenarios

### 5.1 Error message assertion granularity — LOW

`test_ingest_ipc_batch_invalid_bytes` asserts `result.is_err()` but does not inspect
the error variant or message. The SDK returns `ZerobusError::InvalidArgument` for this
case. A more precise assertion (e.g., matching on the error variant) would prevent
regressions where the wrong error type is returned.

**Severity**: Low

### 5.2 Mixed IPC + RecordBatch ingestion — MEDIUM

As discussed in section 1.7. No test exercises both `ingest_batch` and
`ingest_ipc_batch` on the same `ArrowFlightStream` instance. The `ArrowPayload` enum
has two variants stored in `pending_batches`, and `get_unacked_batches` calls
`materialize()` on each. A mixed test would verify that both `ArrowPayload::Batch` and
`ArrowPayload::Ipc` materialise correctly when interleaved.

**Severity**: Medium

### 5.3 IPC with exactly 0 or 2+ RecordBatches — MEDIUM

As discussed in sections 1.8 and 1.9. The validation function
`validate_ipc_stream_exactly_one_record_batch` has three branches:

- `None` → zero batches → error
- `Some(Ok(_))` followed by `Some(Ok(_))` → multiple batches → error
- `Some(Ok(_))` followed by `None` → exactly one → success

Only the success branch and the garbage-bytes branch are tested. The zero-batch and
multi-batch branches lack dedicated tests.

**Severity**: Medium

### 5.4 IPC schema mismatch vs stream schema — NOT TESTED

`test_schema_mismatch_rejected` (line 363) tests `ingest_batch(wrong_batch)` where the
RecordBatch has a different schema. There is no equivalent for `ingest_ipc_batch` with
IPC bytes whose embedded schema differs from the stream's schema. The SDK validates
this during `ipc_bytes_to_flight_data` or at the server — it's unclear from the code
whether the client rejects it or the server does.

**Severity**: Medium — schema mismatch is a common user error and should be tested for
the IPC path explicitly.

### 5.5 Concurrent IPC ingestion — NOT TESTED

`test_concurrent_batch_ingestion` (line 637) tests concurrent `ingest_batch` calls from
multiple Tokio tasks. There is no equivalent for concurrent `ingest_ipc_batch` calls.
The `ingest_mutex` is shared between both methods, so this would exercise lock
contention between IPC and RecordBatch ingestion.

**Severity**: Low — the concurrency mechanism is shared, but a dedicated test would
increase confidence.

### 5.6 `get_unacked_batches` on an open stream — NOT TESTED

The SDK rejects `get_unacked_batches()` on a non-closed stream with
`InvalidStateError`. No test verifies this error path (calling before close).

**Severity**: Low — it's a simple guard, but untested guards are easy to accidentally
remove.

### 5.7 Empty IPC bytes (zero-length `Bytes`) — NOT TESTED

Passing `Bytes::new()` (empty) to `ingest_ipc_batch` should fail at the
`StreamReader::try_new` step. Not tested, though `test_ingest_ipc_batch_invalid_bytes`
covers a related path with non-empty garbage.

**Severity**: Low

### 5.8 IPC batch with 0 rows — NOT TESTED

A valid IPC stream containing a single RecordBatch with 0 rows is structurally valid
but semantically questionable. The SDK's `ipc_bytes_to_flight_data` would compute
`num_rows = 0`. It's unclear if this is intentionally allowed. A test would document
the expected behaviour.

**Severity**: Low

---

## 6. Summary

| Area | Verdict | Notes |
|---|---|---|
| Basic IPC ingestion | **Pass** | Thorough happy path |
| Multiple IPC batches | **Pass** | Sequential with offset checks |
| Invalid IPC rejection | **Pass** | Garbage bytes only; see 5.3 |
| Ingestion after close | **Pass** | Both `ingest_batch` and `ingest_ipc_batch` |
| IPC recovery | **Pass** | Retriable error with reconnect |
| Unacked IPC round-trip | **Pass** | Full materialisation verification |
| Mixed IPC + RecordBatch | **Gap** | Not tested (5.2) |
| Multi-batch IPC rejection | **Gap** | Not tested (1.8/5.3) |
| Zero-batch IPC rejection | **Gap** | Not tested (1.9/5.3) |
| Large IPC batch | **Gap** | Not tested (1.10) |
| IPC schema mismatch | **Gap** | Not tested (5.4) |
| File reorganisation | **Pass** | Clean, no tests lost |
| Module structure | **Pass** | Well-organised 10-module hierarchy |
| Mock Flight server | **Pass** | Protocol-correct, cross-connection state |
| Test utilities | **Pass** | Correct IPC serialisation round-trip |

**Overall assessment**: The IPC ingestion test suite covers the core happy paths and
the most critical failure/recovery scenarios well. The 6 tests in `ipc_ingestion_tests`
are correctly structured and test the right things. The main gaps are **validation
edge-cases** (zero/multi-batch IPC, schema mismatch) and **mixed-mode usage** (IPC +
RecordBatch on the same stream). None of the gaps represent likely production failures
today, but they leave the validation code under-tested and could hide regressions in
future refactors.

**Recommended additions** (priority order):

1. Mixed IPC + RecordBatch test (medium effort, high value)
2. Multi-batch IPC rejection test (low effort, medium value)
3. Zero-batch IPC rejection test (low effort, medium value)
4. IPC schema mismatch test (medium effort, medium value)
5. `get_unacked_batches` on open stream test (low effort, low value)
