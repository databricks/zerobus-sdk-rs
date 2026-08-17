# Arrow Flight SDK Architecture

This document describes the internal architecture and correctness invariants of the
Rust Arrow Flight ingestion stream. For public API usage, see the
[Rust SDK README](https://github.com/databricks/zerobus-sdk/blob/main/rust/README.md)
and the
[Arrow example](https://github.com/databricks/zerobus-sdk/blob/main/rust/examples/arrow/README.md).

Arrow Flight support is currently behind the `arrow-flight` Cargo feature.

## Data flow

```text
RecordBatch
    |
    v
ZerobusArrowStream::ingest_batch
    |
    +-- logical offset + pending record range
    +-- backpressure permit
    |
    v
FlightDataEncoder
    |
    +-- schema message
    +-- one or more physical RecordBatch messages
    |
    v
Arrow Flight DoPut request
    |
    v
PutResult acknowledgments
    |
    +-- cumulative durable record count
    +-- optional server-rotation signal
    |
    v
pending-range removal / recovery suffix
```

The caller task queues each batch onto the active request channel, and tonic
drives the encoded request body. The supervisor owns acknowledgment processing,
rotation, recovery, and terminal finalization.

## Module responsibilities

- [`mod.rs`](mod.rs) owns the public stream API, admission checks, logical offsets,
  shared state, and stream construction.
- [`connection.rs`](connection.rs) creates Flight clients and DoPut exchanges,
  encodes request bodies, observes request EOF, and tracks request-body ownership.
- [`acks.rs`](acks.rs) validates and applies acknowledgments, enforces pending
  deadlines, and runs active/rotation/close transport states.
- [`supervisor.rs`](supervisor.rs) owns the background lifecycle, retry policy,
  reconnect, replay, sender publication, and terminal error selection.
- [`close.rs`](close.rs) publishes the close request/result and performs the
  idempotent terminal pending-to-failed transition.
- [`batch.rs`](batch.rs) owns pending batch ranges, unacknowledged suffixes, replay
  rebuilding, and IPC materialization.
- [`metadata.rs`](metadata.rs) defines request and acknowledgment metadata.
- [`options.rs`](options.rs) defines Arrow-specific configuration and contracts.
- `c_data.rs`, when enabled internally, imports canonical Arrow C Data into a
  Rust-owned `RecordBatch`.

## Logical offsets and wire offsets

The SDK has two intentionally different offset domains.

### Logical SDK offsets

`ingest_batch()` assigns one logical `OffsetId` to each input `RecordBatch`. This
is the offset returned to callers and used by `wait_for_offset()` and `flush()`.
The corresponding `PendingBatch` also stores a cumulative record range:
`[start_record, end_record)`.

Logical offsets are monotonic for the stream lifetime. Record ranges,
`submitted_records`, and `last_acked_records` are connection-relative and are
renumbered from zero when pending work is rebuilt for replay.

### Physical Flight offsets

`FlightDataEncoder` may split a large input batch into multiple physical
`FlightData` record-batch messages. Each physical message receives a sequential,
connection-local wire offset in `FlightBatchMetadata`.

The first Flight message is the schema and carries no batch offset. Dictionary
arrays use Arrow Flight's default `DictionaryHandling::Hydrate`, so dictionary
values are expanded and do not introduce dictionary side messages.

The server's `ack_up_to_offset` describes the physical Flight sequence. The SDK
does not use that value to complete caller offsets. Completion is derived from
`ack_up_to_records` and local pending ranges, which preserves correct behavior
when one logical batch is split into several wire messages.

## Ingestion and backpressure

`max_inflight_batches` is represented by a semaphore. A `PendingBatch` owns one
permit until it is acknowledged, removed during replay rebuilding, or moved into
the terminal failed set.

The ingestion sequence is:

1. Acquire an inflight permit before `ingest_mutex`.
2. Under `ingest_mutex`, re-check terminal admission.
3. Assign the logical offset and cumulative record range.
4. Insert the `PendingBatch`.
5. If paused, return the logical offset and leave the batch buffered.
6. Otherwise reserve a request-channel slot, publish `submitted_records`, and send.

The semaphore and request channel use the same capacity. A task that owns an
inflight permit therefore has a request-channel slot available while the active
sender is attached.

If reservation fails with recovery enabled, the batch remains pending and the
send-failure notification starts recovery. With recovery disabled, ingestion
withdraws the pending batch, rolls back its logical offset and record range,
claims terminal admission, wakes the supervisor, and waits for the shared
terminal result.

`flush()` snapshots the latest logical offset under `ingest_mutex`, so it cannot
observe an offset while a failed enqueue is rolling that offset back.

## Admission, pause, and closure

Three flags represent different lifecycle facts:

- `is_paused` is a reversible transport gate. New batches are accepted and
  buffered, but not sent. Successful reconnect clears it.
- `admission_closed` is a one-way API gate. New batches are permanently rejected
  once terminal finalization or an unrecoverable enqueue failure owns admission.
- `is_closed` indicates that terminal failed-batch retrieval is available.

`CloseState` separately publishes the explicit-close protocol:

```text
Open -> Requested(target, deadline) -> Finalized(result)
  \----------------------------------> Finalized(result)
```

The direct `Open -> Finalized` edge is used by background terminal failures that
occur without an explicit close request. The first request and final result are
sticky. Cancelling a caller's `close()` future does not cancel supervisor-owned
teardown; calling `close()` again waits for the same result.

## Acknowledgment processing

The authoritative durable watermark is `ack_up_to_records`.

Applying an acknowledgment:

1. Rejects a watermark beyond `submitted_records`.
2. Advances `last_acked_records` monotonically.
3. Removes fully acknowledged pending ranges.
4. Publishes the highest completed logical offset.
5. Records when an explicit close target became durable.

Partial acknowledgments leave a `PendingBatch` in place. Recovery or terminal
retrieval slices that batch to its unacknowledged suffix rather than replaying
the already-durable prefix.

During normal active operation, the oldest submitted pending batch has an
absolute ACK deadline. Responses and partial progress do not extend it. No ACK
timer runs while there are no submitted pending batches. A completed replay
refreshes pending deadlines before the replacement sender becomes active.

## Server-initiated rotation

A rotation signal pauses sends and snapshots the records submitted on the
current connection. Batches accepted after that snapshot remain pending for the
next connection and do not extend the old connection's target.

The ACK-wait period is capped by both the server-advertised grace and
`stream_paused_max_wait_time_ms`. The client reserves transport-cleanup time,
half-closes the request, and drains responses for a bounded interval. Setting
the option to `Some(0)` skips ACK waiting but does not skip transport cleanup.

When recovery is disabled, the same half-close/drain still runs, but the stream
then terminates instead of reconnecting.

## Recovery and replay

Only the supervisor reconnects or publishes a replacement sender.

On a retryable active failure:

1. Pause ingestion sends and detach the failed sender under `ingest_mutex`.
2. Wait the configured backoff unless explicit close interrupts it.
3. Establish a replacement DoPut exchange and wait for READY.
4. Rebuild pending ranges from the durable record watermark.
5. Replay every unacknowledged suffix and any batches buffered during replay.
6. Under `ingest_mutex`, refresh ACK deadlines, publish the replacement sender,
   and clear `is_paused`.

The replacement sender is not visible until replay succeeds. A failed or
cancelled attempt drops its private sender. Explicit close during an uncommitted
recovery attempt cancels that attempt best-effort, retains pending suffixes, and
returns the trigger for the interrupted attempt.

Authentication rejection invalidates cached credentials under a bounded
deadline. Invalidation runs independently of explicit close so a rejected token
is not left cached merely because close won the recovery race.

## Transport and wrapper ownership

`FlightConnection` owns both halves of one DoPut exchange: the response stream,
the request-body control, and a sender for logical `RecordBatch` values. Normal
supervisor handoff drops the connection's redundant sender clone because the
stream's shared sender slot owns the active ingest path.

`RequestBodyControl` uses a cancellation token to stop the request at a
`FlightData` boundary and a watch channel to confirm that tonic polled the body
to EOF. Transport cleanup half-closes through that control before bounded
response draining; dropping only the response side would instead reset the
exchange and lose late acknowledgments.

Language wrappers never own Rust stream internals. IPC inputs are materialized
into Rust `RecordBatch` values before regular ingestion. The internal Arrow C
Data path imports an owner whose lifetime follows the resulting batch, including
pending, replay, and terminal-retention paths. Wrapper handles must remain valid
until their asynchronous operation completes and must use the matching
close/free operation.

## Explicit close and terminal finalization

The first `close()` call snapshots its target offset and deadline under
`ingest_mutex`, atomically with ingest admission and replacement-sender
publication.

On an active connection, ACK processing waits for the target or deadline, then
half-closes and drains the transport. A target whose durable watermark was
applied before the deadline succeeds even if timeout observation races with it.

Close during an existing server rotation or uncommitted recovery returns that
attempt's trigger, even when every record is already durable. Callers should
inspect `get_unacked_batches()` after an error; the returned set may be empty.

`CloseFinalizer`:

1. Closes admission and detaches the sender under `ingest_mutex`.
2. Publishes the selected terminal error for waiters.
3. Moves unacknowledged pending suffixes into `failed_batches`.
4. Publishes the sticky `CloseState::Finalized` result.

The supervisor worker has a detached reaper that owns its `JoinHandle`. If the
worker panics or is aborted before finalization, the reaper performs terminal
finalization and wakes close waiters.

## Concurrency invariants

`ingest_mutex` serializes:

- logical offset and pending-range assignment or rollback;
- pause and sender detachment;
- replay rebuilding and replacement-sender publication;
- explicit-close target publication;
- terminal admission and finalization.

Important invariants:

- A batch is either ordered before close publication or rejected/buffered after
  the relevant lifecycle transition; it is never half-admitted.
- A replacement sender is published only after complete replay.
- A recovery-disabled failed enqueue claims terminal admission before releasing
  `ingest_mutex`, so concurrent close cannot replace its error with success.
- On the normal ingest path, `submitted_records` advances in the same pending-lock
  critical section as the infallible request-channel handoff. Replay advances it
  under `ingest_mutex`; no ACK processor observes the replacement until publication.
- `failed_batches` and `pending_batches` are always locked in that order during
  terminal retrieval, so concurrent drains are idempotent.
- `get_unacked_batches()` re-runs the pending-to-failed drain before returning,
  ensuring a complete snapshot even if it races terminal finalization.

Do not reorder `biased` `select!` arms, deadline reads, atomic stores, or lock
acquisitions without adding a deterministic race test for the intended change.

## Testing

Unit tests beside the modules validate range rebuilding, ACK deadlines, close
selection, and sender publication. Integration tests in
[`rust/tests/src/arrow_tests.rs`](https://github.com/databricks/zerobus-sdk/blob/main/rust/tests/src/arrow_tests.rs)
use a mock Flight server plus `test-hooks` barriers to reproduce recovery and
close interleavings without relying on sleeps.
