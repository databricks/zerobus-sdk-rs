# Pure-Go Zerobus SDK — Architecture Design

> Reference/design note. **Partially implemented.** Describes how the pure-Go SDK
> layers above the existing `internal/transport` should be structured so that
> proto/JSON and Arrow ingestion share as much logic as possible, the API stays
> evolvable, concurrency is sound, and memory is well-managed.
>
> Status: transport, auth (StaticHeadersProvider + per-table token cache + UC
> OAuthHeadersProvider), and the generic proto/JSON core (`internal/stream`,
> with `wirestream.go` as the transport seam) all exist. Still to build: the
> public `zerobus` package and the Arrow wire path.

---

## 1. The fact that shapes everything

Proto/JSON and Arrow **cannot share a wire path**, so "share as much as possible"
means sharing the *upper layers*, not the transport.

| | Proto / JSON | Arrow |
|---|---|---|
| RPC | `EphemeralStream` (one bidi gRPC stream) | Arrow Flight `do_put` (separate `FlightClient`) |
| Unit on the wire | a record *or* a batch (`ingest_record` / `ingest_record_batch`), **atomic per offset** — one `offset_id` covers the whole message and the server never acks partway through it | a whole `RecordBatch`, Flight-framed and **re-chunked by rows at 2 MiB**, so one batch spans several wire messages and an ack can land mid-batch |
| Ack model | server: "durable **up to offset** N" | server: "durable **up to record-count** N" |
| Recovery | re-send unacked records | re-send unacked batches, slicing partially-acked ones |

Note that proto/JSON already batch: `EphemeralStreamRequest`'s oneof has an
`ingest_record_batch` arm (`ProtoEncodedRecordBatch` / `JsonRecordBatch`), so a
single wire message can carry many rows. The distinction above is atomicity, not
size — a proto batch shares **one** `offset_id` and is indivisible to the server,
whereas an Arrow batch is re-chunked by rows and can be acked partway through
(the root of the record-count ack model in §3a).

The pure-Go proto (`internal/zerobuspb`) has **zero Arrow** today: no ARROW enum,
no Arrow oneof arm, no Arrow RPC. Arrow needs a wire path built (see §6).

The anti-pattern to avoid is the current cgo SDK (`go/`), which reacted to this
divergence by building **two parallel stacks that share no Go code**
(`ZerobusStream` vs `ZerobusArrowStream`) — duplicated `Flush`/`Close`/
`WaitForOffset`, a 5×-repeated `interface{}` type-switch, two overlapping config
structs, and the `Recovery bool` zero-value footgun. Even the Rust core has a
standing TODO (`stream/mod.rs:1-8`) to unify these but hasn't.

The divergence is only at **three narrow points**. Everything else is identical.

---

## 2. Central idea: one generic core, three specialized edges

```
                        PUBLIC API  (package zerobus)
        ┌───────────────────────────────────────────────────────┐
        │  SDK.New(endpoint, ...Option)                          │
        │  Stream  (an INTERFACE)                                │
        │     ├─ ProtoStream   ┐                                 │
        │     ├─ JSONStream    ├─ all embed the SAME *coreStream  │
        │     └─ ArrowStream   ┘                                 │
        └───────────────────────────┬───────────────────────────┘
                                     │
   ┌─────────────────────────────────┴──────────────────────────────┐
   │            coreStream   —   GENERIC, WRITTEN ONCE               │
   │                                                                 │
   │   • bounded buffer (assigns offsets, caps in-flight memory)     │
   │   • monotonic ack watermark                                     │
   │   • Flush / WaitForOffset / Close / GetUnacked / IsClosed       │
   │   • sender goroutine   (single writer, drains buffer)           │
   │   • receiver goroutine (reads acks, advances watermark)         │
   │   • supervisor goroutine (create → run → recover → fail)        │
   │   • graceful close / pause handling                             │
   │   • ack-callback dispatch                                       │
   └──────┬──────────────────────┬───────────────────────┬──────────┘
          │                      │                        │
   ┌──────┴──────┐        ┌──────┴───────┐        ┌───────┴────────┐
   │  encoder    │        │   ackModel   │        │   wireStream   │
   │             │        │              │        │                │
   │ record →    │        │ server resp  │        │ Send / Recv /  │
   │ wire message│        │ → "durable   │        │ handshake /    │
   │             │        │   up to X"   │        │ Close          │
   └─────────────┘        └──────────────┘        └────────────────┘
   proto | json | arrow   offset | recordCount    EphemeralStream | Flight
        (3 impls)             (2 impls)                (2 impls)
```

The buffer, offsets, ack watermark, flush/wait/close, the three goroutines, and
recovery are written **once** in `coreStream`. The only per-encoding code is the
three interfaces below.

---

## 3. The three specialization points

Data flow through them:

```
  user calls stream.IngestProto(bytes)
             │
             ▼
   ┌───────────────────┐
   │   encoder.Encode  │   ← SPECIALIZATION #1
   │  record → wire msg│      proto/json build an EphemeralStream payload
   └─────────┬─────────┘      arrow builds a Flight frame
             │  (already-encoded bytes + assigned offset)
             ▼
   ┌───────────────────┐
   │   bounded buffer  │   ← GENERIC (offset assigned here, backpressure here)
   └─────────┬─────────┘
             │   sender goroutine pulls
             ▼
   ┌───────────────────┐
   │  wireStream.Send  │   ← SPECIALIZATION #3  (EphemeralStream vs Flight)
   └─────────┬─────────┘
             │  ...network...  server acks come back
             ▼
   ┌───────────────────┐
   │  wireStream.Recv  │   ← SPECIALIZATION #3
   └─────────┬─────────┘
             │  raw server response
             ▼
   ┌───────────────────┐
   │  ackModel.Parse   │   ← SPECIALIZATION #2
   │ resp → "up to X"  │      offset-watermark vs record-count
   └─────────┬─────────┘
             │  a resolved offset
             ▼
   ┌───────────────────┐
   │   ack watermark   │   ← GENERIC (Flush/WaitForOffset unblock here)
   └───────────────────┘
```

**#1 `encoder` — "turn a record into a wire message."** The sharpest divergence.
Proto/JSON turn a record into an `EphemeralStream` payload (shapes already exist
in `internal/zerobuspb`). Arrow turns a `RecordBatch` into a Flight frame.
Encoding is **eager, at the `Ingest` call**, so the buffer holds already-
serialized bytes, never live user objects (matches Rust).

**#2 `ackModel` — "interpret the server response."** Proto/JSON: read
`durability_ack_up_to_offset`. Arrow: map a cumulative record count back to an
offset via each batch's `[start, end)` range. Both reduce to one number:
"everything up to offset X is durable." Above that number, the machinery is
identical. This point is subtler than #1 and #3 — see §3a for why Arrow acks are
record-based and how the interface is shaped.

**#3 `wireStream` — "the actual transport."** Proto/JSON already exists
(`transport.Stream` over `EphemeralStream`). Arrow needs a new
`transport.FlightStream`. Both are built on the transport's existing
`rawStream[Req,Resp]` generic:

```
      rawStream[Req, Resp]         ← generic, exists today in raw_stream.go
      ├─ Send / Recv / CloseSend / Close   (identical for any bidi RPC)
      └─ handshake(sendSetup, confirmReady) ← two hooks, the ONLY difference
                    │
        ┌───────────┴────────────┐
        │                        │
   proto/JSON Stream        Arrow FlightStream
   sendSetup = create-      sendSetup = send
     stream request           schema
   confirmReady = read      confirmReady = wait
     stream_id                for ready sentinel
```

`raw_stream.go`'s own comment already says the `bidiRPC` interface is what
`rawStream` needs and that *"EphemeralStream satisfies it, as will Arrow Flight's
DoPut."* Both `wireStream`s embed `rawStream` and supply two small hooks.

---

## 3a. Why Arrow acks are record-based (and the `ackModel` shape)

This is the least obvious part of the design, so it gets its own section.
Reference: Rust `arrow_stream.rs` and `arrow_metadata.rs`.

### Two different "slicings" — don't conflate them

1. **Transport chunking (`FlightDataEncoderBuilder`) — automatic, size-driven.**
   When one `RecordBatch` is handed to the encoder, it is split into multiple
   `FlightData` wire messages if it exceeds **2 MiB** (`arrow_stream.rs:508-509`:
   "automatic batch chunking at 2 MiB"). The split is **by rows** (contiguous
   row-ranges, each carrying the full column set), *not* by columns — despite
   Arrow being columnar in memory, the chunk boundary is a row boundary. Each
   emitted data message gets its own sequential wire offset in `app_metadata`
   (`arrow_stream.rs:512-524`). Note: the 2 MiB is the client-side chunk
   threshold; a separate ~5 MB server-side gRPC message limit is why the client
   chunks below it.
2. **Recovery slicing (`slice_batch_for_recovery`) — manual, row-driven.** On
   reconnect, a partially-acked batch is sliced with
   `batch.slice(records_already_acked, remaining_rows)` so only the un-acked tail
   is replayed (`arrow_stream.rs:76-109`).

### Why by-record, not by-batch

Because chunking can make a **server ack land mid-batch**:

```
  ONE RecordBatch you ingest  (e.g. 5000 rows, 6 MiB)
        │  FlightDataEncoderBuilder.build(...)   ← chunks at 2 MiB
        ▼
  ┌──────────┬───────────┬──────────┐
  │ Flight#1 │ Flight#2  │ Flight#3 │   3 wire messages, each ≤ 2 MiB
  │ rows     │ rows      │ rows     │
  │ 0..2100  │ 2100..4200│4200..5000│
  └──────────┴───────────┴──────────┘
        │  server acks independently...
        ▼
   server: "durable up to 4200 records"   ← ack lands MID-BATCH
```

By-batch acking cannot represent "4200 of this batch's 5000 rows are durable." On
reconnect you'd have to replay the whole 5000-row batch, re-sending 4200 already-
durable rows (duplication + wasted bandwidth). So the model tracks **cumulative
record counts**.

### The `ackModel` interface shape (Option 1: core stays offset-only)

```go
type ackModel interface {
    // called when a batch/record is queued, so Arrow can record the
    // cumulative [start,end) range this item occupies. proto/json no-op.
    Track(offset int64, recordCount int)

    // translate a raw server response into the new highest fully-acked
    // offset (or "no advance"). Arrow does the count→offset range math
    // here; proto/json just reads the offset.
    Resolve(serverResp) (highestAckedOffset int64, ok bool)

    // called on reconnect: given what the server confirmed, report which
    // queued items must be replayed (and, for Arrow, how to slice a
    // straddling batch). Keeps recovery record-math out of the core.
    Unacked() []replayItem
}
```

| | Proto/JSON `ackModel` | Arrow `ackModel` |
|---|---|---|
| Server sends | `durability_ack_up_to_offset` (already an offset) | `ack_up_to_records` (cumulative record count) |
| Atomic unit | 1 message = 1 offset (a `ingest_record_batch` shares one offset for the whole batch); indivisible to the server | 1 batch spans a **range** of records |
| `Resolve` work | trivial: the offset *is* the answer | translate count → highest offset with `count >= end_record` |
| Internal state | none (or a trivial passthrough) | `cumulative_records_sent` + per-batch `[start,end)` |
| Partial coverage | impossible (records atomic) | possible; batch stays pending until fully covered |

**Why Option 1 over the alternative** (making the core track a generic
`Countable` unit count, à la Rust's `LandingZone<T: Countable>`): keeping the core
offset-only makes the core simpler and totally blind to the record-vs-batch
distinction — the entire "partial batch" complexity is quarantined inside the
Arrow `ackModel`, including the recovery-slice math. The core's watermark,
`Flush`, and `WaitForOffset` only ever compare offsets. The cost is that the
`ackModel` interface is slightly wider (it owns `Track`/`Unacked`, not just
`Resolve`), but that width lives exactly where the complexity belongs.

---

## 4. Goal: Evolvability

```
  New encoding?        → new constructor + new Ingest methods.  Nothing else changes.
  New config knob?     → new WithX() option.  Every existing call site still compiles.
  New stream method?   → Stream is an interface, concretes are unexported → add freely.
```

- **Functional options** on `New(...)` and `CreateStream(...)` — same pattern as
  `transport.Dial`. This is the Go equivalent of the Rust builder.
- **One config struct**, unexported, behind options. **Recovery is an enum, never
  a `bool`** (adopt the Arrow side's `RecoverySetting`, zero value =
  `RecoveryEnabled`), killing the cgo `types.go:31` footgun. Encoding-specific
  knobs (Arrow compression, proto descriptor) are just more options; irrelevant
  ones are ignored, not errors.
- **Typed ingest methods** (`IngestProto`/`IngestJSON`/`IngestArrow`, batch
  variants) — no `interface{}`, no runtime type-switch, compile-time safe.

---

## 5. Goal: Concurrency & Memory

Concurrency:

```
   many goroutines                  ONE                    ONE
   calling Ingest*  ──enqueue──▶  sender  ──Send──▶ ... ──▶ receiver ──▶ watermark
   (safe, guarded)                goroutine                goroutine        ▲
                                                                            │
                            supervisor goroutine owns create→run→recover    │
                                                                     Flush/Wait block here
```

- Public `Ingest*` only **enqueues** and returns (throughput-correct: queue-then-
  `Flush`, never wait per record).
- `rawStream` is "not safe for concurrent Send," so the core uses **exactly one
  sender goroutine** — no lock around the socket.
- **Bounded buffer** (semaphore / buffered channel sized to `MaxInflight`) =
  backpressure. Mirrors Rust `LandingZone`.
- **Ack watermark** = Go equivalent of Rust `tokio::watch`: `Flush`/
  `WaitForOffset` block on "last acked ≥ target," racing a stored terminal error
  and the flush timeout.
- Teardown is context-driven (transport ties stream lifetime to a child context).

Memory:

```
   cgo SDK memory surface          pure-Go SDK memory surface
   ─────────────────────           ──────────────────────────
   runtime.Pinner (pin/unpin)  ┐
   cgo.Handle registry + mutex ├──▶   (all gone — GC owns everything)
   manual free / free_error    ┘
   finalizers for safety              Close is explicit + idempotent
```

- **No cgo → no `runtime.Pinner`, no `cgo.Handle` registry, no manual frees.**
- **Eager encoding** → buffer holds bounded `[]byte`, not user objects. Only
  unacked records are retained; dropped as the watermark advances.
- `GetUnacked` returns typed copies, never aliases of internal buffers.
- Live memory capped at `MaxInflight × record size`.

---

## 6. Prerequisite: Arrow's wire path

Arrow has no wire path in the pure-Go module yet. Two options:

```
  Option A (recommended)              Option B
  ──────────────────────              ────────
  Add Arrow Flight do_put +           Add an Arrow arm to the existing
  a transport.FlightStream            EphemeralStream RPC + an ARROW
                                      RecordType
  • matches Rust exactly              • smaller Go surface
  • client-only, no server change     • but a PROTO + SERVER CONTRACT change
  • arrow-flight dep isolated           you don't control unilaterally
    to this module
```

Recommend **A** — additive, no server-contract change, `arrow-flight` dep
contained, mirrors the proven Rust split. Raise B only if the service team
prefers one RPC.

---

## 7. Package layout

Present today: `internal/transport`, `internal/auth` (with UC OAuth),
`internal/stream`, `internal/zerobuspb`. Not yet built: the public `zerobus`
package and the Arrow wire path.

```
purego/
├── zerobus/                  PUBLIC API                             (NOT BUILT)
│   ├── sdk.go                  SDK, New(...Option), Close
│   ├── stream.go               Stream interface + typed Ingest* methods
│   ├── options.go              Option/StreamOption, RecoverySetting enum
│   └── errors.go               exported error + Retryable()
├── internal/stream/          THE GENERIC CORE                       (BUILT: proto/JSON)
│   ├── core.go                 buffer, watermark, flush/wait/close,
│   │                             callback dispatcher, sender/receiver
│   ├── supervisor.go           create → run → recover → fail-pending
│   ├── encoder.go              encoder interface + proto/json impls
│   ├── ackmodel.go             ackModel interface + offset resolver
│   ├── wirestream.go           transport seam (opener + wireStream)
│   ├── errors.go               errClosed, ErrPayloadTooLarge, pauseSignal
│   └── buffer.go               bounded offset-assigning buffer
├── internal/transport/       EXISTS; add FlightStream for Arrow
├── internal/auth/            EXISTS; HeadersProvider + token cache + UC OAuth
└── internal/zerobuspb/       EXISTS
```

---

## 8. Suggested build order (stacked branches)

```
  purego-transport   ◀── DONE (merged)
        │
        ├─▶ purego-auth    ◀── DONE (merged)
        │                     HeadersProvider seam + StaticHeadersProvider +
        │                     per-table token cache + UC OAuthHeadersProvider.
        │
        ├─▶ purego-core    ◀── IN PROGRESS
        │                     Generic core + proto/JSON encoder/ackModel +
        │                     transport seam. No public API yet.
        │
        ├─▶ purego-api     public zerobus pkg: SDK, Stream interface, typed methods, options
        │
        └─▶ purego-arrow   FlightStream + Arrow encoder + record-count ackModel
                           ── minimal core changes: adds Track / Resolve /
                              Unacked to the ackModel seam (see §3a) so partial
                              batch acks can slice a straddling batch on
                              reconnect. Sender/receiver/supervisor/buffer stay
                              as-is.
```

The core's proto/JSON test suite is JSON-first with limited proto coverage;
extending it to run the same ingest→flush→recover→drain tests against every
encoder is a `purego-arrow` acceptance bar, not something today's suite yet
demonstrates.

---

## 9. Reuse (don't reinvent)

- `internal/transport/raw_stream.go` — `rawStream[Req,Resp]` + two-hook
  `handshake(sendSetup, confirmReady)`. Both wireStreams embed this.
- `internal/transport/transport.go` — `Dial` + `DialOption` pattern to mirror;
  `Conn` backs all streams.
- `internal/transport/headers.go` — the `HeadersProvider` auth seam. The transport
  calls `GetHeaders(ctx, tableName)` at open and attaches the returned metadata
  verbatim (the value is sent as the provider formats it — the transport no longer
  prefixes `Bearer` or inspects the scheme), invalidating on auth rejection via
  `Invalidate(ctx, tableName)`.
- `internal/auth/` — `HeadersProvider` implementations that feed that seam.
  `StaticHeadersProvider`, an internal per-table token cache (single-flight,
  proactive refresh), and the UC `OAuthHeadersProvider` (which formats
  `"Bearer <token>"` itself, matching the Rust core and the other SDKs) all
  exist today.
- `internal/zerobuspb` — proto/JSON wire message shapes for the encoders.
