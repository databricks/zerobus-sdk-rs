# Design: Pluggable stats exporter

Status: **in progress.** Initial cut is **Arrow Flight only** (behind `arrow-flight`);
gRPC support and the FFI vtable are follow-ups. The transport-agnostic parts of this
doc describe the intended direction, not what ships in the first cut.

## Problem

The SDK emits telemetry (per-batch byte sizes, acks, reconnects) that consumers want
to route into their own metrics systems. Today this is piecemeal: gRPC has
`AckCallback` (ack/error only), Arrow has none. We want one seam that generalizes
across transports and stat kinds.

## Prior art

- **gRPC `stats.Handler`** — one `HandleRPC(ctx, RPCStats)`; `OutPayload` already
  carries `Length` (uncompressed), `CompressedLength`, `WireLength`. Typed events,
  push, per-message wire-vs-uncompressed. Closest model.
- **OpenTelemetry metrics** — `MeterProvider` + `MetricReader`/`Exporter`, push and
  periodic pull. Separates "measure" from "export".
- **Rust `metrics` crate** — thin global `Recorder` trait; app picks the backend.
- **Prometheus `Collector`/`Registry`** — pull; aggregation lives in the producer.
- **Kafka `MetricsReporter`, tracing `Layer`** — sink registered at construction,
  fed a stream of typed records.

## Proposed interface

Push seam, transport-agnostic, registered on the builder:

```rust
#[non_exhaustive]
pub struct BatchStats { pub records: u64, pub wire_bytes: u64, pub uncompressed_bytes: u64 }

#[non_exhaustive]
#[derive(Clone, Copy)]
pub enum StreamStat {
    BatchSent    { offset: OffsetId, stats: BatchStats },   // encoded & sent (byte sizes here)
    BatchAcked   { offset: OffsetId },                      // durability only
    Reconnected  { attempt: u32 },
}

pub trait StatsExporter: Send + Sync {
    fn record(&self, stat: StreamStat);
}

// builder (Arrow today)
sdk.stream_builder().table(t).arrow(schema)
   .stats_exporter(Arc<dyn StatsExporter>)
```

The SDK calls `record(...)` at seams it already has: the Flight encoder
(`BatchSent`), the ack processor (`BatchAcked`), the supervisor (`Reconnected`).
Both enum and trait stay `#[non_exhaustive]`; consumers `match` with a `_ => {}` arm
so new stats land in minor releases. `StreamStat` is owned and `Copy` (no borrowed
error), so it forwards across a channel with no owned mirror. A future failure event
carrying `&ZerobusError` would reintroduce a borrowed/owned split.

An **offset is one ingest call (one batch), not one row** — a 1000-row
`RecordBatch` gets a single `OffsetId` spanning records `[start, start+1000)`. So
the SDK emits one `BatchSent`/`BatchAcked` per batch/offset (`BatchStats.records`
carries the row count); byte sizes are per-batch by nature (the IPC frame is a batch).

## Emission timing

`BatchSent` carries the byte sizes and is emitted at **encode/send** time, not at
ack:

- `uncompressed_bytes` is measured from the `RecordBatch` when it is pulled into the
  encoder (`get_slice_memory_size` sum — codec-independent, no re-encode);
  `wire_bytes` is summed from the emitted `FlightData` frames (after IPC compression).
- These live in a **single current-batch accumulator**, not an offset-keyed map. The
  encoder drains one batch's frames before pulling the next, so `BatchSent` is emitted
  when the next batch is pulled (previous batch complete), at natural end-of-stream,
  and on **graceful close** (the last batch is fully sent + acked and won't be
  replayed, so the request-body shutdown flushes it). A batch cut off mid-encode by a
  **recovery/rotation** cancel is **not** emitted — it is re-sent (and re-emitted) on
  the next connection.
- Consequences: `BatchSent` **counts retransmits** (accurate for a bytes-sent meter).
  Durability is a separate signal (`BatchAcked`). The only thing not captured is a
  per-retry breakdown.

## Built-in channel exporter

Ship a batteries-included impl so consumers don't hand-roll the common case —
forward stats to a channel and drain them elsewhere:

```rust
pub struct ChannelExporter { tx: mpsc::Sender<StreamStat> }

impl StatsExporter for ChannelExporter {
    fn record(&self, stat: StreamStat) {
        // non-blocking: drop on full so a slow consumer never stalls ingest
        let _ = self.tx.try_send(stat);
    }
}

pub fn channel_exporter(cap: usize) -> (Arc<ChannelExporter>, mpsc::Receiver<StreamStat>);
```

- `StreamStat` is owned and `Copy`, so `record` forwards it across the channel
  directly — no owned mirror / conversion needed.
- **Bounded + `try_send`, drop-on-full with a dropped counter** — a slow consumer
  can't stall ingest or grow memory unbounded (stats are droppable). (gRPC's
  callback handler uses an unbounded channel; for stats prefer bounded.)
- The consumer drains the `Receiver` as a stream at its own pace, fully off the IO
  path — this is the decoupled path, so it is not for per-request read-after-wait.

## Design decisions

1. **Push, not pull.** Push gives per-event / per-request data (needed for
   per-request byte metrics) and keeps no per-offset state in the SDK — a single
   current-batch accumulator in the encoder replaces any offset-keyed map.
2. **Inline dispatch, documented lightweight.** `record` runs on hot/IO paths, so
   it must be cheap (same contract as `AckCallback` today). Consumers needing heavy
   work buffer internally. An async channel+task (like gRPC's callback handler)
   decouples the IO loop but breaks request-lifecycle coupling (read-after-wait
   race), so it is not the default.
3. **SDK emits values, never pre-labeled metric names.** The exporter owns labels
   and label cardinality (OTel/Prometheus lesson; matches the repo metric-label
   rule).

## Cost: FFI

Rust-native is easy (Rust SDK, PyO3, NAPI). Crossing the C FFI (Go/Java) needs a C
vtable — `struct { void* ctx; void (*record)(void* ctx, CStreamStat); void (*free)(void* ctx); }`
— with the enum flattened to a tagged C struct plus lifetime/thread-safety rules
(Go `cgo.Handle`, JNI global refs). Same pattern as `HeadersProvider`. This is the
bulk of the work and the reason a general exporter is a bigger commitment than the
accessor.

## Recommendation

Ship Rust-core-only first: `StatsExporter` + `StreamStat`, inline dispatch, wired
at the existing encoder/ack/supervisor seams, shared by gRPC and Arrow (unifying
the split where gRPC has `AckCallback` and Arrow has none). Add the FFI vtable
later, per wrapper demand.
