# Zerobus pure-Go SDK examples

Runnable examples for the pure-Go Zerobus SDK (`github.com/databricks/zerobus-sdk/purego`).
All examples live in a single module that `replace`s the SDK with the local
checkout, so they build against your working tree with no extra setup.

## Available examples

| Example      | Format           | Pattern                          | Location            |
| ------------ | ---------------- | -------------------------------- | ------------------- |
| JSON single  | JSON             | Single-record loop, then `Flush` | `json/single`       |
| JSON batch   | JSON             | One atomic batch                 | `json/batch`        |
| Proto single | Protocol Buffers | Single-record loop, then `Flush` | `proto/single`      |
| Proto batch  | Protocol Buffers | One atomic batch                 | `proto/batch`       |
| Continuous   | JSON             | Periodic `Flush` + ack callback  | `continuous`        |
| Benchmark    | JSON + Proto     | ClickBench throughput (MiB/s)    | `benchmark`         |

Every example uses the idiomatic **loop-then-`Flush()`** pattern: queue records
without waiting, then confirm durability once (or periodically for the
continuous stream). Never wait for an acknowledgement after every record — that
collapses throughput to one record per server round-trip.

### Benchmark

`benchmark` measures durably-acked ingestion throughput against a wide-schema
ClickBench "hits" table (105 columns) using a parallel-streams harness: a fixed
pool of distinct pre-built records, a warmup, then a timed window with periodic
flushes. It prints both `queued MiB/s` (ingest loop only — an upper bound, not
durable) and `acked MiB/s` (includes the final flush, so every byte is durably
acknowledged — the honest number), aggregated across all streams.

It runs **both** formats and prints a row for each, so you can compare JSON
against Protocol Buffers on identical data:

- `json` — each row is ingested as a JSON string.
- `proto` — each row is encoded to protobuf bytes with a static, hand-maintained
  105-column descriptor (`benchmark/clickbench/clickbench.proto`) and ingested on
  a proto stream. Set `ZEROBUS_BENCH_FORMAT=json` or `=proto` to run only one.

It targets `shinkansen.default.clickbench_load_test_zlata` by default. Create a
matching ClickBench `hits` table (or point `ZEROBUS_BENCH_TABLE` at your own),
then:

```bash
go run ./benchmark                                       # both formats, 512 MiB, 1 stream
ZEROBUS_BENCH_STREAMS=4 go run ./benchmark               # 4 parallel streams
ZEROBUS_BENCH_FORMAT=proto go run ./benchmark            # proto only
ZEROBUS_BENCH_TOTAL_BYTES=2147483648 go run ./benchmark  # 2 GiB headline run
```

Tuning env vars (all optional): `ZEROBUS_BENCH_TOTAL_BYTES`,
`ZEROBUS_BENCH_STREAMS`, `ZEROBUS_BENCH_RECORDS_PER_BATCH`,
`ZEROBUS_BENCH_FLUSH_EVERY_N`, `ZEROBUS_BENCH_MAX_INFLIGHT`,
`ZEROBUS_BENCH_FORMAT`, `ZEROBUS_BENCH_TABLE`.

The proto descriptor is generated from `clickbench/clickbench.proto` into
`clickbench/pb/`. Regenerate it after editing the schema (requires `protoc` and
`protoc-gen-go` on `PATH`):

```bash
cd benchmark/clickbench && ./generate_proto.sh
```

`clickbench.proto` is a hand-maintained copy of the table's schema (field numbers
= column ordinals) — keep it in sync with the table, or records land in the wrong
columns. Unlike the C++ ClickBench benchmark, the descriptor is built from this
static `.proto` rather than from Unity Catalog metadata (the pure-Go SDK has no
UC-metadata schema builder).

## Prerequisites

### 1. Create a Delta table

```sql
CREATE TABLE shinkansen.default.air_quality_zlata (
  device_name STRING,
  temp INT,
  humidity INT
);
```

### 2. Set up a service principal

Create a service principal with `SELECT` and `MODIFY` on the table, and an OAuth
client id / secret for it.

### 3. Configure connection info

Non-secret connection info — endpoint, workspace URL, and table — is baked into
`config/config.go` (pointing at the demo table by default). Edit it to target
your own table/workspace, or override any value at runtime:

```bash
export ZEROBUS_SERVER_ENDPOINT="https://<workspace-id>.zerobus.<region>.cloud.databricks.com"
export DATABRICKS_WORKSPACE_URL="https://<your-workspace>.cloud.databricks.com"
export ZEROBUS_TABLE_NAME="catalog.schema.table"
```

### 4. Provide credentials (secrets, never checked in)

```bash
export DATABRICKS_CLIENT_ID="<oauth-client-id>"
export DATABRICKS_CLIENT_SECRET="<oauth-client-secret>"
```

## Running

From this directory:

```bash
go run ./json/single
go run ./json/batch
go run ./proto/single
go run ./proto/batch
go run ./continuous
```

## Regenerating the proto bindings

The proto examples use bindings generated from `proto/air_quality.proto` into
`proto/pb/`. To regenerate after editing the schema (requires `protoc` and
`protoc-gen-go` on `PATH`):

```bash
cd proto && ./generate_proto.sh
```

`air_quality.proto` is a hand-maintained copy of the table's schema — keep it in
sync with the table, or records land in the wrong columns.
