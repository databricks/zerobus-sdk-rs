# Zerobus pure-Go SDK examples

Runnable examples for the pure-Go Zerobus SDK (`github.com/databricks/zerobus-sdk/purego`).
All examples live in a single module that `replace`s the SDK with the local
checkout, so they build against your working tree with no extra setup.

## Available examples

| Example      | Format           | Pattern                          | Location       |
| ------------ | ---------------- | -------------------------------- | -------------- |
| JSON single  | JSON             | Single-record loop, then `Flush` | `json/single`  |
| JSON batch   | JSON             | One atomic batch                 | `json/batch`   |
| Proto single | Protocol Buffers | Single-record loop, then `Flush` | `proto/single` |
| Proto batch  | Protocol Buffers | One atomic batch                 | `proto/batch`  |

Every example uses the idiomatic **loop-then-`Flush()`** pattern: queue records
without waiting, then confirm durability once at the end (or periodically for a
continuous stream). Never wait for an acknowledgement after every record — that
collapses throughput to one record per server round-trip.

## Prerequisites

### 1. Create a Delta table

```sql
CREATE TABLE catalog.schema.orders (
  id INT,
  customer_name STRING,
  product_name STRING,
  quantity INT,
  price DOUBLE,
  status STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);
```

### 2. Set up a service principal

Create a service principal with `SELECT` and `MODIFY` on the table, and an OAuth
client id / secret for it.

### 3. Configure connection info

Every connection setting is read from the environment — nothing is baked into
source. Export these before running:

```bash
export ZEROBUS_SERVER_ENDPOINT="https://<workspace-id>.zerobus.<region>.cloud.databricks.com"
export DATABRICKS_WORKSPACE_URL="https://<your-workspace>.cloud.databricks.com"
export ZEROBUS_TABLE_NAME="catalog.schema.orders"
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
```

## Regenerating the proto bindings

The proto examples use bindings generated from `proto/orders.proto` into
`proto/pb/`. To regenerate after editing the schema (requires `protoc` and
`protoc-gen-go` on `PATH`):

```bash
cd proto && ./generate_proto.sh
```

`orders.proto` is a hand-maintained copy of the table's schema — keep it in sync
with the table, or records land in the wrong columns. The pure-Go SDK uses this
static descriptor (marshaled from the generated bindings) rather than building
one at runtime from Unity Catalog metadata.
