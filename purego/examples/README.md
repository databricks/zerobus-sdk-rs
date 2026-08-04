# Zerobus pure-Go SDK examples

Runnable examples for `github.com/databricks/zerobus-sdk/purego`.

## Available examples

| Example      | Format           | Pattern                          | Location       |
| ------------ | ---------------- | -------------------------------- | -------------- |
| JSON single  | JSON             | Single-record loop, then `Flush` | `json/single`  |
| JSON batch   | JSON             | One atomic batch                 | `json/batch`   |
| Proto single | Protocol Buffers | Single-record loop, then `Flush` | `proto/single` |
| Proto batch  | Protocol Buffers | One atomic batch                 | `proto/batch`  |
| Proto runtime | Runtime Protocol Buffers | Descriptor and messages built in Go | `proto/runtime` |
| Dynamic single | Dynamic Proto (UC schema fetch) | Single-record loop, then `Flush` | `dynamic/single` |
| Dynamic batch  | Dynamic Proto (UC schema fetch) | One atomic batch | `dynamic/batch` |
| Dynamic message | Protocol Buffers (UC schema fetch) | Runtime messages, then `Flush` | `dynamic/proto` |

Every example uses **loop-then-`Flush()`**: queue records, then flush once.

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

Create a service principal with `SELECT` and `MODIFY` on the table and an OAuth client id/secret.

### 3. Configure connection info

Export these before running:

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
go run ./proto/runtime
go run ./dynamic/single
go run ./dynamic/batch
go run ./dynamic/proto
```

## Regenerating the proto bindings

The proto single and batch examples use bindings generated from
`proto/orders.proto` into `proto/pb/`. Regenerate after schema edits (`protoc`
and `protoc-gen-go` required):

```bash
cd proto && ./generate_proto.sh
```

`orders.proto` mirrors the table schema. Keep them in sync.

The proto runtime example builds its descriptor and messages directly in Go. It
does not use `.proto` files or generated bindings.
