# Zerobus Rust Tools

A tool for generating Protocol Buffer schemas and Rust code from Unity Catalog table definitions.

## Overview

This tool fetches table schema information from Unity Catalog and generates:
- Protocol Buffer (`.proto`) files that match the table schema
- Rust structs and serialization code via `tonic-build`
- Binary descriptor files for runtime schema validation

## Supported Data Types

| Unity Catalog Type | Probuf Type | Notes                                                        |
|--------------------|----------------------|--------------------------------------------------------------|
| `SMALLINT`, `SHORT`, `INT` | `int32`              |                                                              |
| `BIGINT`, `LONG`   | `int64`              |                                                              |
| `FLOAT`            | `float`              |                                                              |
| `DOUBLE`           | `double`             |                                                              |
| `STRING`, `VARCHAR(n)` | `string`             |                                                              |
| `BOOLEAN`          | `bool`               |                                                              |
| `BINARY`           | `bytes`              |                                                              |
| `DATE`             | `int32`              | Mapped to `int32` (days since Unix epoch)                    |
| `TIMESTAMP`        | `int64`              | Mapped to `int64` (microseconds since Unix epoch)            |
| `ARRAY<T>`         | `repeated T_pb`      | Supports arrays of any other supported type, including structs. |
| `STRUCT<...>`      | `message`            | Nested structs are fully supported.                          |

## How to use

### Basic example

```bash
cargo run -- \
  --uc-endpoint "https://your-workspace.cloud.databricks.com" \
  --uc-token "dapi123..." \
  --table "catalog.schema.table_name"
```

### Full Options

```bash
cargo run -- \
  --uc-endpoint "https://your-workspace.cloud.databricks.com" \
  --uc-token "dapi123..." \
  --table "catalog.schema.table_name" \
  --output "my_table.proto" \
  --proto-msg "MyTableMessage" \
  --output-dir "generated"
```


## Command Line Arguments

- `--uc-endpoint` - Unity Catalog endpoint URL (e.g., "https://your-workspace.cloud.databricks.com")
- `--uc-token` - UC authentication token (e.g., "dapi123...")
- `--table` - Full table name in format "catalog.schema.table_name"
- `--output` - Output proto file path (optional, defaults to "{table_name}.proto")
- `--proto-msg` - Message name in proto (optional, defaults to table name)
- `--output-dir` - Output directory for generated files (optional, defaults to "output"). Can be relative or absolute path

**Note:** UC token ([`PAT token`](https://docs.databricks.com/aws/en/dev-tools/auth/pat#databricks-personal-access-tokens-for-workspace-users)) is needed for this tool.
Go to: Workspace -> Click user located on top right -> Settings -> Developer -> Access Token -> Manage -> Generate New Token 

## Output Files

When you run the tool, it generates:

1. **`{table_name}.proto`** - The Protocol Buffer schema
2. **`{table_name}.rs`** - Generated Rust structs  
3. **`{table_name}.descriptor`** - Binary descriptor file

## Limitations
- Direct nested arrays (e.g., `ARRAY<ARRAY<STRING>>`) are not supported as top-level column types.
- `MAP<STRUCT<_>, _>` - `STRUCT` can't be key.
- `MAP<MAP<_>, _>` - use `MAP<STRUCT<m: MAP<_>>, _>` instead.
- `MAP<_, MAP<_>>` - use `MAP<_, STRUCT<m: MAP<_>>>` instead.
- `MAP<ARRAY<_>, _>` - use `MAP<STRUCT<a: ARRAY<_>>, _>` instead.
- `MAP<_, ARRAY<_>>` - use `MAP<_, STRUCT<a: ARRAY<_>>>` instead.
- `ARRAY<MAP<_, _>>` - use `ARRAY<STRUCT<m: MAP<_, _>>>` instead.
- `ARRAY<ARRAY<_>>` - use `ARRAY<STRUCT<a: ARRAY<_>>>` instead.

