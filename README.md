# Zerobus SDKs

Monorepo for Databricks Zerobus Ingest SDKs.

## Disclaimer

[GA](https://docs.databricks.com/release-notes/release-types.html): This SDK is generally available and supported for production use cases. Minor and patch version updates will not contain breaking changes. Major version updates may include breaking changes.

We are keen to hear feedback from you. Please [file issues](https://github.com/databricks/zerobus-sdk/issues), and we will address them.

## What is Zerobus?

Zerobus is a high-throughput streaming service for direct data ingestion into Databricks Delta tables, optimized for real-time data pipelines and high-volume workloads.

## SDKs

| Language   | Directory                    | Package                                                                                                        |
| ---------- | ---------------------------- | -------------------------------------------------------------------------------------------------------------- |
| Rust       | [`rust/`](rust/)             | [`databricks-zerobus-ingest-sdk`](https://crates.io/crates/databricks-zerobus-ingest-sdk)                      |
| Python     | [`python/`](python/)         | [`databricks-zerobus-ingest-sdk`](https://pypi.org/project/databricks-zerobus-ingest-sdk/)                     |
| Go         | [`go/`](go/)                 | [`github.com/databricks/zerobus-sdk/go`](https://pkg.go.dev/github.com/databricks/zerobus-sdk/go)              |
| TypeScript | [`typescript/`](typescript/) | [`@databricks/zerobus-ingest-sdk`](https://www.npmjs.com/package/@databricks/zerobus-ingest-sdk)               |
| Java       | [`java/`](java/)             | [`com.databricks:zerobus-ingest-sdk`](https://central.sonatype.com/artifact/com.databricks/zerobus-ingest-sdk) |
| C++        | [`cpp/`](cpp/)               | Source / CMake (`zerobus::zerobus`)                                                                            |
| C#         | [`dotnet/`](dotnet/)         | [`Databricks.Zerobus.Ingest.Sdk`](https://www.nuget.org/packages/Databricks.Zerobus.Ingest.Sdk)                |

## Platform Support

We try to provide prebuilt native binaries for the following platforms:

| Platform | Architecture            |
| -------- | ----------------------- |
| Linux    | x86_64                  |
| Linux    | aarch64                 |
| Windows  | x86_64                  |
| macOS    | x86_64                  |
| macOS    | aarch64 (Apple Silicon) |

> **Note:** We do not currently have macOS CI runners, so macOS binaries are built locally and may not be available for every SDK or release. If your platform is not supported or you encounter compatibility issues, you can [build from source](CONTRIBUTING.md) or [file an issue](https://github.com/databricks/zerobus-sdk/issues).

## Prerequisites

Before using any SDK, you need the following:

### 1. Workspace URL and Workspace ID

After logging into your Databricks workspace, look at the browser URL:

```
https://<databricks-instance>.cloud.databricks.com/o=<workspace-id>
```

- **Workspace URL**: The part before `/o=` (e.g., `https://dbc-a1b2c3d4-e5f6.cloud.databricks.com`)
- **Workspace ID**: The part after `/o=` (e.g., `1234567890123456`)

> **Note:** The examples above show AWS endpoints (`.cloud.databricks.com`). For Azure deployments, the workspace URL will be `https://<databricks-instance>.azuredatabricks.net`.

### 2. Create a Delta Table

Create a table using Databricks SQL:

```sql
CREATE TABLE <catalog_name>.default.<table_name> (
    device_name STRING,
    temp INT,
    humidity BIGINT
)
USING DELTA;
```

Replace `<catalog_name>` with your catalog name (e.g., `main`).

### 3. Create a Service Principal

1. Navigate to **Settings > Identity and Access** in your Databricks workspace
2. Click **Service principals** and create a new service principal
3. Generate a new secret for the service principal and save it securely
4. Grant the following permissions:
   - `USE_CATALOG` on the catalog (e.g., `main`)
   - `USE_SCHEMA` on the schema (e.g., `default`)
   - `MODIFY` and `SELECT` on the table

Grant permissions using SQL:

```sql
-- Grant catalog permission
GRANT USE CATALOG ON CATALOG <catalog_name> TO `<service-principal-application-id>`;

-- Grant schema permission
GRANT USE SCHEMA ON SCHEMA <catalog_name>.default TO `<service-principal-application-id>`;

-- Grant table permissions
GRANT SELECT, MODIFY ON TABLE <catalog_name>.default.<table_name> TO `<service-principal-application-id>`;
```

The service principal's **Application ID** is your OAuth **Client ID**, and the generated secret is your **Client Secret**.

## Ingestion APIs

Pick the API that matches your data.

### Standard gRPC ingestion

Supported by all SDKs. Records are sent over a gRPC stream in one of two serialization formats:

- **JSON** - Simple, schema-free ingestion. Pass a JSON string or native object (dict, map, etc.) and the SDK serializes it. No compilation step required. Good for getting started or dynamic schemas.
- **Protocol Buffers** - Strongly-typed, schema-validated ingestion. More efficient over the wire. Recommended for production workloads.

#### Protocol Buffers

Use `proto2` syntax with `optional` fields to correctly represent nullable Delta table columns.

##### Delta → Protobuf Type Mappings

| Delta Type                          | Proto2 Type          |
| ----------------------------------- | -------------------- |
| TINYINT, BYTE, INT, SMALLINT, SHORT | int32                |
| BIGINT, LONG                        | int64                |
| FLOAT                               | float                |
| DOUBLE                              | double               |
| STRING, VARCHAR                     | string               |
| BOOLEAN                             | bool                 |
| BINARY                              | bytes                |
| DATE                                | int32                |
| TIMESTAMP, TIMESTAMP_NTZ            | int64                |
| ARRAY\<type\>                       | repeated type        |
| MAP\<key, value\>                   | map\<key, value\>    |
| STRUCT\<fields\>                    | nested message       |
| VARIANT                             | string (JSON string) |

#### Schema Generation

Instead of writing `.proto` files by hand, each SDK ships a tool to generate protobuf schemas directly from an existing Unity Catalog table. See the individual SDK READMEs for language-specific usage.

### Arrow Flight ingestion

Arrow Flight ingestion is generally available in the SDKs that expose it, from the versions in the table below. It is a third record format alongside JSON and Protocol Buffers: send Apache Arrow `RecordBatch` data directly to Zerobus over the Arrow Flight protocol, on the same gRPC connection. Best fit when:

- Your workload is naturally columnar or batched — analytics pipelines, gateways aggregating short windows of rows, wide/numeric schemas where row-by-row serialization adds noticeable CPU overhead.
- Your application already produces Arrow data — pyarrow, the [arrow-rs](https://github.com/apache/arrow-rs) crates, DataFusion, Polars, or other libraries built on Arrow.

For sparse, one-row-at-a-time traffic, JSON or Protocol Buffers over the standard SDK gRPC path are usually simpler. Most SDKs ship a runnable `examples/arrow/` directory (see each SDK's README for details).

| SDK | Standard gRPC | Arrow Flight |
| --- | --- | --- |
| Rust | Available | Available since 2.8.0 |
| Python | Available | Available since 1.8.0 |
| Go (cgo) | Available | Available since 1.6.0 |
| Pure Go | Available | Not available |
| TypeScript | Available | Available since 1.3.0 |
| Java | Available | Available since 1.6.0 |
| C++ | Available | Available since 0.3.0 |
| .NET (C#) | Available | Not available |

### Acknowledgments and throughput

Ingestion is asynchronous in every SDK. An `ingest` call returns as soon as the record is queued — the SDK sends it and tracks its acknowledgment on a background task. To confirm that records were durably committed, call `flush()`; it returns once everything queued so far has been acknowledged.

The idiomatic flow is therefore **ingest in a loop, then `flush()`** — once at the end of a bounded batch, or periodically for a long-running stream. Where the SDK supports it, you can instead register an ack callback and be notified as records commit, without blocking at all.

Each `ingest` also returns the record's offset, and `wait_for_offset(offset)` blocks until that offset is acknowledged. That's useful when a particular record must be confirmed before you continue; because acknowledgments are ordered, waiting on the last offset of a run confirms the whole run. The one thing to avoid is waiting on every record inside a tight loop — that turns the asynchronous pipeline into a synchronous request/response and limits throughput to a single record per network round-trip.

See each SDK's README for exact method names and a runnable example.

## HTTP Proxy Support

All SDKs support HTTP CONNECT proxies via environment variables, following gRPC core conventions. The first variable found (in order) is used:

| Proxy                         | No-proxy                          |
| ----------------------------- | --------------------------------- |
| `grpc_proxy` / `GRPC_PROXY`   | `no_grpc_proxy` / `NO_GRPC_PROXY` |
| `https_proxy` / `HTTPS_PROXY` | `no_proxy` / `NO_PROXY`           |
| `http_proxy` / `HTTP_PROXY`   |                                   |

The `no_proxy` value is a comma-separated list of hostnames (suffix-matched) or `*` to bypass the proxy entirely.

```bash
export https_proxy=http://my-proxy:8080
export no_proxy=localhost,127.0.0.1
```

The SDK establishes a plaintext HTTP CONNECT tunnel through the proxy, then performs a TLS handshake end-to-end with the Databricks server. The proxy never sees decrypted traffic.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Each SDK also has its own contributing guide with language-specific setup instructions.

## License

This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for the full text.
