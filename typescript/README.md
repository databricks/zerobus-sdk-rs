# Databricks Zerobus Ingest SDK for TypeScript

The Databricks Zerobus Ingest SDK for TypeScript provides a high-performance client for ingesting data directly into Databricks Delta tables using the Zerobus streaming protocol. This SDK wraps the high-performance [Rust SDK](https://github.com/databricks/zerobus-sdk/tree/main/rust) using native bindings for optimal performance.

> **v2.0 introduces a new options-bag API.** `new ZerobusSdk({endpoint, ...})` and
> `sdk.createStream({table, auth, format, ...})` replace the v1.x positional
> constructor and `createStream` shape. See [v2.0 Migration](#v20-migration) below.
> Arrow Flight ingestion is now **Beta** — production-eligible but the API may
> still change before GA.

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Quick Start User Guide](#quick-start-user-guide)
  - [Installation](#installation)
  - [Choose Your Serialization Format](#choose-your-serialization-format)
  - [Option 1: Using JSON (Quick Start)](#option-1-using-json-quick-start)
  - [Option 2: Using Protocol Buffers (Recommended)](#option-2-using-protocol-buffers-recommended)
- [Usage Examples](#usage-examples)
- [Authentication](#authentication)
- [Configuration](#configuration)
- [Descriptor Utilities](#descriptor-utilities)
- [Error Handling](#error-handling)
- [API Reference](#api-reference)
- [Best Practices](#best-practices)
- [Platform Support](#platform-support)
- [Architecture](#architecture)
- [Community and Contributing](#community-and-contributing)
- [License](#license)

## Features

- **High-throughput ingestion**: Native Rust implementation under a thin TypeScript facade
- **Three serialization formats**: JSON, Protocol Buffers, and **Arrow Flight (Beta)** with optional LZ4 / ZSTD compression
- **Zero-copy Arrow path**: Arrow IPC bytes are forwarded straight to Flight (no parse/re-encode round trip) when compression is disabled
- **Type widening**: Accept high-level types (plain objects, protobuf messages) or low-level types (strings, buffers) — serialization is automatic
- **Automatic recovery**: Built-in retry and recovery for transient failures
- **Flexible auth**: OAuth 2.0 client credentials, a custom `getHeaders` callback, or `noAuth` for local / sidecar-proxy deployments
- **Pluggable TLS**: `secure` (system CA, default) or `none` (for local plaintext servers)
- **TypeScript first**: Options-bag API with discriminated unions for `auth` and `format`
- **Cross-platform**: Linux, macOS, Windows

## Requirements

### Runtime Requirements

- **Node.js**: >= 20 (Node 18 went EOL April 2025; v2.0 dropped support)
- **Databricks workspace** with Zerobus access enabled

### Build Requirements

- **Rust toolchain**: 1.70 or higher - [Install Rust](https://rustup.rs/)
- **Cargo**: Included with Rust

### Dependencies

These will be installed automatically:

```json
{
  "@napi-rs/cli": "^2.18.4",
  "napi-build": "^0.3.3"
}
```

## Quick Start User Guide

### Prerequisites

Before using the SDK, you need a Databricks workspace URL, a Delta table, and a service principal. See the [monorepo prerequisites](https://github.com/databricks/zerobus-sdk/blob/main/README.md#prerequisites) for detailed setup instructions.

### Installation

#### Prerequisites

Before installing the SDK, ensure you have the required tools:

**1. Node.js >= 20**

Check if Node.js is installed:
```bash
node --version
```

If not installed, download from [nodejs.org](https://nodejs.org/).

**2. Rust Toolchain (1.70+)**

The SDK requires Rust to compile the native addon. Install using `rustup` (the official Rust installer):

**On Linux and macOS:**
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Follow the prompts (typically just press Enter to accept defaults).

**On Windows:**

Download and run the installer from [rustup.rs](https://rustup.rs/), or use:
```powershell
# Using winget
winget install Rustlang.Rustup

# Or download from https://rustup.rs/
```

**Verify Installation:**
```bash
rustc --version
cargo --version
```

You should see version 1.70 or higher. If the commands aren't found, restart your terminal or add Rust to your PATH:
```bash
# Linux/macOS
source $HOME/.cargo/env

# Windows (PowerShell)
# Restart your terminal
```

**Additional Platform Requirements:**

- **Linux**: Build essentials
  ```bash
  # Ubuntu/Debian
  sudo apt-get install build-essential

  # CentOS/RHEL
  sudo yum groupinstall "Development Tools"
  ```

- **macOS**: Xcode Command Line Tools
  ```bash
  xcode-select --install
  ```

- **Windows**: Visual Studio Build Tools
  - Install [Visual Studio Build Tools](https://visualstudio.microsoft.com/downloads/#build-tools-for-visual-studio-2022)
  - During installation, select "Desktop development with C++"

#### Installation Steps

**macOS users**: prebuilt binaries are published for both Intel and Apple Silicon — `npm install` picks the right one automatically. No Rust toolchain or Xcode Command Line Tools are needed unless you're modifying the SDK from source.

1. Clone the repository:
   ```bash
   git clone https://github.com/databricks/zerobus-sdk.git
   cd zerobus-sdk/typescript
   ```

2. Install dependencies:
   ```bash
   npm install
   ```

3. Build the native addon:
   ```bash
   npm run build
   ```

   This will compile the Rust code into a native Node.js addon (`.node` file) for your platform.

4. Verify the build:
   ```bash
   # You should see a .node file
   ls -la *.node
   ```

5. The SDK is now ready to use! You can:
   - Use it directly in this directory for examples
   - Link it globally: `npm link`
   - Or copy it into your project's `node_modules`

**Troubleshooting:**

- **"rustc: command not found"**: Restart your terminal after installing Rust
- **Build fails on Windows**: Ensure Visual Studio Build Tools are installed with C++ support
- **Build fails on Linux**: Install build-essential or equivalent package
- **Permission errors**: Don't use `sudo` with npm/cargo commands

### Choose Your Serialization Format

The SDK supports two gRPC serialization formats — both are explicit choices in v2 (there is no implicit default; you must pass `format` on `createStream`):

- **Protocol Buffers** — recommended for production. Strongly-typed schemas, efficient binary encoding, best performance.
- **JSON** — simpler getting-started; no schema compilation needed. Good for quick prototyping or when schema flexibility matters more than wire efficiency.

> **Note:** Pick the format via `format: { type: 'json' }` or `format: { type: 'proto', descriptor }` on `createStream(...)`. Use `createArrowStream(...)` for Arrow Flight (Beta).

### Option 1: Using JSON (Quick Start)

JSON mode is the simplest way to get started — no schema compilation needed.

```typescript
import { ZerobusSdk } from '@databricks/zerobus-ingest-sdk';

const sdk = new ZerobusSdk({
    // For AWS:
    endpoint: 'https://<workspace-id>.zerobus.<region>.cloud.databricks.com',
    unityCatalogUrl: 'https://<workspace-name>.cloud.databricks.com',
    // For Azure:
    // endpoint: 'https://<workspace-id>.zerobus.<region>.azuredatabricks.net',
    // unityCatalogUrl: 'https://<workspace-name>.azuredatabricks.net',
    applicationName: 'my-service/1.0', // optional, appended to user-agent
});

const stream = await sdk.createStream({
    table: 'main.default.air_quality',
    auth: {
        type: 'oauth',
        clientId: process.env.DATABRICKS_CLIENT_ID!,
        clientSecret: process.env.DATABRICKS_CLIENT_SECRET!,
    },
    format: { type: 'json' },
    maxInflightRequests: 1000,
    recovery: true,
});

try {
    let lastOffset: bigint = 0n;
    for (let i = 0; i < 100; i++) {
        lastOffset = await stream.ingestRecord({
            device_name: `sensor-${i % 10}`,
            temp: 20 + (i % 15),
            humidity: 50 + (i % 40),
        });
    }
    await stream.waitForOffset(lastOffset);
    console.log('Successfully ingested 100 records!');
} finally {
    await stream.close();
}
```

### Option 2: Using Protocol Buffers (Recommended)

Protocol Buffers provides efficient binary encoding with schema validation, and is the recommended format for production use. This section covers the complete setup process.

#### Prerequisites

Before starting, ensure you have:

1. **Protocol Buffer Compiler (`protoc`)** - Required for generating descriptor files
2. **protobufjs** and **protobufjs-cli** - Already included in package.json devDependencies

#### Step 1: Install Protocol Buffer Compiler

**Linux:**

```bash
# Ubuntu/Debian
sudo apt-get update && sudo apt-get install -y protobuf-compiler

# CentOS/RHEL
sudo yum install -y protobuf-compiler

# Alpine
apk add protobuf
```

**macOS:**

```bash
brew install protobuf
```

**Windows:**

```powershell
# Using Chocolatey
choco install protoc

# Or download from: https://github.com/protocolbuffers/protobuf/releases
```

**Verify Installation:**

```bash
protoc --version
# Should show: libprotoc 3.x.x or higher
```

#### Step 2: Define Your Protocol Buffer Schema

The SDK includes an example schema at `schemas/air_quality.proto`:

```protobuf
syntax = "proto2";

package examples;

// Example message representing air quality sensor data
message AirQuality {
    optional string device_name = 1;
    optional int32 temp = 2;
    optional int64 humidity = 3;
}
```

#### Step 3: Generate TypeScript Code

Generate TypeScript code from your proto schema:

```bash
npm run build:proto
```

This runs:
```bash
pbjs -t static-module -w commonjs -o examples/generated/air_quality.js schemas/air_quality.proto
pbts -o examples/generated/air_quality.d.ts examples/generated/air_quality.js
```

**Output:**
- `examples/generated/air_quality.js` - JavaScript protobuf code
- `examples/generated/air_quality.d.ts` - TypeScript type definitions

#### Step 4: Generate Descriptor File for Databricks

Databricks requires descriptor metadata about your protobuf schema.

**Generate Binary Descriptor:**

```bash
protoc --descriptor_set_out=schemas/air_quality_descriptor.pb \
       --include_imports \
       schemas/air_quality.proto
```

**Important flags:**
- `--descriptor_set_out` - Output path for the binary descriptor
- `--include_imports` - Include all imported proto files (required)

That's it! The SDK will automatically extract the message descriptor from this file.

#### Step 5: Use in Your Code

```typescript
import { ZerobusSdk } from '@databricks/zerobus-ingest-sdk';
import { loadDescriptorProto } from '@databricks/zerobus-ingest-sdk/utils/descriptor';
import * as airQuality from './examples/generated/air_quality';

const sdk = new ZerobusSdk({
    endpoint: 'https://<workspace-id>.zerobus.<region>.cloud.databricks.com',
    unityCatalogUrl: 'https://<workspace-name>.cloud.databricks.com',
});

// Extract the AirQuality message's DescriptorProto from the .pb FileDescriptorSet.
const descriptor = loadDescriptorProto({
    descriptorPath: 'schemas/air_quality_descriptor.pb',
    protoFileName: 'air_quality.proto',
    messageName: 'AirQuality',
});

const stream = await sdk.createStream({
    table: 'main.default.air_quality',
    auth: {
        type: 'oauth',
        clientId: process.env.DATABRICKS_CLIENT_ID!,
        clientSecret: process.env.DATABRICKS_CLIENT_SECRET!,
    },
    format: { type: 'proto', descriptor },
    maxInflightRequests: 1000,
    recovery: true,
});

try {
    const AirQuality = airQuality.examples.AirQuality;
    let lastOffset: bigint = 0n;
    for (let i = 0; i < 100; i++) {
        const record = AirQuality.create({
            deviceName: `sensor-${i}`,
            temp: 20 + i,
            humidity: 50 + i,
        });
        lastOffset = await stream.ingestRecord(record); // queue, returns offset
    }
    await stream.waitForOffset(lastOffset);
    console.log('Successfully ingested 100 records!');
} finally {
    await stream.close();
}
```

#### Type Mapping: Delta ↔ Protocol Buffers

When creating your proto schema, use these type mappings:

| Delta Type | Proto2 Type | Notes |
|-----------|-------------|-------|
| STRING, VARCHAR | string | |
| INT, SMALLINT, SHORT | int32 | |
| BIGINT, LONG | int64 | |
| FLOAT | float | |
| DOUBLE | double | |
| BOOLEAN | bool | |
| BINARY | bytes | |
| DATE | int32 | Days since epoch |
| TIMESTAMP | int64 | Microseconds since epoch |
| ARRAY\<type\> | repeated type | Use repeated field |
| MAP\<key, value\> | map\<key, value\> | Use map field |
| STRUCT\<fields\> | message | Define nested message |

**Example: Complex Schema**

```protobuf
syntax = "proto2";

package examples;

message ComplexRecord {
    optional string id = 1;
    optional int64 timestamp = 2;
    repeated string tags = 3;                    // ARRAY<STRING>
    map<string, int32> metrics = 4;              // MAP<STRING, INT>
    optional NestedData nested = 5;              // STRUCT
}

message NestedData {
    optional string field1 = 1;
    optional double field2 = 2;
}
```

#### Using Your Own Schema

1. **Create your proto file:**
   ```bash
   cat > schemas/my_schema.proto << 'EOF'
   syntax = "proto2";

   package my_schema;

   message MyMessage {
       optional string field1 = 1;
       optional int32 field2 = 2;
   }
   EOF
   ```

2. **Add build script to package.json:**
   ```json
   {
     "scripts": {
       "build:proto:myschema": "pbjs -t static-module -w commonjs -o examples/generated/my_schema.js schemas/my_schema.proto && pbts -o examples/generated/my_schema.d.ts examples/generated/my_schema.js"
     }
   }
   ```

3. **Generate code and descriptor:**
   ```bash
   npm run build:proto:myschema
   protoc --descriptor_set_out=schemas/my_schema_descriptor.pb --include_imports schemas/my_schema.proto
   ```

4. **Load descriptor in your code:**
   ```typescript
   import { loadDescriptorProto } from '@databricks/zerobus-ingest-sdk/utils/descriptor';
   const descriptorBase64 = loadDescriptorProto({
       descriptorPath: 'schemas/my_schema_descriptor.pb',
       protoFileName: 'my_schema.proto',
       messageName: 'MyMessage'
   });
   ```

#### Troubleshooting Protocol Buffers

**"protoc: command not found"**
- Install `protoc` (see Step 1 above)

**"Cannot find module './generated/air_quality'"**
- Run `npm run build:proto` to generate TypeScript code

**"Descriptor file not found"**
- Generate the descriptor file using the commands in Step 4

**"Invalid descriptor"**
- Ensure you used `--include_imports` flag when generating the descriptor
- Verify the `.pb` file was created: `ls -lh schemas/*.pb`
- Check that `protoFileName` and `messageName` match your proto file
- Make sure you're using `loadDescriptorProto()` from the utils

**Build fails on proto generation**
- Ensure protobufjs is installed: `npm install --save-dev protobufjs protobufjs-cli`

#### Quick Reference

Complete setup from scratch:
```bash
# Install dependencies and build SDK
npm install
npm run build

# Setup Protocol Buffers
npm run build:proto
protoc --descriptor_set_out=schemas/air_quality_descriptor.pb --include_imports schemas/air_quality.proto

# Run example
npm run example:proto:single
```

#### Why Two Steps (TypeScript + Descriptor)?

1. **TypeScript Code Generation** (`npm run build:proto`):
   - Creates JavaScript/TypeScript code for your application
   - Provides type-safe message creation and encoding
   - Used in your application code

2. **Descriptor File Generation** (`protoc --descriptor_set_out`):
   - Creates metadata about your schema for Databricks
   - Required by Zerobus service for schema validation
   - Uploaded as base64 string when creating a stream

Both are necessary for Protocol Buffers ingestion!

## Usage Examples

See the `examples/` directory for complete, runnable examples. See [examples/README.md](examples/README.md) for detailed instructions.

### Running Examples

```bash
# Set environment variables
export ZEROBUS_ENDPOINT="https://<workspace-id>.zerobus.<region>.cloud.databricks.com"
export DATABRICKS_WORKSPACE_URL="https://<workspace-name>.cloud.databricks.com"
export DATABRICKS_CLIENT_ID="your-client-id"
export DATABRICKS_CLIENT_SECRET="your-client-secret"
export ZEROBUS_TABLE_NAME="main.default.air_quality"

# JSON
npm run example:json:single
npm run example:json:batch

# Protocol Buffers (compile the schema once)
npm run build:proto
npm run example:proto:single
npm run example:proto:batch

# Arrow Flight (Beta) — included in the default npm build
npm run example:arrow:single
npm run example:arrow:batch
```

Run against a local plaintext test server by adding `ZEROBUS_TLS=none
ZEROBUS_NO_AUTH=1` — see `examples/_config.ts` and
[`examples/README.md`](examples/README.md).

### Batch Ingestion

`ingestRecords(array)` queues many records atomically and resolves to the
batch's offset ID. Use it for higher throughput than per-record calls.

```typescript
// JSON: plain objects (auto-stringify'd) or pre-serialized strings
const offset = await stream.ingestRecords([
    { device_name: 'sensor-0', temp: 20, humidity: 50 },
    { device_name: 'sensor-1', temp: 21, humidity: 51 },
]);

// Protobuf: protobufjs message instances or Buffers of pre-encoded bytes
const offset = await stream.ingestRecords([
    AirQuality.create({ deviceName: 'sensor-0', temp: 20, humidity: 50n }),
    AirQuality.create({ deviceName: 'sensor-1', temp: 21, humidity: 51n }),
]);

if (offset !== null) {
    await stream.waitForOffset(offset);
}
```

- Mixed types (objects + strings, or messages + Buffers) are supported in the same batch.
- Empty batches resolve to `null` — no error, no offset.
- Use `sdk.recreateStream(stream)` for recovery — unacked records are replayed automatically.

## Arrow Flight (Beta)

> **Beta**: API is stabilising but may still change before reaching GA.

Arrow Flight is included in the default `npm install` — no rebuild needed.
Each batch is supplied as an Arrow IPC stream (`tableToIPC(table, 'stream')`
from `apache-arrow`).

When `compression: 'none'` (the default) the SDK forwards the IPC bytes
directly to the Rust SDK's zero-copy path — no parse / re-encode round trip.
Setting `'lz4_frame'` or `'zstd'` trades CPU for fewer bytes on the wire and
forces the SDK onto the parsed-RecordBatch path so it can apply the codec.

```typescript
import { ZerobusSdk, ArrowDataType } from '@databricks/zerobus-ingest-sdk';
import { Field, Int32, Int64, RecordBatch, Schema, Struct, Table, Utf8,
         makeData, makeVector, tableToIPC, vectorFromArray } from 'apache-arrow';

const sdk = new ZerobusSdk({ endpoint, unityCatalogUrl });

const stream = await sdk.createArrowStream({
    table: 'catalog.schema.air_quality',
    auth: { type: 'oauth', clientId, clientSecret },
    schema: [
        { name: 'device_name', dataType: ArrowDataType.Utf8 },
        { name: 'temp',        dataType: ArrowDataType.Int32 },
        { name: 'humidity',    dataType: ArrowDataType.Int64 },
    ],
    compression: 'zstd', // or 'lz4_frame' / 'none'
    maxInflightBatches: 100,
});

// Build an Arrow RecordBatch with an explicit schema so nullability matches.
const arrowSchema = new Schema([
    new Field('device_name', new Utf8(), true),
    new Field('temp',        new Int32(), true),
    new Field('humidity',    new Int64(), true),
]);
const dev = vectorFromArray(['s1', 's2'], new Utf8());
const t   = makeVector(Int32Array.from([21, 19]));
const h   = makeVector(BigInt64Array.from([55n, 60n]));
const data = makeData({ type: new Struct(arrowSchema.fields), length: 2,
                        children: [dev.data[0], t.data[0], h.data[0]] });
const ipc = Buffer.from(tableToIPC(new Table(new RecordBatch(arrowSchema, data)), 'stream'));

const offset = await stream.ingestBatch(ipc);
await stream.waitForOffset(offset);
await stream.close();
```

See `examples/arrow/single.ts` and `examples/arrow/batch.ts` for full runnable
examples.

## Authentication

The `auth` field on `createStream` / `createArrowStream` is a discriminated
union with three arms:

### OAuth 2.0 Client Credentials (recommended)

```typescript
auth: { type: 'oauth', clientId, clientSecret }
```

The SDK fetches access tokens from Unity Catalog and attaches the
`authorization` and `x-databricks-zerobus-table-name` headers automatically.
The SDK identifies itself via the `user-agent` HTTP header
(`zerobus-sdk-ts/<version>`); pass `applicationName` to the `ZerobusSdk`
constructor to append your app's identifier.

### Custom headers provider

For PATs, M2M tokens, or other auth methods, pass a callback that produces
headers on demand:

```typescript
import { bearerTokenProvider } from '@databricks/zerobus-ingest-sdk';

auth: {
    type: 'headersProvider',
    // The callback must return a Promise. It is invoked once per stream
    // (the result is cached); call `sdk.createStream(...)` again for a fresh token.
    getHeaders: bearerTokenProvider('catalog.schema.table', () => fetchMyToken()),
}

// Or the raw form:
auth: {
    type: 'headersProvider',
    getHeaders: async () => [
        ['authorization', `Bearer ${myToken}`],
        ['x-databricks-zerobus-table-name', 'catalog.schema.table'],
    ],
}
```

The callback **must** return at minimum the `authorization` and
`x-databricks-zerobus-table-name` headers.

### `noAuth`

```typescript
auth: { type: 'noAuth' }
```

For local development against a server that doesn't enforce auth, or
sidecar-proxy deployments where authentication is injected upstream. The SDK
still attaches placeholder canonical headers because the wire protocol
requires them.

## Configuration

### Stream Configuration Options

These optional fields go on the same options object you pass to
`createStream` / `createArrowStream`:

| Option | Default | Applies to | Description |
|--------|---------|------------|-------------|
| `recovery` | `true` | both | Auto-retry transient failures |
| `recoveryTimeoutMs` | `15000` | both | Per-attempt recovery timeout |
| `recoveryBackoffMs` | `2000` | both | Delay between retries |
| `recoveryRetries` | `4` | both | Max retry attempts |
| `flushTimeoutMs` | `300000` | both | `flush()` timeout |
| `serverLackOfAckTimeoutMs` | `60000` | both | Server ack timeout |
| `streamPausedMaxWaitTimeMs` | `undefined` | both | Graceful pause wait cap |
| `maxInflightRequests` | `10000` | gRPC | Unacked records in-flight |
| `maxInflightBatches` | `1000` | Arrow | Unacked batches in-flight |
| `connectionTimeoutMs` | `30000` | Arrow | Initial connection timeout |
| `compression` | `'none'` | Arrow | `'none'` (zero-copy) \| `'lz4_frame'` \| `'zstd'` |

### Example

```typescript
const stream = await sdk.createStream({
    table: 'main.default.air_quality',
    auth: { type: 'oauth', clientId, clientSecret },
    format: { type: 'json' },
    maxInflightRequests: 10_000,
    recovery: true,
    recoveryTimeoutMs: 20_000,
    recoveryRetries: 4,
});
```

## Descriptor Utilities

The SDK provides a helper function to extract Protocol Buffer descriptors from FileDescriptorSets.

### loadDescriptorProto()

Extracts a specific message descriptor from a FileDescriptorSet:

```typescript
import { loadDescriptorProto } from '@databricks/zerobus-ingest-sdk/utils/descriptor';

const descriptorBase64 = loadDescriptorProto({
    descriptorPath: 'schemas/my_schema_descriptor.pb',
    protoFileName: 'my_schema.proto',  // Name of your .proto file
    messageName: 'MyMessage'            // The specific message to use
});
```

**Parameters:**
- `descriptorPath`: Path to the `.pb` file generated by `protoc --descriptor_set_out`
- `protoFileName`: Name of the proto file (e.g., `"air_quality.proto"`)
- `messageName`: Name of the message type to extract (e.g., `"AirQuality"`)

**Why use this utility?**
- Extracts the specific message descriptor you need
- No manual base64 conversion required
- Clear error messages if the file or message isn't found
- Flexible for complex schemas with multiple messages or imports

**Example with multiple messages:**
```typescript
// Your proto file has: Order, OrderItem, Customer
// You want to ingest Orders:
const descriptorBase64 = loadDescriptorProto({
    descriptorPath: 'schemas/orders_descriptor.pb',
    protoFileName: 'orders.proto',
    messageName: 'Order'  // Explicitly choose Order
});
```

## Error Handling

`recovery: true` (the default) auto-retries transient failures inside the
stream. For terminal failures, call `sdk.recreateStream(...)` (or
`sdk.recreateArrowStream(...)`) on the failed stream **before closing it** —
the SDK reads the unacked records from the old handle, opens a fresh stream
with the same configuration, and replays them. Close the old stream
afterwards.

```typescript
try {
    const offset = await stream.ingestRecord(record);
    await stream.waitForOffset(offset);
} catch (err) {
    const fresh = await sdk.recreateStream(stream); // unacked records replayed
    await stream.close();                            // release the old handle
    stream = fresh;
}
```

Inspect unacked work yourself with `getUnackedRecords()` / `getUnackedBatches()`
on the failed (but **not yet closed**) stream — useful for diagnostics, not
required for recovery. Once `close()` returns, the Rust handle is gone and
these methods error.

## API Reference

### `ZerobusSdk`

```typescript
new ZerobusSdk(options: SdkOptions): ZerobusSdk

interface SdkOptions {
    endpoint: string;                    // Zerobus endpoint URL
    unityCatalogUrl?: string;            // Required for OAuth, optional for custom headers
    tls?: 'secure' | 'none';             // Default 'secure' (system CAs); 'none' for plaintext local servers
    applicationName?: string;            // Appended to user-agent: '<sdk>/<ver> <applicationName>'
    sdkIdentifier?: string;              // Overrides the default 'zerobus-sdk-ts/<ver>' prefix
}

// Open a JSON or protobuf stream.
sdk.createStream(options: CreateStreamOptions): Promise<ZerobusStream>

// Open an Arrow Flight stream (Beta).
sdk.createArrowStream(options: CreateArrowStreamOptions): Promise<ZerobusArrowStream>

// Replay unacked work on a fresh stream with the same config.
sdk.recreateStream(stream: ZerobusStream): Promise<ZerobusStream>
sdk.recreateArrowStream(stream: ZerobusArrowStream): Promise<ZerobusArrowStream>
```

### `CreateStreamOptions`

```typescript
interface CreateStreamOptions {
    table: string;            // Fully-qualified Unity Catalog table name
    auth: Auth;               // See Authentication
    format: GrpcFormat;       // { type: 'json' } | { type: 'proto', descriptor: string }
    recovery?: boolean;                  // Default true
    recoveryTimeoutMs?: number;          // Default 15000
    recoveryBackoffMs?: number;          // Default 2000
    recoveryRetries?: number;            // Default 4
    serverLackOfAckTimeoutMs?: number;   // Default 60000
    flushTimeoutMs?: number;             // Default 300000
    maxInflightRequests?: number;        // Default 10000
    streamPausedMaxWaitTimeMs?: number;
}

type GrpcFormat =
    | { type: 'json' }
    | { type: 'proto'; descriptor: string };  // base64-encoded DescriptorProto
```

### `CreateArrowStreamOptions`

```typescript
interface CreateArrowStreamOptions {
    table: string;
    auth: Auth;
    schema: ArrowField[];                // Arrow schema; nullable defaults to true
    compression?: 'none' | 'lz4_frame' | 'zstd';   // Default 'none' (zero-copy path)
    recovery?: boolean;
    recoveryTimeoutMs?: number;
    recoveryBackoffMs?: number;
    recoveryRetries?: number;
    serverLackOfAckTimeoutMs?: number;
    flushTimeoutMs?: number;
    maxInflightBatches?: number;         // Default 1000
    connectionTimeoutMs?: number;        // Default 30000
    streamPausedMaxWaitTimeMs?: number;
}

interface ArrowField {
    name: string;
    dataType: ArrowDataType;             // Boolean | Int8…64 | UInt8…64 | Float32/64 | Utf8 | LargeUtf8 | Binary | LargeBinary | Date32/64 | TimestampMicros/Nanos
    nullable?: boolean;                  // Default true
}
```

### `Auth`

Discriminated union with three arms:

```typescript
type Auth =
    | { type: 'oauth';            clientId: string; clientSecret: string }
    | { type: 'headersProvider';  getHeaders: () => Promise<Array<[string, string]>> }
    | { type: 'noAuth' };
```

`'oauth'` runs the OAuth 2.0 Client Credentials flow against
`unityCatalogUrl`. `'headersProvider'` calls your callback once at stream
open — it must return at least `authorization` and
`x-databricks-zerobus-table-name`. `'noAuth'` is for local-only servers /
sidecar-proxy deployments.

### `ZerobusStream`

```typescript
class ZerobusStream {
    ingestRecord(record: unknown): Promise<bigint>;
    // record may be a Buffer (proto bytes), string (JSON), protobufjs message
    // (auto-encoded), or plain object (auto-stringify'd).

    ingestRecords(records: unknown[]): Promise<bigint | null>;
    // Atomic batch. `null` for an empty array.

    waitForOffset(offset: bigint): Promise<void>;
    flush(): Promise<void>;
    close(): Promise<void>;

    // Inspect unacked work on a failed-but-not-yet-closed stream. Prefer
    // `sdk.recreateStream(...)` for actual recovery; this is for diagnostics.
    // Errors after `close()` because the underlying handle is gone.
    getUnackedRecords(): Promise<Buffer[]>;
    getUnackedBatches(): Promise<Buffer[][]>;
}
```

Both `ingestRecord` and `ingestRecords` resolve **at queue time** (before
server ack). Use `waitForOffset(offset)` to wait for acknowledgment.

### `ZerobusArrowStream` (Beta)

```typescript
class ZerobusArrowStream {
    ingestBatch(ipcBuffer: Buffer): Promise<bigint>;
    // `ipcBuffer` is the output of `tableToIPC(table, 'stream')` from
    // apache-arrow. When `compression: 'none'` the buffer is forwarded
    // zero-copy; otherwise the SDK parses + re-encodes with the codec.

    waitForOffset(offset: bigint): Promise<void>;
    flush(): Promise<void>;
    close(): Promise<void>;

    get isClosed(): boolean;
    get tableName(): string;

    getUnackedBatches(): Promise<Buffer[]>;  // IPC-encoded
}
```

### Helpers

```typescript
// Build a headers-provider callback from a (refreshable) bearer token.
function bearerTokenProvider(
    table: string,
    getToken: () => string | Promise<string>,
): () => Promise<Array<[string, string]>>;

// Plain `as const` object (not an `enum`) — usable as a value from JS.
const ArrowDataType: { Boolean, Int8, …, TimestampNanos };
// Record type for gRPC streams is selected via `format` (`{ type: 'json' }` or
// `{ type: 'proto', descriptor }`) on `createStream`; no separate enum to import.
```

## Best Practices

1. **Reuse SDK instances**: one `ZerobusSdk` per process.
2. **Close in `finally`**: always close streams to flush pending records.
3. **Batch for throughput**: `ingestRecords` amortises the JS↔Rust crossing; aim for 100–1000 records per batch.
4. **Wait once, not per record**: capture the last offset and call `waitForOffset` once at the end.
5. **Don't catch transient errors**: the SDK retries them internally when `recovery: true` (default). Reserve catch / `recreateStream` for terminal failures.
6. **Pick Arrow for columnar pipelines**: when you already have Arrow data, `createArrowStream` skips per-record encoding entirely.
7. **Use `tls: 'none'` only for local / sidecar-proxy setups** — production traffic should always go through `'secure'`.

## Platform Support

The SDK supports all platforms where Node.js and Rust are available.

### Pre-built Binaries

Pre-built native binaries are published for:

- **Linux**: x64 (`x86_64-unknown-linux-gnu`), ARM64 (`aarch64-unknown-linux-gnu`)
- **macOS**: x64 / Intel (`x86_64-apple-darwin`), ARM64 / Apple Silicon (`aarch64-apple-darwin`)
- **Windows**: x64 (`x86_64-pc-windows-msvc`)

`npm install` resolves the correct platform package automatically via npm's
`optionalDependencies` mechanism — only the binary matching your OS+arch is
downloaded.

### Build from Source

You only need this if you're modifying the SDK or running on an unsupported
platform. Requires:

- **Rust toolchain** (1.70+): `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
- **A C/C++ toolchain**: Xcode Command Line Tools on macOS (`xcode-select --install`); `build-essential` on Linux; MSVC on Windows.

Then from `typescript/`:

```bash
npm install
npm run build              # release
npm run build:debug        # debug (faster iteration)
```

## Architecture

This SDK wraps the high-performance [Rust Zerobus SDK](https://github.com/databricks/zerobus-sdk/tree/main/rust) using [NAPI-RS](https://napi.rs):

```
┌─────────────────────────────┐
│   TypeScript Application    │
└─────────────┬───────────────┘
              │ (NAPI-RS bindings)
┌─────────────▼───────────────┐
│   Rust Zerobus SDK          │
│   - gRPC communication      │
│   - OAuth authentication    │
│   - Stream management       │
└─────────────┬───────────────┘
              │ (gRPC/TLS)
┌─────────────▼───────────────┐
│   Databricks Zerobus Service│
└─────────────────────────────┘
```

**Benefits:**
- **Native performance** - Rust implementation for high-throughput ingestion
- **Native async/await support** - Rust futures become JavaScript Promises
- **Automatic memory management** - No manual cleanup required
- **Type safety** - Compile-time checks on both sides

## v2.0 Migration

v2.0 is a breaking release. The biggest shifts:

### `ZerobusSdk` constructor — options bag

```typescript
// v1.x
const sdk = new ZerobusSdk(endpoint, unityCatalogUrl);

// v2.0
const sdk = new ZerobusSdk({
    endpoint,
    unityCatalogUrl,                    // optional with custom headers provider
    tls: 'secure' | 'none',             // 'secure' is the default
    applicationName: 'my-service/1.0',  // optional
});
```

### `createStream` — options bag with discriminated unions

```typescript
// v1.x
const stream = await sdk.createStream(
    { tableName, descriptorProto },
    clientId, clientSecret,
    { recordType: RecordType.Json, ... },
    headersProvider,
);

// v2.0
const stream = await sdk.createStream({
    table: 'catalog.schema.table',
    auth:   { type: 'oauth', clientId, clientSecret },        // | 'headersProvider' | 'noAuth'
    format: { type: 'json' },                                  // | 'proto' (+ descriptor)
    recovery: true,
    maxInflightRequests: 1000,
});
```

### Arrow Flight has its own factory

```typescript
const arrow = await sdk.createArrowStream({
    table, auth,
    schema: [{ name: 'id', dataType: ArrowDataType.Int64 }, ...],
    compression: 'zstd', // 'none' (default) keeps the SDK on the zero-copy path
});
```

### Other changes

- **`ingestRecord` / `ingestRecords` now resolve at queue time** (not server ack). v1.x had a deprecated blocking variant; it was removed. Use `waitForOffset` to wait for acknowledgment.
- **`recordType` is inferred from `format`** — no longer specified separately.
- **SDK identity** is now sent in the `user-agent` HTTP header
  (`zerobus-sdk-ts/<version>`). The `x-zerobus-sdk` gRPC metadata header
  (used by v1.x via the Rust SDK's default headers provider) is no longer
  emitted. Override via `sdkIdentifier`; append via `applicationName`.
- **Compression is a string** (`'none' | 'lz4_frame' | 'zstd'`) on Arrow streams instead of the v1.x numeric `IpcCompressionType` enum.
- `RecordType` and `ArrowDataType` are now plain `as const` objects (not `const enum`) so they survive `--isolatedModules`.

## Community and Contributing

This is an open source project. We welcome contributions, feedback, and bug reports.

- **[Contributing Guide](https://github.com/databricks/zerobus-sdk/blob/main/typescript/CONTRIBUTING.md)**: TypeScript-specific development setup and workflow.
- **[General Contributing Guide](https://github.com/databricks/zerobus-sdk/blob/main/CONTRIBUTING.md)**: Pull request process, commit requirements, and policies.
- **[Changelog](https://github.com/databricks/zerobus-sdk/blob/main/typescript/CHANGELOG.md)**: See the history of changes in the SDK.
- **[Security Policy](https://github.com/databricks/zerobus-sdk/blob/main/SECURITY.md)**: Read about our security process and how to report vulnerabilities.
- **[Developer Certificate of Origin (DCO)](https://github.com/databricks/zerobus-sdk/blob/main/DCO)**: Understand the agreement for contributions.

## License

This SDK is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for the full text.
