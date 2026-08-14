# Databricks Zerobus Ingest SDK for TypeScript

The Databricks Zerobus Ingest SDK for TypeScript provides a high-performance client for ingesting data directly into Databricks Delta tables using the Zerobus streaming protocol. This SDK wraps the high-performance [Rust SDK](https://github.com/databricks/zerobus-sdk/tree/main/rust) using native bindings for optimal performance.

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Quick Start User Guide](#quick-start-user-guide)
  - [Installation](#installation)
  - [Choose Your Serialization Format](#choose-your-serialization-format)
  - [Option 1: Using JSON (Quick Start)](#option-1-using-json-quick-start)
  - [Option 2: Using Protocol Buffers (Default, Recommended)](#option-2-using-protocol-buffers-default-recommended)
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

- **High-throughput ingestion**: Optimized for high-volume data ingestion with native Rust implementation
- **Automatic recovery**: Built-in retry and recovery mechanisms for transient failures
- **Flexible configuration**: Customizable stream behavior and timeouts
- **Multiple serialization formats**: Support for JSON, Protocol Buffers, and Arrow Flight (Beta) with optional LZ4 / ZSTD compression
- **Type widening**: Accept high-level types (plain objects, protobuf messages) or low-level types (strings, buffers) - automatically handles serialization
- **Batch ingestion**: Ingest multiple records with a single acknowledgment for higher throughput
- **OAuth 2.0 authentication**: Secure authentication with client credentials
- **TypeScript support**: Full type definitions for excellent IDE support
- **Cross-platform**: Supports Linux, macOS, and Windows

## Requirements

### Runtime Requirements

- **Node.js**: >= 16
- **Databricks workspace** with Zerobus access enabled

### Source Build Requirements

- **Rust toolchain**: 1.70 or higher - [Install Rust](https://rustup.rs/)
- **Cargo**: Included with Rust
- Platform C/C++ build tools

You only need these source-build tools when npm cannot use a pre-built native
package for your platform, or when developing the SDK from this repository.

## Quick Start User Guide

### Prerequisites

Before using the SDK, you need a Databricks workspace URL, a Delta table, and a service principal. See the [monorepo prerequisites](https://github.com/databricks/zerobus-sdk/blob/main/README.md#prerequisites) for detailed setup instructions.

### Installation

```bash
npm install @databricks/zerobus-ingest-sdk
```

On supported platforms, npm installs the TypeScript package and the matching
pre-built native binary package automatically.

#### Local Development From Source

Clone and build from source only when modifying this SDK or when your platform
does not have a pre-built native binary:

```bash
git clone https://github.com/databricks/zerobus-sdk.git
cd zerobus-sdk/typescript
npm install
npm run build
```

**Troubleshooting:**

- **Unsupported platform or source build requested**: Install Rust 1.70+,
  Cargo, and your platform C/C++ build tools, clone this repository, and run
  `npm install` followed by `npm run build` from `zerobus-sdk/typescript`
- **"rustc: command not found"**: Restart your terminal after installing Rust
- **Build fails on Windows**: Ensure Visual Studio Build Tools are installed with C++ support
- **Build fails on Linux**: Install build-essential or equivalent package
- **Permission errors**: Don't use `sudo` with npm/cargo commands

### Choose Your Serialization Format

The SDK supports two serialization formats. **Protocol Buffers is the default** and recommended for production use:

- **Protocol Buffers (Default)** - Strongly-typed schemas, efficient binary encoding, better performance. This is the default format.
- **JSON** - Simple, no schema compilation needed. Good for getting started quickly or when schema flexibility is needed.

> **Note:** If you don't specify `recordType`, the SDK will use Protocol Buffers by default. To use JSON, explicitly set `recordType: RecordType.Json`.

### Acknowledgments and throughput

Ingestion is asynchronous. `ingestRecordOffset()` (and `ingestRecordsOffset()`) resolves as soon as the record is queued; the SDK sends it and tracks its acknowledgment in the background. To confirm records are durably committed, call `flush()` — it resolves once everything queued so far is acknowledged. The idiomatic flow is **ingest in a loop, then `flush()`** (once for a bounded batch, or periodically for a long-running stream). Each ingest also returns the record's offset, and `waitForOffset(offset)` resolves when that offset is acknowledged — handy when a specific record must be confirmed before continuing (acks are ordered, so waiting on the last offset confirms the whole run). Just avoid calling `waitForOffset()` after every record in a tight loop, since that limits throughput to one record per round-trip. The examples below follow this pattern.

### Option 1: Using JSON (Quick Start)

JSON mode is the simplest way to get started. You don't need to define or compile protobuf schemas, but you must explicitly specify `RecordType.Json`.

```typescript
import { ZerobusSdk, RecordType } from '@databricks/zerobus-ingest-sdk';

async function main(): Promise<void> {
// Configuration
// For AWS:
const zerobusEndpoint = 'https://<workspace-id>.zerobus.<region>.cloud.databricks.com';
const workspaceUrl = 'https://<workspace-name>.cloud.databricks.com';
// For Azure:
// const zerobusEndpoint = '<workspace-id>.zerobus.<region>.azuredatabricks.net';
// const workspaceUrl = 'https://<workspace-name>.azuredatabricks.net';

const tableName = 'main.default.air_quality';
const clientId = process.env.DATABRICKS_CLIENT_ID!;
const clientSecret = process.env.DATABRICKS_CLIENT_SECRET!;

// Initialize SDK
const sdk = new ZerobusSdk(zerobusEndpoint, workspaceUrl);

// Configure table properties (no descriptor needed for JSON)
const tableProperties = { tableName };

// Configure stream with JSON record type
const options = {
    recordType: RecordType.Json,  // JSON encoding
    maxInflightRequests: 1000,
    recovery: true
};

// Create stream
const stream = await sdk.createStream(
    tableProperties,
    clientId,
    clientSecret,
    options
);

try {
    // Send all records
    for (let i = 0; i < 100; i++) {
        const record = {
            device_name: `sensor-${i % 10}`,
            temp: 20 + (i % 15),
            humidity: 50 + (i % 40)
        };

        // Queue the record; do not wait for its acknowledgement here
        await stream.ingestRecordOffset(record);
    }

    // Wait for all records to be acknowledged
    await stream.flush();
    console.log('Successfully ingested 100 records!');
} finally {
    await stream.close();
}

}

main().catch((error) => {
  console.error('Fatal error:', error);
  process.exitCode = 1;
});
```

### Option 2: Using Protocol Buffers (Default, Recommended)

Protocol Buffers is the default serialization format and provides efficient binary encoding with schema validation. This is recommended for production use. This section covers the complete setup process.

#### Prerequisites

Before starting, ensure you have:

1. **Protocol Buffer Compiler (`protoc`)** - Required for generating descriptor files
2. **protobufjs** - Required at runtime by your generated Protocol Buffer code
3. **protobufjs-cli** - Required during development to generate JavaScript and type declarations

Install the JavaScript runtime and code-generation tools in your application:

```bash
npm install protobufjs
npm install --save-dev protobufjs-cli
```

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

Create `schemas/air_quality.proto` in your application with the following
example schema. Also create an `examples/generated` directory for the generated
JavaScript and type declarations:

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
npx pbjs -t static-module -w commonjs -o examples/generated/air_quality.js schemas/air_quality.proto
npx pbts -o examples/generated/air_quality.d.ts examples/generated/air_quality.js
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
import { ZerobusSdk, RecordType } from '@databricks/zerobus-ingest-sdk';
import * as airQuality from './examples/generated/air_quality';
import { loadDescriptorProto } from '@databricks/zerobus-ingest-sdk/utils/descriptor.js';

async function main(): Promise<void> {
// Configuration
const zerobusEndpoint = 'https://<workspace-id>.zerobus.<region>.cloud.databricks.com';
const workspaceUrl = 'https://<workspace-name>.cloud.databricks.com';
const tableName = 'main.default.air_quality';
const clientId = process.env.DATABRICKS_CLIENT_ID!;
const clientSecret = process.env.DATABRICKS_CLIENT_SECRET!;

// Load and extract the descriptor for your specific message
const descriptorBase64 = loadDescriptorProto({
    descriptorPath: 'schemas/air_quality_descriptor.pb',
    protoFileName: 'air_quality.proto',
    messageName: 'AirQuality'
});

// Initialize SDK
const sdk = new ZerobusSdk(zerobusEndpoint, workspaceUrl);

// Configure table properties with protobuf descriptor
const tableProperties = {
    tableName,
    descriptorProto: descriptorBase64  // Required for Protocol Buffers
};

// Configure stream with Protocol Buffers record type
const options = {
    recordType: RecordType.Proto,  // Protocol Buffers encoding
    maxInflightRequests: 1000,
    recovery: true
};

// Create stream
const stream = await sdk.createStream(tableProperties, clientId, clientSecret, options);

try {
    const AirQuality = airQuality.examples.AirQuality;

    // Send all records
    for (let i = 0; i < 100; i++) {
        const record = AirQuality.create({
            deviceName: `sensor-${i}`,
            temp: 20 + i,
            humidity: 50 + i
        });

        // Queue the record; do not wait for its acknowledgement here
        await stream.ingestRecordOffset(record);
    }

    // Wait for all records to be acknowledged
    await stream.flush();
    console.log('Successfully ingested 100 records!');
} finally {
    await stream.close();
}

}

main().catch((error) => {
  console.error('Fatal error:', error);
  process.exitCode = 1;
});
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

2. **Generate code and descriptor:**
   ```bash
   npx pbjs -t static-module -w commonjs -o examples/generated/my_schema.js schemas/my_schema.proto
   npx pbts -o examples/generated/my_schema.d.ts examples/generated/my_schema.js
   protoc --descriptor_set_out=schemas/my_schema_descriptor.pb --include_imports schemas/my_schema.proto
   ```

3. **Load descriptor in your code:**
   ```typescript
   import { loadDescriptorProto } from '@databricks/zerobus-ingest-sdk/utils/descriptor.js';
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
- Run the `npx pbjs` and `npx pbts` commands from Step 3

**"Descriptor file not found"**
- Generate the descriptor file using the commands in Step 4

**"Invalid descriptor"**
- Ensure you used `--include_imports` flag when generating the descriptor
- Verify the `.pb` file was created: `ls -lh schemas/*.pb`
- Check that `protoFileName` and `messageName` match your proto file
- Make sure you're using `loadDescriptorProto()` from the utils

**Build fails on proto generation**
- Ensure the runtime and CLI are installed: `npm install protobufjs` and
  `npm install --save-dev protobufjs-cli`

#### Quick Reference

After creating `schemas/air_quality.proto` and the `examples/generated`
directory as described above:
```bash
# Install the SDK and protobuf codegen tools
npm install @databricks/zerobus-ingest-sdk protobufjs
npm install --save-dev protobufjs-cli

# Generate protobuf code and descriptor
npx pbjs -t static-module -w commonjs -o examples/generated/air_quality.js schemas/air_quality.proto
npx pbts -o examples/generated/air_quality.d.ts examples/generated/air_quality.js
protoc --descriptor_set_out=schemas/air_quality_descriptor.pb --include_imports schemas/air_quality.proto
```

#### Why Two Steps (TypeScript + Descriptor)?

1. **TypeScript Code Generation** (`npx pbjs` and `npx pbts`):
   - Creates JavaScript/TypeScript code for your application
   - Provides type-safe message creation and encoding
   - Used in your application code

2. **Descriptor File Generation** (`protoc --descriptor_set_out`):
   - Creates metadata about your schema for Databricks
   - Required by Zerobus service for schema validation
   - Uploaded as base64 string when creating a stream

Both are necessary for Protocol Buffers ingestion!

## Usage Examples

The source repository contains complete, runnable examples in `examples/`.
Clone and build the repository using the [local development](#local-development-from-source)
instructions, then see [examples/README.md](examples/README.md) for details.

### Running Examples

Run these commands from the cloned repository's `typescript` directory:

```bash
# Set environment variables
export ZEROBUS_SERVER_ENDPOINT="https://<workspace-id>.zerobus.<region>.cloud.databricks.com"
export DATABRICKS_WORKSPACE_URL="https://<workspace-name>.cloud.databricks.com"
export DATABRICKS_CLIENT_ID="your-client-id"
export DATABRICKS_CLIENT_SECRET="your-client-secret"
export ZEROBUS_TABLE_NAME="main.default.air_quality"

# Run JSON examples
npm run example:json:single
npm run example:json:batch

# For Protocol Buffers, generate TypeScript code and descriptor
npm run build:proto
protoc --descriptor_set_out=schemas/air_quality_descriptor.pb --include_imports schemas/air_quality.proto

# Run Protocol Buffers examples
npm run example:proto:single
npm run example:proto:batch
```

### Batch Ingestion

For higher throughput, use batch ingestion to send multiple records with a single acknowledgment:

#### Protocol Buffers

```typescript
const records = Array.from({ length: 1000 }, (_, i) =>
  AirQuality.create({ deviceName: `sensor-${i}`, temp: 20 + i, humidity: 50 + i })
);

// Protobuf Type 1: Message objects (high-level) - SDK auto-serializes
const offsetId = await stream.ingestRecordsOffset(records);

// Protobuf Type 2: Buffers (low-level) - pre-serialized bytes
// const buffers = records.map(r => Buffer.from(AirQuality.encode(r).finish()));
// const offsetId = await stream.ingestRecordsOffset(buffers);

if (offsetId !== null) {
  await stream.waitForOffset(offsetId);
  console.log(`Batch acknowledged at offset ${offsetId}`);
}
```

#### JSON

```typescript
const records = Array.from({ length: 1000 }, (_, i) => ({
  device_name: `sensor-${i}`,
  temp: 20 + i,
  humidity: 50 + i
}));

// JSON Type 1: objects (high-level) - SDK auto-stringifies
const offsetId = await stream.ingestRecordsOffset(records);

// JSON Type 2: strings (low-level) - pre-serialized JSON
// const jsonRecords = records.map(r => JSON.stringify(r));
// const offsetId = await stream.ingestRecordsOffset(jsonRecords);

if (offsetId !== null) {
  await stream.waitForOffset(offsetId);
}
```

**Type Widening Support:**
- JSON mode: Accept `object[]` (auto-stringify) or `string[]` (pre-stringified)
- Proto mode: Accept protobuf messages with `.encode()` method (auto-serialize) or `Buffer[]` (pre-serialized)
- Mixed types are supported in the same batch

**Best Practices**:
- Batch size: 100-1,000 records for optimal throughput/latency balance
- Empty batches return `null` (no error, no offset)
- Use `recreateStream()` for recovery - it automatically handles unacknowledged batches

**Examples:**
See `examples/json/batch.ts` and `examples/proto/batch.ts` for batch ingestion examples.

## Authentication

The SDK uses OAuth 2.0 Client Credentials for authentication:

```typescript
import { ZerobusSdk } from '@databricks/zerobus-ingest-sdk';

const sdk = new ZerobusSdk(zerobusEndpoint, workspaceUrl);

// Create stream with OAuth authentication
const stream = await sdk.createStream(
    tableProperties,
    clientId,
    clientSecret,
    options
);
```

The SDK automatically fetches access tokens and includes these headers:
- `"authorization": "Bearer <oauth_token>"` - Obtained via OAuth 2.0 Client Credentials flow
- `"x-databricks-zerobus-table-name": "<table_name>"` - The fully qualified table name

### Custom Authentication

Beyond OAuth, you can use custom headers for Personal Access Tokens (PAT) or other auth methods:

```typescript
const stream = await sdk.createStream(
  tableProperties,
  '', // client_id (ignored when headers_provider is provided)
  '', // client_secret (ignored when headers_provider is provided)
  options,
  {
    getHeadersCallback: () => [
      ["authorization", `Bearer ${myToken}`],
      ["x-databricks-zerobus-table-name", tableName]
    ]
  }
);
```

**Required headers:**
- `authorization` - Bearer token or other auth header
- `x-databricks-zerobus-table-name` - The fully qualified table name

**Note:** The SDK automatically adds the `user-agent` header if not provided.

## Configuration

### Stream Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `recordType` | `RecordType.Proto` | Serialization format: `RecordType.Json` or `RecordType.Proto` |
| `maxInflightRequests` | 1,000,000 | Maximum number of unacknowledged requests |
| `recovery` | true | Enable automatic stream recovery |
| `recoveryTimeoutMs` | 15,000 | Timeout for recovery operations (ms) |
| `recoveryBackoffMs` | 2,000 | Delay between recovery attempts (ms) |
| `recoveryRetries` | 4 | Maximum number of recovery attempts |
| `flushTimeoutMs` | 300,000 | Timeout for flush operations (ms) |
| `serverLackOfAckTimeoutMs` | 60,000 | Server acknowledgment timeout (ms) |
| `streamPausedMaxWaitTimeMs` | undefined | Max wait time during graceful stream close (ms) |

### Example Configuration

```typescript
import { StreamConfigurationOptions, RecordType } from '@databricks/zerobus-ingest-sdk';

const options: StreamConfigurationOptions = {
    recordType: RecordType.Json,  // JSON encoding
    maxInflightRequests: 10000,
    recovery: true,
    recoveryTimeoutMs: 20000,
    recoveryBackoffMs: 2000,
    recoveryRetries: 4
};

const stream = await sdk.createStream(
    tableProperties,
    clientId,
    clientSecret,
    options
);
```

## Descriptor Utilities

The SDK provides a helper function to extract Protocol Buffer descriptors from FileDescriptorSets.
Use the `.js` subpath shown below for compatibility with CommonJS and native
Node.js ESM imports.

### loadDescriptorProto()

Extracts a specific message descriptor from a FileDescriptorSet:

```typescript
import { loadDescriptorProto } from '@databricks/zerobus-ingest-sdk/utils/descriptor.js';

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

The SDK includes automatic recovery for transient failures (enabled by default with `recovery: true`). `getUnackedBatches()` and `recreateStream()` succeed only after a terminal native-stream failure, which already closes the stream. An enqueue failure leaves the wrapper active, so those calls reject; rethrow the original error. Do not call `stream.close()` before `recreateStream()`, because close releases the native handle.

```typescript
let replacement;
try {
    const offset = await stream.ingestRecordOffset(record);
    await stream.flush();
    console.log(`Success: offset ${offset}`);
} catch (error) {
    console.error('Ingestion failed:', error);
    try {
        const unackedBatches = await stream.getUnackedBatches();
        console.log(`Batches to recover: ${unackedBatches.length}`);
        replacement = await sdk.recreateStream(stream);
        await replacement.flush();
    } catch (recoveryError) {
        console.error('Stream was not terminal or recovery failed:', recoveryError);
        throw error;
    } finally {
        if (replacement) {
            await replacement.close();
        }
    }
} finally {
    try {
        await stream.close();
    } catch (closeError) {
        console.error('Failed stream released:', closeError);
    }
}
```

**Best Practices:**
- **Rely on automatic recovery** (default): The SDK will automatically retry transient failures
- **Use `recreateStream()` for permanent failures**: Automatically recovers all unacknowledged batches
- **Use `getUnackedRecords()` for inspection only**: Primarily for debugging or understanding failed records
- Always close streams in a `finally` block to ensure proper cleanup

## API Reference

### ZerobusSdk

Main entry point for the SDK.

**Constructor:**

```typescript
new ZerobusSdk(zerobusEndpoint: string, unityCatalogUrl: string, options?: ZerobusSdkOptions)
```

**Parameters:**
- `zerobusEndpoint` (string) - The Zerobus gRPC endpoint (e.g., `https://<workspace-id>.zerobus.<region>.cloud.databricks.com` for AWS, or `https://<workspace-id>.zerobus.<region>.azuredatabricks.net` for Azure)
- `unityCatalogUrl` (string) - The Unity Catalog endpoint (your workspace URL)
- `options` (ZerobusSdkOptions, optional) - Additional SDK configuration:
  - `applicationName` (string, optional) - Application identifier appended to the HTTP `user-agent` header, conventionally `"<product>/<version>"` (e.g. `"my-app/1.0"`). The header becomes `zerobus-sdk-ts/<version> <applicationName>`, enabling server-side attribution.

**Methods:**

```typescript
async createStream(
    tableProperties: TableProperties,
    clientId: string,
    clientSecret: string,
    options?: StreamConfigurationOptions
): Promise<ZerobusStream>
```

Creates a new ingestion stream using OAuth 2.0 Client Credentials authentication.

Automatically includes these headers:
- `"authorization": "Bearer <oauth_token>"` (fetched via OAuth 2.0 Client Credentials flow)
- `"x-databricks-zerobus-table-name": "<table_name>"`

Returns a `ZerobusStream` instance.

---

```typescript
async recreateStream(stream: ZerobusStream): Promise<ZerobusStream>
```

Recreates a stream with the same configuration and automatically re-ingests all unacknowledged batches.

This method is the **recommended approach** for recovering from stream failures. It:
1. Retrieves all unacknowledged batches from the failed stream
2. Creates a new stream with identical configuration (same table, auth, options)
3. Re-ingests all unacknowledged batches in their original order
4. Returns the new stream ready for continued ingestion

**Parameters:**
- `stream` - The terminally failed stream to recreate. Do not call `stream.close()`
  first because the TypeScript wrapper releases its native handle on close.

**Returns:** Promise resolving to a new `ZerobusStream` with all unacknowledged batches re-ingested

**Example:**
```typescript
try {
  await stream.ingestRecordsOffset(batch);
  await stream.flush();
} catch (error) {
  // recreateStream() rejects unless the native stream already failed closed.
  const newStream = await sdk.recreateStream(stream);
  try {
    await newStream.flush();
  } finally {
    await newStream.close();
  }
}
```

**Note:** This method preserves batch structure and re-ingests batches atomically. For
debugging, inspect `getUnackedBatches()` after a terminal failure and before closing the wrapper.

---

### ZerobusStream

Represents an active ingestion stream.

**Methods:**

```typescript
async ingestRecordOffset(payload: Buffer | string | object): Promise<bigint>
```

**(Recommended)** Ingests a single record. The Promise resolves immediately after the record is queued (before server acknowledgment); the round-trip happens in the background. The idiomatic flow is to ingest in a loop and then `flush()` once to confirm everything queued so far. The returned offset, together with `waitForOffset()`, lets you confirm a specific record when needed — prefer that for bulk over waiting after each record, since per-record waiting limits throughput to one round-trip per record.

```typescript
// Idiomatic flow: ingest in a loop, then flush once
let lastOffset: bigint | null = null;
for (const record of records) {
  lastOffset = await stream.ingestRecordOffset(record);  // Resolves immediately
}
await stream.flush();  // Resolves once everything queued so far is acknowledged
// (Or, to confirm a specific record: if (lastOffset !== null) await stream.waitForOffset(lastOffset))
```

---

```typescript
async ingestRecordsOffset(payloads: Array<Buffer | string | object>): Promise<bigint | null>
```

**(Recommended)** Ingests multiple records as a batch. The Promise resolves immediately after the batch is queued (before server acknowledgment); the round-trip happens in the background. Returns `null` for empty batches. As with `ingestRecordOffset()`, the idiomatic flow is to ingest in a loop and `flush()` once to confirm; reach for `waitForOffset()` when a specific batch must be confirmed before continuing.

---

```typescript
async waitForOffset(offsetId: bigint): Promise<void>
```

Waits for the server to acknowledge all records up to and including the specified offset ID. Acks are ordered, so waiting on the **last** offset confirms every prior record too. Use this when a specific record must be confirmed before continuing; for confirming a bulk run, `flush()` is usually simpler. Avoid calling it after every record in a tight loop, since that limits throughput to one record per round-trip.

---

```typescript
async ingestRecord(payload: Buffer | string | object): Promise<bigint>
```

**@deprecated** Use `ingestRecordOffset()` instead.

Ingests a single record. Unlike `ingestRecordOffset()`, the Promise only resolves **after the server acknowledges** the record. This is slower for high-throughput scenarios.

**Parameters:**
- `payload` - Record data. The SDK supports 4 input types for flexibility:
  - **JSON Mode** (`RecordType.Json`):
    - **Type 1 - object** (high-level): Plain JavaScript object - SDK auto-stringifies with `JSON.stringify()`
    - **Type 2 - string** (low-level): Pre-serialized JSON string
  - **Protocol Buffers Mode** (`RecordType.Proto`):
    - **Type 3 - Message** (high-level): Protobuf message object - SDK calls `.encode().finish()` automatically
    - **Type 4 - Buffer** (low-level): Pre-serialized protobuf bytes

**All 4 Type Examples:**
```typescript
// JSON Type 1: object (high-level) - SDK auto-stringifies
await stream.ingestRecord({ device: 'sensor-1', temp: 25 });

// JSON Type 2: string (low-level) - pre-serialized
await stream.ingestRecord(JSON.stringify({ device: 'sensor-1', temp: 25 }));

// Protobuf Type 3: Message object (high-level) - SDK auto-serializes
const message = MyMessage.create({ device: 'sensor-1', temp: 25 });
await stream.ingestRecord(message);

// Protobuf Type 4: Buffer (low-level) - pre-serialized bytes
const buffer = Buffer.from(MyMessage.encode(message).finish());
await stream.ingestRecord(buffer);
```

**Note:** The SDK automatically detects protobufjs message objects by checking if the constructor has a static `.encode()` method. This works seamlessly with messages created via `MyMessage.create()` or `new MyMessage()`.

**Returns:** Promise resolving to the offset ID when the server acknowledges the record

---

```typescript
async ingestRecords(payloads: Array<Buffer | string | object>): Promise<bigint | null>
```

**@deprecated** Use `ingestRecordsOffset()` instead.

Ingests multiple records as a batch. Unlike `ingestRecordsOffset()`, the Promise only resolves **after the server acknowledges** the batch. This is slower for high-throughput scenarios.

**Parameters:**
- `payloads` - Array of record data. Supports the same 4 types as `ingestRecord()`:
  - **JSON Mode**: Array of **objects** (Type 1) or **strings** (Type 2)
  - **Proto Mode**: Array of **Message objects** (Type 3) or **Buffers** (Type 4)
  - Mixed types within the same array are supported

**All 4 Type Examples:**
```typescript
// JSON Type 1: objects (high-level) - SDK auto-stringifies
await stream.ingestRecords([
  { device: 'sensor-1', temp: 25 },
  { device: 'sensor-2', temp: 26 }
]);

// JSON Type 2: strings (low-level) - pre-serialized
await stream.ingestRecords([
  JSON.stringify({ device: 'sensor-1', temp: 25 }),
  JSON.stringify({ device: 'sensor-2', temp: 26 })
]);

// Protobuf Type 3: Message objects (high-level) - SDK auto-serializes
await stream.ingestRecords([
  MyMessage.create({ device: 'sensor-1', temp: 25 }),
  MyMessage.create({ device: 'sensor-2', temp: 26 })
]);

// Protobuf Type 4: Buffers (low-level) - pre-serialized bytes
const buffers = [
  Buffer.from(MyMessage.encode(msg1).finish()),
  Buffer.from(MyMessage.encode(msg2).finish())
];
await stream.ingestRecords(buffers);
```

**Returns:** Promise resolving to:
- `bigint` - Offset ID when the server acknowledges the entire batch
- `null` - If the batch was empty (no records sent)

**Best Practices:**
- Batch size: 100-1,000 records for optimal throughput/latency balance
- Empty batches are allowed and return `null`

---

```typescript
async flush(): Promise<void>
```

Flushes all pending records and waits for acknowledgments. This is the recommended way to confirm a batch of `ingestRecordOffset()` / `ingestRecordsOffset()` calls: ingest in a loop without waiting, then `flush()` once at the end instead of calling `waitForOffset()` after every record.

```typescript
async close(): Promise<void>
```

Closes the stream gracefully, flushing all pending data. **Always call this in a finally block!**

```typescript
async getUnackedRecords(): Promise<Buffer[]>
```

Returns unacknowledged record payloads as a flat array for inspection purposes.

**Important:** This can only be called after a terminal stream failure. Do not call
`stream.close()` first: the TypeScript wrapper releases the underlying stream handle on close.

**Returns:** Array of Buffer containing the raw record payloads

**Use case:** For inspecting unacknowledged individual records when using `ingestRecord()`. **Note:** This method is primarily for debugging and inspection. For recovery, use `recreateStream()` (recommended) or automatic recovery (default).

---

```typescript
async getUnackedBatches(): Promise<Buffer[][]>
```

Returns unacknowledged records grouped by their original batches for inspection purposes.

**Important:** This can only be called after a terminal stream failure. Do not call
`stream.close()` first: the TypeScript wrapper releases the underlying stream handle on close.

**Returns:** Array of arrays, where each inner array represents a batch of records as Buffers

**Use case:** For inspecting unacknowledged batches when using `ingestRecords()`. Preserves the original batch structure. **Note:** This method is primarily for debugging and inspection. For recovery, use `recreateStream()` (recommended) or automatic recovery (default).

**Example:**
```typescript
try {
  await stream.ingestRecords(batch1);
  await stream.ingestRecords(batch2);
  // ... error occurs
} catch (error) {
  const unackedBatches = await stream.getUnackedBatches();
  // unackedBatches[0] contains records from batch1 (if not acked)
  // unackedBatches[1] contains records from batch2 (if not acked)

  console.log(`Batches available for recovery: ${unackedBatches.length}`);
}
```

---

### TableProperties

Configuration for the target table.

**Interface:**

```typescript
interface TableProperties {
    tableName: string;              // Fully qualified table name (e.g., "catalog.schema.table")
    descriptorProto?: string;       // Base64-encoded protobuf descriptor (required for Protocol Buffers)
}
```

**Examples:**

```typescript
// JSON mode
const jsonTableProperties = { tableName: 'main.default.air_quality' };

// Protocol Buffers mode
const protoTableProperties = {
    tableName: 'main.default.air_quality',
    descriptorProto: descriptorBase64  // Required for protobuf
};
```

---

### StreamConfigurationOptions

Configuration options for stream behavior.

**Interface:**

```typescript
interface StreamConfigurationOptions {
    recordType?: RecordType;              // RecordType.Json or RecordType.Proto. Default: RecordType.Proto
    maxInflightRequests?: number;         // Default: 1,000,000
    recovery?: boolean;                   // Default: true
    recoveryTimeoutMs?: number;           // Default: 15,000
    recoveryBackoffMs?: number;           // Default: 2,000
    recoveryRetries?: number;             // Default: 4
    flushTimeoutMs?: number;              // Default: 300,000
    serverLackOfAckTimeoutMs?: number;    // Default: 60,000
    streamPausedMaxWaitTimeMs?: number;   // Default: undefined (wait for full server duration)
}

enum RecordType {
    Json = 0,   // JSON encoding
    Proto = 1   // Protocol Buffers encoding
}
```

## Best Practices

1. **Reuse SDK instances**: Create one `ZerobusSdk` instance per application
2. **Stream lifecycle**: Always close streams in a `finally` block to ensure all records are flushed
3. **Batch size**: Adjust `maxInflightRequests` based on your throughput requirements (default: 1,000,000)
4. **Error handling**: The stream handles errors internally with automatic retry. Only use `recreateStream()` for persistent failures after internal retries are exhausted.
5. **Use Protocol Buffers for production**: Protocol Buffers (the default) provides better performance and schema validation. Use JSON only when you need schema flexibility or for quick prototyping.
6. **Store credentials securely**: Use environment variables, never hardcode credentials
7. **Use batch ingestion**: For high-throughput scenarios, use `ingestRecordsOffset()` instead of individual `ingestRecordOffset()` calls
8. **Ingest in a loop, then `flush()`**: See [Acknowledgments and throughput](#acknowledgments-and-throughput) above for the full explanation.

## Platform Support

The SDK supports all platforms where Node.js and Rust are available.

### Pre-built Binaries

Pre-built native binaries are available for:

- **Linux**: x64, ARM64
- **macOS**: x64, ARM64
- **Windows**: x64

### Build from Source

Other platforms (e.g. Linux musl, FreeBSD) need to build from source during `npm install`, which requires:

- **Rust toolchain** (1.70+): Install via `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
- Platform C/C++ build tools (Xcode CLT on macOS, build-essential on Debian/Ubuntu, etc.)

The build process happens automatically during installation and typically takes 2-3 minutes.

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
- **Automatic memory management** for native objects. You still must `await stream.close()` to flush and release the stream.
- **Type safety** - Compile-time checks on both sides

## Community and Contributing

This is an open source project. We welcome contributions, feedback, and bug reports.

- **[Contributing Guide](https://github.com/databricks/zerobus-sdk/blob/main/typescript/CONTRIBUTING.md)**: TypeScript-specific development setup and workflow.
- **[General Contributing Guide](https://github.com/databricks/zerobus-sdk/blob/main/CONTRIBUTING.md)**: Pull request process, commit requirements, and policies.
- **[Changelog](https://github.com/databricks/zerobus-sdk/blob/main/typescript/CHANGELOG.md)**: See the history of changes in the SDK.
- **[Security Policy](https://github.com/databricks/zerobus-sdk/blob/main/SECURITY.md)**: Read about our security process and how to report vulnerabilities.
- **[Developer Certificate of Origin (DCO)](https://github.com/databricks/zerobus-sdk/blob/main/DCO)**: Understand the agreement for contributions.

## License

This SDK is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for the full text.
