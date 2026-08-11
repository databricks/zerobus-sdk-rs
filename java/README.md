# Databricks Zerobus Ingest SDK for Java

The Databricks Zerobus Ingest SDK for Java provides a high-performance client for ingesting data directly into Databricks Delta tables using the Zerobus streaming protocol.

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Requirements](#requirements)
- [Quick Start User Guide](#quick-start-user-guide)
  - [Building Your Application](#building-your-application)
  - [Choose Your Serialization Format](#choose-your-serialization-format)
- [Usage Examples](#usage-examples)
  - [Protocol Buffers Examples](#protocol-buffers-examples)
  - [JSON Examples](#json-examples)
  - [Arrow Flight Examples (Beta)](#arrow-flight-examples-beta)
- [Authentication](#authentication)
- [API Styles](#api-styles)
  - [Offset-Based API (Recommended)](#offset-based-api-recommended)
  - [Future-Based API (Deprecated)](#future-based-api-deprecated)
- [Configuration](#configuration)
- [Logging](#logging)
- [Error Handling](#error-handling)
- [API Reference](#api-reference)
- [Best Practices](#best-practices)
- [Community and Contributing](#community-and-contributing)
- [License](#license)

## Features

- **High-throughput ingestion**: Optimized for high-volume data ingestion via native Rust backend
- **Native performance**: JNI bindings to a high-performance Rust implementation
- **Automatic recovery**: Built-in retry and recovery mechanisms
- **Flexible configuration**: Customizable stream behavior and timeouts
- **Protocol Buffers**: Strongly-typed schema using protobuf
- **JSON support**: Ingest JSON records without Protocol Buffer schemas
- **Arrow Flight (Beta)**: Columnar ingestion of Apache Arrow `VectorSchemaRoot` batches
- **Offset-based API**: Low-overhead alternative to CompletableFuture for high throughput
- **OAuth 2.0 authentication**: Secure authentication with client credentials
- **Custom authentication**: Supply request headers with `HeadersProvider`
- **Framework compatible**: Works inside Spring Boot and other frameworks with isolated classloaders

## Architecture

The Java SDK uses JNI (Java Native Interface) to call a high-performance Rust implementation. This architecture provides:

- **Lower latency**: Direct native calls avoid Java gRPC overhead
- **Reduced memory**: Offset-based API eliminates CompletableFuture allocation per record
- **Better throughput**: Optimized Rust async runtime handles network I/O efficiently

```
┌───────────────────────────────────────────────────────────────┐
│                      Java Application                         │
├───────────────────────────────────────────────────────────────┤
│  ZerobusSdk │ ZerobusProtoStream │ ZerobusJsonStream          │
├───────────────────────────────────────────────────────────────┤
│                  BaseZerobusStream (JNI)                      │
├───────────────────────────────────────────────────────────────┤
│               Native Rust SDK (libzerobus_jni)                │
│         ┌─────────────┐  ┌─────────────┐                      │
│         │   Tokio     │  │   gRPC/     │                      │
│         │   Runtime   │  │   HTTP/2    │                      │
│         └─────────────┘  └─────────────┘                      │
└───────────────────────────────────────────────────────────────┘
```

## Requirements

### Runtime Requirements

- **Java**: 8 or higher - [Download Java](https://adoptium.net/)
- **Databricks workspace** with Zerobus access enabled

### Supported Platforms

This SDK includes native libraries for the following platforms:

| Platform | Architecture | Status |
|----------|--------------|--------|
| Linux (glibc) | x86_64  | Supported |
| Linux (glibc) | aarch64 | Supported |
| Linux (musl / Alpine) | x86_64  | Supported |
| Linux (musl / Alpine) | aarch64 | Supported |
| Windows  | x86_64       | Supported |
| macOS    | x86_64       | Supported |
| macOS    | aarch64 (Apple Silicon) | Supported |

Linux glibc builds support glibc 2.26 and newer, including Amazon Linux 2.
On Linux, the libc flavor (glibc vs musl) is detected at runtime. To override detection, set
`-Dzerobus.libc=musl` or `-Dzerobus.libc=glibc` on the JVM command line.

### Dependencies

**When using the fat JAR** (recommended for most users):
- No additional dependencies required - all dependencies are bundled

**When using the regular JAR**:
- [`protobuf-java` 4.33.0](https://mvnrepository.com/artifact/com.google.protobuf/protobuf-java/4.33.0)
- [`slf4j-api` 2.0.17](https://mvnrepository.com/artifact/org.slf4j/slf4j-api/2.0.17)
- An SLF4J implementation such as [`slf4j-simple` 2.0.17](https://mvnrepository.com/artifact/org.slf4j/slf4j-simple/2.0.17) or [`logback-classic` 1.4.14](https://mvnrepository.com/artifact/ch.qos.logback/logback-classic/1.4.14)

**When using Arrow Flight ingestion (`ZerobusArrowStream`)** — additional dependencies, opt-in:
- [`arrow-vector` 17.0.0](https://mvnrepository.com/artifact/org.apache.arrow/arrow-vector/17.0.0)
- [`arrow-memory-netty` 17.0.0](https://mvnrepository.com/artifact/org.apache.arrow/arrow-memory-netty/17.0.0)
- JDK 9+ also requires `--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.nio=org.apache.arrow.memory.core` on the launcher (see [`examples/arrow/README.md`](https://github.com/databricks/zerobus-sdk/blob/main/java/examples/arrow/README.md#jvm-module-flags-jdk-9))

```xml
<dependency>
    <groupId>org.apache.arrow</groupId>
    <artifactId>arrow-vector</artifactId>
    <version>17.0.0</version>
</dependency>
<dependency>
    <groupId>org.apache.arrow</groupId>
    <artifactId>arrow-memory-netty</artifactId>
    <version>17.0.0</version>
</dependency>
```

### Build Requirements (only for building from source)

- **Java**: 8 or higher - [Download Java](https://adoptium.net/)
- **Maven**: 3.6 or higher - [Download Maven](https://maven.apache.org/download.cgi)
- **Protocol Buffers Compiler** (`protoc`): 33.0 - [Download protoc](https://github.com/protocolbuffers/protobuf/releases/tag/v33.0) (for compiling your own `.proto` schemas)

## Quick Start User Guide

### Prerequisites

Before using the SDK, you need a Databricks workspace URL, a Delta table, and a service principal. See the [monorepo prerequisites](https://github.com/databricks/zerobus-sdk/blob/main/README.md#prerequisites) for detailed setup instructions.

### Building Your Application

#### Option 1: Using Maven Central (Recommended)

**Regular JAR (with dependency management):**

Add the SDK as a dependency in your `pom.xml`:

```xml
<dependencies>
    <dependency>
        <groupId>com.databricks</groupId>
        <artifactId>zerobus-ingest-sdk</artifactId>
        <version>0.2.0</version>
    </dependency>
</dependencies>
```

Or with Gradle (`build.gradle`):

```groovy
dependencies {
    implementation 'com.databricks:zerobus-ingest-sdk:0.2.0'
}
```

**Important**: You must also add the required dependencies manually, as they are not automatically included:

```xml
<!-- Add these dependencies in addition to the SDK -->
<dependencies>
    <!-- Zerobus SDK -->
    <dependency>
        <groupId>com.databricks</groupId>
        <artifactId>zerobus-ingest-sdk</artifactId>
        <version>0.2.0</version>
    </dependency>

    <!-- Required dependencies -->
    <dependency>
        <groupId>com.google.protobuf</groupId>
        <artifactId>protobuf-java</artifactId>
        <version>4.33.0</version>
    </dependency>
    <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-api</artifactId>
        <version>2.0.17</version>
    </dependency>
    <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-simple</artifactId>
        <version>2.0.17</version>
    </dependency>
</dependencies>
```

**Fat JAR (with all dependencies bundled):**

If you prefer the self-contained fat JAR with all dependencies included:

```xml
<dependencies>
    <dependency>
        <groupId>com.databricks</groupId>
        <artifactId>zerobus-ingest-sdk</artifactId>
        <version>0.2.0</version>
        <classifier>jar-with-dependencies</classifier>
    </dependency>
</dependencies>
```

Or with Gradle:

```groovy
dependencies {
    implementation 'com.databricks:zerobus-ingest-sdk:0.2.0:jar-with-dependencies'
}
```

**Note:** The fat JAR is typically not needed for Maven/Gradle projects. Use the regular JAR (without classifier) unless you have a specific reason to bundle all dependencies.

#### Option 2: Build from Source

Clone and build the SDK:

```bash
git clone https://github.com/databricks/zerobus-sdk.git
cd zerobus-sdk/java
mvn clean package -Dzerobus.skipNativeLibCheck=true
```

This generates two JAR files in the `target/` directory:

- **Regular JAR**: `zerobus-ingest-sdk-0.2.0.jar` (~12MB, includes native libraries)
  - Contains only the SDK classes
  - Requires all dependencies on the classpath

- **Fat JAR**: `zerobus-ingest-sdk-0.2.0-jar-with-dependencies.jar` (~19MB, includes native libraries + all dependencies)
  - Contains SDK classes plus all dependencies bundled
  - Self-contained, easier to deploy

The skip flag is for local Java-only builds. Release builds must stage the JNI
libraries under `src/main/resources/native/` and run Maven without that flag so
the packaged JAR includes native libraries.

**Which JAR to use?**
- **Regular JAR**: When using Maven/Gradle (recommended)
- **Fat JAR**: For standalone scripts or CLI tools without a build system

### Create Your Application Project

#### Using Maven

Create a new Maven project:

```bash
mkdir my-zerobus-app
cd my-zerobus-app
```

Create `pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                             http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.example</groupId>
    <artifactId>my-zerobus-app</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>

    <dependencies>
        <!-- Zerobus SDK -->
        <dependency>
            <groupId>com.databricks</groupId>
            <artifactId>zerobus-ingest-sdk</artifactId>
            <version>0.2.0</version>
        </dependency>

        <!-- Required dependencies (see above for full list) -->
        <dependency>
            <groupId>com.google.protobuf</groupId>
            <artifactId>protobuf-java</artifactId>
            <version>4.33.0</version>
        </dependency>
        <!-- Add other dependencies from the list above -->
    </dependencies>
</project>
```

Create project structure:

```bash
mkdir -p src/main/java/com/example
mkdir -p src/main/proto
```

#### Define Your Protocol Buffer Schema

Create `src/main/proto/record.proto`:

```protobuf
syntax = "proto2";

package com.example;

option java_package = "com.example.proto";
option java_outer_classname = "Record";

message AirQuality {
    optional string device_name = 1;
    optional int32 temp = 2;
    optional int64 humidity = 3;
}
```

**Compile the protobuf:**

```bash
protoc --java_out=src/main/java src/main/proto/record.proto
```

This generates `src/main/java/com/example/proto/Record.java`.

**Note**: Ensure you have `protoc` version 33.0 installed. [Download protoc](https://github.com/protocolbuffers/protobuf/releases/tag/v33.0) if needed. The generated Java files are compatible with `protobuf-java` 4.33.0.

### Generate Protocol Buffer Schema from Unity Catalog (Alternative)

Instead of manually writing and compiling your protobuf schema, you can automatically generate it from an existing Unity Catalog table schema using the included `GenerateProto` tool.

#### Using the Proto Generation Tool

The `GenerateProto` tool fetches your table schema from Unity Catalog and generates a corresponding proto2 definition file with the correct type mappings.

**First, download the fat JAR:**

The proto generation tool requires the fat JAR (all dependencies included):

```bash
# Download from Maven Central
wget https://repo1.maven.org/maven2/com/databricks/zerobus-ingest-sdk/0.2.0/zerobus-ingest-sdk-0.2.0-jar-with-dependencies.jar

# Or if you built from source, it's in target/
# cp target/zerobus-ingest-sdk-0.2.0-jar-with-dependencies.jar .
```

**Run the tool:**

```bash
java -jar zerobus-ingest-sdk-0.2.0-jar-with-dependencies.jar \
  --uc-endpoint "https://dbc-a1b2c3d4-e5f6.cloud.databricks.com" \
  --client-id "your-service-principal-application-id" \
  --client-secret "your-service-principal-secret" \
  --table "main.default.air_quality" \
  --output "src/main/proto/record.proto" \
  --proto-msg "AirQuality"
```

**Parameters:**
- `--uc-endpoint`: Your workspace URL (e.g., `https://dbc-a1b2c3d4-e5f6.cloud.databricks.com`)
- `--client-id`: Service principal application ID
- `--client-secret`: Service principal secret
- `--table`: Fully qualified table name (catalog.schema.table)
- `--output`: Output path for the generated proto file
- `--proto-msg`: (Optional) Name for the protobuf message (defaults to table name)

**Example:**

For a table defined as:
```sql
CREATE TABLE main.default.air_quality (
    device_name STRING,
    temp INT,
    humidity BIGINT
)
USING DELTA;
```

Running the generation tool will create `src/main/proto/record.proto`:
```protobuf
syntax = "proto2";

package com.example;

option java_package = "com.example.proto";
option java_outer_classname = "Record";

message AirQuality {
    optional string device_name = 1;
    optional int32 temp = 2;
    optional int64 humidity = 3;
}
```

After generating the proto file, compile it as shown above:
```bash
protoc --java_out=src/main/java src/main/proto/record.proto
```

**Type Mappings:**

The tool automatically maps Unity Catalog types to proto2 types:

| Delta Type | Proto2 Type |
|-----------|-------------|
| INT, SMALLINT, SHORT | int32 |
| BIGINT, LONG | int64 |
| FLOAT | float |
| DOUBLE | double |
| STRING, VARCHAR | string |
| BOOLEAN | bool |
| BINARY | bytes |
| DATE | int32 |
| TIMESTAMP | int64 |
| ARRAY\<type\> | repeated type |
| MAP\<key, value\> | map\<key, value\> |
| STRUCT\<fields\> | nested message |

**Benefits:**
- No manual schema creation required
- Ensures schema consistency between your table and protobuf definitions
- Automatically handles complex types (arrays, maps, structs)
- Reduces errors from manual type mapping
- No need to clone the repository - runs directly from the SDK JAR

For detailed documentation and examples, see [tools/README.md](https://github.com/databricks/zerobus-sdk/blob/main/java/tools/README.md).

#### 4. Write Your Client Code

> The idiomatic flow is **ingest in a loop, then `flush()`** once at the end. See
> [Acknowledgments and throughput](#acknowledgments-and-throughput) below the example for
> how acknowledgment works and when to use `waitForOffset()` or an `AckCallback`.

Create `src/main/java/com/example/ZerobusClient.java`:

```java
package com.example;

import com.databricks.zerobus.*;
import com.example.proto.Record.AirQuality;

public class ZerobusClient {
    public static void main(String[] args) throws Exception {
        // Configuration
        String serverEndpoint = "https://1234567890123456.zerobus.us-west-2.cloud.databricks.com";
        String workspaceUrl = "https://dbc-a1b2c3d4-e5f6.cloud.databricks.com";
        String tableName = "main.default.air_quality";
        String clientId = "your-service-principal-application-id";
        String clientSecret = "your-service-principal-secret";

        // Initialize SDK
        ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);

        // Create stream (recommended offset-based proto stream)
        ZerobusProtoStream stream = sdk.streamBuilder()
            .table(tableName)
            .oauth(clientId, clientSecret)
            .compiledProto(AirQuality.getDescriptor().toProto())
            .build()
            .join();

        try {
            long lastOffset = -1;

            // Ingest in a loop. ingestRecordOffset() returns as soon as the record is
            // queued; the SDK sends it and tracks its acknowledgment in the background.
            for (int i = 0; i < 100; i++) {
                AirQuality record = AirQuality.newBuilder()
                    .setDeviceName("sensor-" + (i % 10))
                    .setTemp(20 + (i % 15))
                    .setHumidity(50 + (i % 40))
                    .build();

                lastOffset = stream.ingestRecordOffset(record); // returns immediately
            }

            // Confirm everything is durably committed. flush() does the same;
            // waiting on the last offset works because acks are ordered.
            stream.waitForOffset(lastOffset);

            System.out.println("Successfully ingested 100 records!");
        } finally {
            stream.close();
            sdk.close();
        }
    }
}
```

#### Compile and Run

**Using Maven:**

```bash
# First, compile the proto file to generate Java classes
protoc --java_out=src/main/java src/main/proto/record.proto

# Compile and run
mvn compile
mvn exec:java -Dexec.mainClass="com.example.ZerobusClient"
```

**Or build as standalone JAR:**

```bash
# Generate proto classes
protoc --java_out=src/main/java src/main/proto/record.proto

# Package into executable JAR (add maven-shade-plugin to pom.xml)
mvn package

# Run the JAR
java -jar target/my-zerobus-app-1.0-SNAPSHOT.jar
```

**Using downloaded JAR (without Maven):**

```bash
# Generate proto classes
protoc --java_out=src/main/java src/main/proto/record.proto

# Compile
javac -cp "lib/*" -d out src/main/java/com/example/ZerobusClient.java src/main/java/com/example/proto/Record.java

# Run
java -cp "lib/*:out" com.example.ZerobusClient
```

You should see output like:
```
Successfully ingested 100 records!
```

### Acknowledgments and throughput

Ingestion is asynchronous. `ingestRecordOffset()` returns as soon as the record is queued;
the SDK sends it and tracks its acknowledgment in the background. To confirm records are
durably committed, call `flush()` — it returns once everything queued so far is
acknowledged. The idiomatic flow is **ingest in a loop, then `flush()`** (once for a
bounded batch, or periodically for a long-running stream); or register an
[`AckCallback`](#ackcallback-interface) to be notified as records commit.

Each ingest also returns the record's offset, and `waitForOffset(offset)` blocks until that
offset is acknowledged — handy when a specific record must be confirmed before continuing
(acks are ordered, so waiting on the last offset confirms the whole run). Just avoid calling
`waitForOffset()` (or `.join()` on the deprecated per-record future) after every record in a
tight loop, since that limits throughput to one record per round-trip.

## Usage Examples

The `examples/` directory contains complete working examples organized by stream type:

```
examples/
├── README.md              # Overview and comparison
├── proto/                 # ZerobusProtoStream examples
│   ├── README.md
│   ├── SingleRecordExample.java
│   └── BatchIngestionExample.java
├── json/                  # ZerobusJsonStream examples
│   ├── README.md
│   ├── SingleRecordExample.java
│   └── BatchIngestionExample.java
├── arrow/                 # ZerobusArrowStream example (Beta)
│   ├── README.md
│   └── ArrowIngestionExample.java
└── legacy/                # ZerobusStream (deprecated)
    └── LegacyStreamExample.java
```

### Creating Streams (Stream Builder)

`ZerobusSdk.streamBuilder()` is the recommended way to create a stream. It exposes a single fluent
API for all stream types and mirrors the Rust SDK's `stream_builder()`:

```java
// Protocol Buffer
ZerobusProtoStream protoStream = sdk.streamBuilder()
    .table("catalog.schema.table")
    .oauth(clientId, clientSecret)
    .compiledProto(MyProto.getDescriptor().toProto())
    .build()
    .join();

// JSON
ZerobusJsonStream jsonStream = sdk.streamBuilder()
    .table("catalog.schema.table")
    .oauth(clientId, clientSecret)
    .json()
    .build()
    .join();

// Arrow Flight (Beta)
ZerobusArrowStream arrowStream = sdk.streamBuilder()
    .table("catalog.schema.table")
    .oauth(clientId, clientSecret)
    .arrow(schema)
    .ipcCompression(IPCCompressionType.ZSTD)
    .build()
    .join();
```

Stream configuration is set directly on the builder (for example `.maxInflightRecords(50000)`,
`.recovery(true)`, `.recoveryRetries(5)`). Arrow-specific options such as `.maxInflightBatches(...)`
and `.ipcCompression(...)` are available after calling `.arrow(...)`.

> **Note:** `createJsonStream`, `createProtoStream`, and `createArrowStream` are deprecated in favor
> of `streamBuilder()` and will be removed in the next major release.

### Protocol Buffers Examples

Best for production systems with type safety and schema validation:

```bash
# Single record ingestion
cd examples/proto
protoc --java_out=. air_quality.proto
javac -d . -cp "../../target/zerobus-ingest-sdk-*-jar-with-dependencies.jar:." *.java
java -cp "../../target/zerobus-ingest-sdk-*-jar-with-dependencies.jar:." \
  com.databricks.zerobus.examples.proto.SingleRecordExample

# Batch ingestion
java -cp "../../target/zerobus-ingest-sdk-*-jar-with-dependencies.jar:." \
  com.databricks.zerobus.examples.proto.BatchIngestionExample
```

### JSON Examples

Best for rapid prototyping and flexible schemas. No Protocol Buffer types required:

```bash
cd examples/json
javac -d . -cp "../../target/zerobus-ingest-sdk-*-jar-with-dependencies.jar:." *.java
java -cp "../../target/zerobus-ingest-sdk-*-jar-with-dependencies.jar:." \
  com.databricks.zerobus.examples.json.SingleRecordExample
```

**Clean JSON API** - use the stream builder for a simplified experience:

```java
// No proto types or configuration needed!
ZerobusJsonStream stream = sdk.streamBuilder()
    .table(tableName)
    .oauth(clientId, clientSecret)
    .json()
    .build()
    .join();
stream.ingestRecordOffset("{\"field\": \"value\"}");
```

See [`examples/README.md`](https://github.com/databricks/zerobus-sdk/blob/main/java/examples/README.md) for detailed documentation.

### Arrow Flight Examples (Beta)

Best for columnar data, wide/numeric schemas, or applications that already produce Apache Arrow `VectorSchemaRoot` batches (Spark, pandas via PyArrow bridges, columnar gateways). Requires the `arrow-vector` and `arrow-memory-netty` dependencies plus the JDK 9+ `--add-opens` flags (see [Dependencies](#dependencies) and [`examples/arrow/README.md`](https://github.com/databricks/zerobus-sdk/blob/main/java/examples/arrow/README.md)):

```bash
cd examples
ARROW_CP=$(echo ../target/arrow-deps/*.jar | tr ' ' ':')
javac -d . -cp "../target/classes:$ARROW_CP" arrow/ArrowIngestionExample.java
java --add-opens=java.base/java.nio=ALL-UNNAMED \
     --add-opens=java.base/java.nio=org.apache.arrow.memory.core \
     -cp ".:../target/zerobus-ingest-sdk-*-jar-with-dependencies.jar:$ARROW_CP" \
     com.databricks.zerobus.examples.arrow.ArrowIngestionExample
```

The bundled example opens three streams in sequence — one per IPC compression codec (`NONE`, `LZ4_FRAME`, `ZSTD`) — and ingests 10 batches × 10 rows on each, demonstrating the full `ingestBatch` → `waitForOffset` → `flush` → `close` lifecycle.

```java
Schema schema = new Schema(Arrays.asList(
    Field.nullable("device_name", ArrowType.LargeUtf8.INSTANCE),
    Field.nullable("temp", new ArrowType.Int(32, true))));

ZerobusArrowStream stream = sdk.streamBuilder()
    .table(tableName)
    .oauth(clientId, clientSecret)
    .arrow(schema)
    .build()
    .join();

try (VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
    // populate batch...
    Optional<Long> offset = stream.ingestBatch(batch);
    if (offset.isPresent()) {
        stream.waitForOffset(offset.get());
    }
}
stream.close();
```

> **Beta.** Arrow Flight ingestion is in Beta. The API is stabilising but may still change before reaching GA.

---

## Authentication

OAuth client credentials are the default authentication mechanism. For personal access tokens,
custom identity providers, or externally managed credentials, implement `HeadersProvider`:

```java
HeadersProvider provider = () -> {
    Map<String, String> headers = new HashMap<>();
    headers.put("authorization", "Bearer " + fetchToken());
    headers.put("x-databricks-zerobus-table-name", tableName);
    return headers;
};

ZerobusJsonStream stream = sdk.streamBuilder()
    .table(tableName)
    .headersProvider(provider)
    .json()
    .build()
    .join();
```

The same provider works with the builder's `compiledProto()` and `arrow()` format selectors.
Providers may override `invalidate()` to clear cached credentials after an authentication
rejection. Implementations must be thread-safe because the SDK can invoke them from internal
threads during creation and recovery.

---

## API Styles

The SDK provides two ingestion styles:

| Style | Status | Best For | Overhead |
|-------|--------|----------|----------|
| **Offset-Based** | Recommended | All use cases | Minimal - no object allocation |
| **Future-Based** | Deprecated | Legacy code | CompletableFuture per record |

`ZerobusArrowStream` is offset-based as well, but `ingestBatch(VectorSchemaRoot)` returns `Optional<Long>` (empty for null or zero-row batches) rather than a bare `long`.

### Offset-Based API (Recommended)

Use `ZerobusProtoStream` or `ZerobusJsonStream` for all new code. They use offset-based returns that avoid `CompletableFuture` allocation overhead.

> Each `ingestRecordOffset()` call returns as soon as the record is queued; the SDK sends
> it and tracks its acknowledgment in the background. The idiomatic flow is to ingest in a
> loop, then confirm durability **once** — with `flush()`, or by passing the last offset to
> `waitForOffset()` (acks are ordered, so the last offset confirms every prior record). See
> [Acknowledgments and throughput](#acknowledgments-and-throughput) for the full picture.

```java
ZerobusProtoStream stream = sdk.streamBuilder()
    .table(tableName)
    .oauth(clientId, clientSecret)
    .compiledProto(AirQuality.getDescriptor().toProto())
    .build()
    .join();

try {
    long lastOffset = -1;

    // Ingest in a loop
    for (int i = 0; i < 1000000; i++) {
        AirQuality record = AirQuality.newBuilder()
            .setDeviceName("sensor-" + (i % 100))
            .setTemp(20 + i % 15)
            .setHumidity(50 + i % 40)
            .build();

        // Returns immediately after queuing
        lastOffset = stream.ingestRecordOffset(record);
    }

    // Confirm all records are acknowledged
    stream.waitForOffset(lastOffset);
} finally {
    stream.close();
    sdk.close();
}
```

### Future-Based API (Deprecated)

> **Deprecated:** Use the offset-based API instead for better performance.

The future-based API is still available for backward compatibility but will be removed in a future release:

```java
// DEPRECATED - use ingestRecordOffset() instead.
// Each ingestRecord() returns immediately; keep the last future and join once after
// the loop to confirm durability (joining after every record limits throughput).
try {
    CompletableFuture<Void> lastFuture = null;
    for (int i = 0; i < 1000; i++) {
        AirQuality record = AirQuality.newBuilder()
            .setDeviceName("sensor-" + i)
            .setTemp(20 + i % 15)
            .build();

        lastFuture = stream.ingestRecord(record);  // Deprecated; non-blocking
    }
    if (lastFuture != null) {
        lastFuture.join();  // wait once, after the loop
    }
} finally {
    stream.close();
    sdk.close();
}
```

**Migration:**
```java
// Before (deprecated ZerobusStream):
stream.ingestRecord(record).join();

// After (recommended ZerobusProtoStream):
long offset = stream.ingestRecordOffset(record);
stream.waitForOffset(offset);

// Batch ingestion:
Optional<Long> batchOffset = stream.ingestRecordsOffset(batch);
batchOffset.ifPresent(o -> { try { stream.waitForOffset(o); } catch (Exception e) { throw new RuntimeException(e); } });
```

---

## Choose Your Serialization Format

| Format | Best For | Pros | Cons |
|--------|----------|------|------|
| **Protocol Buffers** | Production systems | Type-safe, compact, fast | Requires schema compilation |
| **JSON** | Prototyping, flexible schemas | Human-readable, no compilation, clean API | Larger payload, slower |
| **Arrow Flight** (Beta) | Columnar/analytics workloads, wide/numeric schemas, applications that already produce Arrow data | High throughput, native Apache Arrow types, optional IPC compression (LZ4 / ZSTD) | Requires `arrow-vector` + `arrow-memory-netty` deps and JDK 9+ `--add-opens` flags; API may still change before GA |

### JSON Streams (Recommended for JSON)

Use the stream builder for a clean API that doesn't require Protocol Buffer types:

```java
// Create JSON stream - no proto types needed!
ZerobusJsonStream stream = sdk.streamBuilder()
    .table("catalog.schema.table")
    .oauth(clientId, clientSecret)
    .json()
    .build()
    .join();

try {
    // Ingest JSON string directly
    long offset = stream.ingestRecordOffset("{\"device_name\": \"sensor-1\", \"temp\": 25}");
    stream.waitForOffset(offset);

    // Or use objects with a serializer (Gson, Jackson, etc.)
    Gson gson = new Gson();
    Map<String, Object> data = new HashMap<>();
    data.put("device_name", "sensor-2");
    data.put("temp", 26);
    offset = stream.ingestRecordOffset(data, gson::toJson);

    // Batch ingestion
    List<String> batch = Arrays.asList(
        "{\"device_name\": \"sensor-1\", \"temp\": 25}",
        "{\"device_name\": \"sensor-2\", \"temp\": 26}"
    );
    Optional<Long> batchOffset = stream.ingestRecordsOffset(batch);
    if (batchOffset.isPresent()) {
        stream.waitForOffset(batchOffset.get());
    }
} finally {
    stream.close();
    sdk.close();
}
```

With custom configuration set directly on the builder:

```java
ZerobusJsonStream stream = sdk.streamBuilder()
    .table(tableName)
    .oauth(clientId, clientSecret)
    .maxInflightRecords(50000)
    .json()
    .build()
    .join();
```

## Configuration

### Stream Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `maxInflightRecords` | 1000000 | Maximum number of unacknowledged records |
| `recovery` | true | Enable automatic stream recovery |
| `recoveryTimeoutMs` | 15000 | Timeout for recovery operations (ms) |
| `recoveryBackoffMs` | 2000 | Delay between recovery attempts (ms) |
| `recoveryRetries` | 3 | Maximum number of recovery attempts |
| `flushTimeoutMs` | 300000 | Timeout for flush operations (ms) |
| `serverLackOfAckTimeoutMs` | 60000 | Server acknowledgment timeout (ms) |
| `ackCallback` | None | Callback invoked on record acknowledgment |

### Arrow Stream Configuration Options (Beta)

Used with `ZerobusArrowStream`. Build via `ArrowStreamConfigurationOptions.builder()`.

| Option | Default | Description |
|--------|---------|-------------|
| `maxInflightBatches` | 1000 | Maximum number of unacknowledged Arrow batches |
| `recovery` | true | Enable automatic stream recovery |
| `recoveryTimeoutMs` | 15000 | Timeout for recovery operations (ms) |
| `recoveryBackoffMs` | 2000 | Delay between recovery attempts (ms) |
| `recoveryRetries` | 4 | Maximum number of recovery attempts |
| `serverLackOfAckTimeoutMs` | 60000 | Server acknowledgment timeout (ms) |
| `flushTimeoutMs` | 300000 | Timeout for flush operations (ms) |
| `connectionTimeoutMs` | 30000 | Timeout for establishing a gRPC connection (ms) |
| `ipcCompression` | `IPCCompressionType.NONE` | Arrow IPC compression codec on the wire (`NONE`, `LZ4_FRAME`, `ZSTD`) |
| `streamPausedMaxWaitTimeMs` | `-1` | Max time to wait in the paused state during graceful close. `-1` = full server duration, `0` = immediate recovery, `>0` = `min(this, server_duration)` ms |

`ackCallback` is not supported for Arrow Flight streams. Configuring it on `StreamBuilder` before calling `ArrowStreamBuilder.build()` throws `IllegalStateException`.

## Logging

The Databricks Zerobus Ingest SDK for Java uses the standard [SLF4J logging framework](https://www.slf4j.org/). The SDK only depends on `slf4j-api`, which means **you need to add an SLF4J implementation** to your classpath to see log output.

### Adding a Logging Implementation

**Option 1: Using slf4j-simple** (simplest for getting started)

Add to your Maven dependencies:
```xml
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-simple</artifactId>
    <version>2.0.17</version>
</dependency>
```

Control log levels with system properties:
```bash
java -Dorg.slf4j.simpleLogger.log.com.databricks.zerobus=debug -cp "lib/*:out" com.example.ZerobusClient
```

Available log levels: `trace`, `debug`, `info`, `warn`, `error`

**Option 2: Using Logback** (recommended for production)

Add to your Maven dependencies:
```xml
<dependency>
    <groupId>ch.qos.logback</groupId>
    <artifactId>logback-classic</artifactId>
    <version>1.4.14</version>
</dependency>
```

Create `logback.xml` in your resources directory:
```xml
<configuration>
    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>

    <logger name="com.databricks.zerobus" level="DEBUG"/>

    <root level="INFO">
        <appender-ref ref="STDOUT"/>
    </root>
</configuration>
```

**Option 3: Using Log4j 2**

Add to your Maven dependencies:
```xml
<dependency>
    <groupId>org.apache.logging.log4j</groupId>
    <artifactId>log4j-slf4j-impl</artifactId>
    <version>2.20.0</version>
</dependency>
```

Create `log4j2.xml` in your resources directory:
```xml
<Configuration>
    <Appenders>
        <Console name="Console" target="SYSTEM_OUT">
            <PatternLayout pattern="%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n"/>
        </Console>
    </Appenders>
    <Loggers>
        <Logger name="com.databricks.zerobus" level="debug"/>
        <Root level="info">
            <AppenderRef ref="Console"/>
        </Root>
    </Loggers>
</Configuration>
```

### No Logging Implementation

If you don't add an SLF4J implementation, you'll see a warning like:
```
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
```

The SDK will still work, but no log messages will be output.

### What Gets Logged

At the **DEBUG** level, the SDK logs:
- Stream lifecycle events (creation, closure)
- SDK initialization

At the **INFO** level, the SDK logs:
- Native library loading
- Stream flush completion
- Stream closure

At the **WARN** level, the SDK logs:
- Deprecation warnings

At the **ERROR** level, the SDK logs:
- Native library loading failures

> **Note:** Most detailed logging (token generation, gRPC, retries) is handled internally by the native Rust SDK and uses its own logging configuration.

## Error Handling

The SDK throws two types of exceptions:

- `ZerobusException`: Retriable errors (e.g., network issues, temporary server errors)
- `NonRetriableException`: Non-retriable errors (e.g., invalid credentials, missing table)

```java
try {
    stream.ingestRecord(record);
} catch (NonRetriableException e) {
    // Fatal error - do not retry
    logger.error("Non-retriable error: " + e.getMessage());
    throw e;
} catch (ZerobusException e) {
    // Retriable error - can retry with backoff
    logger.warn("Retriable error: " + e.getMessage());
    // Implement retry logic
}
```

## API Reference

### ZerobusSdk

Main entry point for the SDK.

**Constructors:**
```java
ZerobusSdk(String serverEndpoint, String unityCatalogEndpoint)
ZerobusSdk(String serverEndpoint, String unityCatalogEndpoint, String applicationName)
```
- `serverEndpoint` - The Zerobus gRPC endpoint (e.g., `https://<workspace-id>.zerobus.region.cloud.databricks.com`)
- `unityCatalogEndpoint` - The Unity Catalog endpoint (your workspace URL)
- `applicationName` (optional) - Application identifier appended to the HTTP `user-agent` header, conventionally `"<product>/<version>"` (e.g. `"my-app/1.0"`). Pass `null` to omit.

**Methods:**

```java
StreamBuilder streamBuilder()
```
Returns a fluent [`StreamBuilder`](#streambuilder) for creating JSON, Protocol Buffer, or Arrow Flight streams. This is the recommended way to create streams.

```java
<RecordType extends Message> CompletableFuture<ZerobusStream<RecordType>> createStream(
    TableProperties<RecordType> tableProperties,
    String clientId,
    String clientSecret,
    StreamConfigurationOptions options
)
```
Creates a new Protocol Buffer ingestion stream with custom configuration. Returns a CompletableFuture that completes when the stream is ready. _Deprecated — use [`streamBuilder()`](#streambuilder)._

```java
<RecordType extends Message> CompletableFuture<ZerobusStream<RecordType>> createStream(
    TableProperties<RecordType> tableProperties,
    String clientId,
    String clientSecret
)
```
Creates a new Protocol Buffer ingestion stream with default configuration. Returns a CompletableFuture that completes when the stream is ready. _Deprecated — use [`streamBuilder()`](#streambuilder)._

```java
CompletableFuture<ZerobusJsonStream> createJsonStream(
    String tableName,
    String clientId,
    String clientSecret
)
```
Creates a new JSON ingestion stream with default configuration. No Protocol Buffer types required. _Deprecated — use [`streamBuilder()`](#streambuilder)._

```java
CompletableFuture<ZerobusJsonStream> createJsonStream(
    String tableName,
    String clientId,
    String clientSecret,
    StreamConfigurationOptions options
)
```
Creates a new JSON ingestion stream with custom configuration. No Protocol Buffer types required. _Deprecated — use [`streamBuilder()`](#streambuilder)._

```java
CompletableFuture<ZerobusProtoStream> recreateStream(ZerobusProtoStream closedStream)
```
Recreates a Proto stream from a closed stream, re-ingesting unacknowledged records and flushing.

```java
CompletableFuture<ZerobusJsonStream> recreateStream(ZerobusJsonStream closedStream)
```
Recreates a JSON stream from a closed stream, re-ingesting unacknowledged records and flushing.

```java
CompletableFuture<ZerobusArrowStream> createArrowStream(
    String tableName,
    org.apache.arrow.vector.types.pojo.Schema schema,
    String clientId,
    String clientSecret
)
```
**Beta.** Creates a new Arrow Flight ingestion stream with default configuration. Requires `arrow-vector` + `arrow-memory-netty` on the classpath. _Deprecated — use [`streamBuilder()`](#streambuilder)._

```java
CompletableFuture<ZerobusArrowStream> createArrowStream(
    String tableName,
    org.apache.arrow.vector.types.pojo.Schema schema,
    String clientId,
    String clientSecret,
    ArrowStreamConfigurationOptions options
)
```
**Beta.** Same as above with custom configuration. _Deprecated — use [`streamBuilder()`](#streambuilder)._

```java
CompletableFuture<ZerobusArrowStream> recreateArrowStream(ZerobusArrowStream closedStream)
```
**Beta.** Recreates an Arrow stream from a closed stream, re-ingesting unacknowledged batches and flushing.

---

### StreamBuilder

Fluent builder for creating streams, returned by [`ZerobusSdk.streamBuilder()`](#zerobussdk). Set the table, authentication, and stream configuration, then select a record format to obtain a typed sub-builder whose `build()` returns the matching stream type.

```java
StreamBuilder table(String tableName)
StreamBuilder oauth(String clientId, String clientSecret)
StreamBuilder headersProvider(HeadersProvider headersProvider)
StreamBuilder recovery(boolean recovery)
StreamBuilder recoveryTimeoutMs(int ms)
StreamBuilder recoveryBackoffMs(int ms)
StreamBuilder recoveryRetries(int n)
StreamBuilder serverLackOfAckTimeoutMs(int ms)
StreamBuilder flushTimeoutMs(int ms)
StreamBuilder maxInflightRecords(int n)          // JSON/proto only
StreamBuilder ackCallback(AckCallback callback)  // JSON/proto only

// Format selection -> typed sub-builder:
JsonStreamBuilder  json()
ProtoStreamBuilder compiledProto(DescriptorProto descriptorProto)
ArrowStreamBuilder arrow(Schema schema)          // Beta
```

Each sub-builder exposes `build()`:

```java
CompletableFuture<ZerobusJsonStream>  StreamBuilder.JsonStreamBuilder.build()
CompletableFuture<ZerobusProtoStream> StreamBuilder.ProtoStreamBuilder.build()
CompletableFuture<ZerobusArrowStream> StreamBuilder.ArrowStreamBuilder.build()   // Beta
```

`ArrowStreamBuilder` additionally supports `maxInflightBatches(int)`, `connectionTimeoutMs(long)`, `ipcCompression(IPCCompressionType)`, and `streamPausedMaxWaitTimeMs(long)`.

Configuring `ackCallback` before calling `ArrowStreamBuilder.build()` throws `IllegalStateException` because Arrow Flight streams do not support ACK callbacks.

---

### ZerobusProtoStream

Stream for Protocol Buffer ingestion with method-level generics. Use `ZerobusSdk.streamBuilder()` to create instances.

**Single Record Methods:**

```java
<T extends Message> long ingestRecordOffset(T record) throws ZerobusException
```
Ingests a Protocol Buffer message and returns the offset immediately.

```java
long ingestRecordOffset(byte[] encodedBytes) throws ZerobusException
```
Ingests pre-encoded bytes and returns the offset immediately.

**Batch Methods:**

```java
<T extends Message> Optional<Long> ingestRecordsOffset(Iterable<T> records) throws ZerobusException
```
Ingests multiple messages and returns the batch offset.

```java
Optional<Long> ingestRecordsOffset(List<byte[]> encodedRecords) throws ZerobusException
```
Ingests multiple pre-encoded byte arrays and returns the batch offset.

**Recovery Methods:**

```java
List<byte[]> getUnackedRecords() throws ZerobusException
```
Returns unacknowledged records as raw byte arrays.

```java
<T extends Message> List<T> getUnackedRecords(Parser<T> parser) throws ZerobusException
```
Returns unacknowledged records parsed into messages.

```java
List<EncodedBatch> getUnackedBatches() throws ZerobusException
```
Returns unacknowledged records grouped by batch.

**Lifecycle Methods:** `waitForOffset()`, `flush()`, `close()`, `isClosed()`, `getTableName()`, `getOptions()`

---

### ZerobusJsonStream

Stream for JSON ingestion with method-level generics. Use `ZerobusSdk.streamBuilder().json()` to create instances.

**Single Record Methods:**

```java
<T> long ingestRecordOffset(T object, JsonSerializer<T> serializer) throws ZerobusException
```
Ingests an object serialized to JSON and returns the offset immediately.

```java
long ingestRecordOffset(String json) throws ZerobusException
```
Ingests a JSON string and returns the offset immediately.

**Batch Methods:**

```java
<T> Optional<Long> ingestRecordsOffset(Iterable<T> objects, JsonSerializer<T> serializer) throws ZerobusException
```
Ingests multiple objects as JSON and returns the batch offset.

```java
Optional<Long> ingestRecordsOffset(Iterable<String> jsonStrings) throws ZerobusException
```
Ingests multiple JSON strings and returns the batch offset.

**Recovery Methods:**

```java
List<String> getUnackedRecords() throws ZerobusException
```
Returns unacknowledged records as JSON strings.

```java
<T> List<T> getUnackedRecords(JsonDeserializer<T> deserializer) throws ZerobusException
```
Returns unacknowledged records deserialized into objects.

```java
List<EncodedBatch> getUnackedBatches() throws ZerobusException
```
Returns unacknowledged records grouped by batch.

**Lifecycle Methods:** `waitForOffset()`, `flush()`, `close()`, `isClosed()`, `getTableName()`, `getOptions()`

---

### ZerobusArrowStream (Beta)

> **Beta.** Arrow Flight ingestion is in Beta. The API is stabilising but may still change before reaching GA.

Stream for Apache Arrow Flight ingestion of `VectorSchemaRoot` batches. Use `ZerobusSdk.streamBuilder().arrow()` to create instances. Requires `arrow-vector` + `arrow-memory-netty` on the classpath and JDK 9+ `--add-opens` flags (see [Dependencies](#dependencies)).

**Batch Ingestion:**

```java
Optional<Long> ingestBatch(VectorSchemaRoot batch) throws ZerobusException
```
Serializes the batch to Arrow IPC, queues it for transmission, and returns the assigned offset. Returns an empty `Optional` if the batch is `null` or has zero rows. The batch's schema must match the schema used to create the stream.

**Recovery:**

```java
List<byte[]> getUnackedBatches() throws ZerobusException
```
Returns unacknowledged batches as serialized Arrow IPC byte arrays. After the stream is closed, returns cached data captured at close time. Each element can be re-ingested into a recreated stream, or deserialized with `ArrowStreamReader`.

**Lifecycle Methods:** `waitForOffset()`, `flush()`, `close()`, `isClosed()`, `getTableName()`, `getOptions()`

---

### ArrowStreamConfigurationOptions (Beta)

Configuration options for `ZerobusArrowStream`. Build via `ArrowStreamConfigurationOptions.builder()`. See [Arrow Stream Configuration Options](#arrow-stream-configuration-options-beta) for the full table of fields and defaults.

**Static Methods:**

```java
static ArrowStreamConfigurationOptions getDefault()
static ArrowStreamConfigurationOptionsBuilder builder()
```

**Builder methods (chainable):**

```java
setMaxInflightBatches(int)
setRecovery(boolean)
setRecoveryTimeoutMs(long)
setRecoveryBackoffMs(long)
setRecoveryRetries(int)
setServerLackOfAckTimeoutMs(long)
setFlushTimeoutMs(long)
setConnectionTimeoutMs(long)
setIpcCompression(IPCCompressionType)
setStreamPausedMaxWaitTimeMs(long)
build()
```

Example:

```java
ArrowStreamConfigurationOptions options = ArrowStreamConfigurationOptions.builder()
    .setMaxInflightBatches(2000)
    .setIpcCompression(IPCCompressionType.ZSTD)
    .setRecovery(true)
    .setRecoveryRetries(5)
    .build();
```

---

### IPCCompressionType (Enum, Beta)

Selects the Arrow IPC compression codec applied to each batch on the wire. Set via `ArrowStreamConfigurationOptions.builder().setIpcCompression(...)`.

**Values:**

- `NONE` — No compression (default).
- `LZ4_FRAME` — LZ4 frame compression. Fast, modest ratio.
- `ZSTD` — Zstandard. Higher ratio at higher CPU cost.

Compression trades client CPU for fewer bytes on the wire. Enable only when network bandwidth limits throughput.

---

### ZerobusStream\<RecordType\> (Deprecated)

Legacy stream with class-level generics and Future-based API. Use `ZerobusProtoStream` instead.

**Methods:**

```java
CompletableFuture<Void> ingestRecord(RecordType record) throws ZerobusException
```
Ingests a record and returns a Future that completes on acknowledgment.

**Lifecycle Methods:** `waitForOffset()`, `flush()`, `close()`, `isClosed()`

**Accessors:**

```java
TableProperties<RecordType> getTableProperties()
StreamConfigurationOptions getOptions()
String getClientId()
String getClientSecret()
```

---

### TableProperties\<RecordType\>

Configuration for the target table.

**Constructor:**
```java
TableProperties(String tableName, RecordType defaultInstance)
```
- `tableName` - Fully qualified table name (e.g., `catalog.schema.table`)
- `defaultInstance` - Protobuf message default instance (e.g., `MyMessage.getDefaultInstance()`)

**Methods:**

```java
String getTableName()
```
Returns the table name.

```java
Message getDefaultInstance()
```
Returns the protobuf message default instance.

---

### StreamConfigurationOptions

Configuration options for stream behavior.

**Static Methods:**

```java
static StreamConfigurationOptions getDefault()
```
Returns default configuration options.

```java
static StreamConfigurationOptionsBuilder builder()
```
Returns a new builder for creating custom configurations.

---

### StreamConfigurationOptions.StreamConfigurationOptionsBuilder

Builder for creating `StreamConfigurationOptions`.

**Methods:**

```java
StreamConfigurationOptionsBuilder setMaxInflightRecords(int maxInflightRecords)
```
Sets the maximum number of unacknowledged records (default: 50000).

```java
StreamConfigurationOptionsBuilder setRecovery(boolean recovery)
```
Enables or disables automatic stream recovery (default: true).

```java
StreamConfigurationOptionsBuilder setRecoveryTimeoutMs(int recoveryTimeoutMs)
```
Sets the recovery operation timeout in milliseconds (default: 15000).

```java
StreamConfigurationOptionsBuilder setRecoveryBackoffMs(int recoveryBackoffMs)
```
Sets the delay between recovery attempts in milliseconds (default: 2000).

```java
StreamConfigurationOptionsBuilder setRecoveryRetries(int recoveryRetries)
```
Sets the maximum number of recovery attempts (default: 3).

```java
StreamConfigurationOptionsBuilder setFlushTimeoutMs(int flushTimeoutMs)
```
Sets the flush operation timeout in milliseconds (default: 300000).

```java
StreamConfigurationOptionsBuilder setServerLackOfAckTimeoutMs(int serverLackOfAckTimeoutMs)
```
Sets the server acknowledgment timeout in milliseconds (default: 60000).

```java
StreamConfigurationOptionsBuilder setAckCallback(AckCallback ackCallback)
```
Sets a callback to be invoked when records are acknowledged by the server.

```java
StreamConfigurationOptionsBuilder setAckCallback(Consumer<IngestRecordResponse> ackCallback)
```
**Deprecated:** This callback is no longer invoked by the native Rust backend. Use `setAckCallback(AckCallback)` instead.

```java
StreamConfigurationOptions build()
```
Builds and returns the `StreamConfigurationOptions` instance.

---

### IngestRecordResponse (Deprecated)

> **Deprecated:** This type is only used by the deprecated `Consumer<IngestRecordResponse>` callback, which is no longer invoked by the native Rust backend. Use `AckCallback` instead.

Server acknowledgment response containing durability information.

**Methods:**

```java
long getDurabilityAckUpToOffset()
```
Returns the offset up to which all records have been durably written.

---

### StreamState (Enum)

Represents the lifecycle state of a stream.

> **Note:** The native Rust backend does not expose detailed stream states. The deprecated `ZerobusStream.getState()` method only returns `OPENED` or `CLOSED`.

**Values:**
- `UNINITIALIZED` - Stream created but not yet initialized
- `OPENED` - Stream is open and accepting records
- `FLUSHING` - Stream is flushing pending records
- `RECOVERING` - Stream is recovering from a failure
- `CLOSED` - Stream has been gracefully closed
- `FAILED` - Stream has failed and cannot be recovered

---

### ZerobusException

Base exception for retriable errors.

**Constructors:**
```java
ZerobusException(String message)
ZerobusException(String message, Throwable cause)
```

---

### NonRetriableException

Exception for non-retriable errors (extends `ZerobusException`).

**Constructors:**
```java
NonRetriableException(String message)
NonRetriableException(String message, Throwable cause)
```

---

### JsonSerializer\<T\> (Functional Interface)

Interface for serializing objects to JSON strings. Defined in `ZerobusJsonStream`.

```java
String serialize(T object)
```

**Usage with Gson:**
```java
Gson gson = new Gson();
stream.ingestRecordOffset(myObject, gson::toJson);
```

---

### JsonDeserializer\<T\> (Functional Interface)

Interface for deserializing JSON strings to objects. Defined in `ZerobusJsonStream`.

```java
T deserialize(String json)
```

**Usage with Gson:**
```java
List<MyData> unacked = stream.getUnackedRecords(json -> gson.fromJson(json, MyData.class));
```

---

### AckCallback (Interface)

Callback interface for acknowledgment notifications.

```java
void onAck(long offsetId)
```
Called when records up to `offsetId` are acknowledged.

```java
void onError(long offsetId, String errorMessage)
```
Called when an error occurs for records at or after `offsetId`.

**Track durability progress without blocking.** Register an `AckCallback` to observe
acknowledgments as they arrive on a background thread while you keep ingesting — a natural
fit for high-throughput or long-running streams where you want progress notifications
rather than blocking on `flush()` or `waitForOffset()`:

```java
AckCallback callback = new AckCallback() {
    @Override public void onAck(long offsetId) {
        // records up to offsetId are durable (watermark is monotonic)
    }
    @Override public void onError(long offsetId, String errorMessage) {
        System.err.println("Error at offset " + offsetId + ": " + errorMessage);
    }
};

ZerobusProtoStream stream = sdk.streamBuilder()
    .table(tableName)
    .oauth(clientId, clientSecret)
    .ackCallback(callback)
    .compiledProto(descriptor)
    .build()
    .join();

// Ingest without blocking; the callback fires as acks arrive.
for (AirQuality record : records) {
    stream.ingestRecordOffset(record);
}
stream.flush(); // drain remaining acks before close
```

Implementations must be thread-safe and lightweight (callbacks run on internal
processing threads).

## Best Practices

1. **Reuse SDK instances**: Create one `ZerobusSdk` instance per application
2. **Stream lifecycle**: Always close streams in a `finally` block or use try-with-resources
3. **Use offset-based API for high throughput**: `ingestRecordOffset()` avoids `CompletableFuture` overhead
4. **Ingest in a loop, then `flush()`**: Confirm durability once after a batch with `flush()` (or `waitForOffset()` on the last offset, since acks are ordered). Use per-record waits only when a specific record must be confirmed before continuing.
5. **Batch records when possible**: Use `ingestRecordsOffset()` for multiple records
6. **Configure `maxInflightRecords`**: Adjust based on your throughput and memory requirements
7. **Implement proper error handling**: Distinguish between retriable and non-retriable errors
8. **Use `AckCallback` for monitoring**: Track acknowledgment progress without blocking the ingest loop
9. **Proto generation**: Use the built-in `GenerateProto` tool to generate proto files from table schemas
10. **Choose the right API**:
    - `ingestRecordOffset()` + final `flush()` / `waitForOffset(lastOffset)` → High throughput (recommended)
    - `ingestRecordOffset()` + `waitForOffset()` per record → When a specific record must be confirmed before continuing
    - `ingestRecord().join()` → Deprecated; prefer the offset-based API
11. **Recovery pattern**: Use `sdk.recreateStream(closedStream)` to automatically re-ingest unacknowledged records, or manually use `getUnackedBatches()` after stream close

## Community and Contributing

This is an open source project. We welcome contributions, feedback, and bug reports.

- **[Contributing Guide](https://github.com/databricks/zerobus-sdk/blob/main/java/CONTRIBUTING.md)**: Java-specific development setup and workflow.
- **[General Contributing Guide](https://github.com/databricks/zerobus-sdk/blob/main/CONTRIBUTING.md)**: Pull request process, commit requirements, and policies.
- **[Changelog](https://github.com/databricks/zerobus-sdk/blob/main/java/CHANGELOG.md)**: See the history of changes in the SDK.
- **[Security Policy](https://github.com/databricks/zerobus-sdk/blob/main/SECURITY.md)**: Read about our security process and how to report vulnerabilities.
- **[Developer Certificate of Origin (DCO)](https://github.com/databricks/zerobus-sdk/blob/main/DCO)**: Understand the agreement for contributions.
- **[Open Source Attributions](https://github.com/databricks/zerobus-sdk/blob/main/java/NOTICE)**: See a list of the open source libraries we use.

## License

This SDK is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for the full text.
