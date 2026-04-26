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
- [API Styles](#api-styles)
  - [Offset-Based API (Recommended)](#offset-based-api-recommended)
  - [Future-Based API](#future-based-api)
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
- **Offset-based API**: Low-overhead alternative to CompletableFuture for high throughput
- **OAuth 2.0 authentication**: Secure authentication with client credentials
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
| Linux    | x86_64       | Supported |
| Linux    | aarch64      | Supported |
| Windows  | x86_64       | Supported |
| macOS    | x86_64       | Supported |
| macOS    | aarch64 (Apple Silicon) | Supported |

### Dependencies

**When using the fat JAR** (recommended for most users):
- No additional dependencies required - all dependencies are bundled

**When using the regular JAR**:
- [`protobuf-java` 4.33.0](https://mvnrepository.com/artifact/com.google.protobuf/protobuf-java/4.33.0)
- [`slf4j-api` 2.0.17](https://mvnrepository.com/artifact/org.slf4j/slf4j-api/2.0.17)
- An SLF4J implementation such as [`slf4j-simple` 2.0.17](https://mvnrepository.com/artifact/org.slf4j/slf4j-simple/2.0.17) or [`logback-classic` 1.4.14](https://mvnrepository.com/artifact/ch.qos.logback/logback-classic/1.4.14)

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
mvn clean package
```

This generates two JAR files in the `target/` directory:

- **Regular JAR**: `zerobus-ingest-sdk-0.2.0.jar` (~12MB, includes native libraries)
  - Contains only the SDK classes
  - Requires all dependencies on the classpath

- **Fat JAR**: `zerobus-ingest-sdk-0.2.0-jar-with-dependencies.jar` (~19MB, includes native libraries + all dependencies)
  - Contains SDK classes plus all dependencies bundled
  - Self-contained, easier to deploy

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

        // Configure table properties
        TableProperties<AirQuality> tableProperties = new TableProperties<>(
            tableName,
            AirQuality.getDefaultInstance()
        );

        // Create stream
        ZerobusStream<AirQuality> stream = sdk.createStream(
            tableProperties,
            clientId,
            clientSecret
        ).join();

        try {
            // Ingest records
            for (int i = 0; i < 100; i++) {
                AirQuality record = AirQuality.newBuilder()
                    .setDeviceName("sensor-" + (i % 10))
                    .setTemp(20 + (i % 15))
                    .setHumidity(50 + (i % 40))
                    .build();

                stream.ingestRecord(record).join(); // Wait for durability

                System.out.println("Ingested record " + (i + 1));
            }

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
Ingested record 1
Ingested record 2
...
Successfully ingested 100 records!
```

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
└── legacy/                # ZerobusStream (deprecated)
    └── LegacyStreamExample.java
```

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

**Clean JSON API** - use `createJsonStream()` for a simplified experience:

```java
// No proto types or configuration needed!
ZerobusJsonStream stream = sdk.createJsonStream(tableName, clientId, clientSecret).join();
stream.ingestRecordOffset("{\"field\": \"value\"}");
```

See [`examples/README.md`](https://github.com/databricks/zerobus-sdk/blob/main/java/examples/README.md) for detailed documentation.

---

## API Styles

The SDK provides two ingestion styles:

| Style | Status | Best For | Overhead |
|-------|--------|----------|----------|
| **Offset-Based** | Recommended | All use cases | Minimal - no object allocation |
| **Future-Based** | Deprecated | Legacy code | CompletableFuture per record |

### Offset-Based API (Recommended)

Use `ZerobusProtoStream` or `ZerobusJsonStream` for all new code. They use offset-based returns that avoid `CompletableFuture` allocation overhead:

```java
ZerobusProtoStream stream = sdk.createProtoStream(
    tableName, AirQuality.getDescriptor().toProto(), clientId, clientSecret
).join();

try {
    long lastOffset = -1;

    // Ingest records as fast as possible
    for (int i = 0; i < 1000000; i++) {
        AirQuality record = AirQuality.newBuilder()
            .setDeviceName("sensor-" + (i % 100))
            .setTemp(20 + i % 15)
            .setHumidity(50 + i % 40)
            .build();

        // Returns immediately after queuing (non-blocking)
        lastOffset = stream.ingestRecordOffset(record);
    }

    // Wait for all records to be acknowledged
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
// DEPRECATED - use ingestRecordOffset() instead
try {
    for (int i = 0; i < 1000; i++) {
        AirQuality record = AirQuality.newBuilder()
            .setDeviceName("sensor-" + i)
            .setTemp(20 + i % 15)
            .build();

        stream.ingestRecord(record).join();  // Deprecated
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

### JSON Streams (Recommended for JSON)

Use `createJsonStream()` for a clean API that doesn't require Protocol Buffer types:

```java
// Create JSON stream - no proto types needed!
ZerobusJsonStream stream = sdk.createJsonStream(
    "catalog.schema.table",
    clientId,
    clientSecret
).join();

try {
    // Ingest JSON string directly
    long offset = stream.ingestRecordOffset("{\"device_name\": \"sensor-1\", \"temp\": 25}");
    stream.waitForOffset(offset);

    // Or use objects with a serializer (Gson, Jackson, etc.)
    Gson gson = new Gson();
    Map<String, Object> data = Map.of("device_name", "sensor-2", "temp", 26);
    offset = stream.ingestRecordOffset(data, gson::toJson);

    // Batch ingestion
    List<String> batch = Arrays.asList(
        "{\"device_name\": \"sensor-1\", \"temp\": 25}",
        "{\"device_name\": \"sensor-2\", \"temp\": 26}"
    );
    Optional<Long> batchOffset = stream.ingestRecordsOffset(batch);
    batchOffset.ifPresent(stream::waitForOffset);
} finally {
    stream.close();
    sdk.close();
}
```

With custom options:

```java
StreamConfigurationOptions options = StreamConfigurationOptions.builder()
    .setMaxInflightRecords(10000)
    .build();

ZerobusJsonStream stream = sdk.createJsonStream(
    tableName, clientId, clientSecret, options
).join();
```

## Configuration

### Stream Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `maxInflightRecords` | 50000 | Maximum number of unacknowledged records |
| `recovery` | true | Enable automatic stream recovery |
| `recoveryTimeoutMs` | 15000 | Timeout for recovery operations (ms) |
| `recoveryBackoffMs` | 2000 | Delay between recovery attempts (ms) |
| `recoveryRetries` | 3 | Maximum number of recovery attempts |
| `flushTimeoutMs` | 300000 | Timeout for flush operations (ms) |
| `serverLackOfAckTimeoutMs` | 60000 | Server acknowledgment timeout (ms) |
| `ackCallback` | None | Callback invoked on record acknowledgment |

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

**Constructor:**
```java
ZerobusSdk(String serverEndpoint, String unityCatalogEndpoint)
```
- `serverEndpoint` - The Zerobus gRPC endpoint (e.g., `https://<workspace-id>.zerobus.region.cloud.databricks.com`)
- `unityCatalogEndpoint` - The Unity Catalog endpoint (your workspace URL)

**Methods:**

```java
<RecordType extends Message> CompletableFuture<ZerobusStream<RecordType>> createStream(
    TableProperties<RecordType> tableProperties,
    String clientId,
    String clientSecret,
    StreamConfigurationOptions options
)
```
Creates a new Protocol Buffer ingestion stream with custom configuration. Returns a CompletableFuture that completes when the stream is ready.

```java
<RecordType extends Message> CompletableFuture<ZerobusStream<RecordType>> createStream(
    TableProperties<RecordType> tableProperties,
    String clientId,
    String clientSecret
)
```
Creates a new Protocol Buffer ingestion stream with default configuration. Returns a CompletableFuture that completes when the stream is ready.

```java
CompletableFuture<ZerobusJsonStream> createJsonStream(
    String tableName,
    String clientId,
    String clientSecret
)
```
Creates a new JSON ingestion stream with default configuration. No Protocol Buffer types required.

```java
CompletableFuture<ZerobusJsonStream> createJsonStream(
    String tableName,
    String clientId,
    String clientSecret,
    StreamConfigurationOptions options
)
```
Creates a new JSON ingestion stream with custom configuration. No Protocol Buffer types required.

```java
CompletableFuture<ZerobusProtoStream> recreateStream(ZerobusProtoStream closedStream)
```
Recreates a Proto stream from a closed stream, re-ingesting unacknowledged records and flushing.

```java
CompletableFuture<ZerobusJsonStream> recreateStream(ZerobusJsonStream closedStream)
```
Recreates a JSON stream from a closed stream, re-ingesting unacknowledged records and flushing.

---

### ZerobusProtoStream

Stream for Protocol Buffer ingestion with method-level generics. Use `ZerobusSdk.createProtoStream()` to create instances.

**Single Record Methods:**

```java
<T extends Message> long ingestRecordOffset(T record) throws ZerobusException
```
Ingests a Protocol Buffer message and returns the offset immediately.

```java
<T extends Message> void ingestRecordNoWait(T record) throws ZerobusException
```
Queues a protobuf message without returning an offset or waiting for acknowledgment.

```java
long ingestRecordOffset(byte[] encodedBytes) throws ZerobusException
```
Ingests pre-encoded bytes and returns the offset immediately.

```java
void ingestRecordNoWait(byte[] encodedBytes) throws ZerobusException
```
Queues pre-encoded bytes without returning an offset or waiting for acknowledgment.

**Batch Methods:**

```java
<T extends Message> Optional<Long> ingestRecordsOffset(Iterable<T> records) throws ZerobusException
```
Ingests multiple messages and returns the batch offset.

```java
<T extends Message> void ingestRecordsNoWait(Iterable<T> records) throws ZerobusException
```
Queues multiple messages without returning an offset or waiting for acknowledgment.

```java
Optional<Long> ingestRecordsOffset(List<byte[]> encodedRecords) throws ZerobusException
```
Ingests multiple pre-encoded byte arrays and returns the batch offset.

```java
void ingestRecordsNoWait(List<byte[]> encodedRecords) throws ZerobusException
```
Queues multiple pre-encoded byte arrays without returning an offset or waiting for acknowledgment.

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

Stream for JSON ingestion with method-level generics. Use `ZerobusSdk.createJsonStream()` to create instances.

**Single Record Methods:**

```java
<T> long ingestRecordOffset(T object, JsonSerializer<T> serializer) throws ZerobusException
```
Ingests an object serialized to JSON and returns the offset immediately.

```java
<T> void ingestRecordNoWait(T object, JsonSerializer<T> serializer) throws ZerobusException
```
Queues an object serialized to JSON without returning an offset or waiting for acknowledgment.

```java
long ingestRecordOffset(String json) throws ZerobusException
```
Ingests a JSON string and returns the offset immediately.

```java
void ingestRecordNoWait(String json) throws ZerobusException
```
Queues a JSON string without returning an offset or waiting for acknowledgment.

**Batch Methods:**

```java
<T> Optional<Long> ingestRecordsOffset(Iterable<T> objects, JsonSerializer<T> serializer) throws ZerobusException
```
Ingests multiple objects as JSON and returns the batch offset.

```java
<T> void ingestRecordsNoWait(Iterable<T> objects, JsonSerializer<T> serializer) throws ZerobusException
```
Queues multiple objects as JSON without returning an offset or waiting for acknowledgment.

```java
Optional<Long> ingestRecordsOffset(Iterable<String> jsonStrings) throws ZerobusException
```
Ingests multiple JSON strings and returns the batch offset.

```java
void ingestRecordsNoWait(Iterable<String> jsonStrings) throws ZerobusException
```
Queues multiple JSON strings without returning an offset or waiting for acknowledgment.

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

### ZerobusStream\<RecordType\> (Deprecated)

Legacy stream with class-level generics and Future-based API. Use `ZerobusProtoStream` instead.

**Methods:**

```java
CompletableFuture<Void> ingestRecord(RecordType record) throws ZerobusException
```
Ingests a record and returns a Future that completes on acknowledgment.

```java
void ingestRecordNoWait(RecordType record) throws ZerobusException
```
Queues a record without returning an offset or waiting for acknowledgment.

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

## Best Practices

1. **Reuse SDK instances**: Create one `ZerobusSdk` instance per application
2. **Stream lifecycle**: Always close streams in a `finally` block or use try-with-resources
3. **Use no-wait ingestion when per-record offsets are unnecessary**: `ingestRecordNoWait()` applies normal native backpressure but does not return an offset; call `flush()` or `close()` before shutdown when durability matters
4. **Use offset-based API for tracked ingestion**: `ingestRecordOffset()` avoids `CompletableFuture` overhead while still letting you wait on specific offsets
5. **Batch records when possible**: Use `ingestRecordsOffset()` for multiple records
6. **Configure `maxInflightRecords`**: Adjust based on your throughput and memory requirements
7. **Implement proper error handling**: Distinguish between retriable and non-retriable errors
8. **Use `AckCallback` for monitoring**: Track acknowledgment progress without blocking
9. **Proto generation**: Use the built-in `GenerateProto` tool to generate proto files from table schemas
10. **Choose the right API**:
   - `ingestRecord()` → Simple use cases, moderate throughput (deprecated)
   - `ingestRecordNoWait()` → Fire-and-forget when per-record tracking is unnecessary
   - `ingestRecordOffset()` + `waitForOffset()` → High throughput, fine-grained control (recommended)
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
