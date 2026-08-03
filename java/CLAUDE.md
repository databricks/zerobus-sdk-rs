# Java SDK

Java wrapper around the Rust core via JNI.

## Client code patterns (read before writing or reviewing examples/docs)

Ingestion is asynchronous. `ingestRecordOffset()` (and `ingestRecordsOffset()`) return as
soon as the record/batch is queued — the SDK sends it and tracks its acknowledgment in the
background.

- **Idiomatic flow:** ingest in a loop, then call `flush()` once (after a bounded batch, or
  periodically for a long-running stream) to confirm durability. Equivalently, call
  `waitForOffset()` on the **last** returned offset — acks are ordered, so confirming the
  last offset confirms every prior record.
- **Async monitoring:** register an `AckCallback` (`onAck`/`onError`) to track progress
  without blocking the ingest loop.
- **Per-record `waitForOffset()`:** use when a specific record must be confirmed before
  continuing. Avoid calling it after every record in a tight loop, since that limits
  throughput to one record per round-trip.
- New code should use `ZerobusProtoStream` / `ZerobusJsonStream` (offset-based) rather than
  the deprecated `ZerobusStream`. With the deprecated API, keep the last future and `.join()`
  once after the loop rather than joining per record (the latter is discouraged for the same
  throughput reason).

## Structure

```
java/
├── src/main/java/com/databricks/zerobus/
│   ├── ZerobusSdk.java          # Main SDK class
│   ├── ZerobusStream.java       # Legacy generic stream (deprecated)
│   ├── ZerobusProtoStream.java  # Proto ingestion stream
│   ├── ZerobusJsonStream.java   # JSON ingestion stream
│   ├── ZerobusArrowStream.java  # Arrow Flight stream (Beta)
│   ├── NativeLoader.java        # Platform-specific JNI library loading
│   └── ...                      # Config, exceptions, callbacks
├── src/test/                    # JUnit 5 + Mockito tests
├── pom.xml                      # Maven config
└── tools/                       # Schema generation
```

The JNI native library is built from `rust/jni/`. It's packaged inside the JAR for all supported platforms.

## Build commands

Run from `java/`:

- `mvn compile` — Compile
- `mvn test` — Run tests
- `mvn test -Dtest=ClassName#method` — Run specific test
- `mvn clean package -Dzerobus.skipNativeLibCheck=true` — Build local Java JARs without staged release JNI libraries
- `mvn spotless:check` — Check formatting (Google Java Format)
- `mvn spotless:apply` — Auto-format

## FFI boundary: JNI

JNI is the most manual of all the bindings. Key considerations:

- **Handle pattern**: Java holds a `long nativeHandle` field — an opaque pointer to the Rust object. All native methods pass this handle. If the handle is invalid (use-after-close), Rust panics or returns an error.
- **No finalizers**: Unlike Go/Python/TypeScript, Java streams do **not** have GC-triggered cleanup. Users **must** call `close()` explicitly or use try-with-resources. Forgetting this leaks native memory.
- **AutoCloseable**: SDK and stream classes implement `AutoCloseable`. Always prefer try-with-resources.
- **Async bridge**: `CompletableFuture` bridges Rust futures to Java. Stream creation and acknowledgment waiting return futures.
- **Thread safety**: The Java SDK is **not thread-safe**. Do not share SDK or stream instances across threads without external synchronization.
- **Native library loading** (`NativeLoader.java`): Detects OS + arch, extracts the correct `.so`/`.dylib`/`.dll` from the JAR classpath to a temp directory, loads via `System.load()`. Marked `deleteOnExit()`.

## Breaking change rules

Public API is everything in `com.databricks.zerobus` with `public` visibility:

- Removing or renaming public classes, methods, or fields is breaking.
- Changing method signatures (parameter types, return types, checked exceptions) is breaking.
- Deprecate with `@Deprecated` annotation and Javadoc explaining the replacement.
- JNI native method signatures (`private static native ...`) are internal but must stay in sync with `rust/jni/` — changing one without the other causes `UnsatisfiedLinkError` at runtime.

## Performance notes

- JNI calls have non-trivial overhead (~100ns per crossing). Batch APIs reduce crossings.
- Proto descriptors are passed as `byte[]` — copied at the boundary. This is a one-time cost at stream creation.
- Record payloads (`byte[]` for proto, `String` for JSON) are copied across JNI. For high-throughput, prefer proto with batch ingestion.
- Native library extraction happens once at class load time — first-use latency includes temp file I/O.

## Changelog and documentation

- Every PR must update `java/NEXT_CHANGELOG.md` under the appropriate section if it changes user-facing behavior.
- Update `java/README.md` if the change affects usage, setup, or API surface.
- Add or update examples in `java/examples/` for new or modified APIs.
- Add Javadoc for all new public classes/methods.

## Release

- Version source: `java/pom.xml` (`<version>x.y.z</version>`).
- Tag: `java/v<semver>` → triggers `release-java.yml` → builds JNI native libs for all platforms, packages into JAR → publishes to Maven Central.
- The JAR bundles native libraries for all 5 platforms. The JNI release depends on `rust/jni/` — if Rust JNI code changed, both must be coordinated.
- On version bump PR: update version in `pom.xml`, move `NEXT_CHANGELOG.md` contents to `CHANGELOG.md`, reset `NEXT_CHANGELOG.md`.

## Config

- Java 8+ compatibility (source/target 1.8)
- Google Java Format via Spotless
- Arrow version: 17.0.0
