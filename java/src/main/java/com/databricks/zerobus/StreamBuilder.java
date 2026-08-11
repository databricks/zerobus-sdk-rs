package com.databricks.zerobus;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Fluent builder for creating Zerobus ingestion streams.
 *
 * <p>This is the recommended way to create a stream. It mirrors the {@code stream_builder()} API of
 * the Rust SDK. Shared setters may be called in any order before selecting a record format. Because
 * Java cannot return different stream types from a single {@code build()}, the record format is
 * selected with a terminal method that returns a typed sub-builder: {@link #json()}, {@link
 * #compiledProto(DescriptorProto)}, or {@link #arrow(Schema)}. Each sub-builder exposes a {@code
 * build()} that returns the matching stream type.
 *
 * <h3>Thread Safety</h3>
 *
 * <p>A {@code StreamBuilder} is not thread-safe and is intended to be configured and built from a
 * single thread.
 *
 * @see ZerobusSdk#streamBuilder()
 * @see StreamConfigurationOptions
 * @see ArrowStreamConfigurationOptions
 */
public final class StreamBuilder {

  private final ZerobusSdk sdk;

  private String tableName;

  // Authentication. OAuth credentials and a custom headers provider are mutually exclusive.
  private String clientId;
  private String clientSecret;
  private HeadersProvider headersProvider;

  // Shared and gRPC configuration. A {@code null} value means "not set", so each record format
  // falls back to its own defaults (for example Arrow defaults to 4 recovery retries while gRPC
  // defaults to 3). This mirrors the Rust builder, which keeps separate gRPC and Arrow configs.
  private Integer maxInflightRecords;
  private Boolean recovery;
  private Integer recoveryTimeoutMs;
  private Integer recoveryBackoffMs;
  private Integer recoveryRetries;
  private Integer serverLackOfAckTimeoutMs;
  private Integer flushTimeoutMs;
  private AckCallback ackCallback;

  /** Package-private constructor. Use {@link ZerobusSdk#streamBuilder()} to create instances. */
  StreamBuilder(ZerobusSdk sdk) {
    this.sdk = sdk;
  }

  /**
   * Sets the fully qualified Unity Catalog table name (for example {@code "catalog.schema.table"}).
   *
   * @param tableName the fully qualified table name
   * @return this builder for method chaining
   */
  public StreamBuilder table(String tableName) {
    this.tableName = requireNonBlank(tableName, "tableName");
    return this;
  }

  /**
   * Authenticates with OAuth client credentials.
   *
   * @param clientId the OAuth client ID
   * @param clientSecret the OAuth client secret
   * @return this builder for method chaining
   */
  public StreamBuilder oauth(String clientId, String clientSecret) {
    this.clientId = requireNonBlank(clientId, "clientId");
    this.clientSecret = requireNonBlank(clientSecret, "clientSecret");
    this.headersProvider = null;
    return this;
  }

  /**
   * Authenticates with a custom headers provider.
   *
   * @param headersProvider the provider for authentication and request headers
   * @return this builder for method chaining
   */
  public StreamBuilder headersProvider(HeadersProvider headersProvider) {
    this.headersProvider = Objects.requireNonNull(headersProvider, "headersProvider");
    this.clientId = null;
    this.clientSecret = null;
    return this;
  }

  /**
   * Enables or disables automatic stream recovery.
   *
   * @param recovery true to enable automatic recovery, false to disable
   * @return this builder for method chaining
   */
  public StreamBuilder recovery(boolean recovery) {
    this.recovery = recovery;
    return this;
  }

  /**
   * Sets the timeout for recovery operations.
   *
   * @param recoveryTimeoutMs the recovery timeout in milliseconds
   * @return this builder for method chaining
   * @throws IllegalArgumentException if {@code recoveryTimeoutMs} is not positive
   */
  public StreamBuilder recoveryTimeoutMs(int recoveryTimeoutMs) {
    this.recoveryTimeoutMs = requirePositive(recoveryTimeoutMs, "recoveryTimeoutMs");
    return this;
  }

  /**
   * Sets the backoff delay between recovery attempts.
   *
   * @param recoveryBackoffMs the recovery backoff delay in milliseconds
   * @return this builder for method chaining
   * @throws IllegalArgumentException if {@code recoveryBackoffMs} is negative
   */
  public StreamBuilder recoveryBackoffMs(int recoveryBackoffMs) {
    this.recoveryBackoffMs = requireNonNegative(recoveryBackoffMs, "recoveryBackoffMs");
    return this;
  }

  /**
   * Sets the maximum number of recovery attempts.
   *
   * @param recoveryRetries the maximum number of recovery attempts
   * @return this builder for method chaining
   * @throws IllegalArgumentException if {@code recoveryRetries} is negative
   */
  public StreamBuilder recoveryRetries(int recoveryRetries) {
    this.recoveryRetries = requireNonNegative(recoveryRetries, "recoveryRetries");
    return this;
  }

  /**
   * Sets the timeout for server acknowledgment.
   *
   * @param serverLackOfAckTimeoutMs the server acknowledgment timeout in milliseconds
   * @return this builder for method chaining
   * @throws IllegalArgumentException if {@code serverLackOfAckTimeoutMs} is not positive
   */
  public StreamBuilder serverLackOfAckTimeoutMs(int serverLackOfAckTimeoutMs) {
    this.serverLackOfAckTimeoutMs =
        requirePositive(serverLackOfAckTimeoutMs, "serverLackOfAckTimeoutMs");
    return this;
  }

  /**
   * Sets the timeout for flush operations.
   *
   * @param flushTimeoutMs the flush timeout in milliseconds
   * @return this builder for method chaining
   * @throws IllegalArgumentException if {@code flushTimeoutMs} is not positive
   */
  public StreamBuilder flushTimeoutMs(int flushTimeoutMs) {
    this.flushTimeoutMs = requirePositive(flushTimeoutMs, "flushTimeoutMs");
    return this;
  }

  /**
   * Sets the maximum number of in-flight records.
   *
   * <p>Applies to JSON and Protocol Buffer (gRPC) streams. It is ignored for Arrow streams, which
   * use {@link ArrowStreamBuilder#maxInflightBatches(int)} instead.
   *
   * @param maxInflightRecords the maximum number of in-flight records
   * @return this builder for method chaining
   * @throws IllegalArgumentException if {@code maxInflightRecords} is not positive
   */
  public StreamBuilder maxInflightRecords(int maxInflightRecords) {
    this.maxInflightRecords = requirePositive(maxInflightRecords, "maxInflightRecords");
    return this;
  }

  /**
   * Sets the acknowledgment callback.
   *
   * <p>Applies to JSON and Protocol Buffer (gRPC) streams.
   *
   * @param ackCallback the acknowledgment callback
   * @return this builder for method chaining
   */
  public StreamBuilder ackCallback(AckCallback ackCallback) {
    this.ackCallback = Objects.requireNonNull(ackCallback, "ackCallback");
    return this;
  }

  /**
   * Selects the JSON record format.
   *
   * @return a {@link JsonStreamBuilder} that builds a {@link ZerobusJsonStream}
   */
  public JsonStreamBuilder json() {
    return new JsonStreamBuilder(this);
  }

  /**
   * Selects the Protocol Buffer record format.
   *
   * @param descriptorProto the Protocol Buffer descriptor proto for the message type
   * @return a {@link ProtoStreamBuilder} that builds a {@link ZerobusProtoStream}
   */
  public ProtoStreamBuilder compiledProto(DescriptorProto descriptorProto) {
    Objects.requireNonNull(descriptorProto, "descriptorProto");
    return new ProtoStreamBuilder(this, descriptorProto);
  }

  /**
   * Selects the Arrow Flight record format.
   *
   * <p><b>Beta:</b> Arrow Flight ingestion is in Beta. The API is stabilising but may still change
   * before reaching GA.
   *
   * @param schema the Arrow schema describing the columns of the target table
   * @return an {@link ArrowStreamBuilder} that builds a {@link ZerobusArrowStream}
   */
  public ArrowStreamBuilder arrow(Schema schema) {
    Objects.requireNonNull(schema, "schema");
    return new ArrowStreamBuilder(this, schema);
  }

  // ==================== Package-private helpers ====================

  /** Validates that the required table name and authentication have been configured. */
  void validateRequired() {
    if (isBlank(tableName)) {
      throw new IllegalStateException("table name is required: call table()");
    }
    if (headersProvider == null && (isBlank(clientId) || isBlank(clientSecret))) {
      throw new IllegalStateException(
          "authentication is required: call oauth() or headersProvider()");
    }
  }

  // Numeric setters cross the JNI boundary and are cast to unsigned Rust integers (usize / u64 /
  // u32). A negative value would silently become an enormous positive limit or timeout, so reject
  // out-of-range values here rather than letting them through. {@code streamPausedMaxWaitTimeMs} is
  // the one knob where a negative value is meaningful (it means "wait the full server-specified
  // duration") and is handled separately on the Arrow sub-builder.

  private static int requirePositive(int value, String name) {
    if (value <= 0) {
      throw new IllegalArgumentException(name + " must be positive, got: " + value);
    }
    return value;
  }

  private static long requirePositive(long value, String name) {
    if (value <= 0) {
      throw new IllegalArgumentException(name + " must be positive, got: " + value);
    }
    return value;
  }

  private static int requireNonNegative(int value, String name) {
    if (value < 0) {
      throw new IllegalArgumentException(name + " must not be negative, got: " + value);
    }
    return value;
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (isBlank(value)) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }

  /** Builds the gRPC stream options, applying only the values that were explicitly set. */
  StreamConfigurationOptions toStreamOptions() {
    StreamConfigurationOptions.StreamConfigurationOptionsBuilder builder =
        StreamConfigurationOptions.builder();
    if (maxInflightRecords != null) {
      builder.setMaxInflightRecords(maxInflightRecords);
    }
    if (recovery != null) {
      builder.setRecovery(recovery);
    }
    if (recoveryTimeoutMs != null) {
      builder.setRecoveryTimeoutMs(recoveryTimeoutMs);
    }
    if (recoveryBackoffMs != null) {
      builder.setRecoveryBackoffMs(recoveryBackoffMs);
    }
    if (recoveryRetries != null) {
      builder.setRecoveryRetries(recoveryRetries);
    }
    if (serverLackOfAckTimeoutMs != null) {
      builder.setServerLackOfAckTimeoutMs(serverLackOfAckTimeoutMs);
    }
    if (flushTimeoutMs != null) {
      builder.setFlushTimeoutMs(flushTimeoutMs);
    }
    if (ackCallback != null) {
      builder.setAckCallback(ackCallback);
    }
    return builder.build();
  }

  // ==================== Typed sub-builders ====================

  /** Builds a {@link ZerobusJsonStream}. Created via {@link StreamBuilder#json()}. */
  public static final class JsonStreamBuilder {
    private final StreamBuilder base;

    private JsonStreamBuilder(StreamBuilder base) {
      this.base = base;
    }

    /**
     * Builds and opens the JSON stream.
     *
     * @return a future that completes with the {@link ZerobusJsonStream} when ready
     * @throws IllegalStateException if the table name or authentication has not been set
     */
    public CompletableFuture<ZerobusJsonStream> build() {
      base.validateRequired();
      return base.sdk.createJsonStreamInternal(
          base.tableName,
          base.clientId,
          base.clientSecret,
          base.headersProvider,
          base.toStreamOptions());
    }
  }

  /** Builds a {@link ZerobusProtoStream}. Created via {@link StreamBuilder#compiledProto}. */
  public static final class ProtoStreamBuilder {
    private final StreamBuilder base;
    private final DescriptorProto descriptorProto;

    private ProtoStreamBuilder(StreamBuilder base, DescriptorProto descriptorProto) {
      this.base = base;
      this.descriptorProto = descriptorProto;
    }

    /**
     * Builds and opens the Protocol Buffer stream.
     *
     * @return a future that completes with the {@link ZerobusProtoStream} when ready
     * @throws IllegalStateException if the table name or authentication has not been set
     */
    public CompletableFuture<ZerobusProtoStream> build() {
      base.validateRequired();
      return base.sdk.createProtoStreamInternal(
          base.tableName,
          descriptorProto,
          base.clientId,
          base.clientSecret,
          base.headersProvider,
          base.toStreamOptions());
    }
  }

  /**
   * Builds a {@link ZerobusArrowStream}. Created via {@link StreamBuilder#arrow(Schema)}.
   *
   * <p><b>Beta:</b> Arrow Flight ingestion is in Beta.
   */
  public static final class ArrowStreamBuilder {
    private final StreamBuilder base;
    private final Schema schema;

    private Integer maxInflightBatches;
    private Long connectionTimeoutMs;
    private IPCCompressionType ipcCompression;
    private Long streamPausedMaxWaitTimeMs;

    private ArrowStreamBuilder(StreamBuilder base, Schema schema) {
      this.base = base;
      this.schema = schema;
    }

    /**
     * Sets the maximum number of in-flight Arrow batches.
     *
     * @param maxInflightBatches the maximum number of in-flight batches
     * @return this builder for method chaining
     * @throws IllegalArgumentException if {@code maxInflightBatches} is not positive
     */
    public ArrowStreamBuilder maxInflightBatches(int maxInflightBatches) {
      this.maxInflightBatches = requirePositive(maxInflightBatches, "maxInflightBatches");
      return this;
    }

    /**
     * Sets the connection timeout for the Arrow Flight connection.
     *
     * @param connectionTimeoutMs the connection timeout in milliseconds
     * @return this builder for method chaining
     * @throws IllegalArgumentException if {@code connectionTimeoutMs} is not positive
     */
    public ArrowStreamBuilder connectionTimeoutMs(long connectionTimeoutMs) {
      this.connectionTimeoutMs = requirePositive(connectionTimeoutMs, "connectionTimeoutMs");
      return this;
    }

    /**
     * Sets the Arrow IPC compression codec.
     *
     * @param ipcCompression the compression codec to use
     * @return this builder for method chaining
     */
    public ArrowStreamBuilder ipcCompression(IPCCompressionType ipcCompression) {
      this.ipcCompression = ipcCompression;
      return this;
    }

    /**
     * Sets the maximum wait time during a graceful stream pause.
     *
     * @param streamPausedMaxWaitTimeMs the maximum wait time in milliseconds, or a negative value
     *     to wait the full server-specified duration
     * @return this builder for method chaining
     */
    public ArrowStreamBuilder streamPausedMaxWaitTimeMs(long streamPausedMaxWaitTimeMs) {
      this.streamPausedMaxWaitTimeMs = streamPausedMaxWaitTimeMs;
      return this;
    }

    /** Builds the Arrow stream options, applying only the values that were explicitly set. */
    ArrowStreamConfigurationOptions buildOptions() {
      ArrowStreamConfigurationOptions.ArrowStreamConfigurationOptionsBuilder builder =
          ArrowStreamConfigurationOptions.builder();
      if (base.recovery != null) {
        builder.setRecovery(base.recovery);
      }
      if (base.recoveryTimeoutMs != null) {
        builder.setRecoveryTimeoutMs(base.recoveryTimeoutMs);
      }
      if (base.recoveryBackoffMs != null) {
        builder.setRecoveryBackoffMs(base.recoveryBackoffMs);
      }
      if (base.recoveryRetries != null) {
        builder.setRecoveryRetries(base.recoveryRetries);
      }
      if (base.serverLackOfAckTimeoutMs != null) {
        builder.setServerLackOfAckTimeoutMs(base.serverLackOfAckTimeoutMs);
      }
      if (base.flushTimeoutMs != null) {
        builder.setFlushTimeoutMs(base.flushTimeoutMs);
      }
      if (maxInflightBatches != null) {
        builder.setMaxInflightBatches(maxInflightBatches);
      }
      if (connectionTimeoutMs != null) {
        builder.setConnectionTimeoutMs(connectionTimeoutMs);
      }
      if (ipcCompression != null) {
        builder.setIpcCompression(ipcCompression);
      }
      if (streamPausedMaxWaitTimeMs != null) {
        builder.setStreamPausedMaxWaitTimeMs(streamPausedMaxWaitTimeMs);
      }
      return builder.build();
    }

    /**
     * Builds and opens the Arrow Flight stream.
     *
     * @return a future that completes with the {@link ZerobusArrowStream} when ready
     * @throws IllegalStateException if the table name or authentication has not been set
     */
    public CompletableFuture<ZerobusArrowStream> build() {
      base.validateRequired();
      return base.sdk.createArrowStreamInternal(
          base.tableName,
          schema,
          base.clientId,
          base.clientSecret,
          base.headersProvider,
          buildOptions());
    }
  }
}
