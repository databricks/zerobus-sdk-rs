package com.databricks.zerobus;

import com.google.protobuf.Message;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main entry point for the Zerobus SDK.
 *
 * <p>This class provides methods to create streams for ingesting records into Databricks tables. It
 * handles authentication, connection management, and stream lifecycle operations.
 *
 * <p>The SDK uses a native Rust implementation via JNI for optimal performance. The native library
 * is loaded automatically when the SDK is first used.
 *
 * <h3>Resource Management</h3>
 *
 * <p>This class holds native resources that are not automatically released by the garbage
 * collector. You <b>must</b> call {@link #close()} when done to avoid native memory leaks. Use
 * try-with-resources for automatic cleanup:
 *
 * <pre>{@code
 * try (ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, unityCatalogEndpoint)) {
 *     // Use the SDK
 * }
 * }</pre>
 *
 * <h3>Thread Safety</h3>
 *
 * <p>This class is <b>not thread-safe</b>. Each instance should be used from a single thread, or
 * external synchronization must be provided. Do not call {@link #close()} concurrently with other
 * methods.
 *
 * <h3>Example Usage</h3>
 *
 * <p>Use {@link #streamBuilder()} to create streams:
 *
 * <pre>{@code
 * try (ZerobusSdk sdk = new ZerobusSdk(
 *         "https://server-endpoint.databricks.com",
 *         "https://workspace.databricks.com")) {
 *
 *     // For Protocol Buffer ingestion:
 *     try (ZerobusProtoStream protoStream = sdk.streamBuilder()
 *             .table("catalog.schema.table")
 *             .oauth(clientId, clientSecret)
 *             .compiledProto(descriptorProto)
 *             .build()
 *             .join()) {
 *         protoStream.ingestRecordOffset(myProtoMessage);
 *     }
 *
 *     // For JSON ingestion:
 *     try (ZerobusJsonStream jsonStream = sdk.streamBuilder()
 *             .table("catalog.schema.table")
 *             .oauth(clientId, clientSecret)
 *             .json()
 *             .build()
 *             .join()) {
 *         jsonStream.ingestRecordOffset(myObject, gson::toJson);
 *     }
 * }
 * }</pre>
 *
 * @see StreamBuilder
 * @see ZerobusJsonStream
 * @see ZerobusProtoStream
 * @see StreamConfigurationOptions
 */
public class ZerobusSdk implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(ZerobusSdk.class);

  // Ensure native library is loaded.
  static {
    NativeLoader.ensureLoaded();
  }

  private static final StreamConfigurationOptions DEFAULT_OPTIONS =
      StreamConfigurationOptions.getDefault();

  private static final ArrowStreamConfigurationOptions DEFAULT_ARROW_OPTIONS =
      ArrowStreamConfigurationOptions.getDefault();

  // Native handle to the Rust SDK object.
  private volatile long nativeHandle;

  private final String serverEndpoint;
  private final String unityCatalogEndpoint;

  /**
   * Creates a new ZerobusSdk instance.
   *
   * @param serverEndpoint The gRPC endpoint URL for the Zerobus service.
   * @param unityCatalogEndpoint The Unity Catalog endpoint URL.
   * @throws ZerobusException if the SDK cannot be initialized
   */
  public ZerobusSdk(String serverEndpoint, String unityCatalogEndpoint) {
    this(serverEndpoint, unityCatalogEndpoint, null);
  }

  /**
   * Creates a new ZerobusSdk instance with an optional application identifier.
   *
   * @param serverEndpoint The gRPC endpoint URL for the Zerobus service.
   * @param unityCatalogEndpoint The Unity Catalog endpoint URL.
   * @param applicationName Optional application identifier appended to the HTTP {@code user-agent}
   *     header, conventionally {@code "<product>/<version>"} (e.g. {@code "my-app/1.0"}). When set,
   *     the header becomes {@code "zerobus-sdk-java/<version> <applicationName>"}. Pass {@code
   *     null} to omit.
   * @throws ZerobusException if the SDK cannot be initialized
   */
  public ZerobusSdk(String serverEndpoint, String unityCatalogEndpoint, String applicationName) {
    this.serverEndpoint = serverEndpoint;
    this.unityCatalogEndpoint = unityCatalogEndpoint;
    this.nativeHandle = nativeCreate(serverEndpoint, unityCatalogEndpoint, applicationName);
    if (this.nativeHandle == 0) {
      throw new RuntimeException("Failed to create native SDK instance");
    }
    logger.debug("ZerobusSdk created for endpoint: {}", serverEndpoint);
  }

  // ==================== Stream Builder ====================

  /**
   * Returns a fluent {@link StreamBuilder} for creating a stream on this SDK.
   *
   * <p>This is the recommended way to create streams. It supports JSON, Protocol Buffer, and Arrow
   * Flight streams via a single chainable API.
   *
   * @return a new stream builder bound to this SDK
   * @see StreamBuilder
   */
  public StreamBuilder streamBuilder() {
    return new StreamBuilder(this);
  }

  // ==================== Proto Stream Creation ====================

  /**
   * Creates a new Protocol Buffer stream for ingesting proto records into a table.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * ZerobusProtoStream stream = sdk.streamBuilder()
   *     .table("catalog.schema.table")
   *     .oauth(clientId, clientSecret)
   *     .compiledProto(MyProto.getDescriptor().toProto())
   *     .build()
   *     .join();
   *
   * long offset = stream.ingestRecordOffset(myProtoMessage);
   * stream.waitForOffset(offset);
   * stream.close();
   * }</pre>
   *
   * @param tableName The fully qualified table name (catalog.schema.table).
   * @param descriptorProto The Protocol Buffer descriptor proto for the message type.
   * @param clientId The OAuth client ID for authentication.
   * @param clientSecret The OAuth client secret for authentication.
   * @return A CompletableFuture that completes with the ZerobusProtoStream when the stream is
   *     ready.
   * @deprecated Use {@link #streamBuilder()} instead, for example {@code
   *     sdk.streamBuilder().table(tableName).oauth(clientId,
   *     clientSecret).compiledProto(descriptorProto).build()}. This method will be removed in the
   *     next major release.
   */
  @Deprecated
  public CompletableFuture<ZerobusProtoStream> createProtoStream(
      String tableName,
      com.google.protobuf.DescriptorProtos.DescriptorProto descriptorProto,
      String clientId,
      String clientSecret) {
    return createProtoStreamInternal(
        tableName, descriptorProto, clientId, clientSecret, null, DEFAULT_OPTIONS);
  }

  /**
   * Creates a new Protocol Buffer stream for ingesting proto records into a table with custom
   * options.
   *
   * @param tableName The fully qualified table name (catalog.schema.table).
   * @param descriptorProto The Protocol Buffer descriptor proto for the message type.
   * @param clientId The OAuth client ID for authentication.
   * @param clientSecret The OAuth client secret for authentication.
   * @param options Configuration options for the stream.
   * @return A CompletableFuture that completes with the ZerobusProtoStream when the stream is
   *     ready.
   * @deprecated Use {@link #streamBuilder()} instead, for example {@code
   *     sdk.streamBuilder().table(tableName).oauth(clientId,
   *     clientSecret).compiledProto(descriptorProto).build()}. This method will be removed in the
   *     next major release.
   */
  @Deprecated
  public CompletableFuture<ZerobusProtoStream> createProtoStream(
      String tableName,
      com.google.protobuf.DescriptorProtos.DescriptorProto descriptorProto,
      String clientId,
      String clientSecret,
      StreamConfigurationOptions options) {
    return createProtoStreamInternal(
        tableName, descriptorProto, clientId, clientSecret, null, options);
  }

  /**
   * Shared implementation for creating a Protocol Buffer stream. Used by {@link #streamBuilder()}
   * and the deprecated {@code createProtoStream} overloads.
   */
  CompletableFuture<ZerobusProtoStream> createProtoStreamInternal(
      String tableName,
      com.google.protobuf.DescriptorProtos.DescriptorProto descriptorProto,
      String clientId,
      String clientSecret,
      HeadersProvider headersProvider,
      StreamConfigurationOptions options) {

    ensureOpen();

    StreamConfigurationOptions effectiveOptions = options != null ? options : DEFAULT_OPTIONS;

    logger.debug("Creating Proto stream for table: {}", tableName);

    byte[] descriptorProtoBytes = descriptorProto.toByteArray();
    String effectiveClientId = headersProvider == null ? clientId : "";
    String effectiveClientSecret = headersProvider == null ? clientSecret : "";

    CompletableFuture<Long> handleFuture =
        createNativeStream(
            nativeHandle,
            tableName,
            descriptorProtoBytes,
            effectiveClientId,
            effectiveClientSecret,
            headersProvider,
            effectiveOptions,
            false);

    return handleFuture.thenApply(
        handle -> {
          if (handle == null || handle == 0) {
            throw new RuntimeException("Failed to create proto stream: null handle returned");
          }
          return new ZerobusProtoStream(
              handle,
              tableName,
              effectiveOptions,
              descriptorProtoBytes,
              effectiveClientId,
              effectiveClientSecret,
              headersProvider);
        });
  }

  // ==================== JSON Stream Creation ====================

  /**
   * Creates a new JSON stream for ingesting JSON records into a table.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * ZerobusJsonStream stream = sdk.streamBuilder()
   *     .table("catalog.schema.table")
   *     .oauth(clientId, clientSecret)
   *     .json()
   *     .build()
   *     .join();
   *
   * // Main: Ingest objects with a serializer
   * Gson gson = new Gson();
   * long offset = stream.ingestRecordOffset(myObject, gson::toJson);
   * stream.waitForOffset(offset);
   *
   * // Or: Ingest raw JSON strings
   * stream.ingestRecordOffset("{\"field\": \"value\"}");
   *
   * stream.close();
   * }</pre>
   *
   * @param tableName The fully qualified table name (catalog.schema.table).
   * @param clientId The OAuth client ID for authentication.
   * @param clientSecret The OAuth client secret for authentication.
   * @return A CompletableFuture that completes with the ZerobusJsonStream when the stream is ready.
   * @deprecated Use {@link #streamBuilder()} instead, for example {@code
   *     sdk.streamBuilder().table(tableName).oauth(clientId, clientSecret).json().build()}. This
   *     method will be removed in the next major release.
   */
  @Deprecated
  public CompletableFuture<ZerobusJsonStream> createJsonStream(
      String tableName, String clientId, String clientSecret) {
    return createJsonStreamInternal(tableName, clientId, clientSecret, null, DEFAULT_OPTIONS);
  }

  /**
   * Creates a new JSON stream for ingesting JSON records into a table with custom options.
   *
   * @param tableName The fully qualified table name (catalog.schema.table).
   * @param clientId The OAuth client ID for authentication.
   * @param clientSecret The OAuth client secret for authentication.
   * @param options Configuration options for the stream.
   * @return A CompletableFuture that completes with the ZerobusJsonStream when the stream is ready.
   * @deprecated Use {@link #streamBuilder()} instead, for example {@code
   *     sdk.streamBuilder().table(tableName).oauth(clientId, clientSecret).json().build()}. This
   *     method will be removed in the next major release.
   */
  @Deprecated
  public CompletableFuture<ZerobusJsonStream> createJsonStream(
      String tableName, String clientId, String clientSecret, StreamConfigurationOptions options) {
    return createJsonStreamInternal(tableName, clientId, clientSecret, null, options);
  }

  /**
   * Shared implementation for creating a JSON stream. Used by {@link #streamBuilder()} and the
   * deprecated {@code createJsonStream} overloads.
   */
  CompletableFuture<ZerobusJsonStream> createJsonStreamInternal(
      String tableName,
      String clientId,
      String clientSecret,
      HeadersProvider headersProvider,
      StreamConfigurationOptions options) {

    ensureOpen();

    StreamConfigurationOptions effectiveOptions = options != null ? options : DEFAULT_OPTIONS;
    String effectiveClientId = headersProvider == null ? clientId : "";
    String effectiveClientSecret = headersProvider == null ? clientSecret : "";

    logger.debug("Creating JSON stream for table: {}", tableName);

    CompletableFuture<Long> handleFuture =
        createNativeStream(
            nativeHandle,
            tableName,
            null,
            effectiveClientId,
            effectiveClientSecret,
            headersProvider,
            effectiveOptions,
            true);

    return handleFuture.thenApply(
        handle -> {
          if (handle == null || handle == 0) {
            throw new RuntimeException("Failed to create JSON stream: null handle returned");
          }
          return new ZerobusJsonStream(
              handle,
              tableName,
              effectiveOptions,
              effectiveClientId,
              effectiveClientSecret,
              headersProvider);
        });
  }

  // ==================== Legacy Stream Creation (Deprecated) ====================

  /**
   * Creates a new stream for ingesting Protocol Buffer records into a table.
   *
   * @param tableProperties Configuration for the target table including table name and record type
   *     information.
   * @param clientId The OAuth client ID for authentication.
   * @param clientSecret The OAuth client secret for authentication.
   * @param options Configuration options for the stream.
   * @param <RecordType> The type of records to be ingested (must extend Message).
   * @return A CompletableFuture that completes with the ZerobusStream when the stream is ready.
   * @deprecated Use {@link #streamBuilder()} instead.
   */
  @Deprecated
  public <RecordType extends Message> CompletableFuture<ZerobusStream<RecordType>> createStream(
      TableProperties<RecordType> tableProperties,
      String clientId,
      String clientSecret,
      StreamConfigurationOptions options) {

    ensureOpen();

    StreamConfigurationOptions effectiveOptions = options != null ? options : DEFAULT_OPTIONS;

    logger.debug("Creating Proto stream for table: {}", tableProperties.getTableName());

    byte[] descriptorProtoBytes = tableProperties.getDescriptorProto().toByteArray();

    CompletableFuture<Long> handleFuture =
        createNativeStream(
            nativeHandle,
            tableProperties.getTableName(),
            descriptorProtoBytes,
            clientId,
            clientSecret,
            null,
            effectiveOptions,
            false);

    return handleFuture.thenApply(
        handle -> {
          if (handle == null || handle == 0) {
            throw new RuntimeException("Failed to create stream: null handle returned");
          }
          return new ZerobusStream<>(
              handle, tableProperties, effectiveOptions, clientId, clientSecret);
        });
  }

  /**
   * Creates a new stream for ingesting Protocol Buffer records into a table with default options.
   *
   * @param tableProperties Configuration for the target table.
   * @param clientId The OAuth client ID for authentication.
   * @param clientSecret The OAuth client secret for authentication.
   * @param <RecordType> The type of records to be ingested (must extend Message).
   * @return A CompletableFuture that completes with the ZerobusStream when the stream is ready.
   * @deprecated Use {@link #streamBuilder()} instead.
   */
  @Deprecated
  public <RecordType extends Message> CompletableFuture<ZerobusStream<RecordType>> createStream(
      TableProperties<RecordType> tableProperties, String clientId, String clientSecret) {
    return this.createStream(tableProperties, clientId, clientSecret, DEFAULT_OPTIONS);
  }

  // ==================== Arrow Stream Creation ====================

  /**
   * Creates a new Arrow Flight stream for ingesting Arrow record batches into a table.
   *
   * <p><b>Beta:</b> Arrow Flight ingestion is in Beta. The API is stabilising but may still change
   * before reaching GA.
   *
   * @param tableName The fully qualified table name (catalog.schema.table).
   * @param schema The Arrow schema describing the columns of the target table.
   * @param clientId The OAuth client ID for authentication.
   * @param clientSecret The OAuth client secret for authentication.
   * @return A CompletableFuture that completes with the ZerobusArrowStream when the stream is
   *     ready.
   * @deprecated Use {@link #streamBuilder()} instead, for example {@code
   *     sdk.streamBuilder().table(tableName).oauth(clientId, clientSecret).arrow(schema).build()}.
   *     This method will be removed in the next major release.
   */
  @Deprecated
  public CompletableFuture<ZerobusArrowStream> createArrowStream(
      String tableName, Schema schema, String clientId, String clientSecret) {
    return createArrowStreamInternal(
        tableName, schema, clientId, clientSecret, null, DEFAULT_ARROW_OPTIONS);
  }

  /**
   * Creates a new Arrow Flight stream for ingesting Arrow record batches into a table with custom
   * options.
   *
   * @param tableName The fully qualified table name (catalog.schema.table).
   * @param schema The Arrow schema describing the columns of the target table.
   * @param clientId The OAuth client ID for authentication.
   * @param clientSecret The OAuth client secret for authentication.
   * @param options Configuration options for the Arrow stream.
   * @return A CompletableFuture that completes with the ZerobusArrowStream when the stream is
   *     ready.
   * @deprecated Use {@link #streamBuilder()} instead, for example {@code
   *     sdk.streamBuilder().table(tableName).oauth(clientId, clientSecret).arrow(schema).build()}.
   *     This method will be removed in the next major release.
   */
  @Deprecated
  public CompletableFuture<ZerobusArrowStream> createArrowStream(
      String tableName,
      Schema schema,
      String clientId,
      String clientSecret,
      ArrowStreamConfigurationOptions options) {
    return createArrowStreamInternal(tableName, schema, clientId, clientSecret, null, options);
  }

  /**
   * Shared implementation for creating an Arrow Flight stream. Used by {@link #streamBuilder()} and
   * the deprecated {@code createArrowStream} overloads.
   */
  CompletableFuture<ZerobusArrowStream> createArrowStreamInternal(
      String tableName,
      Schema schema,
      String clientId,
      String clientSecret,
      HeadersProvider headersProvider,
      ArrowStreamConfigurationOptions options) {

    ensureOpen();

    ArrowStreamConfigurationOptions effectiveOptions =
        options != null ? options : DEFAULT_ARROW_OPTIONS;
    String effectiveClientId = headersProvider == null ? clientId : "";
    String effectiveClientSecret = headersProvider == null ? clientSecret : "";

    logger.debug("Creating Arrow stream for table: {}", tableName);

    byte[] schemaIpc;
    try {
      schemaIpc = ZerobusArrowStream.serializeSchemaToIpc(schema);
    } catch (ZerobusException e) {
      CompletableFuture<ZerobusArrowStream> failed = new CompletableFuture<>();
      failed.completeExceptionally(e);
      return failed;
    }

    CompletableFuture<Long> handleFuture =
        createNativeArrowStream(
            nativeHandle,
            tableName,
            schemaIpc,
            effectiveClientId,
            effectiveClientSecret,
            headersProvider,
            effectiveOptions);

    return handleFuture.thenApply(
        handle -> {
          if (handle == null || handle == 0) {
            throw new RuntimeException("Failed to create Arrow stream: null handle returned");
          }
          return new ZerobusArrowStream(
              handle,
              tableName,
              effectiveOptions,
              schemaIpc,
              effectiveClientId,
              effectiveClientSecret,
              headersProvider);
        });
  }

  // ==================== Stream Recreation ====================

  /**
   * Recreates a Proto stream from a closed stream, re-ingesting unacknowledged records.
   *
   * <p>This method creates a new stream with the same configuration as the original stream, then
   * re-ingests any records that were not acknowledged before the stream was closed.
   *
   * <p>The original stream must be closed before calling this method.
   *
   * @param closedStream the closed stream to recreate
   * @return a CompletableFuture that completes with the new stream after unacked records are
   *     re-ingested
   * @throws IllegalStateException if the original stream is not closed
   */
  public CompletableFuture<ZerobusProtoStream> recreateStream(ZerobusProtoStream closedStream) {
    if (!closedStream.isClosed()) {
      throw new IllegalStateException("Stream must be closed before recreation");
    }

    ensureOpen();

    // Get unacked batches from the closed stream
    List<EncodedBatch> unackedBatches;
    try {
      unackedBatches = closedStream.getUnackedBatches();
    } catch (ZerobusException e) {
      CompletableFuture<ZerobusProtoStream> failed = new CompletableFuture<>();
      failed.completeExceptionally(e);
      return failed;
    }

    // Create new stream with same parameters
    CompletableFuture<Long> handleFuture =
        createNativeStream(
            nativeHandle,
            closedStream.getTableName(),
            closedStream.getDescriptorProtoBytes(),
            closedStream.getClientId(),
            closedStream.getClientSecret(),
            closedStream.getHeadersProvider(),
            closedStream.getOptions(),
            false);

    return handleFuture.thenApply(
        handle -> {
          if (handle == null || handle == 0) {
            throw new RuntimeException("Failed to recreate proto stream: null handle returned");
          }
          ZerobusProtoStream newStream =
              new ZerobusProtoStream(
                  handle,
                  closedStream.getTableName(),
                  closedStream.getOptions(),
                  closedStream.getDescriptorProtoBytes(),
                  closedStream.getClientId(),
                  closedStream.getClientSecret(),
                  closedStream.getHeadersProvider());

          // Re-ingest unacked records
          try {
            for (EncodedBatch batch : unackedBatches) {
              newStream.ingestRecordsOffset(batch.getRecords());
            }
            newStream.flush();
          } catch (ZerobusException e) {
            throw new RuntimeException("Failed to re-ingest unacked records", e);
          }

          return newStream;
        });
  }

  /**
   * Recreates a JSON stream from a closed stream, re-ingesting unacknowledged records.
   *
   * <p>This method creates a new stream with the same configuration as the original stream, then
   * re-ingests any records that were not acknowledged before the stream was closed.
   *
   * <p>The original stream must be closed before calling this method.
   *
   * @param closedStream the closed stream to recreate
   * @return a CompletableFuture that completes with the new stream after unacked records are
   *     re-ingested
   * @throws IllegalStateException if the original stream is not closed
   */
  public CompletableFuture<ZerobusJsonStream> recreateStream(ZerobusJsonStream closedStream) {
    if (!closedStream.isClosed()) {
      throw new IllegalStateException("Stream must be closed before recreation");
    }

    ensureOpen();

    // Get unacked records from the closed stream
    List<String> unackedRecords;
    try {
      unackedRecords = closedStream.getUnackedRecords();
    } catch (ZerobusException e) {
      CompletableFuture<ZerobusJsonStream> failed = new CompletableFuture<>();
      failed.completeExceptionally(e);
      return failed;
    }

    // Create new stream with same parameters
    CompletableFuture<Long> handleFuture =
        createNativeStream(
            nativeHandle,
            closedStream.getTableName(),
            null,
            closedStream.getClientId(),
            closedStream.getClientSecret(),
            closedStream.getHeadersProvider(),
            closedStream.getOptions(),
            true);

    return handleFuture.thenApply(
        handle -> {
          if (handle == null || handle == 0) {
            throw new RuntimeException("Failed to recreate JSON stream: null handle returned");
          }
          ZerobusJsonStream newStream =
              new ZerobusJsonStream(
                  handle,
                  closedStream.getTableName(),
                  closedStream.getOptions(),
                  closedStream.getClientId(),
                  closedStream.getClientSecret(),
                  closedStream.getHeadersProvider());

          // Re-ingest unacked records
          try {
            for (String json : unackedRecords) {
              newStream.ingestRecordOffset(json);
            }
            newStream.flush();
          } catch (ZerobusException e) {
            throw new RuntimeException("Failed to re-ingest unacked records", e);
          }

          return newStream;
        });
  }

  /**
   * Recreates an Arrow stream from a closed stream, re-ingesting unacknowledged batches.
   *
   * @param closedStream the closed Arrow stream to recreate
   * @return a CompletableFuture that completes with the new stream after unacked batches are
   *     re-ingested
   * @throws IllegalStateException if the original stream is not closed
   */
  public CompletableFuture<ZerobusArrowStream> recreateArrowStream(
      ZerobusArrowStream closedStream) {
    if (!closedStream.isClosed()) {
      throw new IllegalStateException("Arrow stream must be closed before recreation");
    }

    ensureOpen();

    List<byte[]> unackedBatches;
    try {
      unackedBatches = closedStream.getUnackedBatches();
    } catch (ZerobusException e) {
      CompletableFuture<ZerobusArrowStream> failed = new CompletableFuture<>();
      failed.completeExceptionally(e);
      return failed;
    }

    CompletableFuture<Long> handleFuture =
        createNativeArrowStream(
            nativeHandle,
            closedStream.getTableName(),
            closedStream.getSchemaIpc(),
            closedStream.getClientId(),
            closedStream.getClientSecret(),
            closedStream.getHeadersProvider(),
            closedStream.getOptions());

    return handleFuture.thenApply(
        handle -> {
          if (handle == null || handle == 0) {
            throw new RuntimeException("Failed to recreate Arrow stream: null handle returned");
          }
          ZerobusArrowStream newStream =
              new ZerobusArrowStream(
                  handle,
                  closedStream.getTableName(),
                  closedStream.getOptions(),
                  closedStream.getSchemaIpc(),
                  closedStream.getClientId(),
                  closedStream.getClientSecret(),
                  closedStream.getHeadersProvider());

          try {
            for (byte[] batchIpc : unackedBatches) {
              newStream.ingestBatchIpc(batchIpc);
            }
            newStream.flush();
          } catch (ZerobusException e) {
            throw new RuntimeException("Failed to re-ingest unacked Arrow batches", e);
          }

          return newStream;
        });
  }

  /**
   * Recreates a legacy stream from a closed stream, re-ingesting unacknowledged records.
   *
   * <p>This method creates a new stream with the same configuration as the original stream, then
   * re-ingests any records that were not acknowledged before the stream was closed.
   *
   * <p>The original stream must be closed before calling this method.
   *
   * @param closedStream the closed stream to recreate
   * @param <RecordType> the Protocol Buffer message type
   * @return a CompletableFuture that completes with the new stream after unacked records are
   *     re-ingested
   * @throws IllegalStateException if the original stream is not closed
   * @deprecated Use {@link #recreateStream(ZerobusProtoStream)} instead.
   */
  @Deprecated
  public <RecordType extends Message> CompletableFuture<ZerobusStream<RecordType>> recreateStream(
      ZerobusStream<RecordType> closedStream) {
    if (!closedStream.isClosed()) {
      throw new IllegalStateException("Stream must be closed before recreation");
    }

    ensureOpen();

    TableProperties<RecordType> tableProperties = closedStream.getTableProperties();

    // Get cached unacked records from the closed stream
    List<byte[]> unackedRecords = closedStream.getCachedUnackedRecords();

    byte[] descriptorProtoBytes = tableProperties.getDescriptorProto().toByteArray();

    // Create new stream with same parameters
    CompletableFuture<Long> handleFuture =
        createNativeStream(
            nativeHandle,
            tableProperties.getTableName(),
            descriptorProtoBytes,
            closedStream.getClientId(),
            closedStream.getClientSecret(),
            null,
            closedStream.getOptions(),
            false);

    return handleFuture.thenApply(
        handle -> {
          if (handle == null || handle == 0) {
            throw new RuntimeException("Failed to recreate stream: null handle returned");
          }
          ZerobusStream<RecordType> newStream =
              new ZerobusStream<>(
                  handle,
                  tableProperties,
                  closedStream.getOptions(),
                  closedStream.getClientId(),
                  closedStream.getClientSecret());

          // Re-ingest unacked records as raw bytes
          try {
            for (byte[] record : unackedRecords) {
              newStream.nativeIngestRecordOffset(newStream.nativeHandle, record, false);
            }
            newStream.flush();
          } catch (ZerobusException e) {
            throw new RuntimeException("Failed to re-ingest unacked records", e);
          }

          return newStream;
        });
  }

  /**
   * Closes the SDK and releases all resources.
   *
   * <p>After calling this method, the SDK cannot be used to create new streams.
   */
  public void close() {
    long handle = nativeHandle;
    if (handle != 0) {
      nativeHandle = 0;
      nativeDestroy(handle);
      logger.debug("ZerobusSdk closed");
    }
  }

  private void ensureOpen() {
    if (nativeHandle == 0) {
      throw new IllegalStateException("SDK has been closed");
    }
  }

  private CompletableFuture<Long> createNativeStream(
      long sdkHandle,
      String tableName,
      byte[] descriptorProto,
      String clientId,
      String clientSecret,
      HeadersProvider headersProvider,
      Object options,
      boolean isJson) {
    if (headersProvider == null) {
      return nativeCreateStream(
          sdkHandle, tableName, descriptorProto, clientId, clientSecret, options, isJson);
    }
    return nativeCreateStreamWithHeadersProvider(
        sdkHandle,
        tableName,
        descriptorProto,
        clientId,
        clientSecret,
        headersProvider,
        options,
        isJson);
  }

  private CompletableFuture<Long> createNativeArrowStream(
      long sdkHandle,
      String tableName,
      byte[] arrowSchema,
      String clientId,
      String clientSecret,
      HeadersProvider headersProvider,
      Object options) {
    if (headersProvider == null) {
      return nativeCreateArrowStream(
          sdkHandle, tableName, arrowSchema, clientId, clientSecret, options);
    }
    return nativeCreateArrowStreamWithHeadersProvider(
        sdkHandle, tableName, arrowSchema, clientId, clientSecret, headersProvider, options);
  }

  // Native methods implemented in Rust

  private static native long nativeCreate(
      String serverEndpoint, String unityCatalogEndpoint, String applicationName);

  private static native void nativeDestroy(long handle);

  private native CompletableFuture<Long> nativeCreateStream(
      long sdkHandle,
      String tableName,
      byte[] descriptorProto,
      String clientId,
      String clientSecret,
      Object options,
      boolean isJson);

  private native CompletableFuture<Long> nativeCreateStreamWithHeadersProvider(
      long sdkHandle,
      String tableName,
      byte[] descriptorProto,
      String clientId,
      String clientSecret,
      HeadersProvider headersProvider,
      Object options,
      boolean isJson);

  private native CompletableFuture<Long> nativeCreateArrowStream(
      long sdkHandle,
      String tableName,
      byte[] arrowSchema,
      String clientId,
      String clientSecret,
      Object options);

  private native CompletableFuture<Long> nativeCreateArrowStreamWithHeadersProvider(
      long sdkHandle,
      String tableName,
      byte[] arrowSchema,
      String clientId,
      String clientSecret,
      HeadersProvider headersProvider,
      Object options);
}
