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
 * <pre>{@code
 * try (ZerobusSdk sdk = new ZerobusSdk(
 *         "https://server-endpoint.databricks.com",
 *         "https://workspace.databricks.com")) {
 *
 *     // For JSON ingestion (recommended):
 *     try (ZerobusJsonStream jsonStream = sdk.createJsonStream(
 *             "catalog.schema.table",
 *             clientId,
 *             clientSecret).join()) {
 *         jsonStream.ingestRecordOffset(myObject, gson::toJson);
 *     }
 *
 *     // For Protocol Buffer ingestion:
 *     try (ZerobusProtoStream protoStream = sdk.createProtoStream(
 *             "catalog.schema.table",
 *             descriptorProto,
 *             clientId,
 *             clientSecret).join()) {
 *         protoStream.ingestRecordOffset(myProtoMessage);
 *     }
 * }
 * }</pre>
 *
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
    this.serverEndpoint = serverEndpoint;
    this.unityCatalogEndpoint = unityCatalogEndpoint;
    this.nativeHandle = nativeCreate(serverEndpoint, unityCatalogEndpoint);
    if (this.nativeHandle == 0) {
      throw new RuntimeException("Failed to create native SDK instance");
    }
    logger.debug("ZerobusSdk created for endpoint: {}", serverEndpoint);
  }

  // ==================== Proto Stream Creation ====================

  /**
   * Creates a new Protocol Buffer stream for ingesting proto records into a table.
   *
   * <p>This is the recommended method for Protocol Buffer ingestion. It provides a flexible API
   * with method-level generics and batch ingestion support.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * ZerobusProtoStream stream = sdk.createProtoStream(
   *     "catalog.schema.table",
   *     MyProto.getDescriptor().toProto(),
   *     clientId,
   *     clientSecret
   * ).join();
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
   */
  public CompletableFuture<ZerobusProtoStream> createProtoStream(
      String tableName,
      com.google.protobuf.DescriptorProtos.DescriptorProto descriptorProto,
      String clientId,
      String clientSecret) {
    return createProtoStream(tableName, descriptorProto, clientId, clientSecret, DEFAULT_OPTIONS);
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
   */
  public CompletableFuture<ZerobusProtoStream> createProtoStream(
      String tableName,
      com.google.protobuf.DescriptorProtos.DescriptorProto descriptorProto,
      String clientId,
      String clientSecret,
      StreamConfigurationOptions options) {

    ensureOpen();

    StreamConfigurationOptions effectiveOptions = options != null ? options : DEFAULT_OPTIONS;

    logger.debug("Creating Proto stream for table: {}", tableName);

    byte[] descriptorProtoBytes = descriptorProto.toByteArray();

    CompletableFuture<Long> handleFuture =
        nativeCreateStream(
            nativeHandle,
            tableName,
            descriptorProtoBytes,
            clientId,
            clientSecret,
            effectiveOptions,
            false);

    return handleFuture.thenApply(
        handle -> {
          if (handle == null || handle == 0) {
            throw new RuntimeException("Failed to create proto stream: null handle returned");
          }
          return new ZerobusProtoStream(
              handle, tableName, effectiveOptions, descriptorProtoBytes, clientId, clientSecret);
        });
  }

  // ==================== JSON Stream Creation ====================

  /**
   * Creates a new JSON stream for ingesting JSON records into a table.
   *
   * <p>This is the recommended method for JSON ingestion as it provides a clean API without
   * requiring Protocol Buffer types.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * ZerobusJsonStream stream = sdk.createJsonStream(
   *     "catalog.schema.table",
   *     clientId,
   *     clientSecret
   * ).join();
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
   */
  public CompletableFuture<ZerobusJsonStream> createJsonStream(
      String tableName, String clientId, String clientSecret) {
    return createJsonStream(tableName, clientId, clientSecret, DEFAULT_OPTIONS);
  }

  /**
   * Creates a new JSON stream for ingesting JSON records into a table with custom options.
   *
   * @param tableName The fully qualified table name (catalog.schema.table).
   * @param clientId The OAuth client ID for authentication.
   * @param clientSecret The OAuth client secret for authentication.
   * @param options Configuration options for the stream.
   * @return A CompletableFuture that completes with the ZerobusJsonStream when the stream is ready.
   */
  public CompletableFuture<ZerobusJsonStream> createJsonStream(
      String tableName, String clientId, String clientSecret, StreamConfigurationOptions options) {

    ensureOpen();

    StreamConfigurationOptions effectiveOptions = options != null ? options : DEFAULT_OPTIONS;

    logger.debug("Creating JSON stream for table: {}", tableName);

    CompletableFuture<Long> handleFuture =
        nativeCreateStream(
            nativeHandle, tableName, null, clientId, clientSecret, effectiveOptions, true);

    return handleFuture.thenApply(
        handle -> {
          if (handle == null || handle == 0) {
            throw new RuntimeException("Failed to create JSON stream: null handle returned");
          }
          return new ZerobusJsonStream(handle, tableName, effectiveOptions, clientId, clientSecret);
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
   * @deprecated Use {@link #createProtoStream(String,
   *     com.google.protobuf.DescriptorProtos.DescriptorProto, String, String,
   *     StreamConfigurationOptions)} instead.
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
        nativeCreateStream(
            nativeHandle,
            tableProperties.getTableName(),
            descriptorProtoBytes,
            clientId,
            clientSecret,
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
   * @deprecated Use {@link #createProtoStream(String,
   *     com.google.protobuf.DescriptorProtos.DescriptorProto, String, String)} instead.
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
   * @param tableName The fully qualified table name (catalog.schema.table).
   * @param schema The Arrow schema describing the columns of the target table.
   * @param clientId The OAuth client ID for authentication.
   * @param clientSecret The OAuth client secret for authentication.
   * @return A CompletableFuture that completes with the ZerobusArrowStream when the stream is
   *     ready.
   */
  public CompletableFuture<ZerobusArrowStream> createArrowStream(
      String tableName, Schema schema, String clientId, String clientSecret) {
    return createArrowStream(tableName, schema, clientId, clientSecret, DEFAULT_ARROW_OPTIONS);
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
   */
  public CompletableFuture<ZerobusArrowStream> createArrowStream(
      String tableName,
      Schema schema,
      String clientId,
      String clientSecret,
      ArrowStreamConfigurationOptions options) {

    ensureOpen();

    ArrowStreamConfigurationOptions effectiveOptions =
        options != null ? options : DEFAULT_ARROW_OPTIONS;

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
        nativeCreateArrowStream(
            nativeHandle, tableName, schemaIpc, clientId, clientSecret, effectiveOptions);

    return handleFuture.thenApply(
        handle -> {
          if (handle == null || handle == 0) {
            throw new RuntimeException("Failed to create Arrow stream: null handle returned");
          }
          return new ZerobusArrowStream(
              handle, tableName, effectiveOptions, schemaIpc, clientId, clientSecret);
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
        nativeCreateStream(
            nativeHandle,
            closedStream.getTableName(),
            closedStream.getDescriptorProtoBytes(),
            closedStream.getClientId(),
            closedStream.getClientSecret(),
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
                  closedStream.getClientSecret());

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
        nativeCreateStream(
            nativeHandle,
            closedStream.getTableName(),
            null,
            closedStream.getClientId(),
            closedStream.getClientSecret(),
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
                  closedStream.getClientSecret());

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
        nativeCreateArrowStream(
            nativeHandle,
            closedStream.getTableName(),
            closedStream.getSchemaIpc(),
            closedStream.getClientId(),
            closedStream.getClientSecret(),
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
                  closedStream.getClientSecret());

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
        nativeCreateStream(
            nativeHandle,
            tableProperties.getTableName(),
            descriptorProtoBytes,
            closedStream.getClientId(),
            closedStream.getClientSecret(),
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

  // Native methods implemented in Rust

  private static native long nativeCreate(String serverEndpoint, String unityCatalogEndpoint);

  private static native void nativeDestroy(long handle);

  private native CompletableFuture<Long> nativeCreateStream(
      long sdkHandle,
      String tableName,
      byte[] descriptorProto,
      String clientId,
      String clientSecret,
      Object options,
      boolean isJson);

  private native CompletableFuture<Long> nativeCreateArrowStream(
      long sdkHandle,
      String tableName,
      byte[] arrowSchema,
      String clientId,
      String clientSecret,
      Object options);
}
