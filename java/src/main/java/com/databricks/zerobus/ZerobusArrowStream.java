package com.databricks.zerobus;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stream for ingesting Apache Arrow record batches into a table via Arrow Flight.
 *
 * <p><b>Beta:</b> Arrow Flight ingestion is in Beta. The API is stabilising but may still change
 * before reaching GA.
 *
 * <p>This class provides high-performance columnar data ingestion using the Apache Arrow Flight
 * protocol. Data is sent as Arrow {@link VectorSchemaRoot} batches, which are automatically
 * serialized to Arrow IPC format for transmission over the wire.
 *
 * <p>Create instances using {@link ZerobusSdk#streamBuilder()}:
 *
 * <pre>{@code
 * Schema schema = new Schema(Arrays.asList(
 *     Field.nullable("name", new ArrowType.Utf8()),
 *     Field.nullable("age", new ArrowType.Int(32, true))
 * ));
 *
 * ZerobusArrowStream stream = sdk.streamBuilder()
 *     .table("catalog.schema.table")
 *     .oauth(clientId, clientSecret)
 *     .arrow(schema)
 *     .build()
 *     .join();
 *
 * // Create and populate a VectorSchemaRoot, then ingest
 * Optional<Long> offset = stream.ingestBatch(batch);
 * if (offset.isPresent()) {
 *     stream.waitForOffset(offset.get());
 * }
 * stream.close();
 * }</pre>
 *
 * <h3>Resource Management</h3>
 *
 * <p>This class holds native resources that are not automatically released by the garbage
 * collector. You <b>must</b> call {@link #close()} when done to avoid native memory leaks. Use
 * try-with-resources for automatic cleanup.
 *
 * <h3>Thread Safety</h3>
 *
 * <p>This class is <b>not thread-safe</b>. Each stream instance should be used from a single
 * thread.
 *
 * <h3>Dependencies</h3>
 *
 * <p>This class requires Apache Arrow Java libraries on the classpath. Add {@code arrow-vector} and
 * a memory allocator implementation (e.g., {@code arrow-memory-netty}) to your project
 * dependencies.
 *
 * @see ZerobusSdk#streamBuilder()
 * @see ArrowStreamConfigurationOptions
 */
public class ZerobusArrowStream implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(ZerobusArrowStream.class);

  // Ensure native library is loaded.
  static {
    NativeLoader.ensureLoaded();
  }

  // Native handle to the Rust Arrow stream object.
  private volatile long nativeHandle;

  // Stream properties.
  private final String tableName;
  private final ArrowStreamConfigurationOptions options;
  private final byte[] schemaIpc;

  // Credentials stored for stream recreation.
  private final String clientId;
  private final String clientSecret;

  // Cached unacked batches (populated on close for use in recreateArrowStream).
  private volatile List<byte[]> cachedUnackedBatches;

  /** Package-private constructor. Use {@link ZerobusSdk#streamBuilder()} to create instances. */
  ZerobusArrowStream(
      long nativeHandle,
      String tableName,
      ArrowStreamConfigurationOptions options,
      byte[] schemaIpc,
      String clientId,
      String clientSecret) {
    this.nativeHandle = nativeHandle;
    this.tableName = tableName;
    this.options = options;
    this.schemaIpc = schemaIpc;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
  }

  // ==================== Batch Ingestion ====================

  /**
   * Ingests an Arrow record batch and returns the offset immediately.
   *
   * <p>The batch is serialized to Arrow IPC format and sent to the server. The method returns as
   * soon as the batch is queued for transmission, without waiting for server acknowledgment. Use
   * {@link #waitForOffset(long)} or {@link #flush()} to wait for acknowledgment.
   *
   * <p>The batch schema must match the schema used to create this stream.
   *
   * @param batch the Arrow record batch to ingest, or null for a no-op
   * @return the offset ID assigned to this batch, or empty if batch is null
   * @throws ZerobusException if the stream is closed, the schema doesn't match, or an error occurs
   */
  public Optional<Long> ingestBatch(VectorSchemaRoot batch) throws ZerobusException {
    if (batch == null || batch.getRowCount() == 0) {
      return Optional.empty();
    }
    ensureOpen();
    byte[] ipcBytes = serializeBatchToIpc(batch);
    return Optional.of(nativeIngestBatch(nativeHandle, ipcBytes));
  }

  // ==================== Acknowledgment ====================

  /**
   * Waits for a specific offset to be acknowledged by the server.
   *
   * @param offset the offset to wait for (as returned by {@link #ingestBatch})
   * @throws ZerobusException if an error occurs or the wait times out
   */
  public void waitForOffset(long offset) throws ZerobusException {
    ensureOpen();
    nativeWaitForOffset(nativeHandle, offset);
  }

  /**
   * Flushes all pending batches and waits for their acknowledgments.
   *
   * <p>Blocks until all batches that were ingested before this call are acknowledged by the server.
   *
   * @throws ZerobusException if an error occurs or the flush times out
   */
  public void flush() throws ZerobusException {
    ensureOpen();
    nativeFlush(nativeHandle);
    logger.info("All Arrow batches have been flushed");
  }

  // ==================== Lifecycle ====================

  /**
   * Closes the stream, flushing all pending batches first.
   *
   * <p>After closing, unacknowledged batches can still be retrieved via {@link
   * #getUnackedBatches()} for use in stream recreation.
   *
   * @throws ZerobusException if an error occurs during close
   */
  @Override
  public void close() throws ZerobusException {
    long handle = nativeHandle;
    if (handle != 0) {
      // Close the stream first (flushes pending batches)
      nativeClose(handle);

      // Cache unacked batches before destroying the handle (for recreateArrowStream)
      try {
        cachedUnackedBatches = nativeGetUnackedBatches(handle);
      } catch (Exception e) {
        logger.warn("Failed to cache unacked batches: {}", e.getMessage());
        cachedUnackedBatches = new ArrayList<>();
      }

      // Now destroy the handle
      nativeHandle = 0;
      nativeDestroy(handle);
      logger.info("Arrow stream closed");
    }
  }

  /**
   * Returns whether the stream is closed.
   *
   * @return true if the stream is closed
   */
  public boolean isClosed() {
    return nativeHandle == 0 || nativeIsClosed(nativeHandle);
  }

  // ==================== Accessors ====================

  /**
   * Returns the table name for this stream.
   *
   * @return the fully qualified table name
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Returns the stream configuration options.
   *
   * @return the Arrow stream configuration options
   */
  public ArrowStreamConfigurationOptions getOptions() {
    return options;
  }

  // ==================== Unacknowledged Batches ====================

  /**
   * Returns the unacknowledged batches as Arrow IPC byte arrays.
   *
   * <p>Each element in the returned list is a serialized Arrow IPC stream containing one record
   * batch. These can be deserialized using {@code ArrowStreamReader} or re-ingested into a new
   * stream via {@link ZerobusSdk#recreateArrowStream}.
   *
   * <p>After the stream is closed, this method returns cached data.
   *
   * @return a list of unacknowledged batches as IPC byte arrays
   * @throws ZerobusException if an error occurs
   */
  public List<byte[]> getUnackedBatches() throws ZerobusException {
    if (nativeHandle == 0) {
      return cachedUnackedBatches != null ? cachedUnackedBatches : new ArrayList<>();
    }
    return nativeGetUnackedBatches(nativeHandle);
  }

  // ==================== Package-Private Accessors ====================

  /** Returns the IPC-serialized schema used to create this stream. */
  byte[] getSchemaIpc() {
    return schemaIpc;
  }

  /** Returns the client ID used to create this stream. */
  String getClientId() {
    return clientId;
  }

  /** Returns the client secret used to create this stream. */
  String getClientSecret() {
    return clientSecret;
  }

  /**
   * Ingests a pre-serialized Arrow IPC batch. Package-private for use by {@link
   * ZerobusSdk#recreateArrowStream} during re-ingestion of unacked batches.
   */
  long ingestBatchIpc(byte[] ipcBytes) throws ZerobusException {
    ensureOpen();
    return nativeIngestBatch(nativeHandle, ipcBytes);
  }

  // ==================== Serialization Utilities ====================

  /**
   * Serializes an Arrow schema to IPC stream format.
   *
   * <p>Package-private for use by {@link ZerobusSdk#createArrowStreamInternal}.
   */
  /** Shared allocator for schema serialization — lightweight, only used for empty roots. */
  private static final BufferAllocator SCHEMA_ALLOCATOR = new RootAllocator(1024 * 1024);

  static byte[] serializeSchemaToIpc(Schema schema) throws ZerobusException {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, SCHEMA_ALLOCATOR);
          ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
        writer.start();
        writer.end();
      }
      return out.toByteArray();
    } catch (IOException e) {
      throw new ZerobusException("Failed to serialize Arrow schema to IPC: " + e.getMessage(), e);
    }
  }

  private static byte[] serializeBatchToIpc(VectorSchemaRoot batch) throws ZerobusException {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (ArrowStreamWriter writer =
          new ArrowStreamWriter(batch, null, Channels.newChannel(out))) {
        writer.start();
        writer.writeBatch();
        writer.end();
      }
      return out.toByteArray();
    } catch (IOException e) {
      throw new ZerobusException("Failed to serialize Arrow batch to IPC: " + e.getMessage(), e);
    }
  }

  private void ensureOpen() throws ZerobusException {
    if (nativeHandle == 0) {
      throw new ZerobusException("Arrow stream is closed");
    }
    if (nativeIsClosed(nativeHandle)) {
      throw new ZerobusException("Arrow stream is closed");
    }
  }

  // ==================== Native methods implemented in Rust ====================

  private static native void nativeDestroy(long handle);

  private native long nativeIngestBatch(long handle, byte[] batchData);

  private native void nativeWaitForOffset(long handle, long offset);

  private native void nativeFlush(long handle);

  private native void nativeClose(long handle);

  private native boolean nativeIsClosed(long handle);

  private native String nativeGetTableName(long handle);

  @SuppressWarnings("unchecked")
  private native List<byte[]> nativeGetUnackedBatches(long handle);
}
