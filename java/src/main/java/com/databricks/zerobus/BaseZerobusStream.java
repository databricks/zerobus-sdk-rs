package com.databricks.zerobus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for Zerobus streams providing common native method declarations and lifecycle
 * management.
 *
 * <p>This class is not intended to be used directly. Use {@link ZerobusJsonStream} for JSON
 * ingestion, {@link ZerobusProtoStream} for Protocol Buffer ingestion, or the deprecated {@link
 * ZerobusStream} for backward compatibility.
 *
 * <h3>Resource Management</h3>
 *
 * <p>Streams hold native resources that are not automatically released by the garbage collector.
 * You <b>must</b> call {@link #close()} when done to avoid native memory leaks. Use
 * try-with-resources for automatic cleanup:
 *
 * <pre>{@code
 * try (ZerobusJsonStream stream = sdk.createJsonStream(...).join()) {
 *     stream.ingestRecordOffset(record);
 * }
 * }</pre>
 *
 * <h3>Thread Safety</h3>
 *
 * <p>Streams are <b>not thread-safe</b>. Each stream instance should be used from a single thread.
 * Do not call methods on the same stream concurrently from multiple threads.
 */
abstract class BaseZerobusStream implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(BaseZerobusStream.class);

  // Ensure native library is loaded.
  static {
    NativeLoader.ensureLoaded();
  }

  // Native handle to the Rust stream object.
  protected volatile long nativeHandle;

  // Stream properties.
  protected final String tableName;
  protected final StreamConfigurationOptions options;
  protected final boolean isJsonMode;

  // Cached unacked records (populated on close for use in recreateStream).
  protected volatile List<byte[]> cachedUnackedRecords;
  protected volatile List<EncodedBatch> cachedUnackedBatches;

  /**
   * Creates a new BaseZerobusStream.
   *
   * @param nativeHandle the native handle from JNI
   * @param tableName the table name
   * @param options the stream configuration options
   * @param isJsonMode whether this stream is in JSON mode
   */
  protected BaseZerobusStream(
      long nativeHandle, String tableName, StreamConfigurationOptions options, boolean isJsonMode) {
    this.nativeHandle = nativeHandle;
    this.tableName = tableName;
    this.options = options;
    this.isJsonMode = isJsonMode;
  }

  /**
   * Waits for a specific offset to be acknowledged by the server.
   *
   * @param offset the offset to wait for
   * @throws ZerobusException if an error occurs or the wait times out
   */
  public void waitForOffset(long offset) throws ZerobusException {
    ensureOpen();
    nativeWaitForOffset(nativeHandle, offset);
  }

  /**
   * Flushes the stream, waiting for all queued records to be acknowledged.
   *
   * @throws ZerobusException if an error occurs or the flush times out
   */
  public void flush() throws ZerobusException {
    ensureOpen();
    nativeFlush(nativeHandle);
    logger.info("All records have been flushed");
  }

  /**
   * Closes the stream, flushing all pending records first.
   *
   * <p>After closing, unacknowledged records can still be retrieved via subclass methods for use in
   * stream recreation.
   *
   * @throws ZerobusException if an error occurs during close
   */
  @Override
  public void close() throws ZerobusException {
    long handle = nativeHandle;
    if (handle != 0) {
      try {
        // Close the stream first (flushes pending records)
        nativeClose(handle);
      } catch (Exception e) {
        logger.warn("nativeClose failed: {}", e.getMessage());
      }

      // Cache unacked records before destroying the handle (for recreateStream)
      try {
        cachedUnackedRecords = nativeGetUnackedRecords(handle);
        cachedUnackedBatches = nativeGetUnackedBatches(handle);
      } catch (Exception e) {
        logger.warn("Failed to cache unacked records: {}", e.getMessage());
        cachedUnackedRecords = new ArrayList<>();
        cachedUnackedBatches = new ArrayList<>();
      } finally {
        nativeHandle = 0;
        nativeDestroy(handle);
        logger.info("Stream closed");
      }
    }
  }

  /**
   * Returns the cached unacknowledged records as raw bytes.
   *
   * <p>This method returns records that were cached when the stream was closed. It is used
   * internally by recreateStream.
   *
   * @return a list of unacknowledged records as raw bytes, or empty list if none
   */
  protected List<byte[]> getCachedUnackedRecords() {
    return cachedUnackedRecords != null ? cachedUnackedRecords : new ArrayList<>();
  }

  /**
   * Returns the cached unacknowledged batches.
   *
   * <p>This method returns batches that were cached when the stream was closed. It is used
   * internally by recreateStream.
   *
   * @return a list of unacknowledged batches, or empty list if none
   */
  protected List<EncodedBatch> getCachedUnackedBatches() {
    return cachedUnackedBatches != null ? cachedUnackedBatches : new ArrayList<>();
  }

  /**
   * Returns whether the stream is closed.
   *
   * @return true if the stream is closed
   */
  public boolean isClosed() {
    return nativeHandle == 0 || nativeIsClosed(nativeHandle);
  }

  /**
   * Returns the table name for this stream.
   *
   * @return the table name
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Returns the stream configuration options.
   *
   * @return the stream configuration options
   */
  public StreamConfigurationOptions getOptions() {
    return options;
  }

  /**
   * Ensures the stream is open.
   *
   * @throws ZerobusException if the stream is closed
   */
  protected void ensureOpen() throws ZerobusException {
    if (nativeHandle == 0) {
      throw new ZerobusException("Stream is closed");
    }
    if (nativeIsClosed(nativeHandle)) {
      throw new ZerobusException("Stream is closed");
    }
  }

  // ==================== Native methods implemented in Rust ====================

  private static native void nativeDestroy(long handle);

  protected native CompletableFuture<Void> nativeIngestRecord(
      long handle, byte[] payload, boolean isJson);

  protected native long nativeIngestRecordOffset(long handle, byte[] payload, boolean isJson);

  protected native long nativeIngestRecordsOffset(
      long handle, List<byte[]> payloads, boolean isJson);

  private native void nativeWaitForOffset(long handle, long offset);

  private native void nativeFlush(long handle);

  private native void nativeClose(long handle);

  private native boolean nativeIsClosed(long handle);

  protected native List<byte[]> nativeGetUnackedRecords(long handle);

  protected native List<EncodedBatch> nativeGetUnackedBatches(long handle);
}
