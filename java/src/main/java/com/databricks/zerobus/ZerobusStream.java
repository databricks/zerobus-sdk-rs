package com.databricks.zerobus;

import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Zerobus stream for ingesting Protocol Buffer records into a table.
 *
 * <p>This class provides a Future-based API for Protocol Buffer ingestion with class-level generics
 * for type safety.
 *
 * <p>Streams should be created using {@link ZerobusSdk#createStream} and closed when no longer
 * needed.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * ZerobusStream<MyRecord> stream = sdk.createStream(tableProperties, clientId, clientSecret).join();
 *
 * // Ingest a record and wait for acknowledgment
 * stream.ingestRecord(myRecord).join();
 *
 * // Close when done
 * stream.close();
 * }</pre>
 *
 * @param <RecordType> the Protocol Buffer message type for this stream
 * @deprecated Use {@link ZerobusProtoStream} for new code, which provides a more flexible API with
 *     method-level generics and batch ingestion support. This class is maintained for backward
 *     compatibility.
 */
@Deprecated
public class ZerobusStream<RecordType extends Message> extends BaseZerobusStream {
  private static final Logger logger = LoggerFactory.getLogger(ZerobusStream.class);

  // Stream properties (stored for recreation).
  private final TableProperties<RecordType> tableProperties;
  private final String clientId;
  private final String clientSecret;

  /**
   * Package-private constructor. Streams should be created via {@link ZerobusSdk#createStream}.
   *
   * @param nativeHandle the native handle to the Rust stream object
   * @param tableProperties the table properties
   * @param options the stream configuration options
   * @param clientId the OAuth client ID
   * @param clientSecret the OAuth client secret
   */
  ZerobusStream(
      long nativeHandle,
      TableProperties<RecordType> tableProperties,
      StreamConfigurationOptions options,
      String clientId,
      String clientSecret) {
    super(
        nativeHandle,
        tableProperties != null ? tableProperties.getTableName() : null,
        options,
        false);
    this.tableProperties = tableProperties;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
  }

  /**
   * Returns the stream ID.
   *
   * @return the stream ID, or empty string if not available
   * @deprecated The stream ID is no longer exposed by the underlying SDK. This method always
   *     returns an empty string.
   */
  @Deprecated
  public String getStreamId() {
    return "";
  }

  /**
   * Returns the state of the stream.
   *
   * @return the state of the stream
   * @deprecated The stream state is no longer exposed by the underlying SDK. This method always
   *     returns {@link StreamState#OPENED} unless the stream is closed.
   */
  @Deprecated
  public StreamState getState() {
    if (isClosed()) {
      return StreamState.CLOSED;
    }
    return StreamState.OPENED;
  }

  /**
   * Returns the table properties for this stream.
   *
   * @return the table properties
   */
  public TableProperties<RecordType> getTableProperties() {
    return tableProperties;
  }

  /**
   * Returns the OAuth client ID.
   *
   * @return the OAuth client ID
   */
  public String getClientId() {
    return clientId;
  }

  /**
   * Returns the OAuth client secret.
   *
   * @return the OAuth client secret
   */
  public String getClientSecret() {
    return clientSecret;
  }

  /**
   * Ingests a Protocol Buffer record into the stream.
   *
   * <p>This method enqueues the record for ingestion and returns a CompletableFuture that completes
   * when the server acknowledges the record has been durably stored.
   *
   * @param record the Protocol Buffer record to ingest
   * @return a CompletableFuture that completes when the record is acknowledged
   * @throws ZerobusException if the stream is not in a valid state for ingestion
   */
  public CompletableFuture<Void> ingestRecord(RecordType record) throws ZerobusException {
    ensureOpen();
    return nativeIngestRecord(nativeHandle, record.toByteArray(), false);
  }

  /**
   * Returns the unacknowledged records after stream failure.
   *
   * <p>Note: Due to type erasure, this method cannot deserialize records without a parser. Consider
   * using {@link ZerobusProtoStream#getUnackedRecords(com.google.protobuf.Parser)} instead.
   *
   * @return an iterator over the unacknowledged records (currently returns empty iterator)
   * @deprecated This method cannot deserialize records due to type erasure. Use {@link
   *     ZerobusProtoStream#getUnackedRecords(com.google.protobuf.Parser)} instead.
   */
  @Deprecated
  public Iterator<RecordType> getUnackedRecords() {
    logger.warn(
        "getUnackedRecords() cannot deserialize records without a parser. "
            + "Use ZerobusProtoStream.getUnackedRecords(Parser<T>) instead.");
    return new ArrayList<RecordType>().iterator();
  }
}
