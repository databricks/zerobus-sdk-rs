package com.databricks.zerobus;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Stream for ingesting Protocol Buffer records into a table.
 *
 * <p>This class provides a flexible API for Protocol Buffer ingestion with method-level generics,
 * allowing different message types to be ingested through the same stream.
 *
 * <p>Create instances using {@link ZerobusSdk#streamBuilder()}:
 *
 * <pre>{@code
 * ZerobusProtoStream stream = sdk.streamBuilder()
 *     .table("catalog.schema.table")
 *     .oauth(clientId, clientSecret)
 *     .compiledProto(MyProto.getDescriptor().toProto())
 *     .build()
 *     .join();
 *
 * // Ingest proto messages
 * long offset = stream.ingestRecordOffset(myProtoMessage);
 * stream.waitForOffset(offset);
 *
 * // Or ingest pre-encoded bytes
 * stream.ingestRecordOffset(protoBytes);
 *
 * // Batch ingestion
 * List<MyProto> records = ...;
 * Optional<Long> batchOffset = stream.ingestRecordsOffset(records);
 *
 * stream.close();
 * }</pre>
 *
 * @see ZerobusSdk#streamBuilder()
 */
public class ZerobusProtoStream extends BaseZerobusStream {

  // Stream creation parameters (stored for recreateStream)
  private final byte[] descriptorProtoBytes;
  private final String clientId;
  private final String clientSecret;

  /** Package-private constructor. Use {@link ZerobusSdk#streamBuilder()} to create instances. */
  ZerobusProtoStream(
      long nativeHandle,
      String tableName,
      StreamConfigurationOptions options,
      byte[] descriptorProtoBytes,
      String clientId,
      String clientSecret) {
    super(nativeHandle, tableName, options, false);
    this.descriptorProtoBytes = descriptorProtoBytes;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
  }

  /** Returns the descriptor proto bytes used to create this stream. */
  byte[] getDescriptorProtoBytes() {
    return descriptorProtoBytes;
  }

  /** Returns the client ID used to create this stream. */
  public String getClientId() {
    return clientId;
  }

  /** Returns the client secret used to create this stream. */
  public String getClientSecret() {
    return clientSecret;
  }

  // ==================== Single Record Ingestion ====================

  /**
   * Ingests a Protocol Buffer message and returns the offset immediately.
   *
   * <p>This is the main method for ingesting proto records. The message is automatically serialized
   * to bytes.
   *
   * @param record the Protocol Buffer message to ingest
   * @param <T> the message type
   * @return the offset ID assigned to this record
   * @throws ZerobusException if the stream is not in a valid state or an error occurs
   */
  public <T extends Message> long ingestRecordOffset(T record) throws ZerobusException {
    ensureOpen();
    return nativeIngestRecordOffset(nativeHandle, record.toByteArray(), false);
  }

  /**
   * Ingests pre-encoded bytes and returns the offset immediately.
   *
   * <p>Use this method when you have already serialized the record to avoid double-encoding
   * overhead.
   *
   * @param encodedBytes the pre-encoded protobuf bytes
   * @return the offset ID assigned to this record
   * @throws ZerobusException if the stream is not in a valid state or an error occurs
   */
  public long ingestRecordOffset(byte[] encodedBytes) throws ZerobusException {
    ensureOpen();
    return nativeIngestRecordOffset(nativeHandle, encodedBytes, false);
  }

  // ==================== Batch Ingestion ====================

  /**
   * Ingests multiple Protocol Buffer messages and returns the batch offset.
   *
   * <p>This is the main method for batch ingestion. All messages are automatically serialized.
   *
   * @param records the Protocol Buffer messages to ingest
   * @param <T> the message type
   * @return the offset ID for the batch, or empty if the iterable is empty
   * @throws ZerobusException if the stream is not in a valid state or an error occurs
   */
  public <T extends Message> Optional<Long> ingestRecordsOffset(Iterable<T> records)
      throws ZerobusException {
    List<byte[]> payloads = new ArrayList<>();
    for (T record : records) {
      payloads.add(record.toByteArray());
    }
    if (payloads.isEmpty()) {
      return Optional.empty();
    }
    ensureOpen();
    return Optional.of(nativeIngestRecordsOffset(nativeHandle, payloads, false));
  }

  /**
   * Ingests multiple pre-encoded byte arrays and returns the batch offset.
   *
   * <p>Use this method when you have already serialized the records to avoid double-encoding
   * overhead.
   *
   * @param encodedRecords the pre-encoded protobuf byte arrays
   * @return the offset ID for the batch, or empty if the list is empty
   * @throws ZerobusException if the stream is not in a valid state or an error occurs
   */
  public Optional<Long> ingestRecordsOffset(List<byte[]> encodedRecords) throws ZerobusException {
    if (encodedRecords.isEmpty()) {
      return Optional.empty();
    }
    ensureOpen();
    return Optional.of(nativeIngestRecordsOffset(nativeHandle, encodedRecords, false));
  }

  // ==================== Unacknowledged Records ====================

  /**
   * Returns the unacknowledged records as raw byte arrays.
   *
   * <p>Use this method when you want to handle deserialization yourself or when you need to
   * re-ingest the records without parsing them.
   *
   * <p>Note: This method returns cached data after the stream is closed.
   *
   * @return a list of raw encoded records
   * @throws ZerobusException if an error occurs
   */
  public List<byte[]> getUnackedRecords() throws ZerobusException {
    if (nativeHandle == 0) {
      return getCachedUnackedRecords();
    }
    return nativeGetUnackedRecords(nativeHandle);
  }

  /**
   * Returns the unacknowledged records parsed into Protocol Buffer messages.
   *
   * <p>Use this method when you need to inspect or process the unacknowledged records.
   *
   * <p>Example:
   *
   * <pre>{@code
   * List<MyProto> unacked = stream.getUnackedRecords(MyProto.parser());
   * for (MyProto record : unacked) {
   *     // Process or re-ingest
   * }
   * }</pre>
   *
   * @param parser the protobuf parser for the message type
   * @param <T> the message type
   * @return a list of parsed records
   * @throws ZerobusException if an error occurs or parsing fails
   */
  public <T extends Message> List<T> getUnackedRecords(Parser<T> parser) throws ZerobusException {
    List<byte[]> rawRecords = getUnackedRecords();
    List<T> result = new ArrayList<>(rawRecords.size());
    for (byte[] bytes : rawRecords) {
      try {
        result.add(parser.parseFrom(bytes));
      } catch (InvalidProtocolBufferException e) {
        throw new ZerobusException("Failed to parse unacked record: " + e.getMessage(), e);
      }
    }
    return result;
  }

  /**
   * Returns the unacknowledged batches after stream failure.
   *
   * <p>This method preserves the batch grouping from the original ingestion, which can be useful
   * for re-ingesting records in the same batches.
   *
   * <p>Note: This method returns cached data after the stream is closed.
   *
   * @return a list of unacknowledged batches
   * @throws ZerobusException if an error occurs
   */
  public List<EncodedBatch> getUnackedBatches() throws ZerobusException {
    if (nativeHandle == 0) {
      return getCachedUnackedBatches();
    }
    return nativeGetUnackedBatches(nativeHandle);
  }
}
