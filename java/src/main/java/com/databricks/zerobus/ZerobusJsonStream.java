package com.databricks.zerobus;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Stream for ingesting JSON records into a table.
 *
 * <p>This class provides a clean API for JSON ingestion that doesn't require Protocol Buffer types.
 *
 * <p>Create instances using {@link ZerobusSdk#createJsonStream}:
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
 * // Alt: Ingest raw JSON strings
 * stream.ingestRecordOffset("{\"field\": \"value\"}");
 *
 * // Batch ingestion
 * List<MyData> records = ...;
 * Optional<Long> batchOffset = stream.ingestRecordsOffset(records, gson::toJson);
 *
 * stream.close();
 * }</pre>
 *
 * @see ZerobusSdk#createJsonStream(String, String, String)
 */
public class ZerobusJsonStream extends BaseZerobusStream {

  /**
   * A functional interface for JSON serialization.
   *
   * <p>This interface allows using any JSON serialization library (Gson, Jackson, etc.) by passing
   * a serializer function.
   *
   * <p>Example with Gson:
   *
   * <pre>{@code
   * Gson gson = new Gson();
   * stream.ingestRecordOffset(myObject, gson::toJson);
   * }</pre>
   *
   * <p>Example with Jackson:
   *
   * <pre>{@code
   * ObjectMapper mapper = new ObjectMapper();
   * stream.ingestRecordOffset(myObject, obj -> {
   *     try {
   *         return mapper.writeValueAsString(obj);
   *     } catch (JsonProcessingException e) {
   *         throw new RuntimeException(e);
   *     }
   * });
   * }</pre>
   *
   * @param <T> the type of the object to serialize
   */
  @FunctionalInterface
  public interface JsonSerializer<T> {
    /**
     * Serializes an object to a JSON string.
     *
     * @param object the object to serialize
     * @return the JSON string representation
     */
    String serialize(T object);
  }

  /**
   * A functional interface for JSON deserialization.
   *
   * <p>This interface allows using any JSON deserialization library (Gson, Jackson, etc.) by
   * passing a deserializer function.
   *
   * <p>Example with Gson:
   *
   * <pre>{@code
   * Gson gson = new Gson();
   * List<MyData> unacked = stream.getUnackedRecords(json -> gson.fromJson(json, MyData.class));
   * }</pre>
   *
   * @param <T> the type of the object to deserialize to
   */
  @FunctionalInterface
  public interface JsonDeserializer<T> {
    /**
     * Deserializes a JSON string to an object.
     *
     * @param json the JSON string to deserialize
     * @return the deserialized object
     */
    T deserialize(String json);
  }

  // Stream creation parameters (stored for recreateStream)
  private final String clientId;
  private final String clientSecret;

  /** Package-private constructor. Use {@link ZerobusSdk#createJsonStream} to create instances. */
  ZerobusJsonStream(
      long nativeHandle,
      String tableName,
      StreamConfigurationOptions options,
      String clientId,
      String clientSecret) {
    super(nativeHandle, tableName, options, true);
    this.clientId = clientId;
    this.clientSecret = clientSecret;
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
   * Ingests an object as JSON and returns the offset immediately.
   *
   * <p>This is the main method for JSON ingestion. The object is serialized using the provided
   * serializer function.
   *
   * <p>Example with Gson:
   *
   * <pre>{@code
   * Gson gson = new Gson();
   * stream.ingestRecordOffset(myObject, gson::toJson);
   * }</pre>
   *
   * @param object the object to serialize and ingest
   * @param serializer a function that converts the object to a JSON string
   * @param <T> the type of the object
   * @return the offset ID assigned to this record
   * @throws ZerobusException if the stream is not in a valid state or an error occurs
   */
  public <T> long ingestRecordOffset(T object, JsonSerializer<T> serializer)
      throws ZerobusException {
    ensureOpen();
    String json = serializer.serialize(object);
    return nativeIngestRecordOffset(nativeHandle, json.getBytes(StandardCharsets.UTF_8), true);
  }

  /**
   * Ingests an object as JSON asynchronously without exposing the offset or ack future.
   *
   * <p>This fire-and-forget method serializes the object on the calling thread, then hands the
   * encoded payload off to the native runtime; enqueueing into the stream and applying
   * backpressure happen on the runtime's thread pool. Errors raised after this hand-off (e.g.,
   * the stream fails before the record is acknowledged) are not surfaced to the caller — call
   * {@link #flush()} or {@link #close()} before shutdown when durability matters.
   *
   * <p>Subsequent calls to {@link #flush()}, {@link #close()}, {@link #waitForOffset(long)}, or
   * any offset-returning ingest method on this stream wait for previously-submitted no-wait tasks
   * to complete, so a record submitted here is always observable to those operations.
   *
   * @param object the object to serialize and ingest
   * @param serializer a function that converts the object to a JSON string
   * @param <T> the type of the object
   * @throws ZerobusException if the stream is already closed or serialization fails
   */
  public <T> void ingestRecordNoWait(T object, JsonSerializer<T> serializer)
      throws ZerobusException {
    ensureOpen();
    String json = serializer.serialize(object);
    nativeIngestRecordNoWait(nativeHandle, json.getBytes(StandardCharsets.UTF_8), true);
  }

  /**
   * Ingests a JSON string and returns the offset immediately.
   *
   * <p>Use this method when you already have a JSON string.
   *
   * @param json the JSON string to ingest
   * @return the offset ID assigned to this record
   * @throws ZerobusException if the stream is not in a valid state or an error occurs
   */
  public long ingestRecordOffset(String json) throws ZerobusException {
    ensureOpen();
    return nativeIngestRecordOffset(nativeHandle, json.getBytes(StandardCharsets.UTF_8), true);
  }

  /**
   * Ingests a JSON string asynchronously without exposing the offset or ack future.
   *
   * <p>See {@link #ingestRecordNoWait(Object, JsonSerializer)} for the fire-and-forget contract
   * and the ordering guarantee with respect to subsequent {@link #flush()} / {@link #close()} /
   * offset calls.
   *
   * @param json the JSON string to ingest
   * @throws ZerobusException if the stream is already closed
   */
  public void ingestRecordNoWait(String json) throws ZerobusException {
    ensureOpen();
    nativeIngestRecordNoWait(nativeHandle, json.getBytes(StandardCharsets.UTF_8), true);
  }

  // ==================== Batch Ingestion ====================

  /**
   * Ingests multiple objects as JSON and returns the batch offset.
   *
   * <p>This is the main method for batch JSON ingestion. Each object is serialized using the
   * provided serializer function.
   *
   * @param objects the objects to serialize and ingest
   * @param serializer a function that converts each object to a JSON string
   * @param <T> the type of the objects
   * @return the offset ID for the batch, or empty if the iterable is empty
   * @throws ZerobusException if the stream is not in a valid state or an error occurs
   */
  public <T> Optional<Long> ingestRecordsOffset(Iterable<T> objects, JsonSerializer<T> serializer)
      throws ZerobusException {
    List<byte[]> payloads = new ArrayList<>();
    for (T obj : objects) {
      String json = serializer.serialize(obj);
      payloads.add(json.getBytes(StandardCharsets.UTF_8));
    }
    if (payloads.isEmpty()) {
      return Optional.empty();
    }
    ensureOpen();
    return Optional.of(nativeIngestRecordsOffset(nativeHandle, payloads, true));
  }

  /**
   * Ingests multiple JSON strings and returns the batch offset.
   *
   * <p>Use this method when you already have JSON strings.
   *
   * @param jsonStrings the JSON strings to ingest
   * @return the offset ID for the batch, or empty if the iterable is empty
   * @throws ZerobusException if the stream is not in a valid state or an error occurs
   */
  public Optional<Long> ingestRecordsOffset(Iterable<String> jsonStrings) throws ZerobusException {
    List<byte[]> payloads = new ArrayList<>();
    for (String json : jsonStrings) {
      payloads.add(json.getBytes(StandardCharsets.UTF_8));
    }
    if (payloads.isEmpty()) {
      return Optional.empty();
    }
    ensureOpen();
    return Optional.of(nativeIngestRecordsOffset(nativeHandle, payloads, true));
  }

  /**
   * Ingests multiple objects as JSON asynchronously without exposing the batch offset.
   *
   * <p>See {@link #ingestRecordNoWait(Object, JsonSerializer)} for the fire-and-forget contract
   * and the ordering guarantee with respect to subsequent {@link #flush()} / {@link #close()} /
   * offset calls.
   *
   * @param objects the objects to serialize and ingest
   * @param serializer a function that converts each object to a JSON string
   * @param <T> the type of the objects
   * @throws ZerobusException if the stream is already closed or serialization fails
   */
  public <T> void ingestRecordsNoWait(Iterable<T> objects, JsonSerializer<T> serializer)
      throws ZerobusException {
    List<byte[]> payloads = new ArrayList<>();
    for (T obj : objects) {
      String json = serializer.serialize(obj);
      payloads.add(json.getBytes(StandardCharsets.UTF_8));
    }
    if (payloads.isEmpty()) {
      return;
    }
    ensureOpen();
    nativeIngestRecordsNoWait(nativeHandle, payloads, true);
  }

  /**
   * Ingests multiple JSON strings asynchronously without exposing the batch offset.
   *
   * <p>See {@link #ingestRecordNoWait(Object, JsonSerializer)} for the fire-and-forget contract
   * and the ordering guarantee with respect to subsequent {@link #flush()} / {@link #close()} /
   * offset calls.
   *
   * @param jsonStrings the JSON strings to ingest
   * @throws ZerobusException if the stream is already closed
   */
  public void ingestRecordsNoWait(Iterable<String> jsonStrings) throws ZerobusException {
    List<byte[]> payloads = new ArrayList<>();
    for (String json : jsonStrings) {
      payloads.add(json.getBytes(StandardCharsets.UTF_8));
    }
    if (payloads.isEmpty()) {
      return;
    }
    ensureOpen();
    nativeIngestRecordsNoWait(nativeHandle, payloads, true);
  }

  // ==================== Unacknowledged Records ====================

  /**
   * Returns the unacknowledged records as JSON strings.
   *
   * <p>Use this method when you want to handle deserialization yourself or when you need the raw
   * JSON strings.
   *
   * <p>Note: This method returns cached data after the stream is closed.
   *
   * @return a list of JSON strings
   * @throws ZerobusException if an error occurs
   */
  public List<String> getUnackedRecords() throws ZerobusException {
    List<byte[]> rawRecords;
    if (nativeHandle == 0) {
      rawRecords = getCachedUnackedRecords();
    } else {
      rawRecords = nativeGetUnackedRecords(nativeHandle);
    }
    List<String> result = new ArrayList<>(rawRecords.size());
    for (byte[] bytes : rawRecords) {
      result.add(new String(bytes, StandardCharsets.UTF_8));
    }
    return result;
  }

  /**
   * Returns the unacknowledged records deserialized into objects.
   *
   * <p>Use this method when you need to inspect or process the unacknowledged records as typed
   * objects.
   *
   * <p>Example with Gson:
   *
   * <pre>{@code
   * Gson gson = new Gson();
   * List<MyData> unacked = stream.getUnackedRecords(json -> gson.fromJson(json, MyData.class));
   * for (MyData record : unacked) {
   *     // Process or re-ingest
   * }
   * }</pre>
   *
   * @param deserializer a function that converts JSON strings to objects
   * @param <T> the type of the objects
   * @return a list of deserialized objects
   * @throws ZerobusException if an error occurs
   */
  public <T> List<T> getUnackedRecords(JsonDeserializer<T> deserializer) throws ZerobusException {
    List<String> jsonRecords = getUnackedRecords();
    List<T> result = new ArrayList<>(jsonRecords.size());
    for (String json : jsonRecords) {
      result.add(deserializer.deserialize(json));
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
