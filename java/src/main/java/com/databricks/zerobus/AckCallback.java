package com.databricks.zerobus;

/**
 * Callback interface for receiving acknowledgment notifications from the Zerobus stream.
 *
 * <p>This interface provides methods for handling both successful acknowledgments and errors. It
 * replaces the deprecated {@code Consumer<IngestRecordResponse>} callback with a more type-safe and
 * flexible API.
 *
 * <p>A callback is invoked once per logical ingest submission. In particular, a batch ingest call
 * produces one callback rather than one callback per record in the batch.
 *
 * <p>Implementations should be thread-safe as callbacks may be invoked from multiple threads.
 * Callbacks should be lightweight to avoid blocking the internal processing threads.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * AckCallback callback = new AckCallback() {
 *     @Override
 *     public void onAck(long offsetId) {
 *         System.out.println("Record at offset " + offsetId + " acknowledged");
 *     }
 *
 *     @Override
 *     public void onError(long offsetId, String errorMessage) {
 *         System.err.println("Error for offset " + offsetId + ": " + errorMessage);
 *     }
 * };
 *
 * StreamConfigurationOptions options = StreamConfigurationOptions.builder()
 *     .setAckCallback(callback)
 *     .build();
 * }</pre>
 *
 * @see StreamConfigurationOptions.StreamConfigurationOptionsBuilder#setAckCallback(AckCallback)
 */
public interface AckCallback {

  /**
   * Called when a logical ingest submission has been durably acknowledged by the server.
   *
   * <p>The offset ID represents the durability acknowledgment up to and including this offset. All
   * records with offset IDs less than or equal to this value have been durably stored.
   *
   * <p>This method must not throw. JNI does not clear a pending Java exception from the callback,
   * so a thrown exception can poison later callback operations.
   *
   * @param offsetId the offset ID that has been acknowledged
   */
  void onAck(long offsetId);

  /**
   * Called when an error occurs for a logical ingest submission.
   *
   * <p>This method is called when the SDK encounters an error that affects a specific submission
   * offset. The error may be retryable or non-retryable depending on the nature of the failure.
   *
   * <p>This method must not throw. JNI does not clear a pending Java exception from the callback,
   * so a thrown exception can poison later callback operations.
   *
   * @param offsetId the offset ID that encountered an error
   * @param errorMessage a description of the error that occurred
   */
  void onError(long offsetId, String errorMessage);
}
