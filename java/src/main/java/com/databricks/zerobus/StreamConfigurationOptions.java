package com.databricks.zerobus;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * Configuration options for Zerobus streams.
 *
 * <p>This class provides various settings to control stream behavior including performance tuning,
 * error handling, and callback configuration.
 *
 * <p>Use the builder pattern to create instances:
 *
 * <pre>{@code
 * StreamConfigurationOptions options = StreamConfigurationOptions.builder()
 *     .setMaxInflightRecords(50000)
 *     .setRecovery(true)
 *     .setAckCallback(new AckCallback() {
 *         public void onAck(long offsetId) {
 *             System.out.println("Acked offset: " + offsetId);
 *         }
 *         public void onError(long offsetId, String errorMessage) {
 *             System.err.println("Error for offset " + offsetId + ": " + errorMessage);
 *         }
 *     })
 *     .build();
 * }</pre>
 */
public class StreamConfigurationOptions {

  private int maxInflightRecords = 1_000_000;
  private boolean recovery = true;
  private int recoveryTimeoutMs = 15000;
  private int recoveryBackoffMs = 2000;
  private int recoveryRetries = 3;
  private int flushTimeoutMs = 300000;
  private int serverLackOfAckTimeoutMs = 60000;
  private Optional<Consumer<IngestRecordResponse>> ackCallback = Optional.empty();
  private Optional<AckCallback> newAckCallback = Optional.empty();

  private StreamConfigurationOptions() {}

  private StreamConfigurationOptions(
      int maxInflightRecords,
      boolean recovery,
      int recoveryTimeoutMs,
      int recoveryBackoffMs,
      int recoveryRetries,
      int flushTimeoutMs,
      int serverLackOfAckTimeoutMs,
      Optional<Consumer<IngestRecordResponse>> ackCallback,
      Optional<AckCallback> newAckCallback) {
    this.maxInflightRecords = maxInflightRecords;
    this.recovery = recovery;
    this.recoveryTimeoutMs = recoveryTimeoutMs;
    this.recoveryBackoffMs = recoveryBackoffMs;
    this.recoveryRetries = recoveryRetries;
    this.flushTimeoutMs = flushTimeoutMs;
    this.serverLackOfAckTimeoutMs = serverLackOfAckTimeoutMs;
    this.ackCallback = ackCallback;
    this.newAckCallback = newAckCallback;
  }

  /**
   * Returns the maximum number of records that can be in flight.
   *
   * <p>This controls how many records the SDK can accept and send to the server before waiting for
   * acknowledgments. Higher values improve throughput but use more memory.
   *
   * @return the maximum number of in-flight records
   */
  public int maxInflightRecords() {
    return this.maxInflightRecords;
  }

  /**
   * Returns whether automatic recovery is enabled.
   *
   * <p>When enabled, the SDK will automatically attempt to recover from stream failures by
   * recreating the stream and resending unacknowledged records.
   *
   * @return true if automatic recovery is enabled, false otherwise
   */
  public boolean recovery() {
    return this.recovery;
  }

  /**
   * Returns the timeout for recovery operations.
   *
   * <p>This is the maximum time to wait for a recovery operation to complete before considering it
   * failed.
   *
   * @return the recovery timeout in milliseconds
   */
  public int recoveryTimeoutMs() {
    return this.recoveryTimeoutMs;
  }

  /**
   * Returns the backoff delay between recovery attempts.
   *
   * <p>This is the delay to wait between consecutive recovery attempts when automatic recovery is
   * enabled.
   *
   * @return the recovery backoff delay in milliseconds
   */
  public int recoveryBackoffMs() {
    return this.recoveryBackoffMs;
  }

  /**
   * Returns the maximum number of recovery attempts.
   *
   * <p>This is the maximum number of times the SDK will attempt to recover from a stream failure
   * before giving up.
   *
   * @return the maximum number of recovery attempts
   */
  public int recoveryRetries() {
    return this.recoveryRetries;
  }

  /**
   * Returns the timeout for flush operations.
   *
   * <p>This is the maximum time to wait for a flush operation to complete before considering it
   * failed.
   *
   * @return the flush timeout in milliseconds
   */
  public int flushTimeoutMs() {
    return this.flushTimeoutMs;
  }

  /**
   * Returns the timeout for server acknowledgment.
   *
   * <p>This is the maximum time to wait for the server to acknowledge records before considering
   * the server unresponsive.
   *
   * @return the server acknowledgment timeout in milliseconds
   */
  public int serverLackOfAckTimeoutMs() {
    return this.serverLackOfAckTimeoutMs;
  }

  /**
   * Returns the acknowledgment callback function.
   *
   * <p>This callback is invoked whenever the server acknowledges records. If no callback is set,
   * this returns an empty Optional.
   *
   * @return the acknowledgment callback, or an empty Optional if none is set
   * @deprecated This callback is no longer invoked by the native Rust backend. Use {@link
   *     #getNewAckCallback()} instead, which provides working acknowledgment notifications.
   */
  @Deprecated
  public Optional<Consumer<IngestRecordResponse>> ackCallback() {
    return this.ackCallback;
  }

  /**
   * Returns the new-style acknowledgment callback.
   *
   * <p>This callback provides both success and error notifications. If no callback is set, this
   * returns an empty Optional.
   *
   * @return the acknowledgment callback, or an empty Optional if none is set
   */
  public Optional<AckCallback> getNewAckCallback() {
    return this.newAckCallback;
  }

  /**
   * Returns the default stream configuration options.
   *
   * <p>Default values:
   *
   * <ul>
   *   <li>maxInflightRecords: 1_000_000
   *   <li>recovery: true
   *   <li>recoveryTimeoutMs: 15000
   *   <li>recoveryBackoffMs: 2000
   *   <li>recoveryRetries: 3
   *   <li>flushTimeoutMs: 300000
   *   <li>serverLackOfAckTimeoutMs: 60000
   *   <li>ackCallback: empty
   * </ul>
   *
   * @return the default stream configuration options
   */
  public static StreamConfigurationOptions getDefault() {
    return new StreamConfigurationOptions();
  }

  /**
   * Returns a new builder for creating StreamConfigurationOptions.
   *
   * @return a new StreamConfigurationOptionsBuilder
   */
  public static StreamConfigurationOptionsBuilder builder() {
    return new StreamConfigurationOptionsBuilder();
  }

  /**
   * Builder for creating StreamConfigurationOptions instances.
   *
   * <p>This builder provides a fluent API for configuring stream options. All parameters have
   * sensible defaults if not specified.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * StreamConfigurationOptions options = StreamConfigurationOptions.builder()
   *     .setMaxInflightRecords(100000)
   *     .setRecovery(false)
   *     .setAckCallback(new AckCallback() {
   *         public void onAck(long offsetId) { ... }
   *         public void onError(long offsetId, String errorMessage) { ... }
   *     })
   *     .build();
   * }</pre>
   *
   * @see StreamConfigurationOptions
   * @since 1.0.0
   */
  public static class StreamConfigurationOptionsBuilder {
    private StreamConfigurationOptions defaultOptions = StreamConfigurationOptions.getDefault();

    private int maxInflightRecords = defaultOptions.maxInflightRecords;
    private boolean recovery = defaultOptions.recovery;
    private int recoveryTimeoutMs = defaultOptions.recoveryTimeoutMs;
    private int recoveryBackoffMs = defaultOptions.recoveryBackoffMs;
    private int recoveryRetries = defaultOptions.recoveryRetries;
    private int flushTimeoutMs = defaultOptions.flushTimeoutMs;
    private int serverLackOfAckTimeoutMs = defaultOptions.serverLackOfAckTimeoutMs;
    private Optional<Consumer<IngestRecordResponse>> ackCallback = defaultOptions.ackCallback;
    private Optional<AckCallback> newAckCallback = defaultOptions.newAckCallback;

    private StreamConfigurationOptionsBuilder() {}

    /**
     * Sets the maximum number of records that can be in flight.
     *
     * <p>This controls how many records the SDK can accept and send to the server before waiting
     * for acknowledgments. Higher values improve throughput but use more memory.
     *
     * @param maxInflightRecords the maximum number of in-flight records
     * @return this builder for method chaining
     */
    public StreamConfigurationOptionsBuilder setMaxInflightRecords(int maxInflightRecords) {
      if (maxInflightRecords <= 0) {
        throw new IllegalArgumentException(
            "maxInflightRecords must be > 0, got: " + maxInflightRecords);
      }
      this.maxInflightRecords = maxInflightRecords;
      return this;
    }

    /**
     * Sets whether automatic recovery is enabled.
     *
     * <p>When enabled, the SDK will automatically attempt to recover from stream failures by
     * recreating the stream and resending unacknowledged records.
     *
     * @param recovery true to enable automatic recovery, false to disable
     * @return this builder for method chaining
     */
    public StreamConfigurationOptionsBuilder setRecovery(boolean recovery) {
      this.recovery = recovery;
      return this;
    }

    /**
     * Sets the timeout for recovery operations.
     *
     * <p>This is the maximum time to wait for a recovery operation to complete before considering
     * it failed.
     *
     * @param recoveryTimeoutMs the recovery timeout in milliseconds
     * @return this builder for method chaining
     */
    public StreamConfigurationOptionsBuilder setRecoveryTimeoutMs(int recoveryTimeoutMs) {
      if (recoveryTimeoutMs < 0) {
        throw new IllegalArgumentException(
            "recoveryTimeoutMs must be >= 0, got: " + recoveryTimeoutMs);
      }
      this.recoveryTimeoutMs = recoveryTimeoutMs;
      return this;
    }

    /**
     * Sets the backoff delay between recovery attempts.
     *
     * <p>This is the delay to wait between consecutive recovery attempts when automatic recovery is
     * enabled.
     *
     * @param recoveryBackoffMs the recovery backoff delay in milliseconds
     * @return this builder for method chaining
     * @throws IllegalArgumentException if recoveryBackoffMs is less than 0
     */
    public StreamConfigurationOptionsBuilder setRecoveryBackoffMs(int recoveryBackoffMs) {
      if (recoveryBackoffMs < 0) {
        throw new IllegalArgumentException(
            "recoveryBackoffMs must be >= 0, got: " + recoveryBackoffMs);
      }
      this.recoveryBackoffMs = recoveryBackoffMs;
      return this;
    }

    /**
     * Sets the maximum number of recovery attempts.
     *
     * <p>This is the maximum number of times the SDK will attempt to recover from a stream failure
     * before giving up.
     *
     * @param recoveryRetries the maximum number of recovery attempts
     * @return this builder for method chaining
     */
    public StreamConfigurationOptionsBuilder setRecoveryRetries(int recoveryRetries) {
      if (recoveryRetries < 0) {
        throw new IllegalArgumentException("recoveryRetries must be >= 0, got: " + recoveryRetries);
      }
      this.recoveryRetries = recoveryRetries;
      return this;
    }

    /**
     * Sets the timeout for flush operations.
     *
     * <p>This is the maximum time to wait for a flush operation to complete before considering it
     * failed.
     *
     * @param flushTimeoutMs the flush timeout in milliseconds
     * @return this builder for method chaining
     */
    public StreamConfigurationOptionsBuilder setFlushTimeoutMs(int flushTimeoutMs) {
      if (flushTimeoutMs < 0) {
        throw new IllegalArgumentException("flushTimeoutMs must be >= 0, got: " + flushTimeoutMs);
      }
      this.flushTimeoutMs = flushTimeoutMs;
      return this;
    }

    /**
     * Sets the timeout for server acknowledgment.
     *
     * <p>This is the maximum time to wait for the server to acknowledge records before considering
     * the server unresponsive.
     *
     * @param serverLackOfAckTimeoutMs the server acknowledgment timeout in milliseconds
     * @return this builder for method chaining
     */
    public StreamConfigurationOptionsBuilder setServerLackOfAckTimeoutMs(
        int serverLackOfAckTimeoutMs) {
      if (serverLackOfAckTimeoutMs < 0) {
        throw new IllegalArgumentException(
            "serverLackOfAckTimeoutMs must be >= 0, got: " + serverLackOfAckTimeoutMs);
      }
      this.serverLackOfAckTimeoutMs = serverLackOfAckTimeoutMs;
      return this;
    }

    /**
     * Sets the acknowledgment callback function.
     *
     * @param ackCallback the acknowledgment callback function
     * @return this builder for method chaining
     * @deprecated This callback is no longer invoked by the native Rust backend. Use {@link
     *     #setAckCallback(AckCallback)} instead, which provides working acknowledgment
     *     notifications.
     */
    @Deprecated
    public StreamConfigurationOptionsBuilder setAckCallback(
        Consumer<IngestRecordResponse> ackCallback) {
      this.ackCallback = Optional.ofNullable(ackCallback);
      return this;
    }

    /**
     * Sets the acknowledgment callback.
     *
     * <p>This callback is invoked for both successful acknowledgments and errors. It provides more
     * detailed feedback than the deprecated Consumer-based callback.
     *
     * @param ackCallback the acknowledgment callback
     * @return this builder for method chaining
     */
    public StreamConfigurationOptionsBuilder setAckCallback(AckCallback ackCallback) {
      this.newAckCallback = Optional.ofNullable(ackCallback);
      return this;
    }

    /**
     * Builds a new StreamConfigurationOptions instance.
     *
     * @return a new StreamConfigurationOptions with the configured settings
     */
    public StreamConfigurationOptions build() {
      return new StreamConfigurationOptions(
          this.maxInflightRecords,
          this.recovery,
          this.recoveryTimeoutMs,
          this.recoveryBackoffMs,
          this.recoveryRetries,
          this.flushTimeoutMs,
          this.serverLackOfAckTimeoutMs,
          this.ackCallback,
          this.newAckCallback);
    }
  }
}
