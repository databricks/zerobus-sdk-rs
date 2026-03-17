package com.databricks.zerobus;

/**
 * Configuration options for Arrow Flight streams.
 *
 * <p>This class provides settings to control Arrow Flight stream behavior including performance
 * tuning, recovery, and connection management.
 *
 * <p>Use the builder pattern to create instances:
 *
 * <pre>{@code
 * ArrowStreamConfigurationOptions options = ArrowStreamConfigurationOptions.builder()
 *     .setMaxInflightBatches(2000)
 *     .setRecovery(true)
 *     .setFlushTimeoutMs(600000)
 *     .build();
 * }</pre>
 *
 * @see ZerobusArrowStream
 * @see ZerobusSdk#createArrowStream
 */
public class ArrowStreamConfigurationOptions {

  private int maxInflightBatches = 1000;
  private boolean recovery = true;
  private long recoveryTimeoutMs = 15000;
  private long recoveryBackoffMs = 2000;
  private int recoveryRetries = 4;
  private long serverLackOfAckTimeoutMs = 60000;
  private long flushTimeoutMs = 300000;
  private long connectionTimeoutMs = 30000;

  private ArrowStreamConfigurationOptions() {}

  private ArrowStreamConfigurationOptions(
      int maxInflightBatches,
      boolean recovery,
      long recoveryTimeoutMs,
      long recoveryBackoffMs,
      int recoveryRetries,
      long serverLackOfAckTimeoutMs,
      long flushTimeoutMs,
      long connectionTimeoutMs) {
    this.maxInflightBatches = maxInflightBatches;
    this.recovery = recovery;
    this.recoveryTimeoutMs = recoveryTimeoutMs;
    this.recoveryBackoffMs = recoveryBackoffMs;
    this.recoveryRetries = recoveryRetries;
    this.serverLackOfAckTimeoutMs = serverLackOfAckTimeoutMs;
    this.flushTimeoutMs = flushTimeoutMs;
    this.connectionTimeoutMs = connectionTimeoutMs;
  }

  /**
   * Returns the maximum number of batches that can be in flight.
   *
   * <p>This controls how many batches the SDK can accept and send to the server before waiting for
   * acknowledgments. Higher values improve throughput but use more memory.
   *
   * @return the maximum number of in-flight batches
   */
  public int maxInflightBatches() {
    return this.maxInflightBatches;
  }

  /**
   * Returns whether automatic recovery is enabled.
   *
   * <p>When enabled, the SDK will automatically attempt to recover from stream failures by
   * reconnecting and resending unacknowledged batches.
   *
   * @return true if automatic recovery is enabled, false otherwise
   */
  public boolean recovery() {
    return this.recovery;
  }

  /**
   * Returns the timeout for recovery operations.
   *
   * @return the recovery timeout in milliseconds
   */
  public long recoveryTimeoutMs() {
    return this.recoveryTimeoutMs;
  }

  /**
   * Returns the backoff delay between recovery attempts.
   *
   * @return the recovery backoff delay in milliseconds
   */
  public long recoveryBackoffMs() {
    return this.recoveryBackoffMs;
  }

  /**
   * Returns the maximum number of recovery attempts.
   *
   * @return the maximum number of recovery attempts
   */
  public int recoveryRetries() {
    return this.recoveryRetries;
  }

  /**
   * Returns the timeout for server acknowledgment.
   *
   * @return the server acknowledgment timeout in milliseconds
   */
  public long serverLackOfAckTimeoutMs() {
    return this.serverLackOfAckTimeoutMs;
  }

  /**
   * Returns the timeout for flush operations.
   *
   * @return the flush timeout in milliseconds
   */
  public long flushTimeoutMs() {
    return this.flushTimeoutMs;
  }

  /**
   * Returns the timeout for establishing a connection.
   *
   * @return the connection timeout in milliseconds
   */
  public long connectionTimeoutMs() {
    return this.connectionTimeoutMs;
  }

  /**
   * Returns the default Arrow stream configuration options.
   *
   * <p>Default values:
   *
   * <ul>
   *   <li>maxInflightBatches: 1000
   *   <li>recovery: true
   *   <li>recoveryTimeoutMs: 15000
   *   <li>recoveryBackoffMs: 2000
   *   <li>recoveryRetries: 4
   *   <li>serverLackOfAckTimeoutMs: 60000
   *   <li>flushTimeoutMs: 300000
   *   <li>connectionTimeoutMs: 30000
   * </ul>
   *
   * @return the default Arrow stream configuration options
   */
  public static ArrowStreamConfigurationOptions getDefault() {
    return new ArrowStreamConfigurationOptions();
  }

  /**
   * Returns a new builder for creating ArrowStreamConfigurationOptions.
   *
   * @return a new builder
   */
  public static ArrowStreamConfigurationOptionsBuilder builder() {
    return new ArrowStreamConfigurationOptionsBuilder();
  }

  /**
   * Builder for creating {@link ArrowStreamConfigurationOptions} instances.
   *
   * <p>All parameters have sensible defaults if not specified.
   */
  public static class ArrowStreamConfigurationOptionsBuilder {
    private final ArrowStreamConfigurationOptions defaults =
        ArrowStreamConfigurationOptions.getDefault();

    private int maxInflightBatches = defaults.maxInflightBatches;
    private boolean recovery = defaults.recovery;
    private long recoveryTimeoutMs = defaults.recoveryTimeoutMs;
    private long recoveryBackoffMs = defaults.recoveryBackoffMs;
    private int recoveryRetries = defaults.recoveryRetries;
    private long serverLackOfAckTimeoutMs = defaults.serverLackOfAckTimeoutMs;
    private long flushTimeoutMs = defaults.flushTimeoutMs;
    private long connectionTimeoutMs = defaults.connectionTimeoutMs;

    private ArrowStreamConfigurationOptionsBuilder() {}

    /**
     * Sets the maximum number of batches that can be in flight.
     *
     * @param maxInflightBatches the maximum number of in-flight batches
     * @return this builder for method chaining
     */
    public ArrowStreamConfigurationOptionsBuilder setMaxInflightBatches(int maxInflightBatches) {
      this.maxInflightBatches = maxInflightBatches;
      return this;
    }

    /**
     * Sets whether automatic recovery is enabled.
     *
     * @param recovery true to enable automatic recovery, false to disable
     * @return this builder for method chaining
     */
    public ArrowStreamConfigurationOptionsBuilder setRecovery(boolean recovery) {
      this.recovery = recovery;
      return this;
    }

    /**
     * Sets the timeout for recovery operations.
     *
     * @param recoveryTimeoutMs the recovery timeout in milliseconds
     * @return this builder for method chaining
     */
    public ArrowStreamConfigurationOptionsBuilder setRecoveryTimeoutMs(long recoveryTimeoutMs) {
      this.recoveryTimeoutMs = recoveryTimeoutMs;
      return this;
    }

    /**
     * Sets the backoff delay between recovery attempts.
     *
     * @param recoveryBackoffMs the recovery backoff delay in milliseconds
     * @return this builder for method chaining
     */
    public ArrowStreamConfigurationOptionsBuilder setRecoveryBackoffMs(long recoveryBackoffMs) {
      this.recoveryBackoffMs = recoveryBackoffMs;
      return this;
    }

    /**
     * Sets the maximum number of recovery attempts.
     *
     * @param recoveryRetries the maximum number of recovery attempts
     * @return this builder for method chaining
     */
    public ArrowStreamConfigurationOptionsBuilder setRecoveryRetries(int recoveryRetries) {
      this.recoveryRetries = recoveryRetries;
      return this;
    }

    /**
     * Sets the timeout for server acknowledgment.
     *
     * @param serverLackOfAckTimeoutMs the server acknowledgment timeout in milliseconds
     * @return this builder for method chaining
     */
    public ArrowStreamConfigurationOptionsBuilder setServerLackOfAckTimeoutMs(
        long serverLackOfAckTimeoutMs) {
      this.serverLackOfAckTimeoutMs = serverLackOfAckTimeoutMs;
      return this;
    }

    /**
     * Sets the timeout for flush operations.
     *
     * @param flushTimeoutMs the flush timeout in milliseconds
     * @return this builder for method chaining
     */
    public ArrowStreamConfigurationOptionsBuilder setFlushTimeoutMs(long flushTimeoutMs) {
      this.flushTimeoutMs = flushTimeoutMs;
      return this;
    }

    /**
     * Sets the timeout for establishing a connection.
     *
     * @param connectionTimeoutMs the connection timeout in milliseconds
     * @return this builder for method chaining
     */
    public ArrowStreamConfigurationOptionsBuilder setConnectionTimeoutMs(long connectionTimeoutMs) {
      this.connectionTimeoutMs = connectionTimeoutMs;
      return this;
    }

    /**
     * Builds a new ArrowStreamConfigurationOptions instance.
     *
     * @return a new ArrowStreamConfigurationOptions with the configured settings
     */
    public ArrowStreamConfigurationOptions build() {
      return new ArrowStreamConfigurationOptions(
          this.maxInflightBatches,
          this.recovery,
          this.recoveryTimeoutMs,
          this.recoveryBackoffMs,
          this.recoveryRetries,
          this.serverLackOfAckTimeoutMs,
          this.flushTimeoutMs,
          this.connectionTimeoutMs);
    }
  }
}
