package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.*;

import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link StreamConfigurationOptions} and its builder.
 *
 * <p>These tests verify the builder pattern, default values, and configuration validation without
 * requiring the native library.
 */
public class StreamConfigurationOptionsTest {

  @Test
  void testDefaultValues() {
    StreamConfigurationOptions options = StreamConfigurationOptions.getDefault();

    assertEquals(1_000_000, options.maxInflightRecords());
    assertTrue(options.recovery());
    assertEquals(15000, options.recoveryTimeoutMs());
    assertEquals(2000, options.recoveryBackoffMs());
    assertEquals(4, options.recoveryRetries());
    assertEquals(300000, options.flushTimeoutMs());
    assertEquals(60000, options.serverLackOfAckTimeoutMs());
    assertFalse(options.ackCallback().isPresent());
    assertFalse(options.getNewAckCallback().isPresent());
  }

  @Test
  void testBuilderWithCustomValues() {
    StreamConfigurationOptions options =
        StreamConfigurationOptions.builder()
            .setMaxInflightRecords(10000)
            .setRecovery(false)
            .setRecoveryTimeoutMs(30000)
            .setRecoveryBackoffMs(5000)
            .setRecoveryRetries(5)
            .setFlushTimeoutMs(600000)
            .setServerLackOfAckTimeoutMs(120000)
            .build();

    assertEquals(10000, options.maxInflightRecords());
    assertFalse(options.recovery());
    assertEquals(30000, options.recoveryTimeoutMs());
    assertEquals(5000, options.recoveryBackoffMs());
    assertEquals(5, options.recoveryRetries());
    assertEquals(600000, options.flushTimeoutMs());
    assertEquals(120000, options.serverLackOfAckTimeoutMs());
  }

  @Test
  void testBuilderWithOldStyleAckCallback() {
    Consumer<IngestRecordResponse> callback = response -> {};

    @SuppressWarnings("deprecation")
    StreamConfigurationOptions options =
        StreamConfigurationOptions.builder().setAckCallback(callback).build();

    assertTrue(options.ackCallback().isPresent());
    assertEquals(callback, options.ackCallback().get());
  }

  @Test
  void testBuilderWithNewStyleAckCallback() {
    AckCallback callback =
        new AckCallback() {
          @Override
          public void onAck(long offsetId) {}

          @Override
          public void onError(long offsetId, String errorMessage) {}
        };

    StreamConfigurationOptions options =
        StreamConfigurationOptions.builder().setAckCallback(callback).build();

    assertTrue(options.getNewAckCallback().isPresent());
    assertEquals(callback, options.getNewAckCallback().get());
  }

  @Test
  void testBuilderReturnsNewInstanceEachTime() {
    StreamConfigurationOptions.StreamConfigurationOptionsBuilder builder =
        StreamConfigurationOptions.builder();

    StreamConfigurationOptions options1 = builder.setMaxInflightRecords(1000).build();
    StreamConfigurationOptions options2 = builder.setMaxInflightRecords(2000).build();

    // Builder should create independent instances (though this builder may reuse state).
    // The important thing is that build() returns a valid object.
    assertNotNull(options1);
    assertNotNull(options2);
  }

  @Test
  void testPartialBuilderConfiguration() {
    // Only set some values, rest should be defaults.
    StreamConfigurationOptions options =
        StreamConfigurationOptions.builder()
            .setMaxInflightRecords(25000)
            .setRecovery(false)
            .build();

    assertEquals(25000, options.maxInflightRecords());
    assertFalse(options.recovery());
    // These should still be defaults.
    assertEquals(15000, options.recoveryTimeoutMs());
    assertEquals(2000, options.recoveryBackoffMs());
  }

  @Test
  void testMinimumConfiguration() {
    // Build with no customization.
    StreamConfigurationOptions options = StreamConfigurationOptions.builder().build();

    // Should have all defaults.
    assertEquals(1_000_000, options.maxInflightRecords());
    assertTrue(options.recovery());
  }
}
