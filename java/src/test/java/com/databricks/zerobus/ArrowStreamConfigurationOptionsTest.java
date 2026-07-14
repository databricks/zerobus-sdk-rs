package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/** Unit tests for {@link ArrowStreamConfigurationOptions}. */
public class ArrowStreamConfigurationOptionsTest {

  @Test
  void testRejectsInvalidNumericValues() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ArrowStreamConfigurationOptions.builder().setMaxInflightBatches(0));
    assertThrows(
        IllegalArgumentException.class,
        () -> ArrowStreamConfigurationOptions.builder().setRecoveryTimeoutMs(-1));
    assertThrows(
        IllegalArgumentException.class,
        () -> ArrowStreamConfigurationOptions.builder().setRecoveryBackoffMs(-1));
    assertThrows(
        IllegalArgumentException.class,
        () -> ArrowStreamConfigurationOptions.builder().setRecoveryRetries(-1));
    assertThrows(
        IllegalArgumentException.class,
        () -> ArrowStreamConfigurationOptions.builder().setServerLackOfAckTimeoutMs(-1));
    assertThrows(
        IllegalArgumentException.class,
        () -> ArrowStreamConfigurationOptions.builder().setFlushTimeoutMs(-1));
    assertThrows(
        IllegalArgumentException.class,
        () -> ArrowStreamConfigurationOptions.builder().setConnectionTimeoutMs(-1));
    assertThrows(
        IllegalArgumentException.class,
        () -> ArrowStreamConfigurationOptions.builder().setStreamPausedMaxWaitTimeMs(-2));
  }

  @Test
  void testAllowsDocumentedBoundaryValues() {
    ArrowStreamConfigurationOptions options =
        ArrowStreamConfigurationOptions.builder()
            .setRecoveryTimeoutMs(0)
            .setRecoveryBackoffMs(0)
            .setRecoveryRetries(0)
            .setServerLackOfAckTimeoutMs(0)
            .setFlushTimeoutMs(0)
            .setConnectionTimeoutMs(0)
            .setStreamPausedMaxWaitTimeMs(-1)
            .build();

    assertEquals(0, options.recoveryTimeoutMs());
    assertEquals(0, options.recoveryBackoffMs());
    assertEquals(0, options.recoveryRetries());
    assertEquals(0, options.serverLackOfAckTimeoutMs());
    assertEquals(0, options.flushTimeoutMs());
    assertEquals(0, options.connectionTimeoutMs());
    assertEquals(-1, options.streamPausedMaxWaitTimeMs());
  }
}
