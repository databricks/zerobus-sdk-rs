package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link StreamBuilder}.
 *
 * <p>These tests verify required-field validation and configuration accumulation without requiring
 * the native library. They exercise the builder up to (but not including) {@code build()}, which is
 * the only operation that touches native code, so a {@code null} SDK reference is sufficient.
 */
public class StreamBuilderTest {

  private static StreamBuilder builder() {
    return new StreamBuilder(null);
  }

  // ==================== Validation ====================

  @Test
  void validateRequiredThrowsWithoutTable() {
    StreamBuilder b = builder().oauth("client-id", "client-secret");
    IllegalStateException ex = assertThrows(IllegalStateException.class, b::validateRequired);
    assertTrue(ex.getMessage().contains("table name is required"));
  }

  @Test
  void validateRequiredThrowsWithoutAuth() {
    StreamBuilder b = builder().table("catalog.schema.table");
    IllegalStateException ex = assertThrows(IllegalStateException.class, b::validateRequired);
    assertTrue(ex.getMessage().contains("authentication is required"));
  }

  @Test
  void validateRequiredPassesWithTableAndOauth() {
    StreamBuilder b = builder().table("catalog.schema.table").oauth("client-id", "client-secret");
    assertDoesNotThrow(b::validateRequired);
  }

  @Test
  void compiledProtoRejectsNullDescriptor() {
    assertThrows(NullPointerException.class, () -> builder().compiledProto(null));
  }

  @Test
  void arrowRejectsNullSchema() {
    assertThrows(NullPointerException.class, () -> builder().arrow(null));
  }

  // ==================== gRPC option accumulation ====================

  @Test
  void streamOptionsDefaultToGrpcDefaults() {
    StreamConfigurationOptions options = builder().toStreamOptions();
    StreamConfigurationOptions defaults = StreamConfigurationOptions.getDefault();

    assertEquals(defaults.maxInflightRecords(), options.maxInflightRecords());
    assertEquals(defaults.recovery(), options.recovery());
    assertEquals(defaults.recoveryTimeoutMs(), options.recoveryTimeoutMs());
    assertEquals(defaults.recoveryBackoffMs(), options.recoveryBackoffMs());
    assertEquals(defaults.recoveryRetries(), options.recoveryRetries());
    assertEquals(defaults.flushTimeoutMs(), options.flushTimeoutMs());
    assertEquals(defaults.serverLackOfAckTimeoutMs(), options.serverLackOfAckTimeoutMs());
  }

  @Test
  void streamOptionsReflectSetters() {
    StreamConfigurationOptions options =
        builder()
            .maxInflightRecords(123)
            .recovery(false)
            .recoveryTimeoutMs(11)
            .recoveryBackoffMs(22)
            .recoveryRetries(7)
            .serverLackOfAckTimeoutMs(33)
            .flushTimeoutMs(44)
            .toStreamOptions();

    assertEquals(123, options.maxInflightRecords());
    assertFalse(options.recovery());
    assertEquals(11, options.recoveryTimeoutMs());
    assertEquals(22, options.recoveryBackoffMs());
    assertEquals(7, options.recoveryRetries());
    assertEquals(33, options.serverLackOfAckTimeoutMs());
    assertEquals(44, options.flushTimeoutMs());
  }

  @Test
  void ackCallbackIsCarriedThrough() {
    AckCallback callback =
        new AckCallback() {
          @Override
          public void onAck(long offsetId) {}

          @Override
          public void onError(long offsetId, String errorMessage) {}
        };
    StreamConfigurationOptions options = builder().ackCallback(callback).toStreamOptions();
    assertTrue(options.getNewAckCallback().isPresent());
    assertSame(callback, options.getNewAckCallback().get());
  }

  // ==================== Arrow option accumulation ====================

  private static Schema emptySchema() {
    return new Schema(Collections.emptyList());
  }

  @Test
  void arrowOptionsPreserveArrowDefaults() {
    // Crucially, unset shared values must fall back to Arrow's own defaults (e.g. 4 recovery
    // retries) rather than the gRPC defaults (3).
    ArrowStreamConfigurationOptions options = builder().arrow(emptySchema()).buildOptions();
    ArrowStreamConfigurationOptions defaults = ArrowStreamConfigurationOptions.getDefault();

    assertEquals(defaults.maxInflightBatches(), options.maxInflightBatches());
    assertEquals(defaults.recovery(), options.recovery());
    assertEquals(defaults.recoveryTimeoutMs(), options.recoveryTimeoutMs());
    assertEquals(defaults.recoveryBackoffMs(), options.recoveryBackoffMs());
    assertEquals(defaults.recoveryRetries(), options.recoveryRetries());
    assertEquals(defaults.serverLackOfAckTimeoutMs(), options.serverLackOfAckTimeoutMs());
    assertEquals(defaults.flushTimeoutMs(), options.flushTimeoutMs());
    assertEquals(defaults.connectionTimeoutMs(), options.connectionTimeoutMs());
    assertEquals(defaults.ipcCompression(), options.ipcCompression());
    assertEquals(defaults.streamPausedMaxWaitTimeMs(), options.streamPausedMaxWaitTimeMs());
  }

  @Test
  void arrowOptionsApplySharedAndArrowSpecificSetters() {
    ArrowStreamConfigurationOptions options =
        builder()
            .recovery(false)
            .recoveryRetries(9)
            .flushTimeoutMs(55)
            .arrow(emptySchema())
            .maxInflightBatches(7)
            .connectionTimeoutMs(12345L)
            .ipcCompression(IPCCompressionType.ZSTD)
            .streamPausedMaxWaitTimeMs(678L)
            .buildOptions();

    // Shared values set on the base builder.
    assertFalse(options.recovery());
    assertEquals(9, options.recoveryRetries());
    assertEquals(55, options.flushTimeoutMs());

    // Arrow-specific values.
    assertEquals(7, options.maxInflightBatches());
    assertEquals(12345L, options.connectionTimeoutMs());
    assertEquals(IPCCompressionType.ZSTD, options.ipcCompression());
    assertEquals(678L, options.streamPausedMaxWaitTimeMs());
  }

  @Test
  void maxInflightRecordsDoesNotAffectArrowOptions() {
    // maxInflightRecords is a gRPC-only knob; Arrow uses maxInflightBatches.
    ArrowStreamConfigurationOptions options =
        builder().maxInflightRecords(42).arrow(emptySchema()).buildOptions();
    assertEquals(
        ArrowStreamConfigurationOptions.getDefault().maxInflightBatches(),
        options.maxInflightBatches());
  }
}
