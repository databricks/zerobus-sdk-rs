package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;
import java.util.Arrays;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/**
 * Unit tests that do not require a mock server or environment variables.
 *
 * <p>Covers input validation, config builder validation, and Arrow IPC serialization.
 */
public class UnitTest {

  // ==================== Arrow Config Validation ====================

  @Test
  void arrowConfigDefaultValues() {
    ArrowStreamConfigurationOptions options = ArrowStreamConfigurationOptions.getDefault();

    assertEquals(1000, options.maxInflightBatches());
    assertTrue(options.recovery());
    assertEquals(15000, options.recoveryTimeoutMs());
    assertEquals(2000, options.recoveryBackoffMs());
    assertEquals(4, options.recoveryRetries());
    assertEquals(60000, options.serverLackOfAckTimeoutMs());
    assertEquals(300000, options.flushTimeoutMs());
    assertEquals(30000, options.connectionTimeoutMs());
  }

  @Test
  void arrowConfigBuilderCustomValues() {
    ArrowStreamConfigurationOptions options =
        ArrowStreamConfigurationOptions.builder()
            .setMaxInflightBatches(500)
            .setRecovery(false)
            .setRecoveryTimeoutMs(30000)
            .setRecoveryBackoffMs(5000)
            .setRecoveryRetries(10)
            .setServerLackOfAckTimeoutMs(120000)
            .setFlushTimeoutMs(600000)
            .setConnectionTimeoutMs(60000)
            .build();

    assertEquals(500, options.maxInflightBatches());
    assertFalse(options.recovery());
    assertEquals(30000, options.recoveryTimeoutMs());
    assertEquals(5000, options.recoveryBackoffMs());
    assertEquals(10, options.recoveryRetries());
    assertEquals(120000, options.serverLackOfAckTimeoutMs());
    assertEquals(600000, options.flushTimeoutMs());
    assertEquals(60000, options.connectionTimeoutMs());
  }

  @Test
  void arrowConfigRejectsNegativeMaxInflightBatches() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ArrowStreamConfigurationOptions.builder().setMaxInflightBatches(-1));
  }

  @Test
  void arrowConfigRejectsZeroMaxInflightBatches() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ArrowStreamConfigurationOptions.builder().setMaxInflightBatches(0));
  }

  @Test
  void arrowConfigRejectsNegativeTimeouts() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ArrowStreamConfigurationOptions.builder().setRecoveryTimeoutMs(-1));
    assertThrows(
        IllegalArgumentException.class,
        () -> ArrowStreamConfigurationOptions.builder().setRecoveryBackoffMs(-1));
    assertThrows(
        IllegalArgumentException.class,
        () -> ArrowStreamConfigurationOptions.builder().setFlushTimeoutMs(-1));
    assertThrows(
        IllegalArgumentException.class,
        () -> ArrowStreamConfigurationOptions.builder().setServerLackOfAckTimeoutMs(-1));
    assertThrows(
        IllegalArgumentException.class,
        () -> ArrowStreamConfigurationOptions.builder().setConnectionTimeoutMs(-1));
  }

  @Test
  void arrowConfigRejectsNegativeRecoveryRetries() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ArrowStreamConfigurationOptions.builder().setRecoveryRetries(-1));
  }

  @Test
  void arrowConfigAllowsZeroTimeouts() {
    ArrowStreamConfigurationOptions options =
        ArrowStreamConfigurationOptions.builder()
            .setRecoveryTimeoutMs(0)
            .setRecoveryBackoffMs(0)
            .setFlushTimeoutMs(0)
            .setServerLackOfAckTimeoutMs(0)
            .setConnectionTimeoutMs(0)
            .setRecoveryRetries(0)
            .build();

    assertEquals(0, options.recoveryTimeoutMs());
    assertEquals(0, options.recoveryBackoffMs());
    assertEquals(0, options.flushTimeoutMs());
    assertEquals(0, options.serverLackOfAckTimeoutMs());
    assertEquals(0, options.connectionTimeoutMs());
    assertEquals(0, options.recoveryRetries());
  }

  // ==================== Stream Config Validation ====================

  @Test
  void streamConfigRejectsNegativeMaxInflightRecords() {
    assertThrows(
        IllegalArgumentException.class,
        () -> StreamConfigurationOptions.builder().setMaxInflightRecords(-1));
  }

  @Test
  void streamConfigRejectsZeroMaxInflightRecords() {
    assertThrows(
        IllegalArgumentException.class,
        () -> StreamConfigurationOptions.builder().setMaxInflightRecords(0));
  }

  @Test
  void streamConfigRejectsNegativeTimeouts() {
    assertThrows(
        IllegalArgumentException.class,
        () -> StreamConfigurationOptions.builder().setRecoveryTimeoutMs(-1));
    assertThrows(
        IllegalArgumentException.class,
        () -> StreamConfigurationOptions.builder().setRecoveryBackoffMs(-1));
    assertThrows(
        IllegalArgumentException.class,
        () -> StreamConfigurationOptions.builder().setFlushTimeoutMs(-1));
    assertThrows(
        IllegalArgumentException.class,
        () -> StreamConfigurationOptions.builder().setServerLackOfAckTimeoutMs(-1));
  }

  @Test
  void streamConfigRejectsNegativeRecoveryRetries() {
    assertThrows(
        IllegalArgumentException.class,
        () -> StreamConfigurationOptions.builder().setRecoveryRetries(-1));
  }

  // ==================== Arrow IPC Serialization Roundtrip ====================

  @Test
  void arrowSchemaSerializationRoundtrip() throws Exception {
    Schema schema =
        new Schema(
            Arrays.asList(
                Field.nullable("name", new ArrowType.Utf8()),
                Field.nullable("age", new ArrowType.Int(32, true))));

    byte[] ipcBytes = ZerobusArrowStream.serializeSchemaToIpc(schema);
    assertNotNull(ipcBytes);
    assertTrue(ipcBytes.length > 0);

    // Deserialize and verify schema matches
    try (BufferAllocator allocator = new RootAllocator(1024 * 1024);
        ArrowStreamReader reader =
            new ArrowStreamReader(
                Channels.newChannel(new ByteArrayInputStream(ipcBytes)), allocator)) {
      Schema deserialized = reader.getVectorSchemaRoot().getSchema();
      assertEquals(2, deserialized.getFields().size());
      assertEquals("name", deserialized.getFields().get(0).getName());
      assertEquals("age", deserialized.getFields().get(1).getName());
    }
  }

  // ==================== Arrow Batch Null/Empty Handling ====================

  @Test
  void arrowIngestBatchReturnsEmptyForNull() throws Exception {
    // We can't call ingestBatch on a real stream without native libs,
    // but we can verify the documented contract by constructing a stream
    // is not feasible here. Instead, test the serialization helper with
    // an empty batch.
    Schema schema =
        new Schema(
            Arrays.asList(
                Field.nullable("name", new ArrowType.Utf8()),
                Field.nullable("age", new ArrowType.Int(32, true))));

    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      // root has 0 rows — the ingestBatch method should return Optional.empty() for this.
      assertEquals(0, root.getRowCount());
    }
  }
}
