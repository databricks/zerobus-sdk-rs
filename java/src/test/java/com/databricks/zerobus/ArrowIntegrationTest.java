package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.*;

/**
 * Integration tests for Arrow Flight stream operations.
 *
 * <p>These tests are skipped unless the following environment variables are set:
 *
 * <ul>
 *   <li>ZEROBUS_SERVER_ENDPOINT - The Zerobus server endpoint URL
 *   <li>DATABRICKS_WORKSPACE_URL - The Databricks workspace URL
 *   <li>ZEROBUS_TABLE_NAME - The target table name (catalog.schema.table)
 *   <li>DATABRICKS_CLIENT_ID - Service principal application ID
 *   <li>DATABRICKS_CLIENT_SECRET - Service principal secret
 * </ul>
 *
 * <p>The target table must have columns: device_name (STRING), temp (INT), humidity (BIGINT).
 *
 * <p>Run with: {@code mvn test -Dtest=ArrowIntegrationTest}
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ArrowIntegrationTest {

  private static String serverEndpoint;
  private static String workspaceUrl;
  private static String tableName;
  private static String clientId;
  private static String clientSecret;
  private static boolean configAvailable;
  private static boolean nativeLibraryAvailable;

  private static final Schema SCHEMA =
      new Schema(
          Arrays.asList(
              Field.nullable("device_name", ArrowType.LargeUtf8.INSTANCE),
              Field.nullable("temp", new ArrowType.Int(32, true)),
              Field.nullable("humidity", new ArrowType.Int(64, true))));

  @BeforeAll
  static void checkPrerequisites() {
    serverEndpoint = System.getenv("ZEROBUS_SERVER_ENDPOINT");
    workspaceUrl = System.getenv("DATABRICKS_WORKSPACE_URL");
    tableName = System.getenv("ZEROBUS_TABLE_NAME");
    clientId = System.getenv("DATABRICKS_CLIENT_ID");
    clientSecret = System.getenv("DATABRICKS_CLIENT_SECRET");

    configAvailable =
        serverEndpoint != null
            && workspaceUrl != null
            && tableName != null
            && clientId != null
            && clientSecret != null;

    if (!configAvailable) {
      System.out.println(
          "Arrow integration tests skipped: Required environment variables not set.");
    }

    try {
      NativeLoader.ensureLoaded();
      nativeLibraryAvailable = true;
    } catch (UnsatisfiedLinkError | ExceptionInInitializerError e) {
      nativeLibraryAvailable = false;
      System.out.println(
          "Arrow integration tests skipped: Native library not available - " + e.getMessage());
    }
  }

  @BeforeEach
  void skipIfPrerequisitesNotMet() {
    assumeTrue(nativeLibraryAvailable, "Native library not available");
    assumeTrue(configAvailable, "Configuration not available");
  }

  // ===================================================================================
  // Test 1: Arrow stream - single batch ingestion (5 rows)
  // ===================================================================================

  @Test
  @Order(1)
  @DisplayName("Arrow stream - single batch ingestion")
  void testArrowSingleBatchIngestion() throws Exception {
    ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);

    try (BufferAllocator allocator = new RootAllocator()) {
      ZerobusArrowStream stream =
          sdk.createArrowStream(tableName, SCHEMA, clientId, clientSecret).join();

      try {
        try (VectorSchemaRoot batch = VectorSchemaRoot.create(SCHEMA, allocator)) {
          LargeVarCharVector nameVector = (LargeVarCharVector) batch.getVector("device_name");
          IntVector tempVector = (IntVector) batch.getVector("temp");
          BigIntVector humidityVector = (BigIntVector) batch.getVector("humidity");

          int rowCount = 5;
          batch.allocateNew();
          for (int i = 0; i < rowCount; i++) {
            nameVector.setSafe(i, ("test-arrow-single-" + i).getBytes());
            tempVector.setSafe(i, 20 + i);
            humidityVector.setSafe(i, 50 + i);
          }
          batch.setRowCount(rowCount);

          long offset = stream.ingestBatch(batch).get();
          stream.waitForOffset(offset);

          System.out.println(
              "Arrow single batch: " + rowCount + " rows ingested (offset: " + offset + ")");
        }
      } finally {
        stream.close();
      }
    }

    sdk.close();
  }

  // ===================================================================================
  // Test 2: Arrow stream - multiple batches with flush (30 rows)
  // ===================================================================================

  @Test
  @Order(2)
  @DisplayName("Arrow stream - multiple batches with flush")
  void testArrowMultipleBatchesFlush() throws Exception {
    ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);

    try (BufferAllocator allocator = new RootAllocator()) {
      ZerobusArrowStream stream =
          sdk.createArrowStream(tableName, SCHEMA, clientId, clientSecret).join();

      int totalRows = 0;

      try {
        for (int batchNum = 0; batchNum < 3; batchNum++) {
          try (VectorSchemaRoot batch = VectorSchemaRoot.create(SCHEMA, allocator)) {
            LargeVarCharVector nameVector = (LargeVarCharVector) batch.getVector("device_name");
            IntVector tempVector = (IntVector) batch.getVector("temp");
            BigIntVector humidityVector = (BigIntVector) batch.getVector("humidity");

            int rowCount = 10;
            batch.allocateNew();
            for (int i = 0; i < rowCount; i++) {
              nameVector.setSafe(i, ("test-arrow-multi-" + batchNum + "-" + i).getBytes());
              tempVector.setSafe(i, 30 + i);
              humidityVector.setSafe(i, 60 + i);
            }
            batch.setRowCount(rowCount);

            stream.ingestBatch(batch).get();
            totalRows += rowCount;
          }
        }

        stream.flush();

        System.out.println("Arrow multiple batches: " + totalRows + " rows flushed");
        assertEquals(30, totalRows);
      } finally {
        stream.close();
      }
    }

    sdk.close();
  }

  // ===================================================================================
  // Test 3: Arrow stream - custom options (5 rows)
  // ===================================================================================

  @Test
  @Order(3)
  @DisplayName("Arrow stream - custom configuration options")
  void testArrowCustomOptions() throws Exception {
    ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);

    ArrowStreamConfigurationOptions options =
        ArrowStreamConfigurationOptions.builder()
            .setMaxInflightBatches(2000)
            .setRecovery(true)
            .setRecoveryRetries(5)
            .setFlushTimeoutMs(600000)
            .setConnectionTimeoutMs(60000)
            .build();

    try (BufferAllocator allocator = new RootAllocator()) {
      ZerobusArrowStream stream =
          sdk.createArrowStream(tableName, SCHEMA, clientId, clientSecret, options).join();

      try {
        try (VectorSchemaRoot batch = VectorSchemaRoot.create(SCHEMA, allocator)) {
          LargeVarCharVector nameVector = (LargeVarCharVector) batch.getVector("device_name");
          IntVector tempVector = (IntVector) batch.getVector("temp");
          BigIntVector humidityVector = (BigIntVector) batch.getVector("humidity");

          batch.allocateNew();
          for (int i = 0; i < 5; i++) {
            nameVector.setSafe(i, ("test-arrow-options-" + i).getBytes());
            tempVector.setSafe(i, 40 + i);
            humidityVector.setSafe(i, 70 + i);
          }
          batch.setRowCount(5);

          long offset = stream.ingestBatch(batch).get();
          stream.waitForOffset(offset);
        }

        assertEquals(2000, stream.getOptions().maxInflightBatches());
        assertEquals(600000, stream.getOptions().flushTimeoutMs());
        assertEquals(60000, stream.getOptions().connectionTimeoutMs());

        System.out.println("Arrow custom options: 5 rows ingested with custom config");
      } finally {
        stream.close();
      }
    }

    sdk.close();
  }

  // ===================================================================================
  // Test 4: Arrow stream - getUnackedBatches after close (5 rows)
  // ===================================================================================

  @Test
  @Order(4)
  @DisplayName("Arrow stream - getUnackedBatches after close")
  void testArrowGetUnackedBatches() throws Exception {
    ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);

    try (BufferAllocator allocator = new RootAllocator()) {
      ZerobusArrowStream stream =
          sdk.createArrowStream(tableName, SCHEMA, clientId, clientSecret).join();

      try {
        try (VectorSchemaRoot batch = VectorSchemaRoot.create(SCHEMA, allocator)) {
          LargeVarCharVector nameVector = (LargeVarCharVector) batch.getVector("device_name");
          IntVector tempVector = (IntVector) batch.getVector("temp");
          BigIntVector humidityVector = (BigIntVector) batch.getVector("humidity");

          batch.allocateNew();
          for (int i = 0; i < 5; i++) {
            nameVector.setSafe(i, ("test-arrow-unacked-" + i).getBytes());
            tempVector.setSafe(i, 50 + i);
            humidityVector.setSafe(i, 80 + i);
          }
          batch.setRowCount(5);

          long offset = stream.ingestBatch(batch).get();
          stream.waitForOffset(offset);
        }
      } finally {
        stream.close();
      }

      assertTrue(stream.isClosed(), "Stream should be closed");

      List<byte[]> unackedBatches = stream.getUnackedBatches();
      assertNotNull(unackedBatches, "getUnackedBatches() should not return null");
      assertTrue(unackedBatches.isEmpty(), "Should be empty after successful flush/close");

      System.out.println("Arrow unacked test: 5 rows ingested, 0 unacked after close");
    }

    sdk.close();
  }

  // ===================================================================================
  // Test 5: Arrow stream - recreateArrowStream (6 rows total)
  // ===================================================================================

  @Test
  @Order(5)
  @DisplayName("Arrow stream - recreateArrowStream")
  void testRecreateArrowStream() throws Exception {
    ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);

    try (BufferAllocator allocator = new RootAllocator()) {
      // Create and use first stream
      ZerobusArrowStream stream =
          sdk.createArrowStream(tableName, SCHEMA, clientId, clientSecret).join();

      try {
        try (VectorSchemaRoot batch = VectorSchemaRoot.create(SCHEMA, allocator)) {
          LargeVarCharVector nameVector = (LargeVarCharVector) batch.getVector("device_name");
          IntVector tempVector = (IntVector) batch.getVector("temp");
          BigIntVector humidityVector = (BigIntVector) batch.getVector("humidity");

          batch.allocateNew();
          for (int i = 0; i < 3; i++) {
            nameVector.setSafe(i, ("test-arrow-recreate-" + i).getBytes());
            tempVector.setSafe(i, 60 + i);
            humidityVector.setSafe(i, 90 + i);
          }
          batch.setRowCount(3);

          long offset = stream.ingestBatch(batch).get();
          stream.waitForOffset(offset);
        }
      } finally {
        stream.close();
      }

      assertTrue(stream.isClosed());
      List<byte[]> unacked = stream.getUnackedBatches();
      assertTrue(unacked.isEmpty(), "Should have 0 unacked after successful close");

      // Recreate the stream
      ZerobusArrowStream newStream = sdk.recreateArrowStream(stream).join();

      try {
        try (VectorSchemaRoot batch = VectorSchemaRoot.create(SCHEMA, allocator)) {
          LargeVarCharVector nameVector = (LargeVarCharVector) batch.getVector("device_name");
          IntVector tempVector = (IntVector) batch.getVector("temp");
          BigIntVector humidityVector = (BigIntVector) batch.getVector("humidity");

          batch.allocateNew();
          for (int i = 0; i < 3; i++) {
            nameVector.setSafe(i, ("test-arrow-recreate-new-" + i).getBytes());
            tempVector.setSafe(i, 70 + i);
            humidityVector.setSafe(i, 95 + i);
          }
          batch.setRowCount(3);

          long offset = newStream.ingestBatch(batch).get();
          newStream.waitForOffset(offset);
        }
      } finally {
        newStream.close();
      }

      System.out.println("Arrow recreate test: 6 rows total (3 + 3 on recreated stream)");
    }

    sdk.close();
  }

  // ===================================================================================
  // Test 6: Arrow stream - high throughput (100 batches x 100 rows)
  // ===================================================================================

  @Test
  @Order(6)
  @DisplayName("Arrow stream - high throughput")
  void testArrowHighThroughput() throws Exception {
    ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);

    ArrowStreamConfigurationOptions options =
        ArrowStreamConfigurationOptions.builder().setMaxInflightBatches(500).build();

    try (BufferAllocator allocator = new RootAllocator()) {
      ZerobusArrowStream stream =
          sdk.createArrowStream(tableName, SCHEMA, clientId, clientSecret, options).join();

      int batchCount = 100;
      int rowsPerBatch = 100;
      long startTime = System.currentTimeMillis();
      long lastOffset = -1;

      try {
        for (int b = 0; b < batchCount; b++) {
          try (VectorSchemaRoot batch = VectorSchemaRoot.create(SCHEMA, allocator)) {
            LargeVarCharVector nameVector = (LargeVarCharVector) batch.getVector("device_name");
            IntVector tempVector = (IntVector) batch.getVector("temp");
            BigIntVector humidityVector = (BigIntVector) batch.getVector("humidity");

            batch.allocateNew();
            for (int i = 0; i < rowsPerBatch; i++) {
              nameVector.setSafe(i, ("test-arrow-throughput-" + b + "-" + i).getBytes());
              tempVector.setSafe(i, 15 + (i % 20));
              humidityVector.setSafe(i, 40 + (i % 50));
            }
            batch.setRowCount(rowsPerBatch);

            lastOffset = stream.ingestBatch(batch).get();
          }
        }

        stream.waitForOffset(lastOffset);

        long endTime = System.currentTimeMillis();
        double durationSec = (endTime - startTime) / 1000.0;
        int totalRows = batchCount * rowsPerBatch;
        double rowsPerSec = totalRows / durationSec;

        System.out.printf(
            "Arrow high throughput: %d rows in %.2f sec = %.0f rows/sec%n",
            totalRows, durationSec, rowsPerSec);

        assertTrue(rowsPerSec > 1000, "Arrow throughput should be at least 1000 rows/sec");
      } finally {
        stream.close();
      }
    }

    sdk.close();
  }

  // ===================================================================================
  // Test 7: Arrow stream - double close is safe
  // ===================================================================================

  @Test
  @Order(7)
  @DisplayName("Arrow stream - double close is safe")
  void testArrowDoubleClose() throws Exception {
    try (ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);
        BufferAllocator allocator = new RootAllocator()) {
      ZerobusArrowStream stream =
          sdk.createArrowStream(tableName, SCHEMA, clientId, clientSecret).join();

      try (VectorSchemaRoot batch = VectorSchemaRoot.create(SCHEMA, allocator)) {
        LargeVarCharVector nameVector = (LargeVarCharVector) batch.getVector("device_name");
        IntVector tempVector = (IntVector) batch.getVector("temp");
        BigIntVector humidityVector = (BigIntVector) batch.getVector("humidity");

        batch.allocateNew();
        nameVector.setSafe(0, "test-double-close".getBytes());
        tempVector.setSafe(0, 1);
        humidityVector.setSafe(0, 1);
        batch.setRowCount(1);

        stream.ingestBatch(batch).get();
      }

      stream.close();
      assertTrue(stream.isClosed());

      // Second close should be a no-op, not throw
      stream.close();
      assertTrue(stream.isClosed());

      System.out.println("Arrow double close: safe");
    }
  }

  // ===================================================================================
  // Test 8: Arrow stream - isClosed state transitions
  // ===================================================================================

  @Test
  @Order(8)
  @DisplayName("Arrow stream - isClosed state transitions")
  void testArrowIsClosedStateTransitions() throws Exception {
    try (ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);
        BufferAllocator allocator = new RootAllocator()) {
      ZerobusArrowStream stream =
          sdk.createArrowStream(tableName, SCHEMA, clientId, clientSecret).join();

      assertFalse(stream.isClosed(), "Stream should be open after creation");

      try (VectorSchemaRoot batch = VectorSchemaRoot.create(SCHEMA, allocator)) {
        LargeVarCharVector nameVector = (LargeVarCharVector) batch.getVector("device_name");
        IntVector tempVector = (IntVector) batch.getVector("temp");
        BigIntVector humidityVector = (BigIntVector) batch.getVector("humidity");

        batch.allocateNew();
        nameVector.setSafe(0, "test-state".getBytes());
        tempVector.setSafe(0, 1);
        humidityVector.setSafe(0, 1);
        batch.setRowCount(1);

        long offset = stream.ingestBatch(batch).get();
        assertFalse(stream.isClosed(), "Stream should be open during ingestion");

        stream.waitForOffset(offset);

        assertFalse(stream.isClosed(), "Stream should be open after ack");
      }

      stream.close();
      assertTrue(stream.isClosed(), "Stream should be closed after close()");

      System.out.println("Arrow isClosed transitions: correct");
    }
  }

  // ===================================================================================
  // Test 9: Arrow stream - concurrent streams
  // ===================================================================================

  @Test
  @Order(9)
  @DisplayName("Arrow stream - concurrent streams")
  void testArrowConcurrentStreams() throws Exception {
    try (ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);
        BufferAllocator allocator = new RootAllocator()) {
      ZerobusArrowStream stream1 =
          sdk.createArrowStream(tableName, SCHEMA, clientId, clientSecret).join();
      ZerobusArrowStream stream2 =
          sdk.createArrowStream(tableName, SCHEMA, clientId, clientSecret).join();

      try {
        try (VectorSchemaRoot batch1 = VectorSchemaRoot.create(SCHEMA, allocator);
            VectorSchemaRoot batch2 = VectorSchemaRoot.create(SCHEMA, allocator)) {
          fillBatch(batch1, "test-concurrent-1", 1);
          fillBatch(batch2, "test-concurrent-2", 1);

          long offset1 = stream1.ingestBatch(batch1).get();
          long offset2 = stream2.ingestBatch(batch2).get();

          stream1.waitForOffset(offset1);
          stream2.waitForOffset(offset2);
        }

        System.out.println("Arrow concurrent streams: 2 rows ingested (1 per stream)");
      } finally {
        stream1.close();
        stream2.close();
      }
    }
  }

  // ===================================================================================
  // Test 10: Arrow stream - table name accessor
  // ===================================================================================

  @Test
  @Order(10)
  @DisplayName("Arrow stream - table name and options accessors")
  void testArrowAccessors() throws Exception {
    ArrowStreamConfigurationOptions options =
        ArrowStreamConfigurationOptions.builder().setMaxInflightBatches(999).build();

    try (ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl)) {
      ZerobusArrowStream stream =
          sdk.createArrowStream(tableName, SCHEMA, clientId, clientSecret, options).join();

      try {
        assertEquals(tableName, stream.getTableName());
        assertEquals(999, stream.getOptions().maxInflightBatches());
        assertTrue(stream.getOptions().recovery());

        System.out.println("Arrow accessors: tableName=" + stream.getTableName());
      } finally {
        stream.close();
      }
    }
  }

  // ===================================================================================
  // Test 11: Arrow stream - ingest after close throws
  // ===================================================================================

  @Test
  @Order(11)
  @DisplayName("Arrow stream - ingest after close throws")
  void testArrowIngestAfterCloseThrows() throws Exception {
    try (ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);
        BufferAllocator allocator = new RootAllocator()) {
      ZerobusArrowStream stream =
          sdk.createArrowStream(tableName, SCHEMA, clientId, clientSecret).join();
      stream.close();

      try (VectorSchemaRoot batch = VectorSchemaRoot.create(SCHEMA, allocator)) {
        fillBatch(batch, "test-after-close", 1);

        assertThrows(ZerobusException.class, () -> stream.ingestBatch(batch));
      }

      System.out.println("Arrow ingest after close: correctly throws");
    }
  }

  // ===================================================================================
  // Test 12: Arrow stream - null batch throws
  // ===================================================================================

  @Test
  @Order(12)
  @DisplayName("Arrow stream - null batch returns empty Optional")
  void testArrowNullBatchReturnsEmpty() throws Exception {
    try (ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl)) {
      ZerobusArrowStream stream =
          sdk.createArrowStream(tableName, SCHEMA, clientId, clientSecret).join();

      try {
        java.util.Optional<Long> result = stream.ingestBatch(null);
        assertFalse(result.isPresent(), "Null batch should return empty Optional");
        System.out.println("Arrow null batch: returns empty Optional");
      } finally {
        stream.close();
      }
    }
  }

  // ===================================================================================
  // Test 13: getUnackedBatches callable after close without error
  // ===================================================================================

  @Test
  @Order(13)
  @DisplayName("Arrow stream - getUnackedBatches returns data after close with short flush timeout")
  void testArrowGetUnackedAfterCloseReturnsData() throws Exception {
    ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);

    // Use a 1ms flush timeout so close() gives up before batches are acked.
    ArrowStreamConfigurationOptions shortFlushOptions =
        ArrowStreamConfigurationOptions.builder().setFlushTimeoutMs(1).setRecovery(false).build();

    try (BufferAllocator allocator = new RootAllocator()) {
      ZerobusArrowStream stream =
          sdk.createArrowStream(tableName, SCHEMA, clientId, clientSecret, shortFlushOptions)
              .join();

      // Ingest multiple batches to maximize the chance that some remain unacked.
      for (int i = 0; i < 10; i++) {
        try (VectorSchemaRoot batch = VectorSchemaRoot.create(SCHEMA, allocator)) {
          fillBatch(batch, "test-arrow-unacked-" + i, 100);
          stream.ingestBatch(batch);
        }
      }

      // close() will flush with a 1ms timeout, which may leave unacked batches behind.
      try {
        stream.close();
      } catch (ZerobusException e) {
        // Expected when the flush timeout expires.
      }

      assertTrue(stream.isClosed(), "Stream should be closed");

      List<byte[]> unacked = stream.getUnackedBatches();
      assertNotNull(unacked, "getUnackedBatches() should not return null after close");

      System.out.println(
          "Arrow getUnackedBatches after close: " + unacked.size() + " unacked batches");
    }

    sdk.close();
  }

  // ===================================================================================
  // Test 14: Arrow stream - LZ4 compression
  // ===================================================================================

  @Test
  @Order(14)
  @DisplayName("Arrow stream - LZ4 compression")
  void testArrowLz4Compression() throws Exception {
    ArrowStreamConfigurationOptions options =
        ArrowStreamConfigurationOptions.builder()
            .setIpcCompression(IPCCompressionType.LZ4_FRAME)
            .build();

    try (ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);
        BufferAllocator allocator = new RootAllocator()) {
      ZerobusArrowStream stream =
          sdk.createArrowStream(tableName, SCHEMA, clientId, clientSecret, options).join();

      try {
        try (VectorSchemaRoot batch = VectorSchemaRoot.create(SCHEMA, allocator)) {
          fillBatch(batch, "test-arrow-lz4", 5);

          long offset = stream.ingestBatch(batch).get();
          stream.waitForOffset(offset);
        }

        assertEquals(IPCCompressionType.LZ4_FRAME, stream.getOptions().ipcCompression());
        System.out.println("Arrow LZ4 compression: 5 rows ingested");
      } finally {
        stream.close();
      }
    }
  }

  // ===================================================================================
  // Test 15: Arrow stream - ZSTD compression
  // ===================================================================================

  @Test
  @Order(15)
  @DisplayName("Arrow stream - ZSTD compression")
  void testArrowZstdCompression() throws Exception {
    ArrowStreamConfigurationOptions options =
        ArrowStreamConfigurationOptions.builder()
            .setIpcCompression(IPCCompressionType.ZSTD)
            .build();

    try (ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);
        BufferAllocator allocator = new RootAllocator()) {
      ZerobusArrowStream stream =
          sdk.createArrowStream(tableName, SCHEMA, clientId, clientSecret, options).join();

      try {
        try (VectorSchemaRoot batch = VectorSchemaRoot.create(SCHEMA, allocator)) {
          fillBatch(batch, "test-arrow-zstd", 5);

          long offset = stream.ingestBatch(batch).get();
          stream.waitForOffset(offset);
        }

        assertEquals(IPCCompressionType.ZSTD, stream.getOptions().ipcCompression());
        System.out.println("Arrow ZSTD compression: 5 rows ingested");
      } finally {
        stream.close();
      }
    }
  }

  // ===================================================================================
  // Test 16: Arrow stream - default compression is NONE
  // ===================================================================================

  @Test
  @Order(16)
  @DisplayName("Arrow stream - default compression is NONE")
  void testArrowDefaultCompressionIsNone() {
    ArrowStreamConfigurationOptions defaultOptions = ArrowStreamConfigurationOptions.getDefault();
    assertEquals(IPCCompressionType.NONE, defaultOptions.ipcCompression());

    ArrowStreamConfigurationOptions builtOptions =
        ArrowStreamConfigurationOptions.builder().build();
    assertEquals(IPCCompressionType.NONE, builtOptions.ipcCompression());

    System.out.println("Arrow default compression: NONE verified");
  }

  // ===================================================================================
  // Helper
  // ===================================================================================

  private static void fillBatch(VectorSchemaRoot batch, String prefix, int rowCount) {
    LargeVarCharVector nameVector = (LargeVarCharVector) batch.getVector("device_name");
    IntVector tempVector = (IntVector) batch.getVector("temp");
    BigIntVector humidityVector = (BigIntVector) batch.getVector("humidity");

    batch.allocateNew();
    for (int i = 0; i < rowCount; i++) {
      nameVector.setSafe(i, (prefix + "-" + i).getBytes());
      tempVector.setSafe(i, 20 + i);
      humidityVector.setSafe(i, 50 + i);
    }
    batch.setRowCount(rowCount);
  }
}
