package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.databricks.test.table.AirQualityRow.AirQuality;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.*;

/**
 * Integration tests that run against a real Zerobus server.
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
 * <p>Each test follows the pattern: 1 record/batch + wait, then 10 records/batches + wait for last.
 *
 * <p>Run with: {@code mvn test -Dtest=IntegrationTest}
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class IntegrationTest {

  private static final int BATCH_SIZE = 10;

  private static String serverEndpoint;
  private static String workspaceUrl;
  private static String tableName;
  private static String clientId;
  private static String clientSecret;
  private static boolean configAvailable;
  private static boolean nativeLibraryAvailable;

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
          "Integration tests skipped: Required environment variables not set. "
              + "Set ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME, "
              + "DATABRICKS_CLIENT_ID, and DATABRICKS_CLIENT_SECRET");
    }

    try {
      NativeLoader.ensureLoaded();
      nativeLibraryAvailable = true;
    } catch (UnsatisfiedLinkError | ExceptionInInitializerError e) {
      nativeLibraryAvailable = false;
      System.out.println(
          "Integration tests skipped: Native library not available - " + e.getMessage());
    }
  }

  @BeforeEach
  void skipIfPrerequisitesNotMet() {
    assumeTrue(nativeLibraryAvailable, "Native library not available");
    assumeTrue(configAvailable, "Configuration not available");
  }

  // ===================================================================================
  // Test 1: SDK lifecycle (0 records)
  // ===================================================================================

  @Test
  @Order(1)
  @DisplayName("SDK creates and closes successfully")
  void testSdkCreateAndClose() {
    ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);
    assertNotNull(sdk);
    sdk.close();
  }

  // ===================================================================================
  // Test 2: Proto stream - single record methods (22 records)
  // Main: Message, Alt: byte[]
  // ===================================================================================

  @Test
  @Order(2)
  @DisplayName("Proto stream - single record methods (Message + byte[])")
  void testProtoStreamSingleRecordMethods() throws Exception {
    ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);
    ZerobusProtoStream stream =
        sdk.createProtoStream(
                tableName, AirQuality.getDescriptor().toProto(), clientId, clientSecret)
            .join();

    int totalRecords = 0;

    try {
      // Auto-encoded: Message
      // 1 record + wait
      AirQuality record1 =
          AirQuality.newBuilder()
              .setDeviceName("test-proto-main-single")
              .setTemp(20)
              .setHumidity(50L)
              .build();
      long offset = stream.ingestRecordOffset(record1);
      stream.waitForOffset(offset);
      totalRecords++;

      // 10 records + wait for last
      long lastOffset = -1;
      for (int i = 0; i < 10; i++) {
        AirQuality record =
            AirQuality.newBuilder()
                .setDeviceName("test-proto-main-loop-" + i)
                .setTemp(21 + i)
                .setHumidity(51 + i)
                .build();
        lastOffset = stream.ingestRecordOffset(record);
        totalRecords++;
      }
      stream.waitForOffset(lastOffset);

      // Pre-encoded: byte[]
      // 1 record + wait
      AirQuality record2 =
          AirQuality.newBuilder()
              .setDeviceName("test-proto-alt-single")
              .setTemp(30)
              .setHumidity(60L)
              .build();
      offset = stream.ingestRecordOffset(record2.toByteArray());
      stream.waitForOffset(offset);
      totalRecords++;

      // 10 records + wait for last
      for (int i = 0; i < 10; i++) {
        AirQuality record =
            AirQuality.newBuilder()
                .setDeviceName("test-proto-alt-loop-" + i)
                .setTemp(31 + i)
                .setHumidity(61 + i)
                .build();
        lastOffset = stream.ingestRecordOffset(record.toByteArray());
        totalRecords++;
      }
      stream.waitForOffset(lastOffset);

      System.out.println("Proto single record methods: " + totalRecords + " records ingested");
      assertEquals(22, totalRecords);
    } finally {
      stream.close();
      sdk.close();
    }
  }

  // ===================================================================================
  // Test 3: Proto stream - batch methods (220 records)
  // Main: List<Message>, Alt: List<byte[]>
  // ===================================================================================

  @Test
  @Order(3)
  @DisplayName("Proto stream - batch methods (List<Message> + List<byte[]>)")
  void testProtoStreamBatchMethods() throws Exception {
    ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);
    ZerobusProtoStream stream =
        sdk.createProtoStream(
                tableName, AirQuality.getDescriptor().toProto(), clientId, clientSecret)
            .join();

    int totalRecords = 0;

    try {
      // Auto-encoded: List<Message>
      // 1 batch + wait
      List<AirQuality> batch1 = new ArrayList<>();
      for (int i = 0; i < BATCH_SIZE; i++) {
        batch1.add(
            AirQuality.newBuilder()
                .setDeviceName("test-proto-batch-main-single-" + i)
                .setTemp(20 + i)
                .setHumidity(50 + i)
                .build());
      }
      Optional<Long> offset = stream.ingestRecordsOffset(batch1);
      assertTrue(offset.isPresent());
      stream.waitForOffset(offset.get());
      totalRecords += BATCH_SIZE;

      // 10 batches + wait for last
      Optional<Long> lastOffset = Optional.empty();
      for (int b = 0; b < 10; b++) {
        List<AirQuality> batch = new ArrayList<>();
        for (int i = 0; i < BATCH_SIZE; i++) {
          batch.add(
              AirQuality.newBuilder()
                  .setDeviceName("test-proto-batch-main-loop-" + b + "-" + i)
                  .setTemp(21 + i)
                  .setHumidity(51 + i)
                  .build());
        }
        lastOffset = stream.ingestRecordsOffset(batch);
        totalRecords += BATCH_SIZE;
      }
      assertTrue(lastOffset.isPresent());
      stream.waitForOffset(lastOffset.get());

      // Pre-encoded: List<byte[]>
      // 1 batch + wait
      List<byte[]> bytesBatch1 = new ArrayList<>();
      for (int i = 0; i < BATCH_SIZE; i++) {
        bytesBatch1.add(
            AirQuality.newBuilder()
                .setDeviceName("test-proto-batch-alt-single-" + i)
                .setTemp(30 + i)
                .setHumidity(60 + i)
                .build()
                .toByteArray());
      }
      offset = stream.ingestRecordsOffset(bytesBatch1);
      assertTrue(offset.isPresent());
      stream.waitForOffset(offset.get());
      totalRecords += BATCH_SIZE;

      // 10 batches + wait for last
      for (int b = 0; b < 10; b++) {
        List<byte[]> bytesBatch = new ArrayList<>();
        for (int i = 0; i < BATCH_SIZE; i++) {
          bytesBatch.add(
              AirQuality.newBuilder()
                  .setDeviceName("test-proto-batch-alt-loop-" + b + "-" + i)
                  .setTemp(31 + i)
                  .setHumidity(61 + i)
                  .build()
                  .toByteArray());
        }
        lastOffset = stream.ingestRecordsOffset(bytesBatch);
        totalRecords += BATCH_SIZE;
      }
      assertTrue(lastOffset.isPresent());
      stream.waitForOffset(lastOffset.get());

      System.out.println("Proto batch methods: " + totalRecords + " records ingested");
      assertEquals(220, totalRecords);
    } finally {
      stream.close();
      sdk.close();
    }
  }

  // ===================================================================================
  // Test 4: JSON stream - single record methods (22 records)
  // Main: Object + serializer, Alt: String
  // ===================================================================================

  @Test
  @Order(4)
  @DisplayName("JSON stream - single record methods (Object + String)")
  void testJsonStreamSingleRecordMethods() throws Exception {
    ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);
    ZerobusJsonStream stream = sdk.createJsonStream(tableName, clientId, clientSecret).join();

    int totalRecords = 0;

    try {
      // Auto-serialized: Object + serializer
      // 1 record + wait
      Map<String, Object> data1 = new HashMap<>();
      data1.put("device_name", "test-json-main-single");
      data1.put("temp", 20);
      data1.put("humidity", 50L);
      long offset = stream.ingestRecordOffset(data1, this::toJson);
      stream.waitForOffset(offset);
      totalRecords++;

      // 10 records + wait for last
      long lastOffset = -1;
      for (int i = 0; i < 10; i++) {
        Map<String, Object> data = new HashMap<>();
        data.put("device_name", "test-json-main-loop-" + i);
        data.put("temp", 21 + i);
        data.put("humidity", 51 + i);
        lastOffset = stream.ingestRecordOffset(data, this::toJson);
        totalRecords++;
      }
      stream.waitForOffset(lastOffset);

      // Pre-serialized: String
      // 1 record + wait
      String json1 = "{\"device_name\": \"test-json-alt-single\", \"temp\": 30, \"humidity\": 60}";
      offset = stream.ingestRecordOffset(json1);
      stream.waitForOffset(offset);
      totalRecords++;

      // 10 records + wait for last
      for (int i = 0; i < 10; i++) {
        String json =
            String.format(
                "{\"device_name\": \"test-json-alt-loop-%d\", \"temp\": %d, \"humidity\": %d}",
                i, 31 + i, 61 + i);
        lastOffset = stream.ingestRecordOffset(json);
        totalRecords++;
      }
      stream.waitForOffset(lastOffset);

      System.out.println("JSON single record methods: " + totalRecords + " records ingested");
      assertEquals(22, totalRecords);
    } finally {
      stream.close();
      sdk.close();
    }
  }

  // ===================================================================================
  // Test 5: JSON stream - batch methods (220 records)
  // Main: List<Object> + serializer, Alt: List<String>
  // ===================================================================================

  @Test
  @Order(5)
  @DisplayName("JSON stream - batch methods (List<Object> + List<String>)")
  void testJsonStreamBatchMethods() throws Exception {
    ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);
    ZerobusJsonStream stream = sdk.createJsonStream(tableName, clientId, clientSecret).join();

    int totalRecords = 0;

    try {
      // Auto-serialized: List<Object> + serializer
      // 1 batch + wait
      List<Map<String, Object>> batch1 = new ArrayList<>();
      for (int i = 0; i < BATCH_SIZE; i++) {
        Map<String, Object> data = new HashMap<>();
        data.put("device_name", "test-json-batch-main-single-" + i);
        data.put("temp", 20 + i);
        data.put("humidity", 50 + i);
        batch1.add(data);
      }
      Optional<Long> offset = stream.ingestRecordsOffset(batch1, this::toJson);
      assertTrue(offset.isPresent());
      stream.waitForOffset(offset.get());
      totalRecords += BATCH_SIZE;

      // 10 batches + wait for last
      Optional<Long> lastOffset = Optional.empty();
      for (int b = 0; b < 10; b++) {
        List<Map<String, Object>> batch = new ArrayList<>();
        for (int i = 0; i < BATCH_SIZE; i++) {
          Map<String, Object> data = new HashMap<>();
          data.put("device_name", "test-json-batch-main-loop-" + b + "-" + i);
          data.put("temp", 21 + i);
          data.put("humidity", 51 + i);
          batch.add(data);
        }
        lastOffset = stream.ingestRecordsOffset(batch, this::toJson);
        totalRecords += BATCH_SIZE;
      }
      assertTrue(lastOffset.isPresent());
      stream.waitForOffset(lastOffset.get());

      // Pre-serialized: List<String>
      // 1 batch + wait
      List<String> stringBatch1 = new ArrayList<>();
      for (int i = 0; i < BATCH_SIZE; i++) {
        stringBatch1.add(
            String.format(
                "{\"device_name\": \"test-json-batch-alt-single-%d\", \"temp\": %d, \"humidity\": %d}",
                i, 30 + i, 60 + i));
      }
      offset = stream.ingestRecordsOffset(stringBatch1);
      assertTrue(offset.isPresent());
      stream.waitForOffset(offset.get());
      totalRecords += BATCH_SIZE;

      // 10 batches + wait for last
      for (int b = 0; b < 10; b++) {
        List<String> stringBatch = new ArrayList<>();
        for (int i = 0; i < BATCH_SIZE; i++) {
          stringBatch.add(
              String.format(
                  "{\"device_name\": \"test-json-batch-alt-loop-%d-%d\", \"temp\": %d, \"humidity\": %d}",
                  b, i, 31 + i, 61 + i));
        }
        lastOffset = stream.ingestRecordsOffset(stringBatch);
        totalRecords += BATCH_SIZE;
      }
      assertTrue(lastOffset.isPresent());
      stream.waitForOffset(lastOffset.get());

      System.out.println("JSON batch methods: " + totalRecords + " records ingested");
      assertEquals(220, totalRecords);
    } finally {
      stream.close();
      sdk.close();
    }
  }

  // ===================================================================================
  // Test 6: Legacy stream - Future-based API (11 records)
  // ===================================================================================

  @Test
  @Order(6)
  @DisplayName("Legacy stream - Future-based API")
  void testLegacyStreamFutureBased() throws Exception {
    ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);

    TableProperties<AirQuality> tableProperties =
        new TableProperties<>(tableName, AirQuality.getDefaultInstance());

    @SuppressWarnings("deprecation")
    ZerobusStream<AirQuality> stream =
        sdk.createStream(tableProperties, clientId, clientSecret).join();

    int totalRecords = 0;

    try {
      // 1 record + wait
      AirQuality record1 =
          AirQuality.newBuilder()
              .setDeviceName("test-legacy-single")
              .setTemp(20)
              .setHumidity(50L)
              .build();
      stream.ingestRecord(record1).join();
      totalRecords++;

      // 10 records + wait for last
      java.util.concurrent.CompletableFuture<Void> lastFuture = null;
      for (int i = 0; i < 10; i++) {
        AirQuality record =
            AirQuality.newBuilder()
                .setDeviceName("test-legacy-loop-" + i)
                .setTemp(21 + i)
                .setHumidity(51 + i)
                .build();
        lastFuture = stream.ingestRecord(record);
        totalRecords++;
      }
      if (lastFuture != null) {
        lastFuture.join();
      }

      System.out.println("Legacy stream: " + totalRecords + " records ingested");
      assertEquals(11, totalRecords);
    } finally {
      stream.close();
      sdk.close();
    }
  }

  // ===================================================================================
  // Test 7: Flush operation (11 records)
  // ===================================================================================

  @Test
  @Order(7)
  @DisplayName("Flush operation")
  void testFlush() throws Exception {
    ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);
    ZerobusProtoStream stream =
        sdk.createProtoStream(
                tableName, AirQuality.getDescriptor().toProto(), clientId, clientSecret)
            .join();

    int totalRecords = 0;

    try {
      // 1 record (no wait)
      AirQuality record1 =
          AirQuality.newBuilder()
              .setDeviceName("test-flush-single")
              .setTemp(20)
              .setHumidity(50L)
              .build();
      stream.ingestRecordOffset(record1);
      totalRecords++;

      // 10 records (no wait)
      for (int i = 0; i < 10; i++) {
        AirQuality record =
            AirQuality.newBuilder()
                .setDeviceName("test-flush-loop-" + i)
                .setTemp(21 + i)
                .setHumidity(51 + i)
                .build();
        stream.ingestRecordOffset(record);
        totalRecords++;
      }

      // Flush waits for all
      stream.flush();

      System.out.println("Flush test: " + totalRecords + " records flushed");
      assertEquals(11, totalRecords);
    } finally {
      stream.close();
      sdk.close();
    }
  }

  // ===================================================================================
  // Test 8: High throughput (1000 records)
  // ===================================================================================

  @Test
  @Order(8)
  @DisplayName("High throughput ingestion")
  void testHighThroughput() throws Exception {
    ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);

    StreamConfigurationOptions options =
        StreamConfigurationOptions.builder().setMaxInflightRecords(10000).build();

    ZerobusProtoStream stream =
        sdk.createProtoStream(
                tableName, AirQuality.getDescriptor().toProto(), clientId, clientSecret, options)
            .join();

    int recordCount = 1000;
    long startTime = System.currentTimeMillis();
    long lastOffset = -1;

    try {
      for (int i = 0; i < recordCount; i++) {
        AirQuality record =
            AirQuality.newBuilder()
                .setDeviceName("test-throughput-" + (i % 100))
                .setTemp(15 + (i % 20))
                .setHumidity(40 + (i % 50))
                .build();
        lastOffset = stream.ingestRecordOffset(record);
      }

      stream.waitForOffset(lastOffset);

      long endTime = System.currentTimeMillis();
      double durationSec = (endTime - startTime) / 1000.0;
      double recordsPerSec = recordCount / durationSec;

      System.out.printf(
          "High throughput: %d records in %.2f sec = %.0f rec/sec%n",
          recordCount, durationSec, recordsPerSec);

      assertTrue(recordsPerSec > 100, "Throughput should be at least 100 rec/sec");
    } finally {
      stream.close();
      sdk.close();
    }
  }

  // ===================================================================================
  // Test 9: Ack callback (11 records)
  // ===================================================================================

  @Test
  @Order(9)
  @DisplayName("Ack callback notifications")
  void testAckCallback() throws Exception {
    ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);

    List<Long> ackedOffsets = new ArrayList<>();
    List<String> errors = new ArrayList<>();

    AckCallback callback =
        new AckCallback() {
          @Override
          public void onAck(long offsetId) {
            synchronized (ackedOffsets) {
              ackedOffsets.add(offsetId);
            }
          }

          @Override
          public void onError(long offsetId, String errorMessage) {
            synchronized (errors) {
              errors.add(errorMessage);
            }
          }
        };

    StreamConfigurationOptions options =
        StreamConfigurationOptions.builder().setAckCallback(callback).build();

    ZerobusProtoStream stream =
        sdk.createProtoStream(
                tableName, AirQuality.getDescriptor().toProto(), clientId, clientSecret, options)
            .join();

    int totalRecords = 0;

    try {
      // 1 record + wait
      AirQuality record1 =
          AirQuality.newBuilder()
              .setDeviceName("test-callback-single")
              .setTemp(20)
              .setHumidity(50L)
              .build();
      long offset = stream.ingestRecordOffset(record1);
      stream.waitForOffset(offset);
      totalRecords++;

      // 10 records + wait for last
      long lastOffset = -1;
      for (int i = 0; i < 10; i++) {
        AirQuality record =
            AirQuality.newBuilder()
                .setDeviceName("test-callback-loop-" + i)
                .setTemp(21 + i)
                .setHumidity(51 + i)
                .build();
        lastOffset = stream.ingestRecordOffset(record);
        totalRecords++;
      }
      stream.waitForOffset(lastOffset);

      // Give callback time to be invoked
      Thread.sleep(500);

      System.out.println(
          "Ack callback: " + totalRecords + " records, " + ackedOffsets.size() + " acks received");
      assertTrue(errors.isEmpty(), "No errors should have occurred");
      assertEquals(11, totalRecords);
    } finally {
      stream.close();
      sdk.close();
    }
  }

  // ===================================================================================
  // Test 10: Proto getUnackedRecords with Parser (11 records)
  // ===================================================================================

  @Test
  @Order(10)
  @DisplayName("Proto getUnackedRecords - raw bytes and with Parser")
  void testProtoGetUnackedRecords() throws Exception {
    ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);
    ZerobusProtoStream stream =
        sdk.createProtoStream(
                tableName, AirQuality.getDescriptor().toProto(), clientId, clientSecret)
            .join();

    int totalRecords = 0;

    try {
      // Ingest 11 records (1 + wait, then 10 + wait for last)
      AirQuality first =
          AirQuality.newBuilder()
              .setDeviceName("test-proto-unacked-0")
              .setTemp(20)
              .setHumidity(50)
              .build();
      long offset = stream.ingestRecordOffset(first);
      stream.waitForOffset(offset);
      totalRecords++;

      long lastOffset = -1;
      for (int i = 1; i < 11; i++) {
        AirQuality record =
            AirQuality.newBuilder()
                .setDeviceName("test-proto-unacked-" + i)
                .setTemp(20 + i)
                .setHumidity(50 + i)
                .build();
        lastOffset = stream.ingestRecordOffset(record);
        totalRecords++;
      }
      stream.waitForOffset(lastOffset);

      assertEquals(11, totalRecords);
    } finally {
      stream.close();
    }

    // getUnackedRecords must be called after close
    List<byte[]> rawRecords = stream.getUnackedRecords();
    assertNotNull(rawRecords, "getUnackedRecords() should not return null");
    assertTrue(rawRecords.isEmpty(), "Should be empty after successful close");

    // Test parsing with Parser (empty list)
    List<AirQuality> parsedRecords = stream.getUnackedRecords(AirQuality.parser());
    assertNotNull(parsedRecords, "getUnackedRecords(Parser) should not return null");
    assertTrue(parsedRecords.isEmpty(), "Should be empty after successful close");

    // Test getUnackedBatches
    List<EncodedBatch> batches = stream.getUnackedBatches();
    assertNotNull(batches, "getUnackedBatches() should not return null");
    assertTrue(batches.isEmpty(), "Should be empty after successful close");

    System.out.println("Proto unacked test: " + totalRecords + " ingested, 0 unacked after close");

    sdk.close();
  }

  // ===================================================================================
  // Test 11: JSON getUnackedRecords with Deserializer (11 records)
  // ===================================================================================

  @Test
  @Order(11)
  @DisplayName("JSON getUnackedRecords - raw strings and with Deserializer")
  void testJsonGetUnackedRecords() throws Exception {
    ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);
    ZerobusJsonStream stream = sdk.createJsonStream(tableName, clientId, clientSecret).join();

    int totalRecords = 0;

    try {
      // Ingest 11 records (1 + wait, then 10 + wait for last)
      String firstJson =
          "{\"device_name\": \"test-json-unacked-0\", \"temp\": 20, \"humidity\": 50}";
      long offset = stream.ingestRecordOffset(firstJson);
      stream.waitForOffset(offset);
      totalRecords++;

      long lastOffset = -1;
      for (int i = 1; i < 11; i++) {
        String json =
            String.format(
                "{\"device_name\": \"test-json-unacked-%d\", \"temp\": %d, \"humidity\": %d}",
                i, 20 + i, 50 + i);
        lastOffset = stream.ingestRecordOffset(json);
        totalRecords++;
      }
      stream.waitForOffset(lastOffset);

      assertEquals(11, totalRecords);
    } finally {
      stream.close();
    }

    // getUnackedRecords must be called after close
    List<String> rawRecords = stream.getUnackedRecords();
    assertNotNull(rawRecords, "getUnackedRecords() should not return null");
    assertTrue(rawRecords.isEmpty(), "Should be empty after successful close");

    // Test parsing with JsonDeserializer (empty list)
    List<Map<String, Object>> parsedRecords = stream.getUnackedRecords(this::parseSimpleJson);
    assertNotNull(parsedRecords, "getUnackedRecords(Deserializer) should not return null");
    assertTrue(parsedRecords.isEmpty(), "Should be empty after successful close");

    // Test getUnackedBatches
    List<EncodedBatch> batches = stream.getUnackedBatches();
    assertNotNull(batches, "getUnackedBatches() should not return null");
    assertTrue(batches.isEmpty(), "Should be empty after successful close");

    System.out.println("JSON unacked test: " + totalRecords + " ingested, 0 unacked after close");

    sdk.close();
  }

  // ===================================================================================
  // Test 12: EncodedBatch structure verification (22 records)
  // ===================================================================================

  @Test
  @Order(12)
  @DisplayName("EncodedBatch structure and re-ingestion")
  void testEncodedBatchStructure() throws Exception {
    ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);
    ZerobusProtoStream stream =
        sdk.createProtoStream(
                tableName, AirQuality.getDescriptor().toProto(), clientId, clientSecret)
            .join();

    int totalRecords = 0;

    try {
      // Ingest first batch of 11 records + wait
      List<AirQuality> batch1 = new ArrayList<>();
      for (int i = 0; i < 11; i++) {
        batch1.add(
            AirQuality.newBuilder()
                .setDeviceName("test-batch-structure-" + i)
                .setTemp(20 + i)
                .setHumidity(50 + i)
                .build());
      }
      Optional<Long> offset1 = stream.ingestRecordsOffset(batch1);
      offset1.ifPresent(
          o -> {
            try {
              stream.waitForOffset(o);
            } catch (ZerobusException e) {
              throw new RuntimeException(e);
            }
          });
      totalRecords += 11;

      // Ingest second batch of 11 records + wait
      List<AirQuality> batch2 = new ArrayList<>();
      for (int i = 0; i < 11; i++) {
        batch2.add(
            AirQuality.newBuilder()
                .setDeviceName("test-batch-structure-2-" + i)
                .setTemp(30 + i)
                .setHumidity(60 + i)
                .build());
      }
      Optional<Long> offset2 = stream.ingestRecordsOffset(batch2);
      offset2.ifPresent(
          o -> {
            try {
              stream.waitForOffset(o);
            } catch (ZerobusException e) {
              throw new RuntimeException(e);
            }
          });
      totalRecords += 11;

      assertEquals(22, totalRecords);
    } finally {
      stream.close();
    }

    // getUnackedBatches must be called after close
    List<EncodedBatch> batches = stream.getUnackedBatches();
    assertNotNull(batches, "getUnackedBatches() should not return null");
    assertTrue(batches.isEmpty(), "Should be empty after successful close");

    // Also verify getUnackedRecords returns empty
    List<byte[]> rawRecords = stream.getUnackedRecords();
    assertNotNull(rawRecords);
    assertTrue(rawRecords.isEmpty(), "Should be empty after successful close");

    System.out.println(
        "EncodedBatch test: " + totalRecords + " records ingested, 0 unacked after close");

    sdk.close();
  }

  // ===================================================================================
  // Test 13: RecreateStream - Proto (22 records)
  // ===================================================================================

  @Test
  @Order(13)
  @DisplayName("RecreateStream - Proto stream")
  void testRecreateProtoStream() throws Exception {
    ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);

    // Create and use a proto stream
    ZerobusProtoStream stream =
        sdk.createProtoStream(
                tableName, AirQuality.getDescriptor().toProto(), clientId, clientSecret)
            .join();

    int totalRecords = 0;

    try {
      // Ingest 11 records (1 + wait, then 10 + wait for last)
      AirQuality first =
          AirQuality.newBuilder()
              .setDeviceName("test-recreate-proto-0")
              .setTemp(20)
              .setHumidity(50)
              .build();
      long offset = stream.ingestRecordOffset(first);
      stream.waitForOffset(offset);
      totalRecords++;

      long lastOffset = -1;
      for (int i = 1; i < 11; i++) {
        AirQuality record =
            AirQuality.newBuilder()
                .setDeviceName("test-recreate-proto-" + i)
                .setTemp(20 + i)
                .setHumidity(50 + i)
                .build();
        lastOffset = stream.ingestRecordOffset(record);
        totalRecords++;
      }
      stream.waitForOffset(lastOffset);
    } finally {
      stream.close();
    }

    // Verify unacked records are empty after successful close
    List<byte[]> unacked = stream.getUnackedRecords();
    assertTrue(unacked.isEmpty(), "Should have 0 unacked after successful close");

    // Recreate the stream
    ZerobusProtoStream newStream = sdk.recreateStream(stream).join();

    try {
      // Ingest more records on the new stream
      long lastOffset = -1;
      for (int i = 0; i < 11; i++) {
        AirQuality record =
            AirQuality.newBuilder()
                .setDeviceName("test-recreate-proto-new-" + i)
                .setTemp(30 + i)
                .setHumidity(60 + i)
                .build();
        lastOffset = newStream.ingestRecordOffset(record);
        totalRecords++;
      }
      newStream.waitForOffset(lastOffset);
    } finally {
      newStream.close();
    }

    System.out.println("RecreateStream proto: " + totalRecords + " records total");
    assertEquals(22, totalRecords);

    sdk.close();
  }

  // ===================================================================================
  // Test 14: RecreateStream - JSON (22 records)
  // ===================================================================================

  @Test
  @Order(14)
  @DisplayName("RecreateStream - JSON stream")
  void testRecreateJsonStream() throws Exception {
    ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);

    // Create and use a JSON stream
    ZerobusJsonStream stream = sdk.createJsonStream(tableName, clientId, clientSecret).join();

    int totalRecords = 0;

    try {
      // Ingest 11 records (1 + wait, then 10 + wait for last)
      String firstJson =
          "{\"device_name\": \"test-recreate-json-0\", \"temp\": 20, \"humidity\": 50}";
      long offset = stream.ingestRecordOffset(firstJson);
      stream.waitForOffset(offset);
      totalRecords++;

      long lastOffset = -1;
      for (int i = 1; i < 11; i++) {
        String json =
            String.format(
                "{\"device_name\": \"test-recreate-json-%d\", \"temp\": %d, \"humidity\": %d}",
                i, 20 + i, 50 + i);
        lastOffset = stream.ingestRecordOffset(json);
        totalRecords++;
      }
      stream.waitForOffset(lastOffset);
    } finally {
      stream.close();
    }

    // Verify unacked records are empty after successful close
    List<String> unacked = stream.getUnackedRecords();
    assertTrue(unacked.isEmpty(), "Should have 0 unacked after successful close");

    // Recreate the stream
    ZerobusJsonStream newStream = sdk.recreateStream(stream).join();

    try {
      // Ingest more records on the new stream
      long lastOffset = -1;
      for (int i = 0; i < 11; i++) {
        String json =
            String.format(
                "{\"device_name\": \"test-recreate-json-new-%d\", \"temp\": %d, \"humidity\": %d}",
                i, 30 + i, 60 + i);
        lastOffset = newStream.ingestRecordOffset(json);
        totalRecords++;
      }
      newStream.waitForOffset(lastOffset);
    } finally {
      newStream.close();
    }

    System.out.println("RecreateStream JSON: " + totalRecords + " records total");
    assertEquals(22, totalRecords);

    sdk.close();
  }

  // ===================================================================================
  // Test 15: RecreateStream - Legacy (22 records)
  // ===================================================================================

  @Test
  @Order(15)
  @DisplayName("RecreateStream - Legacy stream")
  void testRecreateLegacyStream() throws Exception {
    ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);

    TableProperties<AirQuality> tableProperties =
        new TableProperties<>(tableName, AirQuality.getDefaultInstance());

    // Create and use a legacy stream
    @SuppressWarnings("deprecation")
    ZerobusStream<AirQuality> stream =
        sdk.createStream(tableProperties, clientId, clientSecret).join();

    int totalRecords = 0;

    try {
      // 1 record + wait
      AirQuality record1 =
          AirQuality.newBuilder()
              .setDeviceName("test-recreate-legacy-0")
              .setTemp(20)
              .setHumidity(50L)
              .build();
      stream.ingestRecord(record1).join();
      totalRecords++;

      // 10 records + wait for last
      java.util.concurrent.CompletableFuture<Void> lastFuture = null;
      for (int i = 1; i < 11; i++) {
        AirQuality record =
            AirQuality.newBuilder()
                .setDeviceName("test-recreate-legacy-" + i)
                .setTemp(20 + i)
                .setHumidity(50 + i)
                .build();
        lastFuture = stream.ingestRecord(record);
        totalRecords++;
      }
      if (lastFuture != null) {
        lastFuture.join();
      }
    } finally {
      stream.close();
    }

    // Verify unacked records are empty after successful close
    // Note: getUnackedRecords() returns empty iterator due to type erasure
    // but the underlying cache should have 0 records after successful flush
    Iterator<AirQuality> unacked = stream.getUnackedRecords();
    int unackedCount = 0;
    while (unacked.hasNext()) {
      unacked.next();
      unackedCount++;
    }
    assertEquals(0, unackedCount, "Should have 0 unacked after successful close");

    // Recreate the stream
    @SuppressWarnings("deprecation")
    ZerobusStream<AirQuality> newStream = sdk.recreateStream(stream).join();

    try {
      // Ingest more records on the new stream
      java.util.concurrent.CompletableFuture<Void> lastFuture = null;
      for (int i = 0; i < 11; i++) {
        AirQuality record =
            AirQuality.newBuilder()
                .setDeviceName("test-recreate-legacy-new-" + i)
                .setTemp(30 + i)
                .setHumidity(60 + i)
                .build();
        lastFuture = newStream.ingestRecord(record);
        totalRecords++;
      }
      if (lastFuture != null) {
        lastFuture.join();
      }
    } finally {
      newStream.close();
    }

    System.out.println("RecreateStream legacy: " + totalRecords + " records total");
    assertEquals(22, totalRecords);

    sdk.close();
  }

  // ===================================================================================
  // Test 16: Concurrent streams (2 records)
  // ===================================================================================

  @Test
  @Order(16)
  @DisplayName("Concurrent streams")
  void testConcurrentStreams() throws Exception {
    ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);

    ZerobusProtoStream stream1 =
        sdk.createProtoStream(
                tableName, AirQuality.getDescriptor().toProto(), clientId, clientSecret)
            .join();
    ZerobusProtoStream stream2 =
        sdk.createProtoStream(
                tableName, AirQuality.getDescriptor().toProto(), clientId, clientSecret)
            .join();

    try {
      AirQuality record1 =
          AirQuality.newBuilder()
              .setDeviceName("test-concurrent-stream-1")
              .setTemp(25)
              .setHumidity(60L)
              .build();
      AirQuality record2 =
          AirQuality.newBuilder()
              .setDeviceName("test-concurrent-stream-2")
              .setTemp(26)
              .setHumidity(61L)
              .build();

      long offset1 = stream1.ingestRecordOffset(record1);
      long offset2 = stream2.ingestRecordOffset(record2);

      stream1.waitForOffset(offset1);
      stream2.waitForOffset(offset2);

      System.out.println("Concurrent streams: 2 records ingested (1 per stream)");
    } finally {
      stream1.close();
      stream2.close();
      sdk.close();
    }
  }

  // ===================================================================================
  // Test 17: No-wait ingestion (10 records)
  // ===================================================================================

  @Test
  @Order(17)
  @DisplayName("No-wait ingestion submits without offsets")
  void testNoWaitIngestionSubmitsWithoutOffsets() throws Exception {
    ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);
    ZerobusProtoStream protoStream =
        sdk.createProtoStream(
                tableName, AirQuality.getDescriptor().toProto(), clientId, clientSecret)
            .join();
    ZerobusJsonStream jsonStream = sdk.createJsonStream(tableName, clientId, clientSecret).join();

    int totalRecords = 0;

    try {
      AirQuality protoNoWait =
          AirQuality.newBuilder()
              .setDeviceName("test-nowait-proto-message")
              .setTemp(20)
              .setHumidity(50L)
              .build();
      protoStream.ingestRecordNoWait(protoNoWait);
      totalRecords++;

      AirQuality protoBytes =
          AirQuality.newBuilder()
              .setDeviceName("test-nowait-proto-bytes")
              .setTemp(21)
              .setHumidity(51L)
              .build();
      protoStream.ingestRecordNoWait(protoBytes.toByteArray());
      totalRecords++;

      List<AirQuality> protoBatch = new ArrayList<>();
      protoBatch.add(
          AirQuality.newBuilder()
              .setDeviceName("test-nowait-proto-batch-0")
              .setTemp(22)
              .setHumidity(52L)
              .build());
      protoBatch.add(
          AirQuality.newBuilder()
              .setDeviceName("test-nowait-proto-batch-1")
              .setTemp(23)
              .setHumidity(53L)
              .build());
      protoStream.ingestRecordsNoWait(protoBatch);
      totalRecords += protoBatch.size();

      AirQuality protoTracked =
          AirQuality.newBuilder()
              .setDeviceName("test-nowait-proto-tracked")
              .setTemp(24)
              .setHumidity(54L)
              .build();
      long protoOffset = protoStream.ingestRecordOffset(protoTracked);
      protoStream.waitForOffset(protoOffset);
      totalRecords++;

      Map<String, Object> jsonNoWait = new HashMap<>();
      jsonNoWait.put("device_name", "test-nowait-json-object");
      jsonNoWait.put("temp", 30);
      jsonNoWait.put("humidity", 60L);
      jsonStream.ingestRecordNoWait(jsonNoWait, this::toJson);
      totalRecords++;

      jsonStream.ingestRecordNoWait(
          "{\"device_name\": \"test-nowait-json-string\", \"temp\": 31, \"humidity\": 61}");
      totalRecords++;

      List<String> jsonBatch = new ArrayList<>();
      jsonBatch.add(
          "{\"device_name\": \"test-nowait-json-batch-0\", \"temp\": 32, \"humidity\": 62}");
      jsonBatch.add(
          "{\"device_name\": \"test-nowait-json-batch-1\", \"temp\": 33, \"humidity\": 63}");
      jsonStream.ingestRecordsNoWait(jsonBatch);
      totalRecords += jsonBatch.size();

      long jsonOffset =
          jsonStream.ingestRecordOffset(
              "{\"device_name\": \"test-nowait-json-tracked\", \"temp\": 34, \"humidity\": 64}");
      jsonStream.waitForOffset(jsonOffset);
      totalRecords++;

      protoStream.flush();
      jsonStream.flush();

      assertEquals(10, totalRecords);
      System.out.println("No-wait ingestion: " + totalRecords + " records submitted");
    } finally {
      protoStream.close();
      jsonStream.close();
      sdk.close();
    }
  }

  // ===================================================================================
  // Helpers
  // ===================================================================================

  /** Simple JSON serializer for Map. */
  private String toJson(Map<String, Object> map) {
    StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      if (!first) sb.append(", ");
      first = false;
      sb.append("\"").append(entry.getKey()).append("\": ");
      Object value = entry.getValue();
      if (value instanceof String) {
        sb.append("\"").append(value).append("\"");
      } else {
        sb.append(value);
      }
    }
    sb.append("}");
    return sb.toString();
  }

  /** Simple JSON parser for testing (extracts key-value pairs from flat JSON). */
  private Map<String, Object> parseSimpleJson(String json) {
    Map<String, Object> result = new HashMap<>();
    // Remove braces and split by comma
    String content = json.trim();
    if (content.startsWith("{")) content = content.substring(1);
    if (content.endsWith("}")) content = content.substring(0, content.length() - 1);

    // Simple parsing - handles "key": value or "key": "value"
    String[] pairs = content.split(",(?=\\s*\")");
    for (String pair : pairs) {
      String[] kv = pair.split(":", 2);
      if (kv.length == 2) {
        String key = kv[0].trim().replace("\"", "");
        String value = kv[1].trim();
        if (value.startsWith("\"") && value.endsWith("\"")) {
          result.put(key, value.substring(1, value.length() - 1));
        } else {
          try {
            result.put(key, Long.parseLong(value));
          } catch (NumberFormatException e) {
            result.put(key, value);
          }
        }
      }
    }
    return result;
  }
}
