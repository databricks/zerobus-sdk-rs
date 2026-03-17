package com.databricks.zerobus.examples.arrow;

import com.databricks.zerobus.*;
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

/**
 * Arrow Flight ingestion example.
 *
 * <p>Demonstrates ingesting columnar data using Apache Arrow record batches via the Arrow Flight
 * protocol. This provides high-performance ingestion for large datasets.
 *
 * <p>Prerequisites:
 *
 * <ul>
 *   <li>A Delta table with columns: device_name (STRING), temp (INT), humidity (BIGINT)
 *   <li>Apache Arrow Java libraries on the classpath (arrow-vector, arrow-memory-netty)
 * </ul>
 *
 * <p>Run with: {@code java -cp <classpath> com.databricks.zerobus.examples.arrow.ArrowIngestionExample}
 */
public class ArrowIngestionExample {

  public static void main(String[] args) throws Exception {
    String serverEndpoint = System.getenv("ZEROBUS_SERVER_ENDPOINT");
    String workspaceUrl = System.getenv("DATABRICKS_WORKSPACE_URL");
    String tableName = System.getenv("ZEROBUS_TABLE_NAME");
    String clientId = System.getenv("DATABRICKS_CLIENT_ID");
    String clientSecret = System.getenv("DATABRICKS_CLIENT_SECRET");

    if (serverEndpoint == null
        || workspaceUrl == null
        || tableName == null
        || clientId == null
        || clientSecret == null) {
      System.err.println("Error: Required environment variables not set.");
      System.err.println(
          "Set: ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME,");
      System.err.println("     DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET");
      System.exit(1);
    }

    System.out.println("=== Arrow Flight Ingestion Example ===\n");

    // Define the Arrow schema matching the Delta table
    Schema schema =
        new Schema(
            Arrays.asList(
                Field.nullable("device_name", ArrowType.LargeUtf8.INSTANCE),
                Field.nullable("temp", new ArrowType.Int(32, true)),
                Field.nullable("humidity", new ArrowType.Int(64, true))));

    try (BufferAllocator allocator = new RootAllocator();
        ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl)) {

      // === Single batch ingestion ===
      System.out.println("--- Single Batch Ingestion ---");

      ZerobusArrowStream stream =
          sdk.createArrowStream(tableName, schema, clientId, clientSecret).join();

      try {
        try (VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
          LargeVarCharVector nameVector = (LargeVarCharVector) batch.getVector("device_name");
          IntVector tempVector = (IntVector) batch.getVector("temp");
          BigIntVector humidityVector = (BigIntVector) batch.getVector("humidity");

          int rowCount = 5;
          batch.allocateNew();
          for (int i = 0; i < rowCount; i++) {
            nameVector.setSafe(i, ("arrow-device-" + i).getBytes());
            tempVector.setSafe(i, 20 + i);
            humidityVector.setSafe(i, 50 + i);
          }
          batch.setRowCount(rowCount);

          long offset = stream.ingestBatch(batch).get();
          stream.waitForOffset(offset);
          System.out.println(
              "  " + rowCount + " rows ingested and acknowledged (offset: " + offset + ")");
        }

        // === Multiple batch ingestion ===
        System.out.println("\n--- Multiple Batch Ingestion ---");

        long lastOffset = -1;
        for (int batchNum = 0; batchNum < 3; batchNum++) {
          try (VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
            LargeVarCharVector nameVector = (LargeVarCharVector) batch.getVector("device_name");
            IntVector tempVector = (IntVector) batch.getVector("temp");
            BigIntVector humidityVector = (BigIntVector) batch.getVector("humidity");

            int rowCount = 10;
            batch.allocateNew();
            for (int i = 0; i < rowCount; i++) {
              nameVector.setSafe(i, ("arrow-batch-" + batchNum + "-row-" + i).getBytes());
              tempVector.setSafe(i, 30 + i);
              humidityVector.setSafe(i, 60 + i);
            }
            batch.setRowCount(rowCount);

            lastOffset = stream.ingestBatch(batch).get();
          }
        }
        stream.flush();
        System.out.println("  3 batches (30 rows total) ingested and flushed");

        // === Custom options ===
        System.out.println("\n--- Custom Options ---");

        ArrowStreamConfigurationOptions customOptions =
            ArrowStreamConfigurationOptions.builder()
                .setMaxInflightBatches(2000)
                .setFlushTimeoutMs(600000)
                .setRecovery(true)
                .setRecoveryRetries(5)
                .build();
        System.out.println(
            "  maxInflightBatches: " + customOptions.maxInflightBatches());
        System.out.println("  flushTimeoutMs: " + customOptions.flushTimeoutMs());
        System.out.println("  recoveryRetries: " + customOptions.recoveryRetries());

      } finally {
        stream.close();
      }

      // === Demonstrate getUnackedBatches and recreateArrowStream ===
      System.out.println("\n--- Unacked Batches (after close) ---");

      List<byte[]> unackedBatches = stream.getUnackedBatches();
      System.out.println("  Unacked batches: " + unackedBatches.size());
      System.out.println("  (Expected 0 after successful flush/close)");

      System.out.println("\n--- Recreate Arrow Stream ---");

      ZerobusArrowStream newStream = sdk.recreateArrowStream(stream).join();
      try {
        try (VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
          LargeVarCharVector nameVector = (LargeVarCharVector) batch.getVector("device_name");
          IntVector tempVector = (IntVector) batch.getVector("temp");
          BigIntVector humidityVector = (BigIntVector) batch.getVector("humidity");

          batch.allocateNew();
          nameVector.setSafe(0, "arrow-recreated".getBytes());
          tempVector.setSafe(0, 99);
          humidityVector.setSafe(0, 99);
          batch.setRowCount(1);

          long offset = newStream.ingestBatch(batch).get();
          newStream.waitForOffset(offset);
          System.out.println("  1 row ingested on recreated stream (offset: " + offset + ")");
        }
      } finally {
        newStream.close();
      }

      System.out.println("\n=== Arrow Flight Example Complete ===");
    }
  }
}
