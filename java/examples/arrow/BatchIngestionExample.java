package com.databricks.zerobus.examples.arrow;

import com.databricks.zerobus.*;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
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
 * Arrow Flight batch ingestion example (Beta).
 *
 * <p>Loop-driven usage covering production patterns:
 *
 * <ul>
 *   <li>Many batches with periodic {@link ZerobusArrowStream#waitForOffset} checkpoints
 *   <li>Custom {@link ArrowStreamConfigurationOptions} (max inflight, IPC compression, recovery,
 *       graceful close wait)
 *   <li>Recovery via {@link ZerobusArrowStream#getUnackedBatches} + {@link
 *       ZerobusSdk#recreateArrowStream} after close
 * </ul>
 *
 * <p>Arrow Flight ingestion is in Beta. The API is stabilising but may still change before reaching
 * GA.
 *
 * <p>Prerequisites:
 *
 * <ul>
 *   <li>A Delta table with columns: device_name (STRING), temp (INT), humidity (BIGINT)
 *   <li>Apache Arrow Java libraries on the classpath (arrow-vector, arrow-memory-netty)
 * </ul>
 *
 * <p>Run with: {@code java -cp <classpath> com.databricks.zerobus.examples.arrow.BatchIngestionExample}
 */
public class BatchIngestionExample {

  private static final int NUM_BATCHES = 20;
  private static final int ROWS_PER_BATCH = 50;
  private static final int CHECKPOINT_EVERY = 5;

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

    System.out.println("=== Arrow Flight: Batch Ingestion Example (Beta) ===\n");

    Schema schema =
        new Schema(
            Arrays.asList(
                Field.nullable("device_name", ArrowType.LargeUtf8.INSTANCE),
                Field.nullable("temp", new ArrowType.Int(32, true)),
                Field.nullable("humidity", new ArrowType.Int(64, true))));

    // Custom options: tune throughput, recovery, and graceful-close behavior.
    // IPC compression trades client CPU for fewer bytes on the wire — enable only
    // when network bandwidth limits throughput.
    ArrowStreamConfigurationOptions options =
        ArrowStreamConfigurationOptions.builder()
            .setMaxInflightBatches(2000)
            .setFlushTimeoutMs(600_000)
            .setRecovery(true)
            .setRecoveryRetries(5)
            .setIpcCompression(IPCCompressionType.ZSTD)
            .setStreamPausedMaxWaitTimeMs(5_000)
            .build();

    try (BufferAllocator allocator = new RootAllocator();
        ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl)) {

      ZerobusArrowStream stream =
          sdk.createArrowStream(tableName, schema, clientId, clientSecret, options).join();

      int totalRows = 0;
      try {
        System.out.println(
            "Ingesting " + NUM_BATCHES + " batches of " + ROWS_PER_BATCH + " rows each...");

        Optional<Long> lastOffset = Optional.empty();
        for (int batchNum = 0; batchNum < NUM_BATCHES; batchNum++) {
          try (VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
            populateBatch(batch, batchNum);
            lastOffset = stream.ingestBatch(batch);
            totalRows += ROWS_PER_BATCH;
          }

          // Periodic checkpoint: wait for acknowledgments to bound in-flight memory.
          if ((batchNum + 1) % CHECKPOINT_EVERY == 0 && lastOffset.isPresent()) {
            stream.waitForOffset(lastOffset.get());
            System.out.println(
                "  Acknowledged through batch " + (batchNum + 1) + " (offset " + lastOffset.get() + ")");
          }
        }

        stream.flush();
        System.out.println("\n  Flushed all in-flight batches (" + totalRows + " rows total)");
      } finally {
        stream.close();
      }

      // After close: inspect any batches that were not acknowledged.
      List<byte[]> unacked = stream.getUnackedBatches();
      System.out.println("\nUnacked batches after close: " + unacked.size());

      // Recovery pattern: recreateArrowStream re-ingests unacked batches and flushes.
      // Safe to call even when nothing is unacked; useful as a uniform recovery path.
      ZerobusArrowStream recovered = sdk.recreateArrowStream(stream).join();
      try {
        try (VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
          populateBatch(batch, NUM_BATCHES);
          Optional<Long> offset = recovered.ingestBatch(batch);
          if (offset.isPresent()) {
            recovered.waitForOffset(offset.get());
            System.out.println(
                "  " + ROWS_PER_BATCH + " rows ingested on recovered stream (offset "
                    + offset.get() + ")");
          }
        }
      } finally {
        recovered.close();
      }

      System.out.println("\n=== Done ===");
    }
  }

  private static void populateBatch(VectorSchemaRoot batch, int batchNum) {
    LargeVarCharVector nameVector = (LargeVarCharVector) batch.getVector("device_name");
    IntVector tempVector = (IntVector) batch.getVector("temp");
    BigIntVector humidityVector = (BigIntVector) batch.getVector("humidity");

    batch.allocateNew();
    for (int i = 0; i < ROWS_PER_BATCH; i++) {
      nameVector.setSafe(i, ("arrow-b" + batchNum + "-r" + i).getBytes());
      tempVector.setSafe(i, 20 + (i % 15));
      humidityVector.setSafe(i, 50 + (i % 40));
    }
    batch.setRowCount(ROWS_PER_BATCH);
  }
}
