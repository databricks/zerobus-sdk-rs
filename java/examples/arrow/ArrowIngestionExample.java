package com.databricks.zerobus.examples.arrow;

import com.databricks.zerobus.*;
import java.util.Arrays;
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
 * Arrow Flight ingestion example.
 *
 * <p>Opens three Arrow Flight streams against the same table, one per IPC compression codec
 * ({@link IPCCompressionType#NONE}, {@link IPCCompressionType#LZ4_FRAME}, {@link
 * IPCCompressionType#ZSTD}). For each stream the example ingests {@value #BATCHES_PER_STREAM}
 * batches of {@value #ROWS_PER_BATCH} rows, waits for the last offset to be acknowledged,
 * flushes pending batches, then closes the stream.
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

  private static final int BATCHES_PER_STREAM = 10;
  private static final int ROWS_PER_BATCH = 10;

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

    Schema schema =
        new Schema(
            Arrays.asList(
                Field.nullable("device_name", ArrowType.LargeUtf8.INSTANCE),
                Field.nullable("temp", new ArrowType.Int(32, true)),
                Field.nullable("humidity", new ArrowType.Int(64, true))));

    try (BufferAllocator allocator = new RootAllocator();
        ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl)) {

      runStream(
          sdk, allocator, schema, tableName, clientId, clientSecret,
          "NONE", IPCCompressionType.NONE);
      runStream(
          sdk, allocator, schema, tableName, clientId, clientSecret,
          "LZ4_FRAME", IPCCompressionType.LZ4_FRAME);
      runStream(
          sdk, allocator, schema, tableName, clientId, clientSecret,
          "ZSTD", IPCCompressionType.ZSTD);

      System.out.println("\n=== Done ===");
    }
  }

  /**
   * Opens one stream with the given IPC compression codec, ingests {@value #BATCHES_PER_STREAM}
   * batches, waits for the last batch's offset to be acknowledged, flushes, and closes.
   */
  private static void runStream(
      ZerobusSdk sdk,
      BufferAllocator allocator,
      Schema schema,
      String tableName,
      String clientId,
      String clientSecret,
      String codecLabel,
      IPCCompressionType codec)
      throws Exception {
    System.out.println("--- Stream with ipcCompression=" + codecLabel + " ---");

    ZerobusArrowStream stream =
        sdk.streamBuilder()
            .table(tableName)
            .oauth(clientId, clientSecret)
            .arrow(schema)
            .ipcCompression(codec)
            .build()
            .join();

    try {
      long lastOffset = -1L;
      for (int batchNum = 0; batchNum < BATCHES_PER_STREAM; batchNum++) {
        try (VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
          populateBatch(batch, codecLabel, batchNum);
          Optional<Long> offset = stream.ingestBatch(batch);
          if (offset.isPresent()) {
            lastOffset = offset.get();
          }
        }
      }

      if (lastOffset >= 0) {
        stream.waitForOffset(lastOffset);
        System.out.println(
            "  "
                + BATCHES_PER_STREAM
                + " batches × "
                + ROWS_PER_BATCH
                + " rows acknowledged (lastOffset="
                + lastOffset
                + ")");
      }

      stream.flush();
    } finally {
      stream.close();
    }
  }

  private static void populateBatch(VectorSchemaRoot batch, String codecLabel, int batchNum) {
    LargeVarCharVector nameVector = (LargeVarCharVector) batch.getVector("device_name");
    IntVector tempVector = (IntVector) batch.getVector("temp");
    BigIntVector humidityVector = (BigIntVector) batch.getVector("humidity");

    batch.allocateNew();
    for (int i = 0; i < ROWS_PER_BATCH; i++) {
      nameVector.setSafe(i, ("arrow-" + codecLabel + "-b" + batchNum + "-r" + i).getBytes());
      tempVector.setSafe(i, 20 + i);
      humidityVector.setSafe(i, 50 + i);
    }
    batch.setRowCount(ROWS_PER_BATCH);
  }
}
