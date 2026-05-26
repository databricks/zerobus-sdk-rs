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
 * Single-batch Arrow Flight ingestion example (Beta).
 *
 * <p>Minimal end-to-end usage: build a schema, create the stream, populate a single {@link
 * VectorSchemaRoot}, ingest it, wait for the ack, and close.
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
 * <p>Run with: {@code java -cp <classpath> com.databricks.zerobus.examples.arrow.SingleBatchExample}
 */
public class SingleBatchExample {

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

    System.out.println("=== Arrow Flight: Single Batch Example (Beta) ===\n");

    // Define the Arrow schema matching the Delta table.
    Schema schema =
        new Schema(
            Arrays.asList(
                Field.nullable("device_name", ArrowType.LargeUtf8.INSTANCE),
                Field.nullable("temp", new ArrowType.Int(32, true)),
                Field.nullable("humidity", new ArrowType.Int(64, true))));

    try (BufferAllocator allocator = new RootAllocator();
        ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl)) {

      ZerobusArrowStream stream =
          sdk.createArrowStream(tableName, schema, clientId, clientSecret).join();

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

        Optional<Long> offset = stream.ingestBatch(batch);
        if (offset.isPresent()) {
          stream.waitForOffset(offset.get());
          System.out.println(
              "  " + rowCount + " rows ingested and acknowledged (offset: " + offset.get() + ")");
        }
      } finally {
        stream.close();
      }

      System.out.println("\n=== Done ===");
    }
  }
}
