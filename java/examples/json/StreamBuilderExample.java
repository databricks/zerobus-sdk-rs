package com.databricks.zerobus.examples.json;

import com.databricks.zerobus.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Stream builder example.
 *
 * <p>Demonstrates {@link ZerobusSdk#streamBuilder()}, the recommended way to create a stream. The
 * builder exposes a single fluent API for all stream types; this example uses JSON. The commented
 * snippets show the Protocol Buffer and Arrow Flight variants.
 *
 * <p>Run with: {@code java -cp <classpath>
 * com.databricks.zerobus.examples.json.StreamBuilderExample}
 */
public class StreamBuilderExample {

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

    System.out.println("=== Stream Builder Example ===\n");

    try (ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl)) {

      // Create a JSON stream via the fluent builder. Stream configuration (recovery, in-flight
      // limits, timeouts, etc.) is set directly on the builder before selecting the format.
      ZerobusJsonStream stream =
          sdk.streamBuilder()
              .table(tableName)
              .oauth(clientId, clientSecret)
              .recovery(true)
              .maxInflightRecords(100)
              .json()
              .build()
              .join();

      try {
        Map<String, Object> data = new HashMap<>();
        data.put("device_name", "stream-builder-example");
        data.put("temp", 20);
        data.put("humidity", 50);

        long offset = stream.ingestRecordOffset(data, StreamBuilderExample::toJson);
        stream.waitForOffset(offset);
        System.out.println("Record ingested and acknowledged (offset: " + offset + ")");
      } finally {
        stream.close();
      }

      System.out.println("\n=== Complete ===");
    }

    // Protocol Buffer streams use the same builder with .compiledProto(...):
    //
    //   ZerobusProtoStream protoStream = sdk.streamBuilder()
    //       .table(tableName)
    //       .oauth(clientId, clientSecret)
    //       .compiledProto(MyProto.getDescriptor().toProto())
    //       .build()
    //       .join();
    //
    // Arrow Flight streams (Beta) use .arrow(schema), with Arrow-specific options available
    // after the .arrow(...) call:
    //
    //   ZerobusArrowStream arrowStream = sdk.streamBuilder()
    //       .table(tableName)
    //       .oauth(clientId, clientSecret)
    //       .arrow(schema)
    //       .ipcCompression(IPCCompressionType.ZSTD)
    //       .maxInflightBatches(100)
    //       .build()
    //       .join();
  }

  // Simple JSON serializer for Map (in production, use Gson or Jackson).
  private static String toJson(Map<String, Object> map) {
    StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      if (!first) {
        sb.append(", ");
      }
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
}
