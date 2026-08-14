package com.databricks.zerobus.examples.json;

import com.databricks.zerobus.*;
import java.util.HashMap;
import java.util.Map;

/**
 * JSON single-record ingestion example.
 *
 * <p>Demonstrates both ingestion methods:
 * <ul>
 *   <li>Auto-serialized: Object with serializer function (SDK serializes)</li>
 *   <li>Pre-serialized: Raw JSON string (user provides serialized data)</li>
 * </ul>
 *
 * <p>Run with: {@code java -cp <classpath> com.databricks.zerobus.examples.json.SingleRecordExample}
 */
public class SingleRecordExample {

    public static void main(String[] args) throws Exception {
        String serverEndpoint = System.getenv("ZEROBUS_SERVER_ENDPOINT");
        String workspaceUrl = System.getenv("DATABRICKS_WORKSPACE_URL");
        String tableName = System.getenv("ZEROBUS_TABLE_NAME");
        String clientId = System.getenv("DATABRICKS_CLIENT_ID");
        String clientSecret = System.getenv("DATABRICKS_CLIENT_SECRET");

        if (serverEndpoint == null || workspaceUrl == null || tableName == null
                || clientId == null || clientSecret == null) {
            System.err.println("Error: Required environment variables not set.");
            System.err.println("Set: ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME,");
            System.err.println("     DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET");
            System.exit(1);
        }

        System.out.println("=== JSON Single Record Example ===\n");

        try (ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl)) {
        ZerobusJsonStream stream = sdk.streamBuilder()
                .table(tableName)
                .oauth(clientId, clientSecret)
                .json()
                .build()
                .join();

        int totalRecords = 0;

        try {
            // === Auto-serialized: Object with serializer ===
            System.out.println("Auto-serialized (Object + serializer):");

            for (int i = 0; i < 10; i++) {
                Map<String, Object> data = new HashMap<>();
                data.put("device_name", "json-main-loop-" + i);
                data.put("temp", 20 + i);
                data.put("humidity", 50 + i);
                stream.ingestRecordOffset(data, SingleRecordExample::toJson);
                totalRecords++;
            }
            stream.flush();
            System.out.println("  10 records queued and flushed");

            // === Pre-serialized: Raw JSON string ===
            System.out.println("\nPre-serialized (String):");

            for (int i = 0; i < 10; i++) {
                String json = String.format(
                    "{\"device_name\": \"json-alt-loop-%d\", \"temp\": %d, \"humidity\": %d}",
                    i, 30 + i, 60 + i
                );
                stream.ingestRecordOffset(json);
                totalRecords++;
            }
            stream.flush();
            System.out.println("  10 records queued and flushed");

            System.out.println("\n=== Complete: " + totalRecords + " records ingested ===");

        } finally {
            stream.close();
        }

        // === Demonstrate getUnackedRecords (must be called after close) ===
        System.out.println("\n--- Demonstrating getUnackedRecords (after close) ---");

        // Get unacked as raw JSON strings
        java.util.List<String> unackedRaw = stream.getUnackedRecords();
        System.out.println("  Unacked records (raw strings): " + unackedRaw.size());

        // Get unacked with JsonDeserializer
        java.util.List<Map<String, Object>> unackedParsed =
            stream.getUnackedRecords(SingleRecordExample::parseJson);
        System.out.println("  Unacked records (parsed): " + unackedParsed.size());

        // Get unacked batches
        java.util.List<com.databricks.zerobus.EncodedBatch> unackedBatches = stream.getUnackedBatches();
        System.out.println("  Unacked batches: " + unackedBatches.size());

        System.out.println("\nNote: After successful flush/close, unacked counts should be 0.");

        // === Demonstrate recreateStream ===
        System.out.println("\n--- Demonstrating recreateStream ---");

        // Recreate the stream (would re-ingest any unacked records if there were any)
        try (ZerobusJsonStream newStream = sdk.recreateStream(stream).join()) {
        System.out.println("  New stream created successfully");

        // Ingest a few more records on the new stream
        int newRecords = 0;
            for (int i = 0; i < 3; i++) {
                String json = String.format(
                    "{\"device_name\": \"json-recreate-%d\", \"temp\": %d, \"humidity\": %d}",
                    i, 40 + i, 70 + i
                );
                newStream.ingestRecordOffset(json);
                newRecords++;
            }
            newStream.flush();
            System.out.println("  " + newRecords + " new records ingested on recreated stream");
        }

        System.out.println("\n=== RecreateStream demo complete ===");

        }
    }

    // Simple JSON parser for Map (in production, use Gson or Jackson)
    private static Map<String, Object> parseJson(String json) {
        Map<String, Object> result = new HashMap<>();
        String content = json.trim();
        if (content.startsWith("{")) content = content.substring(1);
        if (content.endsWith("}")) content = content.substring(0, content.length() - 1);
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

    // Simple JSON serializer for Map (in production, use Gson or Jackson)
    private static String toJson(Map<String, Object> map) {
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
}
