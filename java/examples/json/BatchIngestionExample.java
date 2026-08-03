package com.databricks.zerobus.examples.json;

import com.databricks.zerobus.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * JSON batch ingestion example.
 *
 * <p>Demonstrates both batch methods:
 * <ul>
 *   <li>Auto-serialized: List of Objects with serializer function (SDK serializes)</li>
 *   <li>Pre-serialized: List of raw JSON strings (user provides serialized data)</li>
 * </ul>
 *
 * <p>Run with: {@code java -cp <classpath> com.databricks.zerobus.examples.json.BatchIngestionExample}
 */
public class BatchIngestionExample {

    private static final int BATCH_SIZE = 10;

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

        System.out.println("=== JSON Batch Ingestion Example ===\n");

        ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);
        ZerobusJsonStream stream = sdk.streamBuilder()
                .table(tableName)
                .oauth(clientId, clientSecret)
                .json()
                .build()
                .join();

        int totalRecords = 0;

        try {
            // === Auto-serialized: List of Objects with serializer ===
            System.out.println("Auto-serialized (List<Object> + serializer):");

            List<Map<String, Object>> batch1 = new ArrayList<>();
            for (int i = 0; i < BATCH_SIZE; i++) {
                Map<String, Object> data = new HashMap<>();
                data.put("device_name", "json-main-batch-single-" + i);
                data.put("temp", 20 + i);
                data.put("humidity", 50 + i);
                batch1.add(data);
            }
            Optional<Long> offset = stream.ingestRecordsOffset(batch1, BatchIngestionExample::toJson);
            offset.ifPresent(o -> {
                try { stream.waitForOffset(o); } catch (ZerobusException e) { throw new RuntimeException(e); }
            });
            totalRecords += BATCH_SIZE;
            System.out.println("  1 batch (" + BATCH_SIZE + " records) ingested and acknowledged");

            Optional<Long> lastOffset = Optional.empty();
            for (int b = 0; b < 10; b++) {
                List<Map<String, Object>> batch = new ArrayList<>();
                for (int i = 0; i < BATCH_SIZE; i++) {
                    Map<String, Object> data = new HashMap<>();
                    data.put("device_name", "json-main-batch-loop-" + b + "-" + i);
                    data.put("temp", 21 + i);
                    data.put("humidity", 51 + i);
                    batch.add(data);
                }
                lastOffset = stream.ingestRecordsOffset(batch, BatchIngestionExample::toJson);
                totalRecords += BATCH_SIZE;
            }
            lastOffset.ifPresent(o -> {
                try { stream.waitForOffset(o); } catch (ZerobusException e) { throw new RuntimeException(e); }
            });
            System.out.println("  10 batches (" + (10 * BATCH_SIZE) + " records) ingested, last acknowledged");

            // === Pre-serialized: List of raw JSON strings ===
            System.out.println("\nPre-serialized (List<String>):");

            List<String> stringBatch1 = new ArrayList<>();
            for (int i = 0; i < BATCH_SIZE; i++) {
                stringBatch1.add(String.format(
                    "{\"device_name\": \"json-alt-batch-single-%d\", \"temp\": %d, \"humidity\": %d}",
                    i, 30 + i, 60 + i
                ));
            }
            offset = stream.ingestRecordsOffset(stringBatch1);
            offset.ifPresent(o -> {
                try { stream.waitForOffset(o); } catch (ZerobusException e) { throw new RuntimeException(e); }
            });
            totalRecords += BATCH_SIZE;
            System.out.println("  1 batch (" + BATCH_SIZE + " records) ingested and acknowledged");

            for (int b = 0; b < 10; b++) {
                List<String> stringBatch = new ArrayList<>();
                for (int i = 0; i < BATCH_SIZE; i++) {
                    stringBatch.add(String.format(
                        "{\"device_name\": \"json-alt-batch-loop-%d-%d\", \"temp\": %d, \"humidity\": %d}",
                        b, i, 31 + i, 61 + i
                    ));
                }
                lastOffset = stream.ingestRecordsOffset(stringBatch);
                totalRecords += BATCH_SIZE;
            }
            lastOffset.ifPresent(o -> {
                try { stream.waitForOffset(o); } catch (ZerobusException e) { throw new RuntimeException(e); }
            });
            System.out.println("  10 batches (" + (10 * BATCH_SIZE) + " records) ingested, last acknowledged");

            System.out.println("\n=== Complete: " + totalRecords + " records ingested ===");

        } finally {
            stream.close();
        }

        // === Demonstrate getUnackedRecords/Batches (must be called after close) ===
        System.out.println("\n--- Demonstrating getUnackedRecords/Batches (after close) ---");

        // Get unacked as raw JSON strings
        List<String> unackedRaw = stream.getUnackedRecords();
        System.out.println("  Unacked records (raw strings): " + unackedRaw.size());

        // Get unacked with JsonDeserializer
        List<Map<String, Object>> unackedParsed =
            stream.getUnackedRecords(BatchIngestionExample::parseJson);
        System.out.println("  Unacked records (parsed): " + unackedParsed.size());

        // Get unacked batches (preserves batch grouping)
        List<EncodedBatch> unackedBatches = stream.getUnackedBatches();
        System.out.println("  Unacked batches: " + unackedBatches.size());

        System.out.println("\nNote: After successful flush/close, unacked counts should be 0.");

        // === Demonstrate recreateStream ===
        System.out.println("\n--- Demonstrating recreateStream ---");

        // Recreate the stream (would re-ingest any unacked records if there were any)
        ZerobusJsonStream newStream = sdk.recreateStream(stream).join();
        System.out.println("  New stream created successfully");

        // Ingest a batch on the new stream
        int newRecords = 0;
        try {
            List<String> newBatch = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                newBatch.add(String.format(
                    "{\"device_name\": \"json-recreate-batch-%d\", \"temp\": %d, \"humidity\": %d}",
                    i, 40 + i, 70 + i
                ));
            }
            Optional<Long> newOffset = newStream.ingestRecordsOffset(newBatch);
            newRecords = 5;
            newStream.flush();
            System.out.println("  " + newRecords + " new records ingested on recreated stream");
        } finally {
            newStream.close();
        }

        System.out.println("\n=== RecreateStream demo complete ===");

        sdk.close();
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
