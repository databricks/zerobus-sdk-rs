package com.databricks.zerobus.examples.proto;

import com.databricks.zerobus.*;
import com.databricks.zerobus.examples.proto.AirQualityProto.AirQuality;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Proto batch ingestion example.
 *
 * <p>Demonstrates both batch methods:
 * <ul>
 *   <li>Auto-encoded: List of Message objects (SDK encodes to protobuf bytes)</li>
 *   <li>Pre-encoded: List of byte arrays (user provides encoded protobuf data)</li>
 * </ul>
 *
 * <p>Run with: {@code java -cp <classpath> com.databricks.zerobus.examples.proto.BatchIngestionExample}
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

        System.out.println("=== Proto Batch Ingestion Example ===\n");

        ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);
        ZerobusProtoStream stream = sdk.streamBuilder()
            .table(tableName)
            .oauth(clientId, clientSecret)
            .compiledProto(AirQuality.getDescriptor().toProto())
            .build()
            .join();

        int totalRecords = 0;

        try {
            // === Auto-encoded: List of Message objects ===
            System.out.println("Auto-encoded (List<Message>):");

            List<AirQuality> batch1 = new ArrayList<>();
            for (int i = 0; i < BATCH_SIZE; i++) {
                batch1.add(AirQuality.newBuilder()
                    .setDeviceName("proto-main-batch-single-" + i)
                    .setTemp(20 + i)
                    .setHumidity(50 + i)
                    .build());
            }
            Optional<Long> offset = stream.ingestRecordsOffset(batch1);
            offset.ifPresent(o -> {
                try { stream.waitForOffset(o); } catch (ZerobusException e) { throw new RuntimeException(e); }
            });
            totalRecords += BATCH_SIZE;
            System.out.println("  1 batch (" + BATCH_SIZE + " records) ingested and acknowledged");

            Optional<Long> lastOffset = Optional.empty();
            for (int b = 0; b < 10; b++) {
                List<AirQuality> batch = new ArrayList<>();
                for (int i = 0; i < BATCH_SIZE; i++) {
                    batch.add(AirQuality.newBuilder()
                        .setDeviceName("proto-main-batch-loop-" + b + "-" + i)
                        .setTemp(21 + i)
                        .setHumidity(51 + i)
                        .build());
                }
                lastOffset = stream.ingestRecordsOffset(batch);
                totalRecords += BATCH_SIZE;
            }
            lastOffset.ifPresent(o -> {
                try { stream.waitForOffset(o); } catch (ZerobusException e) { throw new RuntimeException(e); }
            });
            System.out.println("  10 batches (" + (10 * BATCH_SIZE) + " records) ingested, last acknowledged");

            // === Pre-encoded: List of byte arrays ===
            System.out.println("\nPre-encoded (List<byte[]>):");

            List<byte[]> bytesBatch1 = new ArrayList<>();
            for (int i = 0; i < BATCH_SIZE; i++) {
                bytesBatch1.add(AirQuality.newBuilder()
                    .setDeviceName("proto-alt-batch-single-" + i)
                    .setTemp(30 + i)
                    .setHumidity(60 + i)
                    .build()
                    .toByteArray());
            }
            offset = stream.ingestRecordsOffset(bytesBatch1);
            offset.ifPresent(o -> {
                try { stream.waitForOffset(o); } catch (ZerobusException e) { throw new RuntimeException(e); }
            });
            totalRecords += BATCH_SIZE;
            System.out.println("  1 batch (" + BATCH_SIZE + " records) ingested and acknowledged");

            for (int b = 0; b < 10; b++) {
                List<byte[]> bytesBatch = new ArrayList<>();
                for (int i = 0; i < BATCH_SIZE; i++) {
                    bytesBatch.add(AirQuality.newBuilder()
                        .setDeviceName("proto-alt-batch-loop-" + b + "-" + i)
                        .setTemp(31 + i)
                        .setHumidity(61 + i)
                        .build()
                        .toByteArray());
                }
                lastOffset = stream.ingestRecordsOffset(bytesBatch);
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

        // Get unacked as raw bytes
        List<byte[]> unackedRaw = stream.getUnackedRecords();
        System.out.println("  Unacked records (raw bytes): " + unackedRaw.size());

        // Get unacked with Parser
        List<AirQuality> unackedParsed = stream.getUnackedRecords(AirQuality.parser());
        System.out.println("  Unacked records (parsed): " + unackedParsed.size());

        // Get unacked batches (preserves batch grouping)
        List<EncodedBatch> unackedBatches = stream.getUnackedBatches();
        System.out.println("  Unacked batches: " + unackedBatches.size());

        System.out.println("\nNote: After successful flush/close, unacked counts should be 0.");

        // === Demonstrate recreateStream ===
        System.out.println("\n--- Demonstrating recreateStream ---");

        // Recreate the stream (would re-ingest any unacked records if there were any)
        ZerobusProtoStream newStream = sdk.recreateStream(stream).join();
        System.out.println("  New stream created successfully");

        // Ingest a batch on the new stream
        int newRecords = 0;
        try {
            List<AirQuality> newBatch = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                newBatch.add(AirQuality.newBuilder()
                    .setDeviceName("proto-recreate-batch-" + i)
                    .setTemp(40 + i)
                    .setHumidity(70 + i)
                    .build());
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
}
