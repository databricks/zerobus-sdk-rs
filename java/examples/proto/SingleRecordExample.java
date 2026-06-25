package com.databricks.zerobus.examples.proto;

import com.databricks.zerobus.*;
import com.databricks.zerobus.examples.proto.AirQualityProto.AirQuality;

/**
 * Proto single-record ingestion example.
 *
 * <p>Demonstrates both ingestion methods:
 * <ul>
 *   <li>Auto-encoded: Message objects (SDK encodes to protobuf bytes)</li>
 *   <li>Pre-encoded: byte arrays (user provides encoded protobuf data)</li>
 * </ul>
 *
 * <p>Run with: {@code java -cp <classpath> com.databricks.zerobus.examples.proto.SingleRecordExample}
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

        System.out.println("=== Proto Single Record Example ===\n");

        ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);
        ZerobusProtoStream stream = sdk.streamBuilder()
            .table(tableName)
            .oauth(clientId, clientSecret)
            .compiledProto(AirQuality.getDescriptor().toProto())
            .build()
            .join();

        int totalRecords = 0;

        try {
            // === Auto-encoded: Message objects ===
            System.out.println("Auto-encoded (Message):");

            AirQuality record1 = AirQuality.newBuilder()
                .setDeviceName("proto-main-single")
                .setTemp(20)
                .setHumidity(50)
                .build();
            long offset = stream.ingestRecordOffset(record1);
            stream.waitForOffset(offset);
            totalRecords++;
            System.out.println("  1 record ingested and acknowledged (offset: " + offset + ")");

            long lastOffset = -1;
            for (int i = 0; i < 10; i++) {
                AirQuality record = AirQuality.newBuilder()
                    .setDeviceName("proto-main-loop-" + i)
                    .setTemp(21 + i)
                    .setHumidity(51 + i)
                    .build();
                lastOffset = stream.ingestRecordOffset(record);
                totalRecords++;
            }
            stream.waitForOffset(lastOffset);
            System.out.println("  10 records ingested, last acknowledged (offset: " + lastOffset + ")");

            // === Pre-encoded: byte arrays ===
            System.out.println("\nPre-encoded (byte[]):");

            AirQuality record2 = AirQuality.newBuilder()
                .setDeviceName("proto-alt-single")
                .setTemp(30)
                .setHumidity(60)
                .build();
            offset = stream.ingestRecordOffset(record2.toByteArray());
            stream.waitForOffset(offset);
            totalRecords++;
            System.out.println("  1 record ingested and acknowledged (offset: " + offset + ")");

            for (int i = 0; i < 10; i++) {
                AirQuality record = AirQuality.newBuilder()
                    .setDeviceName("proto-alt-loop-" + i)
                    .setTemp(31 + i)
                    .setHumidity(61 + i)
                    .build();
                lastOffset = stream.ingestRecordOffset(record.toByteArray());
                totalRecords++;
            }
            stream.waitForOffset(lastOffset);
            System.out.println("  10 records ingested, last acknowledged (offset: " + lastOffset + ")");

            System.out.println("\n=== Complete: " + totalRecords + " records ingested ===");

        } finally {
            stream.close();
        }

        // === Demonstrate getUnackedRecords (must be called after close) ===
        System.out.println("\n--- Demonstrating getUnackedRecords (after close) ---");

        // Get unacked as raw bytes
        java.util.List<byte[]> unackedRaw = stream.getUnackedRecords();
        System.out.println("  Unacked records (raw bytes): " + unackedRaw.size());

        // Get unacked with Parser (deserialize to Message)
        java.util.List<AirQuality> unackedParsed = stream.getUnackedRecords(AirQuality.parser());
        System.out.println("  Unacked records (parsed): " + unackedParsed.size());

        // Get unacked batches
        java.util.List<com.databricks.zerobus.EncodedBatch> unackedBatches = stream.getUnackedBatches();
        System.out.println("  Unacked batches: " + unackedBatches.size());

        System.out.println("\nNote: After successful flush/close, unacked counts should be 0.");

        // === Demonstrate recreateStream ===
        System.out.println("\n--- Demonstrating recreateStream ---");

        // Recreate the stream (would re-ingest any unacked records if there were any)
        ZerobusProtoStream newStream = sdk.recreateStream(stream).join();
        System.out.println("  New stream created successfully");

        // Ingest a few more records on the new stream
        int newRecords = 0;
        try {
            for (int i = 0; i < 3; i++) {
                AirQuality record = AirQuality.newBuilder()
                    .setDeviceName("proto-recreate-" + i)
                    .setTemp(40 + i)
                    .setHumidity(70 + i)
                    .build();
                long newOffset = newStream.ingestRecordOffset(record);
                newRecords++;
            }
            newStream.flush();
            System.out.println("  " + newRecords + " new records ingested on recreated stream");
        } finally {
            newStream.close();
        }

        System.out.println("\n=== RecreateStream demo complete ===");

        sdk.close();
    }
}
