package com.databricks.zerobus.examples.legacy;

import com.databricks.zerobus.*;
import com.databricks.zerobus.examples.proto.AirQualityProto.AirQuality;

/**
 * Legacy ZerobusStream example using Future-based API.
 *
 * <p>Demonstrates the deprecated {@link ZerobusStream} class which uses
 * CompletableFuture for ingestion.
 *
 * <p>Note: New code should use {@link ZerobusProtoStream} or {@link ZerobusJsonStream} instead.
 *
 * <p>With the deprecated future-based API, keep the last future and {@code .join()} once after
 * the loop to confirm durability, as shown below. (Joining after every record limits throughput
 * to one record per round-trip.)
 *
 * <p>Run with: {@code java -cp <classpath> com.databricks.zerobus.examples.legacy.LegacyStreamExample}
 */
public class LegacyStreamExample {

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

        System.out.println("=== Legacy ZerobusStream Example (Future-based) ===\n");

        ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);

        TableProperties<AirQuality> tableProperties = new TableProperties<>(
            tableName,
            AirQuality.getDefaultInstance()
        );

        @SuppressWarnings("deprecation")
        ZerobusStream<AirQuality> stream = sdk.createStream(
            tableProperties, clientId, clientSecret
        ).join();

        int totalRecords = 0;

        try {
            System.out.println("Future-based API (ingestRecord().join()):");

            AirQuality record1 = AirQuality.newBuilder()
                .setDeviceName("legacy-single")
                .setTemp(20)
                .setHumidity(50)
                .build();
            stream.ingestRecord(record1).join();
            totalRecords++;
            System.out.println("  1 record ingested and acknowledged");

            // 10 records in loop + wait for last only
            // Note: With Future-based API, we collect futures and wait at the end
            java.util.concurrent.CompletableFuture<Void> lastFuture = null;
            for (int i = 0; i < 10; i++) {
                AirQuality record = AirQuality.newBuilder()
                    .setDeviceName("legacy-loop-" + i)
                    .setTemp(21 + i)
                    .setHumidity(51 + i)
                    .build();
                lastFuture = stream.ingestRecord(record);
                totalRecords++;
            }
            if (lastFuture != null) {
                lastFuture.join();
            }
            System.out.println("  10 records ingested, last acknowledged");

            System.out.println("\n=== Complete: " + totalRecords + " records ingested ===");

        } finally {
            stream.close();
        }

        // === Demonstrate getUnackedRecords (must be called after close) ===
        System.out.println("\n--- Demonstrating getUnackedRecords (after close) ---");

        // Note: Due to type erasure, getUnackedRecords() returns an empty iterator.
        // This is expected behavior - for typed access, use ZerobusProtoStream with a parser.
        java.util.Iterator<AirQuality> unackedIter = stream.getUnackedRecords();
        int unackedCount = 0;
        while (unackedIter.hasNext()) {
            unackedIter.next();
            unackedCount++;
        }
        System.out.println("  Unacked records (via iterator): " + unackedCount);

        System.out.println("\nNote: Legacy getUnackedRecords() returns empty due to type erasure.");
        System.out.println("      Use ZerobusProtoStream.getUnackedRecords(parser) for typed access.");

        // === Demonstrate recreateStream ===
        System.out.println("\n--- Demonstrating recreateStream ---");

        // Recreate the stream (would re-ingest any unacked records if there were any)
        @SuppressWarnings("deprecation")
        ZerobusStream<AirQuality> newStream = sdk.recreateStream(stream).join();
        System.out.println("  New stream created successfully");

        // Ingest a few more records on the new stream.
        // Idiomatic flow: keep the last future and join once after the loop to confirm
        // durability. New code should prefer the offset-based ZerobusProtoStream /
        // ZerobusJsonStream APIs.
        int newRecords = 0;
        try {
            java.util.concurrent.CompletableFuture<Void> lastFuture = null;
            for (int i = 0; i < 3; i++) {
                AirQuality record = AirQuality.newBuilder()
                    .setDeviceName("legacy-recreate-" + i)
                    .setTemp(40 + i)
                    .setHumidity(70 + i)
                    .build();
                lastFuture = newStream.ingestRecord(record); // returns immediately
                newRecords++;
            }
            if (lastFuture != null) {
                lastFuture.join(); // confirm durability once
            }
            System.out.println("  " + newRecords + " new records ingested on recreated stream");
        } finally {
            newStream.close();
        }

        System.out.println("\n=== RecreateStream demo complete ===");

        sdk.close();
    }
}
