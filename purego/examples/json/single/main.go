// Single-record JSON ingestion example.
//
// Queues records with IngestRecordOffset and calls Flush once at the end.
// On terminal failure, replays unacknowledged records on a fresh stream.
//
// Set these environment variables before running:
//
//	ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME,
//	DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET
//
//	go run ./json/single
//
// Target table:
//
//	orders(id INT, customer_name STRING, product_name STRING, quantity INT,
//	       price DOUBLE, status STRING, created_at TIMESTAMP, updated_at TIMESTAMP)
package main

import (
	"context"
	"log"

	"github.com/databricks/zerobus-sdk/purego/examples/config"
	"github.com/databricks/zerobus-sdk/purego/examples/internal/exampleutil"
	"github.com/databricks/zerobus-sdk/purego/zerobus"
)

func openStream(sdk *zerobus.SDK, cfg config.Settings) (*zerobus.Stream, error) {
	return sdk.CreateStream(context.Background(), cfg.TableName,
		cfg.ClientID, cfg.ClientSecret, zerobus.WithJSON())
}

func main() {
	cfg := config.Load()

	// 1. Create SDK.
	sdk, err := zerobus.New(cfg.ServerEndpoint, cfg.WorkspaceURL,
		zerobus.WithApplicationName("json-single"))
	if err != nil {
		log.Fatalf("create SDK: %v", err)
	}
	defer sdk.Close()

	// 2. Open JSON stream.
	stream, err := openStream(sdk, cfg)
	if err != nil {
		log.Fatalf("create stream: %v", err)
	}

	now := exampleutil.NowMicros()

	// 3. Queue records without per-record waits.
	records := []string{
		exampleutil.MakeOrderJSON(1, "Alice Smith", "Wireless Mouse", 2, 25.99, "pending", now),
		exampleutil.MakeOrderJSON(2, "Bob Johnson", "Mechanical Keyboard", 1, 89.99, "shipped", now),
		exampleutil.MakeOrderJSON(3, "Carol Williams", "USB-C Hub", 3, 45.00, "delivered", now),
	}
	for i, rec := range records {
		offset, err := stream.IngestRecordOffset([]byte(rec))
		if err != nil {
			log.Fatalf("ingest record %d: %v", i+1, err)
		}
		log.Printf("Record %d queued with offset ID: %d", i+1, offset)
	}

	// 4. Flush once, then close. A flush timeout leaves the stream active, so
	// GetUnackedRecords() fails and Close() would wait another FlushTimeout.
	// Leave teardown to sdk.Close(), which terminates without a second flush wait.
	if err := stream.Flush(); err != nil {
		log.Printf("flush failed: %v", err)
		unacked, unackedErr := stream.GetUnackedRecords()
		if unackedErr != nil {
			log.Fatalf("unacked retrieval failed (stream may still be active): %v", unackedErr)
		}
		if len(unacked) == 0 {
			return
		}
		recoverUnacked(sdk, cfg, stream, unacked)
		return
	}
	if err := stream.Close(); err != nil {
		log.Fatalf("close: %v", err)
	}
	log.Println("All records acknowledged. Stream closed successfully.")
}

// recoverUnacked re-ingests previously retrieved records on a new stream.
func recoverUnacked(sdk *zerobus.SDK, cfg config.Settings, failed *zerobus.Stream, unacked [][]byte) {
	defer failed.Close()
	log.Printf("Recovering %d unacknowledged records on a fresh stream.", len(unacked))
	retry, err := openStream(sdk, cfg)
	if err != nil {
		log.Fatalf("reopen stream: %v", err)
	}
	defer retry.Close()
	for _, rec := range unacked {
		if _, err := retry.IngestRecordOffset(rec); err != nil {
			log.Fatalf("re-ingest: %v", err)
		}
	}
	if err := retry.Flush(); err != nil {
		log.Fatalf("re-flush: %v", err)
	}
	log.Println("Recovered records re-ingested and acknowledged.")
}
