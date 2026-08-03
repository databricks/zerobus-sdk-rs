// Batch dynamic-proto ingestion example.
//
// Fetches schema from Unity Catalog, converts JSON records to protobuf at
// runtime, and queues a whole batch in one call.
//
// Set these environment variables before running:
//
//	ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME,
//	DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET
//
//	go run ./dynamic/batch
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
	"github.com/databricks/zerobus-sdk/purego/zerobus"
)

func main() {
	cfg := config.Load()

	sdk, err := zerobus.New(cfg.ServerEndpoint, cfg.WorkspaceURL,
		zerobus.WithApplicationName("dynamic-batch"))
	if err != nil {
		log.Fatalf("create SDK: %v", err)
	}
	defer sdk.Close()

	stream, err := sdk.CreateDynamicProtoStream(context.Background(), cfg.TableName,
		cfg.ClientID, cfg.ClientSecret)
	if err != nil {
		log.Fatalf("create dynamic stream: %v", err)
	}
	defer stream.Close()

	now := config.NowMicros()
	records := []string{
		config.MakeOrderJSON(1, "Alice Smith", "Wireless Mouse", 2, 25.99, "pending", now),
		config.MakeOrderJSON(2, "Bob Johnson", "Mechanical Keyboard", 1, 89.99, "shipped", now),
		config.MakeOrderJSON(3, "Carol Williams", "USB-C Hub", 3, 45.00, "delivered", now),
	}

	batchOffset, err := stream.IngestJSONStringsOffset(records)
	if err != nil {
		log.Fatalf("ingest batch: %v", err)
	}
	log.Printf("Batch of %d records queued; batch offset ID: %d", len(records), batchOffset)

	if batchOffset >= 0 {
		if err := stream.WaitForOffset(batchOffset); err != nil {
			log.Fatalf("wait for offset %d: %v", batchOffset, err)
		}
		log.Printf("Batch acknowledged at offset ID: %d", batchOffset)
	}

	if err := stream.Flush(); err != nil {
		log.Fatalf("flush: %v", err)
	}
	log.Println("All records acknowledged.")
}

