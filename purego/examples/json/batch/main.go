// Batch JSON ingestion example.
//
// Uses IngestRecordsOffset and waits on the batch offset.
// Also demonstrates async acks with WithAckCallback.
//
// Set these environment variables before running:
//
//	ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME,
//	DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET
//
//	go run ./json/batch
//
// Target table:
//
//	orders(id INT, customer_name STRING, product_name STRING, quantity INT,
//	       price DOUBLE, status STRING, created_at TIMESTAMP, updated_at TIMESTAMP)
package main

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"github.com/databricks/zerobus-sdk/purego/examples/config"
	"github.com/databricks/zerobus-sdk/purego/examples/internal/exampleutil"
	"github.com/databricks/zerobus-sdk/purego/zerobus"
)

// ackObserver counts acknowledgements from callback hooks.
type ackObserver struct{ acked atomic.Int64 }

func (o *ackObserver) OnAck(offset int64) { o.acked.Add(1) }

func (o *ackObserver) OnError(offset int64, err error) {
	log.Printf("record at offset %d failed: %v", offset, err)
}

func main() {
	cfg := config.Load()

	sdk, err := zerobus.New(cfg.ServerEndpoint, cfg.WorkspaceURL,
		zerobus.WithApplicationName("json-batch"))
	if err != nil {
		log.Fatalf("create SDK: %v", err)
	}
	defer sdk.Close()

	// Open stream with async ack callback.
	obs := &ackObserver{}
	stream, err := sdk.CreateStream(context.Background(), cfg.TableName,
		cfg.ClientID, cfg.ClientSecret,
		zerobus.WithJSON(),
		zerobus.WithAckCallback(obs),
	)
	if err != nil {
		log.Fatalf("create stream: %v", err)
	}
	defer stream.Close()

	now := exampleutil.NowMicros()

	// Build and queue one batch.
	batch := [][]byte{
		[]byte(exampleutil.MakeOrderJSON(1, "Alice Smith", "Wireless Mouse", 2, 25.99, "pending", now)),
		[]byte(exampleutil.MakeOrderJSON(2, "Bob Johnson", "Mechanical Keyboard", 1, 89.99, "shipped", now)),
		[]byte(exampleutil.MakeOrderJSON(3, "Carol Williams", "USB-C Hub", 3, 45.00, "delivered", now)),
	}
	batchOffset, err := stream.IngestRecordsOffset(batch)
	if err != nil {
		log.Fatalf("ingest batch: %v", err)
	}
	log.Printf("Batch of %d records queued; batch offset ID: %d", len(batch), batchOffset)

	if err := stream.Flush(); err != nil {
		log.Fatalf("flush: %v", err)
	}

	// A batch produces one callback event, not one per record. Callback delivery
	// can still be running when Close() returns, so wait for it before exit.
	deadline := time.Now().Add(5 * time.Second)
	for obs.acked.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	if err := stream.Close(); err != nil {
		log.Fatalf("close: %v", err)
	}
	log.Printf("Stream closed. Callback observed %d acknowledgements (expected 1 for the batch).", obs.acked.Load())
}
