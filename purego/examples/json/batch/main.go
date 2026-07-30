// Batch JSON ingestion with the Zerobus pure-Go SDK.
//
// Opens a JSON stream and ingests records with the BATCH API,
// IngestRecordsOffset, which queues a whole slice of records in one call. Prefer
// the batch API in hot paths: a batch is one buffer entry and one atomic ack.
//
// The batch call returns a single logical offset for the whole batch; waiting on
// that one offset confirms every record in it.
//
// It also demonstrates an async ack callback (WithAckCallback) that observes
// acknowledgements on a background worker without blocking the ingest loop.
//
// Configuration — every connection setting is read from the environment. Export
// these before running (see the examples README):
//
//	ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME,
//	DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET
//
//	go run ./json/batch
//
// Target table (see the examples README for the CREATE TABLE statement):
//
//	orders(id INT, customer_name STRING, product_name STRING, quantity INT,
//	       price DOUBLE, status STRING, created_at TIMESTAMP, updated_at TIMESTAMP)
package main

import (
	"context"
	"log"
	"sync/atomic"

	"github.com/databricks/zerobus-sdk/purego/examples/config"
	"github.com/databricks/zerobus-sdk/purego/zerobus"
)

// ackObserver counts acknowledgements as they arrive. Its methods run on the
// SDK's callback worker — serialized, off the ingest path — so they only touch
// an atomic and never call back into the stream.
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

	// Open a JSON stream with an async ack callback. The observer is declared
	// before the stream so it outlives every callback invocation.
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

	now := config.NowMicros()

	// Build a batch and hand it over in one call. IngestRecordsOffset queues the
	// whole slice and returns the single offset assigned to the batch.
	batch := [][]byte{
		[]byte(config.MakeOrderJSON(1, "Alice Smith", "Wireless Mouse", 2, 25.99, "pending", now)),
		[]byte(config.MakeOrderJSON(2, "Bob Johnson", "Mechanical Keyboard", 1, 89.99, "shipped", now)),
		[]byte(config.MakeOrderJSON(3, "Carol Williams", "USB-C Hub", 3, 45.00, "delivered", now)),
	}
	batchOffset, err := stream.IngestRecordsOffset(batch)
	if err != nil {
		log.Fatalf("ingest batch: %v", err)
	}
	log.Printf("Batch of %d records queued; batch offset ID: %d", len(batch), batchOffset)

	// Confirm the batch. Waiting on its single offset confirms every record. In a
	// hot path you would queue many batches and Flush once instead.
	if batchOffset >= 0 {
		if err := stream.WaitForOffset(batchOffset); err != nil {
			log.Fatalf("wait for offset %d: %v", batchOffset, err)
		}
		log.Printf("Batch acknowledged at offset ID: %d", batchOffset)
	}

	// Flush drains anything still pending, then close at a controlled point.
	if err := stream.Flush(); err != nil {
		log.Fatalf("flush: %v", err)
	}
	if err := stream.Close(); err != nil {
		log.Fatalf("close: %v", err)
	}
	log.Printf("Stream closed successfully. Callback observed %d acknowledgements.", obs.acked.Load())
}
