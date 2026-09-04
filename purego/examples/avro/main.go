//go:build avro

// Avro ingestion example (Beta). Build/run with the `avro` tag:
//
//	go run -tags avro ./avro
//
// Set: ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME,
//
//	DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET
package main

import (
	"context"
	"log"

	"github.com/databricks/zerobus-sdk/purego/examples/config"
	"github.com/databricks/zerobus-sdk/purego/zerobus"
)

// Avro writer schema (JSON), declared once at stream creation.
const avroSchema = `{"type":"record","name":"Order","fields":[` +
	`{"name":"id","type":"long"},{"name":"customer_name","type":"string"}]}`

func main() {
	cfg := config.Load()

	sdk, err := zerobus.New(cfg.ServerEndpoint, cfg.WorkspaceURL,
		zerobus.WithApplicationName("avro"))
	if err != nil {
		log.Fatalf("create SDK: %v", err)
	}
	defer sdk.Close()

	stream, err := sdk.CreateStream(context.Background(), cfg.TableName,
		cfg.ClientID, cfg.ClientSecret, zerobus.WithAvro(avroSchema))
	if err != nil {
		log.Fatalf("create stream: %v", err)
	}

	// Record objects the stream encodes against avroSchema. Queue in a loop,
	// then Flush once — never wait per record.
	orders := []zerobus.AvroRecord{
		{"id": int64(1), "customer_name": "Ada"},
		{"id": int64(2), "customer_name": "Grace"},
	}
	for i, order := range orders {
		if _, err := stream.IngestAvroRecordOffset(order); err != nil {
			log.Fatalf("ingest record %d: %v", i+1, err)
		}
	}

	// Pre-encoded raw Avro datums (bytes) go through IngestRecordOffset:
	//   stream.IngestRecordOffset([]byte{0x02, 0x0a})

	if err := stream.Flush(); err != nil {
		log.Fatalf("flush: %v", err)
	}
	if err := stream.Close(); err != nil {
		log.Fatalf("close: %v", err)
	}
	log.Println("Avro records acknowledged. Stream closed.")
}
