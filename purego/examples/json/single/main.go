// Single-record JSON ingestion with the Zerobus pure-Go SDK.
//
// Opens a JSON stream to a Delta table and ingests a handful of records ONE AT A
// TIME with IngestRecordOffset, then flushes ONCE at the end. That is the
// correct pattern: IngestRecordOffset returns as soon as the record is queued;
// sending and acknowledgement happen in the background. Calling
// WaitForOffset/Flush after every record would force a full server round-trip
// per record and collapse throughput. For high volume, prefer the batch API in
// json/batch.
//
// It also demonstrates recovery: if the stream fails terminally, the records it
// never acknowledged can be recovered via GetUnackedRecords and re-ingested on a
// fresh stream.
//
// Configuration — every connection setting is read from the environment, so no
// value is baked into source. Export these before running (see the examples
// README for what each one is):
//
//	ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME,
//	DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET
//
//	go run ./json/single
//
// Target table (see the examples README for the CREATE TABLE statement):
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

func openStream(sdk *zerobus.SDK, cfg config.Settings) (*zerobus.Stream, error) {
	return sdk.CreateStream(context.Background(), cfg.TableName,
		cfg.ClientID, cfg.ClientSecret, zerobus.WithJSON())
}

func main() {
	cfg := config.Load()

	// 1. Build the SDK — an authenticated connection factory. TLS is on by
	//    default; dialing is lazy, so New does not fail on an unreachable service.
	sdk, err := zerobus.New(cfg.ServerEndpoint, cfg.WorkspaceURL,
		zerobus.WithApplicationName("json-single"))
	if err != nil {
		log.Fatalf("create SDK: %v", err)
	}
	defer sdk.Close()

	// 2. Open a JSON stream. No schema is needed — the server maps each record's
	//    fields onto the table's columns by name.
	stream, err := openStream(sdk, cfg)
	if err != nil {
		log.Fatalf("create stream: %v", err)
	}

	now := config.NowMicros()

	// 3. Ingest records one at a time. Each call queues the record and returns
	//    immediately with the assigned offset — there is NO per-record wait here.
	//    The single wait point is the Flush below.
	records := []string{
		config.MakeOrderJSON(1, "Alice Smith", "Wireless Mouse", 2, 25.99, "pending", now),
		config.MakeOrderJSON(2, "Bob Johnson", "Mechanical Keyboard", 1, 89.99, "shipped", now),
		config.MakeOrderJSON(3, "Carol Williams", "USB-C Hub", 3, 45.00, "delivered", now),
	}
	for i, rec := range records {
		offset, err := stream.IngestRecordOffset([]byte(rec))
		if err != nil {
			log.Fatalf("ingest record %d: %v", i+1, err)
		}
		log.Printf("Record %d queued with offset ID: %d", i+1, offset)
	}

	// 4. Flush once, then close — both at a controlled point. A terminal failure
	//    surfaces here; the SDK recovers transparently from transient
	//    disconnects. On failure, GetUnackedRecords hands back whatever was never
	//    acknowledged so it can be replayed on a fresh stream.
	if err := stream.Flush(); err != nil {
		log.Printf("stream failed: %v", err)
		recoverUnacked(sdk, cfg, stream)
		return
	}
	if err := stream.Close(); err != nil {
		log.Fatalf("close: %v", err)
	}
	log.Println("All records acknowledged. Stream closed successfully.")
}

// recoverUnacked drains the unacknowledged records from a failed stream and
// re-ingests them on a fresh one, using the same loop-then-Flush pattern.
func recoverUnacked(sdk *zerobus.SDK, cfg config.Settings, failed *zerobus.Stream) {
	unacked, err := failed.GetUnackedRecords()
	if err != nil {
		log.Fatalf("get unacked records: %v", err)
	}
	log.Printf("Recovering %d unacknowledged records on a fresh stream.", len(unacked))
	if len(unacked) == 0 {
		return
	}
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
