// Single-record dynamic-proto ingestion example.
// See examples/README.md for setup.
// Run from the examples directory:
//
//	go run ./dynamic/single
package main

import (
	"context"
	"log"

	"github.com/databricks/zerobus-sdk/purego/examples/config"
	"github.com/databricks/zerobus-sdk/purego/examples/internal/exampleutil"
	"github.com/databricks/zerobus-sdk/purego/zerobus"
)

func main() {
	cfg := config.Load()

	sdk, err := zerobus.New(cfg.ServerEndpoint, cfg.WorkspaceURL,
		zerobus.WithApplicationName("dynamic-single"))
	if err != nil {
		log.Fatalf("create SDK: %v", err)
	}
	defer sdk.Close()

	ctx := context.Background()
	// Fetch the descriptor once; reuse it for every stream on this table.
	descriptor, err := sdk.FetchProtoDescriptorFromUC(
		ctx, cfg.TableName, cfg.ClientID, cfg.ClientSecret,
	)
	if err != nil {
		log.Fatalf("fetch descriptor: %v", err)
	}

	stream, err := sdk.CreateStream(
		ctx, cfg.TableName, cfg.ClientID, cfg.ClientSecret,
		zerobus.WithProto(descriptor),
	)
	if err != nil {
		log.Fatalf("create stream: %v", err)
	}
	defer stream.Close()

	now := exampleutil.NowMicros()
	records := [][]byte{
		[]byte(exampleutil.MakeOrderJSON(1, "Alice Smith", "Wireless Mouse", 2, 25.99, "pending", now)),
		[]byte(exampleutil.MakeOrderJSON(2, "Bob Johnson", "Mechanical Keyboard", 1, 89.99, "shipped", now)),
		[]byte(exampleutil.MakeOrderJSON(3, "Carol Williams", "USB-C Hub", 3, 45.00, "delivered", now)),
	}
	for i, rec := range records {
		offset, err := stream.IngestJSONOffset(rec)
		if err != nil {
			log.Fatalf("ingest record %d: %v", i+1, err)
		}
		log.Printf("Record %d queued with offset ID: %d", i+1, offset)
	}

	if err := stream.Flush(); err != nil {
		log.Fatalf("flush: %v", err)
	}
	log.Println("All records acknowledged.")
}
