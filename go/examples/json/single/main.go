package main

import (
	"log"
	"os"

	zerobus "github.com/databricks/zerobus-sdk/go"
)

func main() {
	// Get configuration from environment.
	zerobusEndpoint := os.Getenv("ZEROBUS_SERVER_ENDPOINT")
	unityCatalogURL := os.Getenv("DATABRICKS_WORKSPACE_URL")
	clientID := os.Getenv("DATABRICKS_CLIENT_ID")
	clientSecret := os.Getenv("DATABRICKS_CLIENT_SECRET")
	tableName := os.Getenv("ZEROBUS_TABLE_NAME")

	if zerobusEndpoint == "" || unityCatalogURL == "" || clientID == "" || clientSecret == "" || tableName == "" {
		log.Fatal("Missing required environment variables")
	}

	// Create SDK instance.
	sdk, err := zerobus.NewZerobusSdk(zerobusEndpoint, unityCatalogURL)
	if err != nil {
		log.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	// Configure stream options (optional).
	options := zerobus.DefaultStreamConfigurationOptions()
	options.MaxInflightRequests = 50000
	options.RecordType = zerobus.RecordTypeJson

	// Create stream.
	stream, err := sdk.CreateStream(
		zerobus.TableProperties{
			TableName:       tableName,
			DescriptorProto: nil, // Not needed for JSON.
		},
		clientID,
		clientSecret,
		options,
	)
	if err != nil {
		log.Fatalf("Failed to create stream: %v", err)
	}
	defer stream.Close()

	// Ingest records in a loop. IngestRecordOffset returns as soon as the record
	// is queued; the SDK sends it and tracks its acknowledgment in the background.
	// We collect the last offset and confirm everything below — keeping the ingest
	// loop free of per-record waits is what sustains throughput.
	log.Println("Ingesting records...")
	var lastOffset int64 = -1
	for i := 0; i < 5; i++ {
		// Change this string to match the schema of your table.
		jsonRecord := `{
            "device_name": "sensor-001",
            "temp": 20,
            "humidity": 60
        }`

		offset, err := stream.IngestRecordOffset(jsonRecord)
		if err != nil {
			log.Printf("Failed to ingest record %d: %v", i, err)
			// Check if error is retryable.
			if zerobusErr, ok := err.(*zerobus.ZerobusError); ok && zerobusErr.Retryable() {
				log.Printf("Error is retryable, could retry...")
			}
			continue
		}

		log.Printf("Ingested record %d at offset %d", i, offset)
		lastOffset = offset
	}

	// Wait once on the LAST offset. The ack watermark is monotonic, so confirming
	// the last offset confirms all prior records too. (Equivalently, call
	// stream.Flush() to wait for all pending records.)
	if lastOffset >= 0 {
		log.Println("Waiting for acknowledgments...")
		if err := stream.WaitForOffset(lastOffset); err != nil {
			log.Fatalf("Failed to wait for offset %d: %v", lastOffset, err)
		}
	}

	log.Println("All records successfully ingested and acknowledged!")
}
