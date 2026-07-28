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

	// WithApplicationName is optional. It appends an application identifier to
	// the Go SDK user-agent sent to Zerobus.
	sdk, err := zerobus.NewZerobusSdkWithOptions(
		zerobusEndpoint,
		unityCatalogURL,
		zerobus.WithApplicationName("my-app/1.0"),
	)
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
	// Keeping the ingest loop free of per-record waits sustains throughput.
	log.Println("Ingesting records...")
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
	}

	// Wait once for every successfully queued record to be acknowledged.
	log.Println("Waiting for acknowledgments...")
	if err := stream.Flush(); err != nil {
		log.Fatalf("Failed to flush stream: %v", err)
	}

	log.Println("All records successfully ingested and acknowledged!")
}
