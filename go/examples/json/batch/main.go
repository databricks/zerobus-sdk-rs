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

	// WithApplicationName is optional; appends a caller identifier to the
	// wire user-agent (final: `zerobus-sdk-go/<version> my-app/1.0`).
	sdk, err := zerobus.NewZerobusSdk(
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

	log.Println("Ingesting batch of records...")
	batchRecords := []interface{}{
		`{"device_name": "sensor-001", "temp": 20, "humidity": 60}`,
		`{"device_name": "sensor-002", "temp": 21, "humidity": 61}`,
		`{"device_name": "sensor-003", "temp": 22, "humidity": 62}`,
		`{"device_name": "sensor-004", "temp": 23, "humidity": 63}`,
		`{"device_name": "sensor-005", "temp": 24, "humidity": 64}`,
	}

	lastOffset, err := stream.IngestRecordsOffset(batchRecords)
	if err != nil {
		log.Fatalf("Failed to ingest batch: %v", err)
	}
	log.Printf("Batch of %d records ingested, last offset: %d", len(batchRecords), lastOffset)

	// Wait for the last offset to ensure the entire batch is acknowledged.
	if err := stream.WaitForOffset(lastOffset); err != nil {
		log.Fatalf("Failed to wait for batch acknowledgment: %v", err)
	}
	log.Println("Batch acknowledged!")

	log.Println("All operations completed successfully!")
}
