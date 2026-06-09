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

	log.Println("Ingesting records...")
	var offsets []int64
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
		offsets = append(offsets, offset)
	}

	// Wait for specific offsets to be acknowledged.
	log.Println("Waiting for acknowledgments...")
	for _, offset := range offsets {
		if err := stream.WaitForOffset(offset); err != nil {
			log.Fatalf("Failed to wait for offset %d: %v", offset, err)
		}
		log.Printf("Record at offset %d acknowledged", offset)
	}

	log.Println("All records successfully ingested and acknowledged!")
}
