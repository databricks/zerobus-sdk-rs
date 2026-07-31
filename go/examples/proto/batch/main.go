package main

import (
	"log"
	"os"

	"zerobus-examples/pb"

	zerobus "github.com/databricks/zerobus-sdk/go"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
)

func main() {
	// Get configuration from environment
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

	// Get the file descriptor from generated code.
	fileDesc := pb.File_air_quality_proto

	// Convert to FileDescriptorProto and extract the message descriptor.
	fileDescProto := protodesc.ToFileDescriptorProto(fileDesc)

	// Get the AirQuality message descriptor (first message in the file).
	messageDescProto := fileDescProto.MessageType[0]

	// Marshal the descriptor.
	descriptorBytes, err := proto.Marshal(messageDescProto)
	if err != nil {
		log.Fatalf("Failed to marshal descriptor: %v", err)
	}

	options := zerobus.DefaultStreamConfigurationOptions()

	// Create stream.
	stream, err := sdk.CreateStream(
		zerobus.TableProperties{
			TableName:       tableName,
			DescriptorProto: descriptorBytes,
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
	batchRecords := []interface{}{}
	for i := 0; i < 5; i++ {
		message := &pb.AirQuality{
			DeviceName: proto.String("sensor-batch"),
			Temp:       proto.Int32(int32(25 + i)),
			Humidity:   proto.Int64(int64(70 + i)),
		}
		data, err := proto.Marshal(message)
		if err != nil {
			log.Printf("Failed to marshal batch record %d: %v", i, err)
			continue
		}
		batchRecords = append(batchRecords, data)
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
