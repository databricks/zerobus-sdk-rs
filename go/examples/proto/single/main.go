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

	// Ingest records in a loop. IngestRecordOffset returns as soon as the record
	// is queued; the SDK sends it and tracks its acknowledgment in the background.
	// Keeping the ingest loop free of per-record waits sustains throughput.
	log.Println("Ingesting records...")
	for i := 0; i < 5; i++ {
		// Create a message using the generated struct.
		// Change this message to match the schema of your table.
		message := &pb.AirQuality{
			DeviceName: proto.String("sensor-001"),
			Temp:       proto.Int32(int32(20 + i)),
			Humidity:   proto.Int64(int64(60 + i)),
		}

		// Marshal to bytes.
		data, err := proto.Marshal(message)
		if err != nil {
			log.Printf("Failed to marshal record %d: %v", i, err)
			continue
		}

		// Ingest the record.
		offset, err := stream.IngestRecordOffset(data)
		if err != nil {
			log.Printf("Failed to ingest record %d: %v", i, err)
			continue
		}

		log.Printf("Ingested record %d at offset %d (temp=%d, humidity=%d)",
			i, offset, *message.Temp, *message.Humidity)
	}

	// Wait once for every successfully queued record to be acknowledged.
	log.Println("Waiting for acknowledgments...")
	if err := stream.Flush(); err != nil {
		log.Fatalf("Failed to flush stream: %v", err)
	}

	log.Println("All records successfully ingested and acknowledged!")
}
