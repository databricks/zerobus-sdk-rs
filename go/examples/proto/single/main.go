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

	// Create SDK instance
	sdk, err := zerobus.NewZerobusSdk(zerobusEndpoint, unityCatalogURL)
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
	// We collect the last offset and confirm everything below — keeping the ingest
	// loop free of per-record waits is what sustains throughput.
	log.Println("Ingesting records...")
	var lastOffset int64 = -1
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
