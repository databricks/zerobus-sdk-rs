// Package main demonstrates dynamic proto descriptor generation from a Unity
// Catalog schema, without requiring pre-generated .proto files.
package main

import (
	"log"
	"os"

	zerobus "github.com/databricks/zerobus-sdk/go"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

func main() {
	zerobusEndpoint := os.Getenv("ZEROBUS_SERVER_ENDPOINT")
	unityCatalogURL := os.Getenv("DATABRICKS_WORKSPACE_URL")
	clientID := os.Getenv("DATABRICKS_CLIENT_ID")
	clientSecret := os.Getenv("DATABRICKS_CLIENT_SECRET")
	tableName := os.Getenv("ZEROBUS_TABLE_NAME")

	if zerobusEndpoint == "" || unityCatalogURL == "" || clientID == "" || clientSecret == "" || tableName == "" {
		log.Fatal("Missing required environment variables: ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET, ZEROBUS_TABLE_NAME")
	}

	columns := []zerobus.UcColumn{
		{Name: "device_name", TypeName: "STRING", Nullable: true, Position: 0},
		{Name: "temp", TypeName: "INT", Nullable: false, Position: 1},
		{Name: "humidity", TypeName: "BIGINT", Nullable: true, Position: 2},
	}

	msgDesc, descriptorBytes, err := zerobus.MessageDescriptorFromUcColumns(columns, "SensorReading")
	if err != nil {
		log.Fatalf("Failed to build descriptor: %v", err)
	}

	// Create the SDK instance.
	sdk, err := zerobus.NewZerobusSdk(zerobusEndpoint, unityCatalogURL)
	if err != nil {
		log.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	options := zerobus.DefaultStreamConfigurationOptions()

	// Create a proto stream using the dynamically generated descriptor.
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

	log.Println("Ingesting records with dynamic proto schema...")

	var offsets []int64
	for i := range 5 {
		// Build a dynamic proto message using the runtime descriptor.
		msg := dynamicpb.NewMessage(msgDesc)
		msg.Set(msgDesc.Fields().ByName("device_name"), protoreflect.ValueOfString("sensor-001"))
		msg.Set(msgDesc.Fields().ByName("temp"), protoreflect.ValueOfInt32(int32(20+i)))
		msg.Set(msgDesc.Fields().ByName("humidity"), protoreflect.ValueOfInt64(int64(60+i)))

		data, err := proto.Marshal(msg)
		if err != nil {
			log.Printf("Failed to marshal record %d: %v", i, err)
			continue
		}

		offset, err := stream.IngestRecordOffset(data)
		if err != nil {
			log.Printf("Failed to ingest record %d: %v", i, err)
			continue
		}

		log.Printf("Ingested record %d at offset %d", i, offset)
		offsets = append(offsets, offset)
	}

	log.Println("Waiting for acknowledgments...")
	for _, offset := range offsets {
		if err := stream.WaitForOffset(offset); err != nil {
			log.Fatalf("Failed to wait for offset %d: %v", offset, err)
		}
		log.Printf("Record at offset %d acknowledged", offset)
	}

	log.Println("All records successfully ingested!")
}
