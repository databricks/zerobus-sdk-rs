//go:build avro

package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"os"

	zerobus "github.com/databricks/zerobus-sdk/go"
)

// Simple Avro encoding example for records with int and string fields.
// This is a minimal example; production code would use a proper Avro library.
// For actual use, consider using: github.com/linkedin/goavro
func encodeSimpleAvroRecord(id int32, name string) []byte {
	var result []byte

	// Encode int32: zigzag encoding + variable-length int
	zigzag := (id << 1) ^ (id >> 31)
	for zigzag > 127 {
		result = append(result, byte((zigzag&0x7F)|0x80))
		zigzag >>= 7
	}
	result = append(result, byte(zigzag&0x7F))

	// Encode string: length (varint) + bytes
	len32 := int32(len(name))
	zigzag = (len32 << 1) ^ (len32 >> 31)
	for zigzag > 127 {
		result = append(result, byte((zigzag&0x7F)|0x80))
		zigzag >>= 7
	}
	result = append(result, byte(zigzag&0x7F))
	result = append(result, []byte(name)...)

	return result
}

func main() {
	endpoint := flag.String("zerobus-endpoint", os.Getenv("ZEROBUS_ENDPOINT"), "Zerobus endpoint URL")
	catalogURL := flag.String("catalog-url", os.Getenv("UNITY_CATALOG_URL"), "Unity Catalog URL")
	clientID := flag.String("client-id", os.Getenv("DATABRICKS_CLIENT_ID"), "OAuth2 client ID")
	clientSecret := flag.String("client-secret", os.Getenv("DATABRICKS_CLIENT_SECRET"), "OAuth2 client secret")
	tableName := flag.String("table", "catalog.schema.table", "Target table name")
	flag.Parse()

	// Validate inputs
	if *endpoint == "" || *catalogURL == "" || *clientID == "" || *clientSecret == "" {
		log.Fatal("Missing required parameters. Please set ZEROBUS_ENDPOINT, UNITY_CATALOG_URL, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET or pass as flags.")
	}

	// Create SDK instance
	sdk, err := zerobus.NewZerobusSdk(*endpoint, *catalogURL)
	if err != nil {
		log.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	// Avro schema: a record with id (int) and name (string) fields
	schemaJSON := `{
		"type": "record",
		"name": "SimpleRecord",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"}
		]
	}`

	// Create Avro stream
	tableProps := zerobus.AvroTableProperties{
		TableName:  *tableName,
		SchemaJSON: schemaJSON,
	}

	opts := zerobus.DefaultStreamConfigurationOptions()
	opts.RecordType = zerobus.RecordTypeAvro

	stream, err := sdk.CreateAvroStream(tableProps, *clientID, *clientSecret, opts)
	if err != nil {
		log.Fatalf("Failed to create stream: %v", err)
	}
	defer stream.Close()

	fmt.Println("Avro stream created successfully (Beta - requires server Avro support)")

	// Ingest pre-encoded Avro records
	records := []struct {
		id   int32
		name string
	}{
		{1, "Alice"},
		{2, "Bob"},
		{3, "Charlie"},
	}

	fmt.Printf("Ingesting %d Avro records...\n", len(records))

	// Queue records in a loop
	var lastOffset int64
	for _, rec := range records {
		avroData := encodeSimpleAvroRecord(rec.id, rec.name)
		offset, err := stream.IngestAvroRecordOffset(avroData)
		if err != nil {
			log.Printf("Failed to ingest record: %v", err)
			continue
		}
		lastOffset = offset
		fmt.Printf("Record (%d, %q) queued with offset %d\n", rec.id, rec.name, offset)
	}

	// Flush to ensure all records are acknowledged
	fmt.Println("Flushing pending records...")
	if err := stream.Flush(); err != nil {
		log.Fatalf("Flush failed: %v", err)
	}

	fmt.Printf("Successfully ingested and flushed %d Avro records (last offset: %d)\n", len(records), lastOffset)
}
