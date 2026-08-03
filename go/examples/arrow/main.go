// **Beta**: Arrow Flight ingestion is in Beta. The API is stabilising but may still
// change before reaching GA.
//
// This example shows how to ingest Apache Arrow RecordBatches into a Databricks
// Delta table using the high-performance Arrow Flight protocol.
//
// # Required environment variables
//
//	ZEROBUS_SERVER_ENDPOINT   – Arrow Flight endpoint (e.g. "https://host:443")
//	DATABRICKS_WORKSPACE_URL  – Unity Catalog workspace URL
//	DATABRICKS_CLIENT_ID      – OAuth 2.0 client ID
//	DATABRICKS_CLIENT_SECRET  – OAuth 2.0 client secret
//	ZEROBUS_TABLE_NAME        – Fully-qualified table name (catalog.schema.table)
//
// # Run
//
//	go run main.go
package main

import (
	"bytes"
	"log"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	zerobus "github.com/databricks/zerobus-sdk/go"
)

func main() {
	// ── 1. Read configuration from the environment ────────────────────────────
	endpoint := os.Getenv("ZEROBUS_SERVER_ENDPOINT")
	ucURL := os.Getenv("DATABRICKS_WORKSPACE_URL")
	clientID := os.Getenv("DATABRICKS_CLIENT_ID")
	clientSecret := os.Getenv("DATABRICKS_CLIENT_SECRET")
	tableName := os.Getenv("ZEROBUS_TABLE_NAME")

	if endpoint == "" || ucURL == "" || clientID == "" || clientSecret == "" || tableName == "" {
		log.Fatal("Missing required environment variables: ZEROBUS_SERVER_ENDPOINT, " +
			"DATABRICKS_WORKSPACE_URL, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET, " +
			"ZEROBUS_TABLE_NAME")
	}

	// ── 2. Create the SDK instance ────────────────────────────────────────────
	// WithApplicationName is optional. It appends an application identifier to
	// the Go SDK user-agent sent to Zerobus.
	sdk, err := zerobus.NewZerobusSdkWithOptions(
		endpoint,
		ucURL,
		zerobus.WithApplicationName("my-app/1.0"),
	)
	if err != nil {
		log.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	// ── 3. Define the Arrow schema ────────────────────────────────────────────
	//
	// The schema must match the target Delta table's column names and types.
	// Adjust the fields below to match your table.
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "device_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "temperature", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "humidity", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	// ── 4. Serialize the schema to Arrow IPC bytes ────────────────────────────
	//
	// Use ipc.NewWriter with no written batches; the resulting bytes contain
	// only the schema message and are passed to CreateArrowStream.
	schemaIPC := serializeSchema(schema)

	// ── 5. Create the Arrow Flight stream ─────────────────────────────────────
	opts := zerobus.DefaultArrowStreamConfigurationOptions()

	stream, err := sdk.CreateArrowStream(tableName, schemaIPC, clientID, clientSecret, opts)
	if err != nil {
		log.Fatalf("Failed to create Arrow stream: %v", err)
	}
	defer stream.Close()

	log.Printf("Arrow Flight stream opened for table %s", tableName)

	// ── 6. Build and ingest RecordBatches ─────────────────────────────────────
	//
	// Build 3 batches of sensor readings and ingest them.
	alloc := memory.DefaultAllocator

	batches := [][]sensorReading{
		{{device: "sensor-001", temp: 22.5, humid: 60.1}, {device: "sensor-002", temp: 21.0, humid: 58.3}},
		{{device: "sensor-001", temp: 23.1, humid: 61.0}, {device: "sensor-003", temp: 19.8, humid: 55.7}},
		{{device: "sensor-002", temp: 22.8, humid: 59.5}},
	}

	// Ingest batches in a loop. IngestBatch returns as soon as the batch is
	// queued; the SDK sends it and tracks its acknowledgment in the background.
	// We confirm everything below with a single Flush() — keeping the ingest loop
	// free of per-batch waits is what sustains throughput.
	for i, readings := range batches {
		ipcBytes := serializeBatch(schema, alloc, readings)

		offset, err := stream.IngestBatch(ipcBytes)
		if err != nil {
			// Check whether the error is retryable.
			if zbErr, ok := err.(*zerobus.ZerobusError); ok && zbErr.Retryable() {
				log.Printf("Batch %d ingestion failed (retryable): %v", i, err)
			} else {
				log.Fatalf("Batch %d ingestion failed (non-retryable): %v", i, err)
			}
			continue
		}

		log.Printf("Batch %d (%d rows) queued at offset %d", i, len(readings), offset)
	}

	// ── 7. Wait for server acknowledgments ───────────────────────────────────
	//
	// Flush() blocks until ALL pending batches are durably stored — a single
	// round-trip for the whole stream, and the idiomatic way to confirm
	// durability. (If you only need to confirm up to a specific point, call
	// WaitForOffset() on the last offset instead; the ack watermark is monotonic.)
	log.Println("Waiting for acknowledgments...")
	if err := stream.Flush(); err != nil {
		log.Fatalf("Flush failed: %v", err)
	}

	// ── 8. Close ──────────────────────────────────────────────────────────────
	if err := stream.Close(); err != nil {
		log.Fatalf("Close failed: %v", err)
	}

	log.Println("All batches ingested and acknowledged successfully!")
}

// serializeSchema serialises an Arrow schema to Arrow IPC stream bytes (no batches).
// Pass the returned bytes to CreateArrowStream / CreateArrowStreamWithHeadersProvider.
func serializeSchema(schema *arrow.Schema) []byte {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := w.Close(); err != nil {
		log.Fatalf("Failed to serialize Arrow schema: %v", err)
	}
	return buf.Bytes()
}

// sensorReading holds one row of sensor data for the example.
type sensorReading struct {
	device string
	temp   float64
	humid  float64
}

// serializeBatch builds one Arrow RecordBatch from a slice of readings and serialises
// it to Arrow IPC stream bytes (schema + one batch).
// Pass the returned bytes to ZerobusArrowStream.IngestBatch.
func serializeBatch(schema *arrow.Schema, alloc memory.Allocator, readings []sensorReading) []byte {
	deviceBldr := array.NewStringBuilder(alloc)
	tempBldr := array.NewFloat64Builder(alloc)
	humidBldr := array.NewFloat64Builder(alloc)
	defer deviceBldr.Release()
	defer tempBldr.Release()
	defer humidBldr.Release()

	for _, r := range readings {
		deviceBldr.Append(r.device)
		tempBldr.Append(r.temp)
		humidBldr.Append(r.humid)
	}

	deviceArr := deviceBldr.NewArray()
	tempArr := tempBldr.NewArray()
	humidArr := humidBldr.NewArray()
	defer deviceArr.Release()
	defer tempArr.Release()
	defer humidArr.Release()

	rec := array.NewRecord(schema, []arrow.Array{deviceArr, tempArr, humidArr}, int64(len(readings)))
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := w.Write(rec); err != nil {
		log.Fatalf("Failed to write RecordBatch: %v", err)
	}
	if err := w.Close(); err != nil {
		log.Fatalf("Failed to close IPC writer: %v", err)
	}

	return buf.Bytes()
}
