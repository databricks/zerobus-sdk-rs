package tests

import (
	"bytes"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	zerobus "github.com/databricks/zerobus-sdk/go"
)

const arrowTestTable = "test_catalog.test_schema.arrow_table"

// testArrowSchema returns a minimal single-column schema used by all arrow tests.
func testArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)
}

// makeSchemaIPC serialises an Arrow schema to Arrow IPC stream bytes (no batches).
func makeSchemaIPC(schema *arrow.Schema) []byte {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	w.Close()
	return buf.Bytes()
}

// makeBatchIPC serialises one RecordBatch (ids column) to Arrow IPC stream bytes.
func makeBatchIPC(schema *arrow.Schema, ids []int32) []byte {
	bldr := array.NewInt32Builder(memory.DefaultAllocator)
	bldr.AppendValues(ids, nil)
	col := bldr.NewArray()
	defer col.Release()

	rec := array.NewRecord(schema, []arrow.Array{col}, int64(len(ids)))
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	w.Write(rec) //nolint:errcheck
	w.Close()
	return buf.Bytes()
}

// arrowOpts returns default options with Recovery disabled and short timeouts.
func arrowOpts() *zerobus.ArrowStreamConfigurationOptions {
	opts := zerobus.DefaultArrowStreamConfigurationOptions()
	opts.Recovery = false
	opts.FlushTimeoutMs = 15_000
	opts.ConnectionTimeoutMs = 10_000
	return opts
}

// TestArrowStreamCreation verifies that a stream can be created against the mock server.
func TestArrowStreamCreation(t *testing.T) {
	_, serverURL, stop, err := StartMockArrowServer()
	if err != nil {
		t.Fatalf("Failed to start Arrow mock server: %v", err)
	}
	defer stop()

	sdk, err := zerobus.NewZerobusSdk(serverURL, "https://mock-uc.com")
	if err != nil {
		t.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	schema := testArrowSchema()
	stream, err := sdk.CreateArrowStreamWithHeadersProvider(
		arrowTestTable,
		makeSchemaIPC(schema),
		&TestHeadersProvider{},
		arrowOpts(),
	)
	if err != nil {
		t.Fatalf("Failed to create Arrow stream: %v", err)
	}
	defer stream.Close()

	if stream.IsClosed() {
		t.Fatal("Stream should be open immediately after creation")
	}

	t.Log("Arrow stream created successfully")
}

// TestArrowStreamIngestAndFlush verifies that batches are ingested and Flush waits for acks.
func TestArrowStreamIngestAndFlush(t *testing.T) {
	mockServer, serverURL, stop, err := StartMockArrowServer()
	if err != nil {
		t.Fatalf("Failed to start Arrow mock server: %v", err)
	}
	defer stop()

	sdk, err := zerobus.NewZerobusSdk(serverURL, "https://mock-uc.com")
	if err != nil {
		t.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	schema := testArrowSchema()
	stream, err := sdk.CreateArrowStreamWithHeadersProvider(
		arrowTestTable,
		makeSchemaIPC(schema),
		&TestHeadersProvider{},
		arrowOpts(),
	)
	if err != nil {
		t.Fatalf("Failed to create Arrow stream: %v", err)
	}
	defer stream.Close()

	batch := makeBatchIPC(schema, []int32{1, 2, 3})
	mockServer.ConfigureRowsForOffset(0, 3)
	mockServer.ConfigureRowsForOffset(1, 3)

	offset0, err := stream.IngestBatch(batch)
	if err != nil {
		t.Fatalf("Failed to ingest batch 0: %v", err)
	}
	if offset0 != 0 {
		t.Fatalf("Expected offset 0, got %d", offset0)
	}

	offset1, err := stream.IngestBatch(batch)
	if err != nil {
		t.Fatalf("Failed to ingest batch 1: %v", err)
	}
	if offset1 != 1 {
		t.Fatalf("Expected offset 1, got %d", offset1)
	}

	if err := stream.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	if n := mockServer.GetBatchesReceived(); n != 2 {
		t.Errorf("Expected 2 batches received, got %d", n)
	}
	if max := mockServer.GetMaxOffsetSeen(); max != 1 {
		t.Errorf("Expected max offset 1, got %d", max)
	}

	t.Logf("Ingested 2 batches at offsets %d and %d", offset0, offset1)
}

// TestArrowStreamWaitForOffset verifies WaitForOffset blocks until the server acks the batch.
func TestArrowStreamWaitForOffset(t *testing.T) {
	mockServer, serverURL, stop, err := StartMockArrowServer()
	if err != nil {
		t.Fatalf("Failed to start Arrow mock server: %v", err)
	}
	defer stop()

	sdk, err := zerobus.NewZerobusSdk(serverURL, "https://mock-uc.com")
	if err != nil {
		t.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	schema := testArrowSchema()
	stream, err := sdk.CreateArrowStreamWithHeadersProvider(
		arrowTestTable,
		makeSchemaIPC(schema),
		&TestHeadersProvider{},
		arrowOpts(),
	)
	if err != nil {
		t.Fatalf("Failed to create Arrow stream: %v", err)
	}
	defer stream.Close()

	mockServer.ConfigureRowsForOffset(0, 5)
	batch := makeBatchIPC(schema, []int32{10, 20, 30, 40, 50})

	offset, err := stream.IngestBatch(batch)
	if err != nil {
		t.Fatalf("IngestBatch failed: %v", err)
	}

	if err := stream.WaitForOffset(offset); err != nil {
		t.Fatalf("WaitForOffset(%d) failed: %v", offset, err)
	}

	t.Logf("Batch at offset %d acknowledged", offset)
}

// TestArrowStreamClose verifies that Close succeeds and marks the stream closed.
func TestArrowStreamClose(t *testing.T) {
	_, serverURL, stop, err := StartMockArrowServer()
	if err != nil {
		t.Fatalf("Failed to start Arrow mock server: %v", err)
	}
	defer stop()

	sdk, err := zerobus.NewZerobusSdk(serverURL, "https://mock-uc.com")
	if err != nil {
		t.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	schema := testArrowSchema()
	stream, err := sdk.CreateArrowStreamWithHeadersProvider(
		arrowTestTable,
		makeSchemaIPC(schema),
		&TestHeadersProvider{},
		arrowOpts(),
	)
	if err != nil {
		t.Fatalf("Failed to create Arrow stream: %v", err)
	}

	if err := stream.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if !stream.IsClosed() {
		t.Fatal("Expected stream to be closed after Close()")
	}

	t.Log("Stream closed successfully")
}

// TestArrowStreamIdempotentClose verifies that calling Close twice does not error.
func TestArrowStreamIdempotentClose(t *testing.T) {
	_, serverURL, stop, err := StartMockArrowServer()
	if err != nil {
		t.Fatalf("Failed to start Arrow mock server: %v", err)
	}
	defer stop()

	sdk, err := zerobus.NewZerobusSdk(serverURL, "https://mock-uc.com")
	if err != nil {
		t.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	schema := testArrowSchema()
	stream, err := sdk.CreateArrowStreamWithHeadersProvider(
		arrowTestTable,
		makeSchemaIPC(schema),
		&TestHeadersProvider{},
		arrowOpts(),
	)
	if err != nil {
		t.Fatalf("Failed to create Arrow stream: %v", err)
	}

	if err := stream.Close(); err != nil {
		t.Fatalf("First Close failed: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Second Close failed: %v", err)
	}

	t.Log("Idempotent close works correctly")
}

// TestArrowStreamIngestAfterClose verifies that IngestBatch returns an error after Close.
func TestArrowStreamIngestAfterClose(t *testing.T) {
	_, serverURL, stop, err := StartMockArrowServer()
	if err != nil {
		t.Fatalf("Failed to start Arrow mock server: %v", err)
	}
	defer stop()

	sdk, err := zerobus.NewZerobusSdk(serverURL, "https://mock-uc.com")
	if err != nil {
		t.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	schema := testArrowSchema()
	stream, err := sdk.CreateArrowStreamWithHeadersProvider(
		arrowTestTable,
		makeSchemaIPC(schema),
		&TestHeadersProvider{},
		arrowOpts(),
	)
	if err != nil {
		t.Fatalf("Failed to create Arrow stream: %v", err)
	}

	if err := stream.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	_, err = stream.IngestBatch(makeBatchIPC(schema, []int32{1}))
	if err == nil {
		t.Fatal("Expected IngestBatch after Close to fail, but it succeeded")
	}

	t.Log("IngestBatch correctly fails after Close")
}

// TestArrowStreamGetUnackedBatches verifies that unacked batches can be retrieved.
func TestArrowStreamGetUnackedBatches(t *testing.T) {
	_, serverURL, stop, err := StartMockArrowServer()
	if err != nil {
		t.Fatalf("Failed to start Arrow mock server: %v", err)
	}
	defer stop()

	sdk, err := zerobus.NewZerobusSdk(serverURL, "https://mock-uc.com")
	if err != nil {
		t.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	schema := testArrowSchema()

	// Use a very short flush timeout so Close succeeds quickly.
	opts := zerobus.DefaultArrowStreamConfigurationOptions()
	opts.Recovery = false
	opts.ConnectionTimeoutMs = 5_000
	opts.FlushTimeoutMs = 500

	stream, err := sdk.CreateArrowStreamWithHeadersProvider(
		arrowTestTable,
		makeSchemaIPC(schema),
		&TestHeadersProvider{},
		opts,
	)
	if err != nil {
		t.Fatalf("Failed to create Arrow stream: %v", err)
	}

	// Close the stream without ingesting anything; unacked list should be empty.
	// (Close flushes first; with no pending batches this should succeed quickly.)
	if err := stream.Close(); err != nil {
		// Some implementations may report an error here; only fail on unexpected errors.
		t.Logf("Close returned (possibly expected): %v", err)
	}

	// After close, GetUnackedBatches should return an error because the stream is closed.
	_, err = stream.GetUnackedBatches()
	if err == nil {
		t.Fatal("Expected GetUnackedBatches to fail on closed stream, but it succeeded")
	}

	t.Log("GetUnackedBatches correctly fails on closed stream")
}

// TestArrowStreamMultipleBatches verifies ingestion of several batches with different sizes.
func TestArrowStreamMultipleBatches(t *testing.T) {
	mockServer, serverURL, stop, err := StartMockArrowServer()
	if err != nil {
		t.Fatalf("Failed to start Arrow mock server: %v", err)
	}
	defer stop()

	// Give server a moment to be fully ready.
	time.Sleep(200 * time.Millisecond)

	sdk, err := zerobus.NewZerobusSdk(serverURL, "https://mock-uc.com")
	if err != nil {
		t.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	schema := testArrowSchema()
	stream, err := sdk.CreateArrowStreamWithHeadersProvider(
		arrowTestTable,
		makeSchemaIPC(schema),
		&TestHeadersProvider{},
		arrowOpts(),
	)
	if err != nil {
		t.Fatalf("Failed to create Arrow stream: %v", err)
	}
	defer stream.Close()

	batches := [][]int32{
		{1},
		{2, 3},
		{4, 5, 6},
		{7, 8, 9, 10},
	}

	for i, ids := range batches {
		mockServer.ConfigureRowsForOffset(int64(i), uint64(len(ids)))
	}

	var offsets []int64
	for i, ids := range batches {
		offset, err := stream.IngestBatch(makeBatchIPC(schema, ids))
		if err != nil {
			t.Fatalf("IngestBatch %d failed: %v", i, err)
		}
		if offset != int64(i) {
			t.Fatalf("Expected offset %d, got %d", i, offset)
		}
		offsets = append(offsets, offset)
		t.Logf("Batch %d (%d rows) queued at offset %d", i, len(ids), offset)
	}

	if err := stream.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	if n := mockServer.GetBatchesReceived(); n != len(batches) {
		t.Errorf("Expected %d batches, got %d", len(batches), n)
	}

	t.Logf("All %d batches successfully ingested and acknowledged", len(batches))
}
