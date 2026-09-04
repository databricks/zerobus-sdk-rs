//go:build avro

package zerobus_test

import (
	"context"
	"testing"
	"time"

	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
	"github.com/databricks/zerobus-sdk/purego/zerobus"
)

const avroIntegrationSchema = `{"type":"record","name":"Order","fields":[` +
	`{"name":"id","type":"long"},{"name":"customer","type":"string"}]}`

// Exercises both Avro ingestion paths end-to-end against the in-memory server:
// AvroRecord objects the stream encodes, and pre-encoded datums via the generic
// bytes method. Asserts the server receives Avro wire payloads.
func TestAvroStreamIngestsObjectsAndBytes(t *testing.T) {
	srv := &echoServer{
		streamID: "avro-stream",
		ingests:  make(chan *zerobuspb.EphemeralStreamRequest, 8),
	}
	conn := dialEcho(t, srv)
	sdk := zerobus.NewWithConn(conn, "https://ws.zerobus.databricks.com", "https://ws.databricks.com")
	provider := zerobus.NewStaticHeadersProvider(map[string]string{"authorization": "Bearer t"})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := sdk.CreateStreamWithProvider(ctx, "main.sales.orders", provider,
		zerobus.WithAvro(avroIntegrationSchema))
	if err != nil {
		t.Fatalf("CreateStreamWithProvider: %v", err)
	}
	defer stream.Close()

	// Object record — the stream encodes it against the writer schema.
	if _, err := stream.IngestAvroRecordOffset(zerobus.AvroRecord{"id": int64(1), "customer": "Ada"}); err != nil {
		t.Fatalf("IngestAvroRecordOffset: %v", err)
	}
	// Pre-encoded raw datum through the generic bytes method.
	if _, err := stream.IngestRecordOffset([]byte{0x02, 0x06, 0x41, 0x64, 0x61}); err != nil {
		t.Fatalf("IngestRecordOffset(bytes): %v", err)
	}
	// Object batch.
	if _, err := stream.IngestAvroRecordsOffset([]zerobus.AvroRecord{
		{"id": int64(2), "customer": "Grace"},
		{"id": int64(3), "customer": "Alan"},
	}); err != nil {
		t.Fatalf("IngestAvroRecordsOffset: %v", err)
	}
	if err := stream.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Two single AvroEncodedRecords (object then bytes), then an AvroRecordBatch.
	got := drainIngests(t, srv.ingests, 3)
	for i := 0; i < 2; i++ {
		if got[i].GetIngestRecord().GetAvroEncodedRecord() == nil {
			t.Errorf("record %d: want AvroEncodedRecord, got %v", i, got[i].GetIngestRecord().GetRecord())
		}
	}
	batch := got[2].GetIngestRecordBatch().GetAvroBatch()
	if batch == nil {
		t.Fatalf("record 2: want AvroBatch, got %v", got[2].GetIngestRecordBatch().GetBatch())
	}
	if n := len(batch.GetRecords()); n != 2 {
		t.Errorf("avro batch records = %d, want 2", n)
	}
}

func drainIngests(t *testing.T, ch <-chan *zerobuspb.EphemeralStreamRequest, n int) []*zerobuspb.EphemeralStreamRequest {
	t.Helper()
	out := make([]*zerobuspb.EphemeralStreamRequest, 0, n)
	for len(out) < n {
		select {
		case req := <-ch:
			out = append(out, req)
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out after %d/%d ingests", len(out), n)
		}
	}
	return out
}
