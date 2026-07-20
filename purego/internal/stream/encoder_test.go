package stream

import (
	"testing"

	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

func TestProtoEncoderSetsOffsetAndPayload(t *testing.T) {
	enc := protoEncoder{}
	msg, err := enc.encode(42, []byte{0x0a, 0x01, 0x78})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	ir := msg.GetIngestRecord()
	if ir == nil {
		t.Fatal("want IngestRecord payload, got nil")
	}
	if ir.GetOffsetId() != 42 {
		t.Fatalf("want offset 42, got %d", ir.GetOffsetId())
	}
	if len(ir.GetProtoEncodedRecord()) == 0 {
		t.Fatal("want non-empty proto record")
	}
}

func TestJSONEncoderSetsOffsetAndPayload(t *testing.T) {
	enc := jsonEncoder{}
	msg, err := enc.encode(7, []byte(`{"x":1}`))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	ir := msg.GetIngestRecord()
	if ir == nil {
		t.Fatal("want IngestRecord payload, got nil")
	}
	if ir.GetOffsetId() != 7 {
		t.Fatalf("want offset 7, got %d", ir.GetOffsetId())
	}
	if ir.GetJsonRecord() != `{"x":1}` {
		t.Fatalf("want json record, got %q", ir.GetJsonRecord())
	}
}

func TestProtoBatchEncoderSetsOffsetAndPayload(t *testing.T) {
	enc := protoBatchEncoder{}
	msg, err := enc.encodeBatch(3, [][]byte{{0x01, 0x02}, {0x03, 0x04}, {0x05}})
	if err != nil {
		t.Fatalf("encodeBatch: %v", err)
	}
	ib := msg.GetIngestRecordBatch()
	if ib == nil {
		t.Fatal("want IngestRecordBatch payload, got nil")
	}
	if ib.GetOffsetId() != 3 {
		t.Fatalf("want offset 3, got %d", ib.GetOffsetId())
	}
	pb := ib.GetProtoEncodedBatch()
	if pb == nil {
		t.Fatal("want ProtoEncodedBatch, got nil")
	}
	// The batch must carry all three records, not one concatenated blob.
	if got := len(pb.GetRecords()); got != 3 {
		t.Fatalf("want 3 records in batch, got %d", got)
	}
}

func TestJSONBatchEncoderSetsOffsetAndPayload(t *testing.T) {
	enc := jsonBatchEncoder{}
	msg, err := enc.encodeBatch(5, [][]byte{[]byte(`{"a":1}`), []byte(`{"b":2}`)})
	if err != nil {
		t.Fatalf("encodeBatch: %v", err)
	}
	ib := msg.GetIngestRecordBatch()
	if ib == nil {
		t.Fatal("want IngestRecordBatch payload, got nil")
	}
	if ib.GetOffsetId() != 5 {
		t.Fatalf("want offset 5, got %d", ib.GetOffsetId())
	}
	jb := ib.GetJsonBatch()
	if jb == nil {
		t.Fatal("want JsonBatch, got nil")
	}
	if got := jb.GetRecords(); len(got) != 2 || got[0] != `{"a":1}` || got[1] != `{"b":2}` {
		t.Fatalf("want 2 json records preserved, got %v", got)
	}
}

func TestEncodersRejectEmptyRecord(t *testing.T) {
	for _, enc := range []encoder{protoEncoder{}, jsonEncoder{}} {
		if _, err := enc.encode(1, nil); err == nil {
			t.Errorf("%T: want error for empty record, got nil", enc)
		}
		if _, err := enc.encode(1, []byte{}); err == nil {
			t.Errorf("%T: want error for zero-length record, got nil", enc)
		}
	}
}

func TestBatchEncodersRejectEmptyBatch(t *testing.T) {
	for _, enc := range []batchEncoder{protoBatchEncoder{}, jsonBatchEncoder{}} {
		if _, err := enc.encodeBatch(1, nil); err == nil {
			t.Errorf("%T: want error for empty batch, got nil", enc)
		}
		// A batch containing an empty record is rejected too.
		if _, err := enc.encodeBatch(1, [][]byte{[]byte("ok"), {}}); err == nil {
			t.Errorf("%T: want error for empty record within batch, got nil", enc)
		}
	}
}

func TestExtractRecordsReturnsAllBatchRecords(t *testing.T) {
	// A batch message must yield every record, not just the first, so GetUnacked
	// doesn't silently drop the tail of an unacked batch.
	protoMsg, err := protoBatchEncoder{}.encodeBatch(1, [][]byte{{0x01}, {0x02}, {0x03}})
	if err != nil {
		t.Fatalf("encodeBatch: %v", err)
	}
	if got := extractRecords(protoMsg); len(got) != 3 {
		t.Fatalf("proto batch: want 3 records extracted, got %d", len(got))
	}

	jsonMsg, err := jsonBatchEncoder{}.encodeBatch(1, [][]byte{[]byte(`{"a":1}`), []byte(`{"b":2}`)})
	if err != nil {
		t.Fatalf("encodeBatch: %v", err)
	}
	got := extractRecords(jsonMsg)
	if len(got) != 2 || string(got[0]) != `{"a":1}` || string(got[1]) != `{"b":2}` {
		t.Fatalf("json batch: want both records extracted, got %v", got)
	}

	// A single-record message still yields exactly one entry.
	single, err := jsonEncoder{}.encode(1, []byte(`{"x":1}`))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if got := extractRecords(single); len(got) != 1 || string(got[0]) != `{"x":1}` {
		t.Fatalf("single record: want 1 entry, got %v", got)
	}
}

func TestOffsetAckModelParsesIngestResponse(t *testing.T) {
	am := offsetAckModel{}

	offset := int64(99)
	resp := &zerobuspb.EphemeralStreamResponse{
		Payload: &zerobuspb.EphemeralStreamResponse_IngestRecordResponse{
			IngestRecordResponse: &zerobuspb.IngestRecordResponse{
				DurabilityAckUpToOffset: &offset,
			},
		},
	}
	got, ok := am.parse(resp)
	if !ok {
		t.Fatal("want ok=true for ingest response, got false")
	}
	if got != 99 {
		t.Fatalf("want offset 99, got %d", got)
	}
}

func TestOffsetAckModelIgnoresNonAckResponse(t *testing.T) {
	am := offsetAckModel{}
	resp := &zerobuspb.EphemeralStreamResponse{
		Payload: &zerobuspb.EphemeralStreamResponse_CloseStreamSignal{},
	}
	_, ok := am.parse(resp)
	if ok {
		t.Fatal("want ok=false for close signal, got true")
	}
}

func TestNewAckModelProtoJSON(t *testing.T) {
	for _, rt := range []zerobuspb.RecordType{zerobuspb.RecordType_PROTO, zerobuspb.RecordType_JSON} {
		am, err := newAckModel(rt)
		if err != nil {
			t.Fatalf("newAckModel(%v): %v", rt, err)
		}
		if am == nil {
			t.Fatalf("newAckModel(%v): want non-nil", rt)
		}
	}
}

func TestNewAckModelUnknownErrors(t *testing.T) {
	if _, err := newAckModel(zerobuspb.RecordType_RECORD_TYPE_UNSPECIFIED); err == nil {
		t.Fatal("want error for unspecified record type, got nil")
	}
}
