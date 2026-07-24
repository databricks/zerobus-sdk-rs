package stream

import (
	"testing"
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
	enc.stampOffset(msg, 0)
	if ir.GetOffsetId() != 0 {
		t.Fatalf("want stamped offset 0, got %d", ir.GetOffsetId())
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
	enc := protoEncoder{}
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
	enc := jsonEncoder{}
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
	enc.stampOffset(msg, 0)
	if ib.GetOffsetId() != 0 {
		t.Fatalf("want stamped offset 0, got %d", ib.GetOffsetId())
	}
	jb := ib.GetJsonBatch()
	if jb == nil {
		t.Fatal("want JsonBatch, got nil")
	}
	if got := jb.GetRecords(); len(got) != 2 || got[0] != `{"a":1}` || got[1] != `{"b":2}` {
		t.Fatalf("want 2 json records preserved, got %v", got)
	}
}

// JSON records are text and must be non-empty; proto records may legitimately
// be empty (an all-default message), so only JSON rejects empty input.
func TestJSONEncoderRejectsEmptyRecord(t *testing.T) {
	if _, err := (jsonEncoder{}).encode(1, nil); err == nil {
		t.Error("want error for empty json record, got nil")
	}
	if _, err := (jsonEncoder{}).encode(1, []byte{}); err == nil {
		t.Error("want error for zero-length json record, got nil")
	}
}

// Empty proto records are valid in single and batch requests.
func TestProtoEncoderAcceptsEmptyRecord(t *testing.T) {
	def := []byte{} // zero-length serialization of an all-default message
	msg, err := protoEncoder{}.encode(1, def)
	if err != nil {
		t.Fatalf("encode empty proto record: %v", err)
	}
	if got := (protoEncoder{}).decode(msg); len(got) != 1 || len(got[0]) != 0 {
		t.Fatalf("want one empty record back, got %v", got)
	}

	batch, err := protoEncoder{}.encodeBatch(1, [][]byte{def, {0x01}})
	if err != nil {
		t.Fatalf("encodeBatch with empty proto record: %v", err)
	}
	if got := (protoEncoder{}).decode(batch); len(got) != 2 || len(got[0]) != 0 {
		t.Fatalf("want empty record preserved in batch, got %v", got)
	}
}

func TestBatchEncodersRejectEmptyBatch(t *testing.T) {
	for _, enc := range []encoder[encodedMsg]{protoEncoder{}, jsonEncoder{}} {
		if _, err := enc.encodeBatch(1, nil); err == nil {
			t.Errorf("%T: want error for empty batch, got nil", enc)
		}
	}
	// A JSON batch containing an empty record is still rejected.
	if _, err := (jsonEncoder{}).encodeBatch(1, [][]byte{[]byte("ok"), {}}); err == nil {
		t.Error("jsonEncoder: want error for empty record within batch, got nil")
	}
}

func TestExtractRecordsReturnsAllBatchRecords(t *testing.T) {
	// A batch message must yield every record, not just the first, so GetUnacked
	// doesn't silently drop the tail of an unacked batch.
	protoMsg, err := protoEncoder{}.encodeBatch(1, [][]byte{{0x01}, {0x02}, {0x03}})
	if err != nil {
		t.Fatalf("encodeBatch: %v", err)
	}
	if got := (protoEncoder{}).decode(protoMsg); len(got) != 3 {
		t.Fatalf("proto batch: want 3 records extracted, got %d", len(got))
	}

	jsonMsg, err := jsonEncoder{}.encodeBatch(1, [][]byte{[]byte(`{"a":1}`), []byte(`{"b":2}`)})
	if err != nil {
		t.Fatalf("encodeBatch: %v", err)
	}
	got := (jsonEncoder{}).decode(jsonMsg)
	if len(got) != 2 || string(got[0]) != `{"a":1}` || string(got[1]) != `{"b":2}` {
		t.Fatalf("json batch: want both records extracted, got %v", got)
	}

	// A single-record message still yields exactly one entry.
	single, err := jsonEncoder{}.encode(1, []byte(`{"x":1}`))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if got := (jsonEncoder{}).decode(single); len(got) != 1 || string(got[0]) != `{"x":1}` {
		t.Fatalf("single record: want 1 entry, got %v", got)
	}
}

// The proto encoder must snapshot caller bytes: a caller reusing a scratch
// buffer after Ingest returns must not be able to mutate the queued payload.
func TestProtoEncoderClonesRecord(t *testing.T) {
	rec := []byte{0x01, 0x02, 0x03}
	msg, err := protoEncoder{}.encode(1, rec)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	rec[0] = 0xff // mutate caller buffer post-encode
	got := msg.GetIngestRecord().GetProtoEncodedRecord()
	if len(got) != 3 || got[0] != 0x01 {
		t.Fatalf("queued payload aliased caller buffer: got %v", got)
	}
}

func TestProtoBatchEncoderClonesRecords(t *testing.T) {
	recs := [][]byte{{0x01}, {0x02}}
	msg, err := protoEncoder{}.encodeBatch(1, recs)
	if err != nil {
		t.Fatalf("encodeBatch: %v", err)
	}
	recs[0][0] = 0xff // mutate caller buffer post-encode
	got := msg.GetIngestRecordBatch().GetProtoEncodedBatch().GetRecords()
	if len(got) != 2 || got[0][0] != 0x01 {
		t.Fatalf("queued batch payload aliased caller buffer: got %v", got)
	}
}

// wireSize reports the serialized size including proto framing, so it exceeds
// the raw record length.
func TestWireSizeIncludesFraming(t *testing.T) {
	rec := []byte("hello")
	for _, enc := range []encoder[encodedMsg]{protoEncoder{}, jsonEncoder{}} {
		msg, err := enc.encode(1, rec)
		if err != nil {
			t.Fatalf("%T encode: %v", enc, err)
		}
		if got := enc.wireSize(msg); got <= len(rec) {
			t.Errorf("%T: wireSize %d should exceed raw len %d", enc, got, len(rec))
		}
	}
}
