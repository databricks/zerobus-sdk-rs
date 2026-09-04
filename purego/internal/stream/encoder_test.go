package stream

import (
	"math"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

func TestProtoEncoderSetsOffsetAndPayload(t *testing.T) {
	enc := protoEncoder{}
	msg, err := enc.encode([]byte{0x0a, 0x01, 0x78})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	ir := msg.GetIngestRecord()
	if ir == nil {
		t.Fatal("want IngestRecord payload, got nil")
	}
	enc.stampOffset(msg, 42)
	if ir.GetOffsetId() != 42 {
		t.Fatalf("want stamped offset 42, got %d", ir.GetOffsetId())
	}
	if len(ir.GetProtoEncodedRecord()) == 0 {
		t.Fatal("want non-empty proto record")
	}
}

func TestJSONEncoderSetsOffsetAndPayload(t *testing.T) {
	enc := jsonEncoder{}
	msg, err := enc.encode([]byte(`{"x":1}`))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	ir := msg.GetIngestRecord()
	if ir == nil {
		t.Fatal("want IngestRecord payload, got nil")
	}
	enc.stampOffset(msg, 7)
	if ir.GetJsonRecord() != `{"x":1}` {
		t.Fatalf("want json record, got %q", ir.GetJsonRecord())
	}
}

func TestProtoBatchEncoderSetsOffsetAndPayload(t *testing.T) {
	enc := protoEncoder{}
	msg, err := enc.encodeBatch([][]byte{{0x01, 0x02}, {0x03, 0x04}, {0x05}})
	if err != nil {
		t.Fatalf("encodeBatch: %v", err)
	}
	ib := msg.GetIngestRecordBatch()
	if ib == nil {
		t.Fatal("want IngestRecordBatch payload, got nil")
	}
	enc.stampOffset(msg, 3)
	pb := ib.GetProtoEncodedBatch()
	if pb == nil {
		t.Fatal("want ProtoEncodedBatch, got nil")
	}
	// The batch must carry all three records, not one concatenated blob.
	if got := len(pb.GetRecords()); got != 3 {
		t.Fatalf("want 3 records in batch, got %d", got)
	}
}

func TestAvroEncoderSetsOffsetAndPayload(t *testing.T) {
	enc := avroEncoder{}
	msg, err := enc.encode([]byte{0x02, 0x0a})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	ir := msg.GetIngestRecord()
	if ir == nil {
		t.Fatal("want IngestRecord payload, got nil")
	}
	enc.stampOffset(msg, 9)
	if ir.GetOffsetId() != 9 {
		t.Fatalf("want stamped offset 9, got %d", ir.GetOffsetId())
	}
	if len(ir.GetAvroEncodedRecord()) != 2 {
		t.Fatalf("want avro datum, got %v", ir.GetAvroEncodedRecord())
	}
	if got := enc.decode(msg); len(got) != 1 || len(got[0]) != 2 {
		t.Fatalf("decode roundtrip mismatch: %v", got)
	}
}

func TestAvroBatchEncoderUsesAvroBatch(t *testing.T) {
	enc := avroEncoder{}
	msg, err := enc.encodeBatch([][]byte{{0x01}, {0x02}, {0x03}})
	if err != nil {
		t.Fatalf("encodeBatch: %v", err)
	}
	ab := msg.GetIngestRecordBatch().GetAvroBatch()
	if ab == nil {
		t.Fatal("want AvroBatch, got nil")
	}
	if got := len(ab.GetRecords()); got != 3 {
		t.Fatalf("want 3 records in batch, got %d", got)
	}
	if got := enc.decode(msg); len(got) != 3 {
		t.Fatalf("want 3 decoded records, got %d", len(got))
	}
}

func TestNewEncoderReturnsAvroForAvroRecordType(t *testing.T) {
	enc, err := newEncoder(zerobuspb.RecordType_AVRO)
	if err != nil {
		t.Fatalf("newEncoder(AVRO): %v", err)
	}
	if _, ok := enc.(avroEncoder); !ok {
		t.Fatalf("want avroEncoder, got %T", enc)
	}
}

func TestJSONBatchEncoderSetsOffsetAndPayload(t *testing.T) {
	enc := jsonEncoder{}
	msg, err := enc.encodeBatch([][]byte{[]byte(`{"a":1}`), []byte(`{"b":2}`)})
	if err != nil {
		t.Fatalf("encodeBatch: %v", err)
	}
	ib := msg.GetIngestRecordBatch()
	if ib == nil {
		t.Fatal("want IngestRecordBatch payload, got nil")
	}
	enc.stampOffset(msg, 5)
	if ib.GetOffsetId() != 5 {
		t.Fatalf("want stamped offset 5, got %d", ib.GetOffsetId())
	}
	jb := ib.GetJsonBatch()
	if jb == nil {
		t.Fatal("want JsonBatch, got nil")
	}
	if got := jb.GetRecords(); len(got) != 2 || got[0] != `{"a":1}` || got[1] != `{"b":2}` {
		t.Fatalf("want 2 json records preserved, got %v", got)
	}
}

// Empty JSON payloads are valid and must be preserved end-to-end.
func TestJSONEncoderAcceptsEmptyRecord(t *testing.T) {
	msg, err := (jsonEncoder{}).encode(nil)
	if err != nil {
		t.Fatalf("encode empty json record: %v", err)
	}
	if got := msg.GetIngestRecord().GetJsonRecord(); got != "" {
		t.Fatalf("want empty json string, got %q", got)
	}

	msg, err = (jsonEncoder{}).encode([]byte{})
	if err != nil {
		t.Fatalf("encode zero-length json record: %v", err)
	}
	if got := msg.GetIngestRecord().GetJsonRecord(); got != "" {
		t.Fatalf("want empty json string, got %q", got)
	}
}

// Empty proto records are valid in single and batch requests.
func TestProtoEncoderAcceptsEmptyRecord(t *testing.T) {
	def := []byte{} // zero-length serialization of an all-default message
	msg, err := protoEncoder{}.encode(def)
	if err != nil {
		t.Fatalf("encode empty proto record: %v", err)
	}
	if got := (protoEncoder{}).decode(msg); len(got) != 1 || len(got[0]) != 0 {
		t.Fatalf("want one empty record back, got %v", got)
	}

	batch, err := protoEncoder{}.encodeBatch([][]byte{def, {0x01}})
	if err != nil {
		t.Fatalf("encodeBatch with empty proto record: %v", err)
	}
	if got := (protoEncoder{}).decode(batch); len(got) != 2 || len(got[0]) != 0 {
		t.Fatalf("want empty record preserved in batch, got %v", got)
	}
}

func TestBatchEncodersRejectEmptyBatch(t *testing.T) {
	for _, enc := range []encoder[encodedMsg]{protoEncoder{}, jsonEncoder{}} {
		if _, err := enc.encodeBatch(nil); err == nil {
			t.Errorf("%T: want error for empty batch, got nil", enc)
		}
	}
	// Empty JSON records inside a non-empty batch are valid.
	msg, err := (jsonEncoder{}).encodeBatch([][]byte{[]byte("ok"), {}})
	if err != nil {
		t.Fatalf("jsonEncoder: empty record in batch should be accepted: %v", err)
	}
	got := msg.GetIngestRecordBatch().GetJsonBatch().GetRecords()
	if len(got) != 2 || got[1] != "" {
		t.Fatalf("json batch: want second record empty, got %v", got)
	}
}

func TestExtractRecordsReturnsAllBatchRecords(t *testing.T) {
	// A batch message must yield every record, not just the first, so GetUnacked
	// doesn't silently drop the tail of an unacked batch.
	protoMsg, err := protoEncoder{}.encodeBatch([][]byte{{0x01}, {0x02}, {0x03}})
	if err != nil {
		t.Fatalf("encodeBatch: %v", err)
	}
	if got := (protoEncoder{}).decode(protoMsg); len(got) != 3 {
		t.Fatalf("proto batch: want 3 records extracted, got %d", len(got))
	}

	jsonMsg, err := jsonEncoder{}.encodeBatch([][]byte{[]byte(`{"a":1}`), []byte(`{"b":2}`)})
	if err != nil {
		t.Fatalf("encodeBatch: %v", err)
	}
	got := (jsonEncoder{}).decode(jsonMsg)
	if len(got) != 2 || string(got[0]) != `{"a":1}` || string(got[1]) != `{"b":2}` {
		t.Fatalf("json batch: want both records extracted, got %v", got)
	}

	// A single-record message still yields exactly one entry.
	single, err := jsonEncoder{}.encode([]byte(`{"x":1}`))
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
	msg, err := protoEncoder{}.encode(rec)
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
	msg, err := protoEncoder{}.encodeBatch(recs)
	if err != nil {
		t.Fatalf("encodeBatch: %v", err)
	}
	recs[0][0] = 0xff // mutate caller buffer post-encode
	got := msg.GetIngestRecordBatch().GetProtoEncodedBatch().GetRecords()
	if len(got) != 2 || got[0][0] != 0x01 {
		t.Fatalf("queued batch payload aliased caller buffer: got %v", got)
	}
}

// maxWireSize includes proto framing and the worst-case offset.
func TestWireSizeIncludesFraming(t *testing.T) {
	rec := []byte("hello")
	for _, enc := range []encoder[encodedMsg]{protoEncoder{}, jsonEncoder{}} {
		msg, err := enc.encode(rec)
		if err != nil {
			t.Fatalf("%T encode: %v", enc, err)
		}
		if got := enc.maxWireSize(msg); got <= len(rec) {
			t.Errorf("%T: maxWireSize %d should exceed raw len %d", enc, got, len(rec))
		}
	}
}

func TestEncoderMaxWireSizeCoversEveryOffset(t *testing.T) {
	for _, enc := range []encoder[encodedMsg]{protoEncoder{}, jsonEncoder{}} {
		single, err := enc.encode([]byte("hello"))
		if err != nil {
			t.Fatalf("%T encode: %v", enc, err)
		}
		batch, err := enc.encodeBatch([][]byte{[]byte("hello"), []byte("world")})
		if err != nil {
			t.Fatalf("%T encodeBatch: %v", enc, err)
		}
		for _, msg := range []encodedMsg{single, batch} {
			maxSize := enc.maxWireSize(msg)
			for _, offset := range []int64{0, 127, 128, 16_384, math.MaxInt64} {
				enc.stampOffset(msg, offset)
				if got := proto.Size(msg); got > maxSize {
					t.Fatalf("%T offset %d size = %d, maxWireSize = %d",
						enc, offset, got, maxSize)
				}
			}
		}
	}
}

func TestEncoderReusesOffsetPointer(t *testing.T) {
	enc := jsonEncoder{}
	msg, err := enc.encode([]byte(`{"x":1}`))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	offsetPtr := msg.GetIngestRecord().OffsetId
	enc.stampOffset(msg, 1)
	enc.stampOffset(msg, 2)
	if msg.GetIngestRecord().OffsetId != offsetPtr {
		t.Fatal("stampOffset replaced the offset pointer")
	}
	if got := *offsetPtr; got != 2 {
		t.Fatalf("offset = %d, want 2", got)
	}
}

// TestAtomicEncoderUnitCountAndSlice pins the durability-unit contract for the
// proto and JSON encoders. slice runs on every reconnect via requeueWithSlicer,
// so its identity case is on the recovery path for the shipping protocols.
func TestAtomicEncoderUnitCountAndSlice(t *testing.T) {
	encoders := map[string]encoder[encodedMsg]{
		"proto": protoEncoder{},
		"json":  jsonEncoder{},
	}
	for name, enc := range encoders {
		t.Run(name, func(t *testing.T) {
			single, err := enc.encode([]byte("record"))
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			batch, err := enc.encodeBatch([][]byte{[]byte("a"), []byte("b")})
			if err != nil {
				t.Fatalf("encodeBatch: %v", err)
			}
			// A batch is atomic to the server, so it is one unit despite
			// carrying several records.
			for label, msg := range map[string]encodedMsg{"single": single, "batch": batch} {
				if got := enc.unitCount(msg); got != 1 {
					t.Fatalf("%s unitCount = %d, want 1", label, got)
				}
				got, err := enc.slice(msg, 0)
				if err != nil {
					t.Fatalf("%s slice(0): %v", label, err)
				}
				if got != msg {
					t.Fatalf("%s slice(0) must return the payload unchanged", label)
				}
				if _, err := enc.slice(msg, 1); err == nil {
					t.Fatalf("%s accepted a non-zero acknowledged prefix", label)
				}
			}
		})
	}
}
