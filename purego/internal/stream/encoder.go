package stream

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

// DefaultMaxPayloadBytes is the default per-message wire payload cap: the
// ~10 MiB gRPC server limit minus a 64 KiB envelope headroom. A single
// record or a batch whose serialized wire size exceeds this budget is
// rejected at Ingest so callers see a deterministic input error instead of
// a transport failure that burns the recovery budget.
const DefaultMaxPayloadBytes = 10*1024*1024 - 64*1024

// cloneBytes returns a fresh []byte with the same contents as b so buffered
// records don't alias caller-owned memory. Reusing the same []byte across
// Ingest calls (or mutating it after Ingest returns) would otherwise change
// queued and replayed payloads and race with wire serialization.
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// encodedMsg is a wire-ready EphemeralStream ingest request, built once at
// Ingest time and held in the buffer until the sender transmits it. It is the
// Req type the proto/JSON core is instantiated with; the Arrow path will use a
// Flight frame instead. Encoding is eager so the buffer never retains live user
// objects.
type encodedMsg = *zerobuspb.EphemeralStreamRequest

// encoder turns user records into wire messages for a given offset and
// recovers them again for GetUnacked. It is the send-side per-encoding seam:
// the core is generic over the wire message type Req and never names a
// concrete proto type — proto/JSON supply encoder[encodedMsg]; Arrow will
// supply encoder[flightFrame].
//
// The offset is stamped into the message here so re-sends after recovery reuse
// the same logical offset the server already saw.
type encoder[Req any] interface {
	// encode turns a single user record into one wire message.
	encode(offset int64, record []byte) (Req, error)
	// encodeBatch turns many already-encoded records into one wire message. All
	// records share a single offset and are atomic to the server (it acks the
	// whole batch or none of it), so the batch occupies exactly one logical
	// offset in the core's buffer.
	encodeBatch(offset int64, records [][]byte) (Req, error)
	// stampOffset assigns the given offset to an already-built message so the
	// heavy encoding work (payload copy, framing) can run outside the
	// offset-assignment critical section and only the cheap offset stamp
	// happens under the lock. This lets many-goroutine Ingest scale: a large
	// batch does not serialize concurrent small ingests behind it.
	stampOffset(msg Req, offset int64)
	// decode recovers the raw record bytes from a wire message so GetUnacked can
	// return original content. A single-record message yields one entry; a batch
	// yields all of its records so no unacked record is silently dropped.
	decode(msg Req) [][]byte
	// wireSize returns the exact serialized wire size of the message so the
	// payload cap can be enforced against what the server will actually see,
	// not the raw input bytes (which exclude proto framing).
	wireSize(msg Req) int
}

// protoEncoder builds EphemeralStream payloads for proto-encoded records (raw
// serialized protobuf bytes), single and batched.
type protoEncoder struct{}

func (protoEncoder) encode(offset int64, record []byte) (encodedMsg, error) {
	if len(record) == 0 {
		return nil, fmt.Errorf("stream: proto record must not be empty")
	}
	return &zerobuspb.EphemeralStreamRequest{
		Payload: &zerobuspb.EphemeralStreamRequest_IngestRecord{
			IngestRecord: &zerobuspb.IngestRecordRequest{
				OffsetId: proto.Int64(offset),
				// Copy caller bytes: proto retains them by reference and the
				// buffer may hold this message across recovery re-sends.
				Record: &zerobuspb.IngestRecordRequest_ProtoEncodedRecord{ProtoEncodedRecord: cloneBytes(record)},
			},
		},
	}, nil
}

func (protoEncoder) encodeBatch(offset int64, records [][]byte) (encodedMsg, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("stream: proto batch must not be empty")
	}
	// Snapshot each record so a shared source buffer can't mutate queued or
	// replayed payloads. The outer slice is also copied so mutations to the
	// caller's slice header don't reach the batch.
	copied := make([][]byte, len(records))
	for i, r := range records {
		if len(r) == 0 {
			return nil, fmt.Errorf("stream: proto batch record %d must not be empty", i)
		}
		copied[i] = cloneBytes(r)
	}
	return &zerobuspb.EphemeralStreamRequest{
		Payload: &zerobuspb.EphemeralStreamRequest_IngestRecordBatch{
			IngestRecordBatch: &zerobuspb.IngestRecordBatchRequest{
				OffsetId: proto.Int64(offset),
				Batch: &zerobuspb.IngestRecordBatchRequest_ProtoEncodedBatch{
					ProtoEncodedBatch: &zerobuspb.ProtoEncodedRecordBatch{Records: copied},
				},
			},
		},
	}, nil
}

func (protoEncoder) decode(msg encodedMsg) [][]byte { return extractEphemeralRecords(msg) }

func (protoEncoder) wireSize(msg encodedMsg) int {
	if msg == nil {
		return 0
	}
	return proto.Size(msg)
}

func (protoEncoder) stampOffset(msg encodedMsg, offset int64) { stampEphemeralOffset(msg, offset) }

// jsonEncoder builds EphemeralStream payloads for JSON-encoded records, single
// and batched.
type jsonEncoder struct{}

func (jsonEncoder) encode(offset int64, record []byte) (encodedMsg, error) {
	if len(record) == 0 {
		return nil, fmt.Errorf("stream: json record must not be empty")
	}
	return &zerobuspb.EphemeralStreamRequest{
		Payload: &zerobuspb.EphemeralStreamRequest_IngestRecord{
			IngestRecord: &zerobuspb.IngestRecordRequest{
				OffsetId: proto.Int64(offset),
				Record:   &zerobuspb.IngestRecordRequest_JsonRecord{JsonRecord: string(record)},
			},
		},
	}, nil
}

func (jsonEncoder) encodeBatch(offset int64, records [][]byte) (encodedMsg, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("stream: json batch must not be empty")
	}
	jsonRecords := make([]string, len(records))
	for i, r := range records {
		if len(r) == 0 {
			return nil, fmt.Errorf("stream: json batch record %d must not be empty", i)
		}
		jsonRecords[i] = string(r)
	}
	return &zerobuspb.EphemeralStreamRequest{
		Payload: &zerobuspb.EphemeralStreamRequest_IngestRecordBatch{
			IngestRecordBatch: &zerobuspb.IngestRecordBatchRequest{
				OffsetId: proto.Int64(offset),
				Batch: &zerobuspb.IngestRecordBatchRequest_JsonBatch{
					JsonBatch: &zerobuspb.JsonRecordBatch{Records: jsonRecords},
				},
			},
		},
	}, nil
}

func (jsonEncoder) decode(msg encodedMsg) [][]byte { return extractEphemeralRecords(msg) }

func (jsonEncoder) wireSize(msg encodedMsg) int {
	if msg == nil {
		return 0
	}
	return proto.Size(msg)
}

func (jsonEncoder) stampOffset(msg encodedMsg, offset int64) { stampEphemeralOffset(msg, offset) }

// stampEphemeralOffset mutates the offset field of a proto/JSON
// EphemeralStream message in place. Used to defer offset assignment out of
// the encode step so encoding can happen outside the offset-assignment lock.
func stampEphemeralOffset(msg encodedMsg, offset int64) {
	if msg == nil {
		return
	}
	if ir := msg.GetIngestRecord(); ir != nil {
		ir.OffsetId = proto.Int64(offset)
		return
	}
	if ib := msg.GetIngestRecordBatch(); ib != nil {
		ib.OffsetId = proto.Int64(offset)
	}
}

// newEncoder returns the proto/JSON encoder for the given record type.
func newEncoder(rt zerobuspb.RecordType) (encoder[encodedMsg], error) {
	switch rt {
	case zerobuspb.RecordType_PROTO:
		return protoEncoder{}, nil
	case zerobuspb.RecordType_JSON:
		return jsonEncoder{}, nil
	default:
		return nil, errUnsupportedRecordType(rt)
	}
}

// extractEphemeralRecords recovers the raw record bytes from an EphemeralStream
// wire message. A single-record message yields one entry; a batch yields all of
// its records. Shared by the proto and JSON encoders' decode.
func extractEphemeralRecords(msg encodedMsg) [][]byte {
	if msg == nil {
		return nil
	}
	if ir := msg.GetIngestRecord(); ir != nil {
		if b := ir.GetProtoEncodedRecord(); b != nil {
			return [][]byte{b}
		}
		return [][]byte{[]byte(ir.GetJsonRecord())}
	}
	if ib := msg.GetIngestRecordBatch(); ib != nil {
		if pb := ib.GetProtoEncodedBatch(); pb != nil {
			return pb.GetRecords()
		}
		if jb := ib.GetJsonBatch(); jb != nil {
			recs := jb.GetRecords()
			out := make([][]byte, len(recs))
			for i, r := range recs {
				out[i] = []byte(r)
			}
			return out
		}
	}
	return nil
}
