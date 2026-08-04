package stream

import (
	"bytes"
	"fmt"
	"math"

	"google.golang.org/protobuf/proto"

	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

// encodedMsg is a wire-ready EphemeralStream ingest request, built once at
// Ingest time and held in the buffer until the sender transmits it. It is the
// Req type used by the proto/JSON core. Encoding is eager so the buffer never
// retains live user objects.
type encodedMsg = *zerobuspb.EphemeralStreamRequest

// encoder turns user records into offset-independent wire messages and recovers
// them again for GetUnacked. It is the send-side per-encoding seam. The core is
// generic over the wire message type Req.
//
// The sender replaces the encoded offset with a connection-local wire offset.
type encoder[Req any] interface {
	// encode turns a single user record into an offset-independent wire message.
	encode(record []byte) (Req, error)
	// encodeBatch turns many already-encoded records into one wire message. All
	// records share a single offset and are atomic to the server (it acks the
	// whole batch or none of it), so the batch occupies exactly one logical
	// offset in the core's buffer.
	encodeBatch(records [][]byte) (Req, error)
	// stampOffset assigns the connection-local wire offset.
	stampOffset(msg Req, offset int64)
	// decode recovers the raw record bytes from a wire message so GetUnacked can
	// return original content. A single-record message yields one entry; a batch
	// yields all of its records so no unacked record is silently dropped.
	decode(msg Req) [][]byte
	// maxWireSize reports an upper bound across every offset stamp.
	maxWireSize(msg Req) int
	// retainedSize estimates the heap retained by an encoded message before it
	// is built. It includes raw bytes, per-record containers, framing, and
	// request-object overhead so aggregate backpressure cannot be bypassed by
	// batches of tiny or empty records.
	retainedSize(rawBytes, recordCount int) int64
}

const encodedRequestOverhead = int64(512)
const encodedRecordOverhead = int64(32)

func ephemeralRetainedSize(rawBytes, recordCount int) int64 {
	return int64(rawBytes) +
		int64(recordCount)*encodedRecordOverhead +
		encodedRequestOverhead
}

// protoEncoder builds EphemeralStream payloads for proto-encoded records (raw
// serialized protobuf bytes), single and batched.
type protoEncoder struct{}

func (protoEncoder) encode(record []byte) (encodedMsg, error) {
	offset := int64(math.MaxInt64)
	// Empty is a valid proto encoding (all-default message). Clone so a reused
	// caller buffer can't mutate the queued payload before it's serialized.
	return &zerobuspb.EphemeralStreamRequest{
		Payload: &zerobuspb.EphemeralStreamRequest_IngestRecord{
			IngestRecord: &zerobuspb.IngestRecordRequest{
				OffsetId: proto.Int64(offset),
				Record:   &zerobuspb.IngestRecordRequest_ProtoEncodedRecord{ProtoEncodedRecord: bytes.Clone(record)},
			},
		},
	}, nil
}

func (protoEncoder) encodeBatch(records [][]byte) (encodedMsg, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("stream: proto batch must not be empty")
	}
	// Clone each record (empty is valid) so a reused caller buffer can't mutate
	// a queued payload.
	copied := make([][]byte, len(records))
	for i, r := range records {
		copied[i] = bytes.Clone(r)
	}
	offset := int64(math.MaxInt64)
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

func (protoEncoder) maxWireSize(msg encodedMsg) int {
	if msg == nil {
		return 0
	}
	return proto.Size(msg)
}

func (protoEncoder) stampOffset(msg encodedMsg, offset int64) {
	stampEphemeralOffset(msg, offset)
}

func (protoEncoder) retainedSize(rawBytes, recordCount int) int64 {
	return ephemeralRetainedSize(rawBytes, recordCount)
}

// jsonEncoder builds EphemeralStream payloads for JSON-encoded records, single
// and batched.
type jsonEncoder struct{}

func (jsonEncoder) encode(record []byte) (encodedMsg, error) {
	offset := int64(math.MaxInt64)
	return &zerobuspb.EphemeralStreamRequest{
		Payload: &zerobuspb.EphemeralStreamRequest_IngestRecord{
			IngestRecord: &zerobuspb.IngestRecordRequest{
				OffsetId: proto.Int64(offset),
				Record:   &zerobuspb.IngestRecordRequest_JsonRecord{JsonRecord: string(record)},
			},
		},
	}, nil
}

func (jsonEncoder) encodeBatch(records [][]byte) (encodedMsg, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("stream: json batch must not be empty")
	}
	jsonRecords := make([]string, len(records))
	for i, r := range records {
		jsonRecords[i] = string(r)
	}
	offset := int64(math.MaxInt64)
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

func (jsonEncoder) maxWireSize(msg encodedMsg) int {
	if msg == nil {
		return 0
	}
	return proto.Size(msg)
}

func (jsonEncoder) stampOffset(msg encodedMsg, offset int64) {
	stampEphemeralOffset(msg, offset)
}

func (jsonEncoder) retainedSize(rawBytes, recordCount int) int64 {
	return ephemeralRetainedSize(rawBytes, recordCount)
}

func stampEphemeralOffset(msg encodedMsg, offset int64) {
	if msg == nil {
		return
	}
	if record := msg.GetIngestRecord(); record != nil {
		if record.OffsetId == nil {
			record.OffsetId = proto.Int64(offset)
		} else {
			*record.OffsetId = offset
		}
		return
	}
	if batch := msg.GetIngestRecordBatch(); batch != nil {
		if batch.OffsetId == nil {
			batch.OffsetId = proto.Int64(offset)
		} else {
			*batch.OffsetId = offset
		}
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
		// Switch on oneof type, not nil: an empty proto record must not be read
		// as JSON. Clone so callers (GetUnacked) never alias the retained payload.
		switch r := ir.GetRecord().(type) {
		case *zerobuspb.IngestRecordRequest_ProtoEncodedRecord:
			return [][]byte{bytes.Clone(r.ProtoEncodedRecord)}
		case *zerobuspb.IngestRecordRequest_JsonRecord:
			return [][]byte{[]byte(r.JsonRecord)}
		}
		return nil
	}
	if ib := msg.GetIngestRecordBatch(); ib != nil {
		if pb := ib.GetProtoEncodedBatch(); pb != nil {
			recs := pb.GetRecords()
			out := make([][]byte, len(recs))
			for i, r := range recs {
				out[i] = bytes.Clone(r)
			}
			return out
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
