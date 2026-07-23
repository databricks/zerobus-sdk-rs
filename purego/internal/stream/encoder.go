package stream

import (
	"bytes"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

// encodedMsg is a wire-ready EphemeralStream ingest request, built once at
// Ingest time and held in the buffer until the sender transmits it. It is the
// Req type the proto/JSON core is instantiated with; the Arrow path will use a
// Flight frame instead. Encoding is eager so the buffer never retains live user
// objects.
type encodedMsg = *zerobuspb.EphemeralStreamRequest

// encoder turns user records into wire messages for a given offset and recovers
// them again for GetUnacked. It is the send-side per-encoding
// seam. The core is generic over the wire message type Req and
// never names a concrete proto type — proto/JSON supply encoder[encodedMsg];
// Arrow will supply encoder[flightFrame].
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
	// decode recovers the raw record bytes from a wire message so GetUnacked can
	// return original content. A single-record message yields one entry; a batch
	// yields all of its records so no unacked record is silently dropped.
	decode(msg Req) [][]byte
	// wireSize reports the serialized message size (incl. proto framing) so a
	// payload cap can be enforced against what the server actually receives.
	wireSize(msg Req) int
}

// protoEncoder builds EphemeralStream payloads for proto-encoded records (raw
// serialized protobuf bytes), single and batched.
type protoEncoder struct{}

func (protoEncoder) encode(offset int64, record []byte) (encodedMsg, error) {
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

func (protoEncoder) encodeBatch(offset int64, records [][]byte) (encodedMsg, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("stream: proto batch must not be empty")
	}
	// Clone each record (empty is valid) so a reused caller buffer can't mutate
	// a queued payload.
	copied := make([][]byte, len(records))
	for i, r := range records {
		copied[i] = bytes.Clone(r)
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
		// as JSON.
		switch r := ir.GetRecord().(type) {
		case *zerobuspb.IngestRecordRequest_ProtoEncodedRecord:
			return [][]byte{r.ProtoEncodedRecord}
		case *zerobuspb.IngestRecordRequest_JsonRecord:
			return [][]byte{[]byte(r.JsonRecord)}
		}
		return nil
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
