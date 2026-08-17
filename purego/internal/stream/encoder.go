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
// Req type the proto/JSON core is instantiated with; another protocol
// instantiates the core over its own payload type. Encoding is eager so the
// buffer never retains live user objects.
type encodedMsg = *zerobuspb.EphemeralStreamRequest

// encoder turns user records into offset-independent wire messages and recovers
// them again for GetUnacked. It is the send-side per-encoding seam. The core is
// generic over the wire message type Req and never names a concrete proto type.
// proto/JSON supply encoder[encodedMsg] directly; a protocol defined outside
// this package supplies EncoderHooks, which hookEncoder adapts to this
// interface.
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
	// unitCount reports the number of protocol durability units in msg.
	// Atomic protocols return one even when msg contains a record batch.
	unitCount(msg Req) uint64
	// slice removes an acknowledged durability-unit prefix. It is called only
	// for a partially acknowledged item during recovery or GetUnacked.
	slice(msg Req, acknowledgedPrefix uint64) (Req, error)
	// decode recovers the raw record bytes from a wire message so GetUnacked can
	// return original content. A single-record message yields one entry; a batch
	// yields all of its records so no unacked record is silently dropped.
	decode(msg Req) [][]byte
	// maxWireSize reports an upper bound for any one transport frame produced by
	// Send, across every offset stamp. A wire stream may expand one logical msg
	// into multiple frames behind its single Send call.
	maxWireSize(msg Req) int
	// retainedSize estimates the heap retained by an encoded message before it
	// is built. It includes raw bytes, per-record containers, framing, and
	// request-object overhead so aggregate backpressure cannot be bypassed by
	// batches of tiny or empty records.
	retainedSize(rawBytes, recordCount int) int64
	// actualRetainedSize reports what msg retains now that it exists, replacing
	// the pre-encode estimate. An encoding whose output size is not a function
	// of its input size returns the true figure here; the rest return estimate.
	actualRetainedSize(msg Req, estimate int64) int64
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

func (protoEncoder) unitCount(encodedMsg) uint64 { return 1 }

func (protoEncoder) slice(msg encodedMsg, acknowledgedPrefix uint64) (encodedMsg, error) {
	if acknowledgedPrefix == 0 {
		return msg, nil
	}
	return nil, fmt.Errorf(
		"stream: proto payload is atomic and cannot drop %d acknowledged units",
		acknowledgedPrefix,
	)
}

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

func (protoEncoder) actualRetainedSize(_ encodedMsg, estimate int64) int64 {
	return estimate
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

func (jsonEncoder) unitCount(encodedMsg) uint64 { return 1 }

func (jsonEncoder) slice(msg encodedMsg, acknowledgedPrefix uint64) (encodedMsg, error) {
	if acknowledgedPrefix == 0 {
		return msg, nil
	}
	return nil, fmt.Errorf(
		"stream: JSON payload is atomic and cannot drop %d acknowledged units",
		acknowledgedPrefix,
	)
}

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

func (jsonEncoder) actualRetainedSize(_ encodedMsg, estimate int64) int64 {
	return estimate
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

// EncoderHooks adapts protocol functions into the generic encoding seam. It is
// exported within the internal package boundary for protocol implementations
// whose payload type is defined outside stream.
type EncoderHooks[Req any] struct {
	EncodeRecord func(record []byte) (Req, error)
	EncodeBatch  func(records [][]byte) (Req, error)
	StampOffset  func(msg Req, offset int64)
	UnitCount    func(msg Req) uint64
	Slice        func(msg Req, acknowledgedPrefix uint64) (Req, error)
	Decode       func(msg Req) [][]byte
	MaxWireSize  func(msg Req) int
	RetainedSize func(rawBytes, recordCount int) int64
	// ActualRetainedSize is optional. Set it when RetainedSize cannot predict the
	// encoded size from the input size, so the reservation is reconciled against
	// what the payload really holds instead of the estimate.
	ActualRetainedSize func(msg Req) int64
}

type hookEncoder[Req any] struct {
	hooks EncoderHooks[Req]
}

func (e hookEncoder[Req]) encode(record []byte) (Req, error) {
	return e.hooks.EncodeRecord(record)
}

func (e hookEncoder[Req]) encodeBatch(records [][]byte) (Req, error) {
	return e.hooks.EncodeBatch(records)
}

func (e hookEncoder[Req]) stampOffset(msg Req, offset int64) {
	e.hooks.StampOffset(msg, offset)
}

func (e hookEncoder[Req]) unitCount(msg Req) uint64 {
	return e.hooks.UnitCount(msg)
}

func (e hookEncoder[Req]) slice(msg Req, acknowledgedPrefix uint64) (Req, error) {
	return e.hooks.Slice(msg, acknowledgedPrefix)
}

func (e hookEncoder[Req]) decode(msg Req) [][]byte {
	return e.hooks.Decode(msg)
}

func (e hookEncoder[Req]) maxWireSize(msg Req) int {
	return e.hooks.MaxWireSize(msg)
}

func (e hookEncoder[Req]) retainedSize(rawBytes, recordCount int) int64 {
	return e.hooks.RetainedSize(rawBytes, recordCount)
}

func (e hookEncoder[Req]) actualRetainedSize(msg Req, estimate int64) int64 {
	if e.hooks.ActualRetainedSize == nil {
		return estimate
	}
	return e.hooks.ActualRetainedSize(msg)
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
