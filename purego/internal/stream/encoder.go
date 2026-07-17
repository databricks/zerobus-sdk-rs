package stream

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

// encodedMsg is a wire-ready EphemeralStream ingest request, built once at
// Ingest time and held in the buffer until the sender transmits it.
// Encoding is eager so the buffer never retains live user objects.
type encodedMsg = *zerobuspb.EphemeralStreamRequest

// encoder turns a user record into a wire message for the given offset.
// The offset is stamped into the message here so re-sends after recovery use
// the same logical offset the server already saw.
type encoder interface {
	encode(offset int64, record []byte) (encodedMsg, error)
}

// protoEncoder builds EphemeralStreamRequest_IngestRecord payloads for
// proto-encoded records (raw serialized protobuf bytes).
type protoEncoder struct{}

func (protoEncoder) encode(offset int64, record []byte) (encodedMsg, error) {
	if len(record) == 0 {
		return nil, fmt.Errorf("stream: proto record must not be empty")
	}
	return &zerobuspb.EphemeralStreamRequest{
		Payload: &zerobuspb.EphemeralStreamRequest_IngestRecord{
			IngestRecord: &zerobuspb.IngestRecordRequest{
				OffsetId: proto.Int64(offset),
				Record:   &zerobuspb.IngestRecordRequest_ProtoEncodedRecord{ProtoEncodedRecord: record},
			},
		},
	}, nil
}

// jsonEncoder builds EphemeralStreamRequest_IngestRecord payloads for
// JSON-encoded records.
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

// protoBatchEncoder builds EphemeralStreamRequest_IngestRecordBatch payloads.
// All records in the batch share one offset; the whole batch is atomic to
// the server.
type protoBatchEncoder struct{}

func (protoBatchEncoder) encode(offset int64, batch []byte) (encodedMsg, error) {
	if len(batch) == 0 {
		return nil, fmt.Errorf("stream: proto batch must not be empty")
	}
	// batch is a length-delimited sequence of serialized proto records packed
	// by the caller; we send it as a ProtoEncodedRecordBatch.
	return &zerobuspb.EphemeralStreamRequest{
		Payload: &zerobuspb.EphemeralStreamRequest_IngestRecordBatch{
			IngestRecordBatch: &zerobuspb.IngestRecordBatchRequest{
				OffsetId: proto.Int64(offset),
				Batch: &zerobuspb.IngestRecordBatchRequest_ProtoEncodedBatch{
					ProtoEncodedBatch: &zerobuspb.ProtoEncodedRecordBatch{Records: [][]byte{batch}},
				},
			},
		},
	}, nil
}

// jsonBatchEncoder builds EphemeralStreamRequest_IngestRecordBatch payloads
// for a batch of JSON records.
type jsonBatchEncoder struct{}

func (jsonBatchEncoder) encode(offset int64, batch []byte) (encodedMsg, error) {
	if len(batch) == 0 {
		return nil, fmt.Errorf("stream: json batch must not be empty")
	}
	// batch is a newline-delimited JSON payload; one record per line.
	return &zerobuspb.EphemeralStreamRequest{
		Payload: &zerobuspb.EphemeralStreamRequest_IngestRecordBatch{
			IngestRecordBatch: &zerobuspb.IngestRecordBatchRequest{
				OffsetId: proto.Int64(offset),
				Batch: &zerobuspb.IngestRecordBatchRequest_JsonBatch{
					JsonBatch: &zerobuspb.JsonRecordBatch{Records: []string{string(batch)}},
				},
			},
		},
	}, nil
}
