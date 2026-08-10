package stream

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

func TestClassifyAck(t *testing.T) {
	resp := &zerobuspb.EphemeralStreamResponse{
		Payload: &zerobuspb.EphemeralStreamResponse_IngestRecordResponse{
			IngestRecordResponse: &zerobuspb.IngestRecordResponse{
				DurabilityAckUpToOffset: proto.Int64(42),
			},
		},
	}
	classified := offsetAckModel{}.classify(resp)
	if !classified.hasAck || classified.failure != usableResponse {
		t.Fatalf("want usable ack response, got %+v", classified)
	}
	if classified.legacyOffset != 42 {
		t.Fatalf("want offset 42, got %d", classified.legacyOffset)
	}
}

// Offset 0 is a real ack (stream offsets start at 0), so it must be accepted as
// long as the field is present.
func TestClassifyAckOffsetZero(t *testing.T) {
	resp := &zerobuspb.EphemeralStreamResponse{
		Payload: &zerobuspb.EphemeralStreamResponse_IngestRecordResponse{
			IngestRecordResponse: &zerobuspb.IngestRecordResponse{
				DurabilityAckUpToOffset: proto.Int64(0),
			},
		},
	}
	classified := offsetAckModel{}.classify(resp)
	if !classified.hasAck || classified.legacyOffset != 0 {
		t.Fatalf("want ack response offset 0, got %+v", classified)
	}
}

func TestClassifyPause(t *testing.T) {
	resp := &zerobuspb.EphemeralStreamResponse{
		Payload: &zerobuspb.EphemeralStreamResponse_CloseStreamSignal{
			CloseStreamSignal: &zerobuspb.CloseStreamSignal{
				Duration: durationpb.New(3 * time.Second),
			},
		},
	}
	classified := offsetAckModel{}.classify(resp)
	if classified.pause == nil || classified.failure != usableResponse {
		t.Fatalf("want usable pause response, got %+v", classified)
	}
	if classified.pause.duration != 3*time.Second {
		t.Fatalf("want 3s pause, got %v", classified.pause.duration)
	}
}

// An IngestRecordResponse with an absent offset must be malformed, not an ack
// fabricated at offset 0.
func TestClassifyMalformedAckMissingOffset(t *testing.T) {
	resp := &zerobuspb.EphemeralStreamResponse{
		Payload: &zerobuspb.EphemeralStreamResponse_IngestRecordResponse{
			IngestRecordResponse: &zerobuspb.IngestRecordResponse{}, // offset absent
		},
	}
	classified := offsetAckModel{}.classify(resp)
	if classified.failure != malformedResponse {
		t.Fatalf("want malformedResponse for absent offset, got %+v", classified)
	}
	if classified.legacyOffset != 0 {
		t.Fatalf("want offset 0 for malformed, got %d", classified.legacyOffset)
	}
}

func TestClassifyMalformedAckNegativeOffset(t *testing.T) {
	resp := &zerobuspb.EphemeralStreamResponse{
		Payload: &zerobuspb.EphemeralStreamResponse_IngestRecordResponse{
			IngestRecordResponse: &zerobuspb.IngestRecordResponse{
				DurabilityAckUpToOffset: proto.Int64(-1),
			},
		},
	}
	classified := offsetAckModel{}.classify(resp)
	if classified.failure != malformedResponse {
		t.Fatalf("want malformedResponse for negative offset, got %+v", classified)
	}
	if classified.legacyOffset != 0 {
		t.Fatalf("want offset 0 for malformed, got %d", classified.legacyOffset)
	}
}

// Nil and payload-less responses are unknown, not ignorable, so the receiver
// can fail the stream on a wire-contract mismatch.
func TestClassifyUnknown(t *testing.T) {
	for _, resp := range []ephemeralResp{nil, {}} {
		classified := offsetAckModel{}.classify(resp)
		if classified.failure != unknownResponse {
			t.Fatalf("want unknownResponse, got %+v", classified)
		}
	}
}
