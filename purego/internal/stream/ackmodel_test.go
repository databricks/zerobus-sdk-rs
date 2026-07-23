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
	kind, off, _ := offsetAckModel{}.classify(resp)
	if kind != ackResponse {
		t.Fatalf("want ackResponse, got %v", kind)
	}
	if off != 42 {
		t.Fatalf("want offset 42, got %d", off)
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
	kind, off, _ := offsetAckModel{}.classify(resp)
	if kind != ackResponse || off != 0 {
		t.Fatalf("want ackResponse offset 0, got kind=%v off=%d", kind, off)
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
	kind, _, pause := offsetAckModel{}.classify(resp)
	if kind != pauseResponse {
		t.Fatalf("want pauseResponse, got %v", kind)
	}
	if pause.duration != 3*time.Second {
		t.Fatalf("want 3s pause, got %v", pause.duration)
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
	kind, off, _ := offsetAckModel{}.classify(resp)
	if kind != malformedResponse {
		t.Fatalf("want malformedResponse for absent offset, got %v", kind)
	}
	if off != 0 {
		t.Fatalf("want offset 0 for malformed, got %d", off)
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
	kind, off, _ := offsetAckModel{}.classify(resp)
	if kind != malformedResponse {
		t.Fatalf("want malformedResponse for negative offset, got %v", kind)
	}
	if off != 0 {
		t.Fatalf("want offset 0 for malformed, got %d", off)
	}
}

// Nil and payload-less responses are unknown, not ignorable, so the receiver
// can fail the stream on a wire-contract mismatch.
func TestClassifyUnknown(t *testing.T) {
	for _, resp := range []ephemeralResp{nil, {}} {
		kind, _, _ := offsetAckModel{}.classify(resp)
		if kind != unknownResponse {
			t.Fatalf("want unknownResponse, got %v", kind)
		}
	}
}
