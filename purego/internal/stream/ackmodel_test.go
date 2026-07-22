package stream

import (
	"testing"

	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

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
	kind, got, _ := am.classify(resp)
	if kind != ackResponse {
		t.Fatalf("want ackResponse for ingest response, got %v", kind)
	}
	if got != 99 {
		t.Fatalf("want offset 99, got %d", got)
	}
}

func TestOffsetAckModelClassifiesCloseSignalAsPause(t *testing.T) {
	am := offsetAckModel{}
	resp := &zerobuspb.EphemeralStreamResponse{
		Payload: &zerobuspb.EphemeralStreamResponse_CloseStreamSignal{
			CloseStreamSignal: &zerobuspb.CloseStreamSignal{},
		},
	}
	kind, _, _ := am.classify(resp)
	if kind != pauseResponse {
		t.Fatalf("want pauseResponse for close signal, got %v", kind)
	}
}

func TestOffsetAckModelIgnoresUnknownResponse(t *testing.T) {
	am := offsetAckModel{}
	// An empty response carries neither an ack nor a close signal.
	kind, _, _ := am.classify(&zerobuspb.EphemeralStreamResponse{})
	if kind != otherResponse {
		t.Fatalf("want otherResponse for empty response, got %v", kind)
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
