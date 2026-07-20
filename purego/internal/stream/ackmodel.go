package stream

import (
	"fmt"

	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

// ackModel translates a raw server response into a logical "highest fully-acked
// offset" that the core's watermark can compare against. For proto/JSON over
// EphemeralStream the server sends durability_ack_up_to_offset directly, so the
// translation is trivial. The Arrow path (record-count acks) will provide its
// own implementation.
//
// This interface is intentionally the minimal proto/JSON-only subset: a single
// parse hook. The architecture note (§3a) sketches a wider Track/Resolve/Unacked
// shape for the Arrow record-count model, where an ack can land mid-batch and
// recovery must slice a straddling batch. That width is deferred until the Arrow
// wire path lands; adding it now would be unused machinery. The core stays
// offset-only and blind to the record-vs-batch distinction regardless.
type ackModel interface {
	// parse extracts the highest fully-acked offset from a server response.
	// Returns ok=false when the response carries no ack (e.g. a close signal).
	parse(resp *zerobuspb.EphemeralStreamResponse) (offset int64, ok bool)
}

// offsetAckModel is the proto/JSON ack model: the server sends
// DurabilityAckUpToOffset directly as the logical offset.
type offsetAckModel struct{}

func (offsetAckModel) parse(resp *zerobuspb.EphemeralStreamResponse) (int64, bool) {
	ack := resp.GetIngestRecordResponse()
	if ack == nil {
		return 0, false
	}
	return ack.GetDurabilityAckUpToOffset(), true
}

// newAckModel returns the appropriate ackModel for the given record type.
func newAckModel(rt zerobuspb.RecordType) (ackModel, error) {
	switch rt {
	case zerobuspb.RecordType_PROTO, zerobuspb.RecordType_JSON:
		return offsetAckModel{}, nil
	default:
		return nil, fmt.Errorf("stream: unsupported record type %v", rt)
	}
}
