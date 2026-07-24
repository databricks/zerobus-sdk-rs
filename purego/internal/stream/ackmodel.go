package stream

import (
	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

// ackModel extracts connection-local ack offsets and classifies other responses.
//
// TODO(arrow): the Arrow wire path will supply its own ackModel over a Flight
// response, mapping a cumulative record count back to an offset.
type ackModel[Resp any] interface {
	// classify reports what a server response means to the core: an ack offset,
	// a pause request, or an unknown/malformed response the receiver must fail
	// on. Detecting these here keeps the receiver blind to concrete proto types.
	classify(resp Resp) (kind respKind, off int64, pause pauseSignal)
}

// respKind is the category the ackModel assigns to a server response.
type respKind int

const (
	ackResponse       respKind = iota // carries a durability ack offset
	pauseResponse                     // server-requested pause (close-stream signal)
	unknownResponse                   // unrecognized response type — receiver fails
	malformedResponse                 // ack missing its offset field — receiver fails
)

// ephemeralResp is the proto/JSON server response type. Aliased so the core's
// Resp type parameter is named once here rather than spelled out at every use.
type ephemeralResp = *zerobuspb.EphemeralStreamResponse

// offsetAckModel extracts proto/JSON physical offsets and pause signals.
type offsetAckModel struct{}

func (offsetAckModel) classify(resp ephemeralResp) (respKind, int64, pauseSignal) {
	if resp == nil {
		return unknownResponse, 0, pauseSignal{}
	}
	if sig := resp.GetCloseStreamSignal(); sig != nil {
		return pauseResponse, 0, pauseSignal{duration: sig.GetDuration().AsDuration()}
	}
	if ack := resp.GetIngestRecordResponse(); ack != nil {
		// Absent offset must be malformed, not a fabricated ack for offset 0.
		if ack.DurabilityAckUpToOffset == nil {
			return malformedResponse, 0, pauseSignal{}
		}
		off := *ack.DurabilityAckUpToOffset
		if off < 0 {
			return malformedResponse, 0, pauseSignal{}
		}
		return ackResponse, off, pauseSignal{}
	}
	return unknownResponse, 0, pauseSignal{}
}

// newAckModel returns the proto/JSON ack model for the given record type.
func newAckModel(rt zerobuspb.RecordType) (ackModel[ephemeralResp], error) {
	switch rt {
	case zerobuspb.RecordType_PROTO, zerobuspb.RecordType_JSON:
		return offsetAckModel{}, nil
	default:
		return nil, errUnsupportedRecordType(rt)
	}
}
