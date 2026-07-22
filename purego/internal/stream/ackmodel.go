package stream

import (
	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

// ackModel translates a raw server response into a logical "highest fully-acked
// offset" that the core's watermark can compare against, and classifies
// non-ack responses so the core stays blind to the concrete wire type. It is
// edge #2 of the design and is generic over the response type Resp: proto/JSON
// supply ackModel[ephemeralResp]; Arrow will supply its own over a Flight
// response, mapping a cumulative record count back to an offset.
//
// This interface is intentionally the minimal offset-only subset. The
// architecture note (§3a) sketches a wider Track/Resolve/Unacked shape for the
// Arrow record-count model, where an ack can land mid-batch and recovery must
// slice a straddling batch. That width is deferred until the Arrow wire path
// lands; adding it now would be unused machinery. The core stays offset-only
// and blind to the record-vs-batch distinction regardless.
type ackModel[Resp any] interface {
	// classify inspects a server response and reports what it means to the core:
	//   - kind == ackResponse: off is the highest fully-acked offset.
	//   - kind == pauseResponse: the server asked the client to pause; pause
	//     carries the requested duration. The core waits then reconnects.
	//   - kind == otherResponse: anything else (ignored by the receiver).
	// Keeping close-signal detection here means the receiver never names a
	// concrete proto message.
	classify(resp Resp) (kind respKind, off int64, pause pauseSignal)
}

// respKind is the category the ackModel assigns to a server response.
type respKind int

const (
	otherResponse respKind = iota // no ack, no pause — ignored
	ackResponse                   // carries a durability ack offset
	pauseResponse                 // server-requested pause (close-stream signal)
)

// ephemeralResp is the proto/JSON server response type. Aliased so the core's
// Resp type parameter is named once here rather than spelled out at every use.
type ephemeralResp = *zerobuspb.EphemeralStreamResponse

// offsetAckModel is the proto/JSON ack model: the server sends
// DurabilityAckUpToOffset directly as the logical offset, and a
// CloseStreamSignal requests a pause.
type offsetAckModel struct{}

func (offsetAckModel) classify(resp ephemeralResp) (respKind, int64, pauseSignal) {
	if resp == nil {
		return otherResponse, 0, pauseSignal{}
	}
	if sig := resp.GetCloseStreamSignal(); sig != nil {
		return pauseResponse, 0, pauseSignal{duration: sig.GetDuration().AsDuration()}
	}
	if ack := resp.GetIngestRecordResponse(); ack != nil {
		return ackResponse, ack.GetDurabilityAckUpToOffset(), pauseSignal{}
	}
	return otherResponse, 0, pauseSignal{}
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
