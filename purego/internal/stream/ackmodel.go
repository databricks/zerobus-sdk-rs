package stream

import (
	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

// ackModel translates a raw server response into a logical "highest
// fully-acked offset" that the core's watermark can compare against, and
// classifies non-ack responses so the core stays blind to the concrete wire
// type. It is the receive-side per-encoding seam and is generic over the
// response type Resp: proto/JSON supply ackModel[ephemeralResp]; Arrow will
// supply its own over a Flight response, mapping a cumulative record count
// back to an offset.
//
// This interface is intentionally the minimal offset-only subset. A wider
// Track/Resolve/Unacked shape will be needed for the Arrow record-count
// model, where an ack can land mid-batch and recovery must slice a
// straddling batch; that width is deferred until the Arrow wire path lands.
// The core stays offset-only and blind to the record-vs-batch distinction
// regardless.
type ackModel[Resp any] interface {
	// classify inspects a server response and reports what it means to the core:
	//   - kind == ackResponse: off is the highest fully-acked offset the server
	//     claims (still requires the receiver to bound-check against sent work).
	//   - kind == pauseResponse: the server asked the client to pause; pause
	//     carries the requested duration. The core waits then reconnects.
	//   - kind == unknownResponse: the response type is not one the core
	//     recognises. The receiver fails the stream: silently ignoring
	//     unexpected messages hides server contract violations.
	//   - kind == malformedResponse: the response is an ack but its offset
	//     field is missing/absent. Advancing the watermark to the zero-valued
	//     default would silently acknowledge offset 0 for a stream that may
	//     never have sent it, so the receiver fails instead.
	// Keeping close-signal detection here means the receiver never names a
	// concrete proto message.
	classify(resp Resp) (kind respKind, off int64, pause pauseSignal)
}

// respKind is the category the ackModel assigns to a server response.
type respKind int

const (
	// ackResponse carries a validated durability offset.
	ackResponse respKind = iota
	// pauseResponse is a server-requested pause (close-stream signal).
	pauseResponse
	// unknownResponse is any response the ack model does not recognise. The
	// receiver treats these as protocol errors and fails the stream rather
	// than silently ignoring them; ignoring would hide server contract
	// violations.
	unknownResponse
	// malformedResponse is an ack whose durability-offset field is absent.
	// The zero-valued default would silently advance the watermark to 0, so
	// the receiver fails the stream instead.
	malformedResponse
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
		return unknownResponse, 0, pauseSignal{}
	}
	if sig := resp.GetCloseStreamSignal(); sig != nil {
		return pauseResponse, 0, pauseSignal{duration: sig.GetDuration().AsDuration()}
	}
	if ack := resp.GetIngestRecordResponse(); ack != nil {
		// DurabilityAckUpToOffset is an optional proto field; a missing value
		// silently defaults to 0. Advancing to a fabricated 0 would fake
		// durability for offset 0, so treat "missing" as malformed.
		off := ack.DurabilityAckUpToOffset
		if off == nil {
			return malformedResponse, 0, pauseSignal{}
		}
		return ackResponse, *off, pauseSignal{}
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
