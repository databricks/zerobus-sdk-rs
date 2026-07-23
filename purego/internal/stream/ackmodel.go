package stream

import (
	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

// ackModel translates a raw server response into a logical "highest fully-acked
// offset" that the core's watermark can compare against, and classifies
// non-ack responses so the core stays blind to the concrete wire type. It is
// generic over the response type Resp: proto/JSON supply ackModel[ephemeralResp].
//
// TODO(arrow): the Arrow wire path will supply its own ackModel over a Flight
// response, mapping a cumulative record count back to an offset.
type ackModel[Resp any] interface {
	// classify inspects a server response and reports what it means to the core:
	//   - kind == ackResponse: off is the highest fully-acked offset.
	//   - kind == pauseResponse: the server asked the client to pause; pause
	//     carries the requested duration. The core waits then reconnects.
	//   - kind == unknownResponse: an unrecognized response type. The receiver
	//     must fail the stream rather than ignore it, since silently dropping
	//     unexpected messages hides a wire-contract mismatch.
	//   - kind == malformedResponse: an ack whose offset field is absent.
	//     Treating the zero default as a real ack would fake durability for
	//     offset 0, so the receiver must fail the stream.
	// Keeping close-signal detection here means the receiver never names a
	// concrete proto message.
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
		// DurabilityAckUpToOffset is an optional field; GetDurabilityAckUpToOffset
		// would turn an absent value into 0 and fake an ack for offset 0. Preserve
		// field presence and classify a missing offset as malformed.
		if ack.DurabilityAckUpToOffset == nil {
			return malformedResponse, 0, pauseSignal{}
		}
		return ackResponse, *ack.DurabilityAckUpToOffset, pauseSignal{}
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
