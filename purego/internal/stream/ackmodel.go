package stream

import (
	"fmt"
	"time"

	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

// SubmittedRange describes one logical item submitted on the active connection.
// UnitStart is inclusive and UnitEnd is the exclusive end actually submitted
// on this connection. ItemUnitEnd is the exclusive end of the complete logical
// item; it differs from UnitEnd only when a multi-frame Send fails after
// submitting a prefix. The core always sets it, and a zero value normalizes to
// UnitEnd so a hand-built range cannot report a silently truncated item.
//
// The range is protocol-neutral: proto/JSON assign one unit to every atomic
// request, while a record-count protocol can assign one unit per record.
type SubmittedRange struct {
	WireOffset    int64
	LogicalOffset int64
	UnitStart     uint64
	UnitEnd       uint64
	ItemUnitEnd   uint64
}

// AckState is the connection-local state supplied to an acknowledgment model.
// Ranges contains submitted logical items that have not been fully
// acknowledged, in submission order. A model must not retain or mutate it.
type AckState struct {
	Ranges            []SubmittedRange
	AcknowledgedUnits uint64
	SubmittedUnits    uint64
}

// AckResolution translates one cumulative protocol acknowledgment into logical
// durability progress. FullyAcknowledgedOffset and PartialOffset are -1 when
// absent. PartialUnits is the acknowledged prefix of PartialOffset, measured
// from that item's UnitStart.
type AckResolution struct {
	AcknowledgedUnits       uint64
	FullyAcknowledgedOffset int64
	PartialOffset           int64
	PartialUnits            uint64
}

type invalidAcknowledgment struct {
	cause error
}

func (e *invalidAcknowledgment) Error() string {
	return "stream: invalid acknowledgment: " + e.cause.Error()
}

func (e *invalidAcknowledgment) Unwrap() error { return e.cause }

func (*invalidAcknowledgment) IsRetryable() bool { return false }

// ResolveAcknowledgedUnits maps a cumulative connection-local unit count onto
// logical offsets. It is exported within the internal package boundary so a
// record-count protocol can share the core's range validation and resolution.
func ResolveAcknowledgedUnits(ackedUnits uint64, state AckState) (AckResolution, error) {
	resolution := AckResolution{
		AcknowledgedUnits:       ackedUnits,
		FullyAcknowledgedOffset: -1,
		PartialOffset:           -1,
	}
	if ackedUnits > state.SubmittedUnits {
		return resolution, fmt.Errorf(
			"stream: server ack claims %d units, but only %d units were submitted",
			ackedUnits, state.SubmittedUnits,
		)
	}
	if state.AcknowledgedUnits > state.SubmittedUnits {
		return resolution, fmt.Errorf(
			"stream: acknowledged-unit watermark %d exceeds submitted units %d",
			state.AcknowledgedUnits, state.SubmittedUnits,
		)
	}
	if ackedUnits <= state.AcknowledgedUnits {
		return resolution, nil
	}

resolveLoop:
	for i, submitted := range state.Ranges {
		if submitted.UnitStart >= submitted.UnitEnd {
			return resolution, fmt.Errorf(
				"stream: invalid submitted range [%d,%d) for logical offset %d",
				submitted.UnitStart, submitted.UnitEnd, submitted.LogicalOffset,
			)
		}
		itemUnitEnd := submitted.ItemUnitEnd
		if itemUnitEnd == 0 {
			itemUnitEnd = submitted.UnitEnd
		}
		if itemUnitEnd < submitted.UnitEnd {
			return resolution, fmt.Errorf(
				"stream: logical item end %d precedes submitted range end %d for logical offset %d",
				itemUnitEnd, submitted.UnitEnd, submitted.LogicalOffset,
			)
		}
		if i > 0 && submitted.UnitStart != state.Ranges[i-1].UnitEnd {
			return resolution, fmt.Errorf(
				"stream: submitted unit range starts at %d after %d",
				submitted.UnitStart, state.Ranges[i-1].UnitEnd,
			)
		}
		switch {
		case ackedUnits >= submitted.UnitEnd:
			if submitted.UnitEnd < itemUnitEnd {
				resolution.PartialOffset = submitted.LogicalOffset
				resolution.PartialUnits = submitted.UnitEnd - submitted.UnitStart
				return resolution, nil
			}
			resolution.FullyAcknowledgedOffset = submitted.LogicalOffset
		case ackedUnits > submitted.UnitStart:
			resolution.PartialOffset = submitted.LogicalOffset
			resolution.PartialUnits = ackedUnits - submitted.UnitStart
			return resolution, nil
		default:
			// The ack stops at or below this range's start, so neither this
			// range nor any contiguous later one can resolve it. Whatever
			// earlier ranges resolved still stands; if nothing did, the ack
			// lands in a gap and the check below rejects it.
			break resolveLoop
		}
	}
	if resolution.FullyAcknowledgedOffset < 0 && resolution.PartialOffset < 0 {
		return resolution, fmt.Errorf(
			"stream: ack of %d units does not intersect an unacknowledged submitted range",
			ackedUnits,
		)
	}
	return resolution, nil
}

// responseClassification keeps durability progress and connection rotation
// orthogonal. A single server response may carry either signal or both.
type responseClassification struct {
	hasAck       bool
	legacyOffset int64
	pause        *pauseSignal
	failure      responseFailure
}

// ackModel classifies responses and extracts the legacy connection-local wire
// offset used by proto/JSON. A record-count model additionally implements
// resolvingAckModel to translate its response against submitted ranges.
type ackModel[Resp any] interface {
	// classify reports independent ack and pause signals, or an
	// unknown/malformed response the receiver must fail on.
	classify(resp Resp) responseClassification
}

// resolvingAckModel is the record-count acknowledgment extension point. The
// returned AcknowledgedUnits must use the same connection-local unit domain as
// AckState.Ranges.
type resolvingAckModel[Resp any] interface {
	resolve(resp Resp, state AckState) (AckResolution, error)
}

// responseFailure is the unusable category assigned to a server response.
// Its zero value means the response is structurally usable.
type responseFailure int

const (
	usableResponse responseFailure = iota
	unknownResponse
	malformedResponse
)

// ephemeralResp is the proto/JSON server response type. Aliased so the core's
// Resp type parameter is named once here rather than spelled out at every use.
type ephemeralResp = *zerobuspb.EphemeralStreamResponse

// offsetAckModel extracts proto/JSON physical offsets and pause signals.
type offsetAckModel struct{}

func (offsetAckModel) classify(resp ephemeralResp) responseClassification {
	if resp == nil {
		return responseClassification{failure: unknownResponse}
	}
	if sig := resp.GetCloseStreamSignal(); sig != nil {
		pause := pauseSignal{duration: sig.GetDuration().AsDuration()}
		return responseClassification{pause: &pause}
	}
	if ack := resp.GetIngestRecordResponse(); ack != nil {
		// Absent offset must be malformed, not a fabricated ack for offset 0.
		if ack.DurabilityAckUpToOffset == nil {
			return responseClassification{failure: malformedResponse}
		}
		off := *ack.DurabilityAckUpToOffset
		if off < 0 {
			return responseClassification{failure: malformedResponse}
		}
		return responseClassification{hasAck: true, legacyOffset: off}
	}
	return responseClassification{failure: unknownResponse}
}

func resolveOffsetAck(offset int64, state AckState) (AckResolution, error) {
	if offset < 0 {
		return AckResolution{}, fmt.Errorf("stream: negative ack offset %d", offset)
	}
	return ResolveAcknowledgedUnits(uint64(offset)+1, state)
}

// ResponseStatus is the exported/internal structural status used by
// ResponseClassification.
type ResponseStatus int

const (
	// ResponseOK means the response contains at least one usable signal.
	ResponseOK ResponseStatus = iota
	// ResponseUnknown is an unrecognized response.
	ResponseUnknown
	// ResponseMalformed is a recognized response with invalid fields.
	ResponseMalformed
)

// ResponseClassification describes independent durability and rotation signals.
// A response with both HasAck and HasPause set applies the acknowledgment first.
type ResponseClassification struct {
	Status        ResponseStatus
	HasAck        bool
	LegacyOffset  int64
	HasPause      bool
	PauseDuration time.Duration
}

// AckModelHooks adapts protocol functions into the stream's acknowledgment
// seam. Resolve is optional for atomic offset protocols and required for
// record-count protocols.
type AckModelHooks[Resp any] struct {
	Classify func(resp Resp) ResponseClassification
	Resolve  func(resp Resp, state AckState) (AckResolution, error)
}

type hookAckModel[Resp any] struct {
	hooks AckModelHooks[Resp]
}

func (m hookAckModel[Resp]) classify(resp Resp) responseClassification {
	classified := m.hooks.Classify(resp)
	switch classified.Status {
	case ResponseUnknown:
		return responseClassification{failure: unknownResponse}
	case ResponseMalformed:
		return responseClassification{failure: malformedResponse}
	case ResponseOK:
		if !classified.HasAck && !classified.HasPause {
			return responseClassification{failure: unknownResponse}
		}
		result := responseClassification{
			hasAck:       classified.HasAck,
			legacyOffset: classified.LegacyOffset,
		}
		if classified.HasPause {
			pause := pauseSignal{duration: classified.PauseDuration}
			result.pause = &pause
		}
		return result
	default:
		return responseClassification{failure: unknownResponse}
	}
}

type resolvingHookAckModel[Resp any] struct {
	hookAckModel[Resp]
}

func (m resolvingHookAckModel[Resp]) resolve(resp Resp, state AckState) (AckResolution, error) {
	return m.hooks.Resolve(resp, state)
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
