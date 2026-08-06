package stream

import (
	"context"
	"fmt"

	"github.com/databricks/zerobus-sdk/purego/internal/transport"
)

// WireStream abstracts an open bidirectional transport stream. Send accepts one
// logical item; an implementation may emit multiple transport frames before it
// returns. The core uses one sender goroutine.
type WireStream[Req, Resp any] interface {
	// ServerID returns the identifier assigned when this connection opened.
	ServerID() string
	// Send writes one logical request to the server. It returns only after every
	// transport frame for that request has been submitted.
	Send(req Req) error
	// Recv returns io.EOF when the server ends the stream.
	Recv() (Resp, error)
	// CloseSend half-closes sending while Recv drains.
	CloseSend() error
	// Close aborts the stream and releases resources. Idempotent. It must also
	// unblock an in-progress Send, which teardown relies on to reap the sender.
	Close()
}

// SubmissionReceipt reports how much of one logical request was successfully
// submitted before SendWithReceipt returned. It lets a multi-frame protocol
// preserve authoritative acknowledgments for earlier frames when a later frame
// fails to send.
type SubmissionReceipt struct {
	SubmittedUnits uint64
}

// submissionReceiptStream is an optional extension implemented by transports
// that expand one logical request into multiple independently submitted frames.
// Plain WireStream implementations retain the atomic Send behavior.
type submissionReceiptStream[Req any] interface {
	SendWithReceipt(req Req) (SubmissionReceipt, error)
}

// wireStream keeps existing package-local implementations source-compatible
// while WireStream is the exported/internal protocol extension point.
type wireStream[Req, Resp any] interface {
	WireStream[Req, Resp]
}

// opener creates transport streams.
type opener[Req, Resp any] interface {
	Open(ctx context.Context, p StreamParams) (wireStream[Req, Resp], error)
}

// OpenFunc opens one protocol transport connection.
type OpenFunc[Req, Resp any] func(
	ctx context.Context,
	p StreamParams,
) (WireStream[Req, Resp], error)

type hookOpener[Req, Resp any] struct {
	open OpenFunc[Req, Resp]
}

func (o hookOpener[Req, Resp]) Open(
	ctx context.Context,
	p StreamParams,
) (wireStream[Req, Resp], error) {
	return o.open(ctx, p)
}

// StreamParams aliases the transport parameters.
type StreamParams = transport.StreamParams

// ephemeralOpener adapts a transport connection.
type ephemeralOpener struct{ conn *transport.Conn }

// NewEphemeralOpener returns an opener backed by conn.
func NewEphemeralOpener(conn *transport.Conn) *ephemeralOpener {
	return &ephemeralOpener{conn: conn}
}

func (o *ephemeralOpener) Open(ctx context.Context, p StreamParams) (wireStream[encodedMsg, ephemeralResp], error) {
	s, err := o.conn.Open(ctx, p)
	if err != nil {
		return nil, err
	}
	return s, nil
}

var _ wireStream[encodedMsg, ephemeralResp] = (*transport.Stream)(nil)

// NewProtoJSONStream builds a proto or JSON ingestion stream.
func NewProtoJSONStream(
	openingCtx context.Context,
	conn *transport.Conn,
	params StreamParams,
	cfg Config,
	callback AckCallback,
) (*CoreStream[encodedMsg, ephemeralResp], error) {
	enc, err := newEncoder(params.RecordType)
	if err != nil {
		return nil, err
	}
	ackMdl, err := newAckModel(params.RecordType)
	if err != nil {
		return nil, err
	}
	return NewCoreStream[encodedMsg, ephemeralResp](
		openingCtx, params, cfg, NewEphemeralOpener(conn), enc, ackMdl, callback,
	), nil
}

// NewCoreStreamWithHooks constructs a generic stream from exported/internal
// protocol hooks. It is intended for protocol adapters in sibling internal
// packages; user-facing SDK constructors should wrap it.
func NewCoreStreamWithHooks[Req, Resp any](
	openingCtx context.Context,
	params StreamParams,
	cfg Config,
	open OpenFunc[Req, Resp],
	enc EncoderHooks[Req],
	acks AckModelHooks[Resp],
	callback AckCallback,
) (*CoreStream[Req, Resp], error) {
	if open == nil {
		return nil, fmt.Errorf("stream: protocol Open hook is required")
	}
	if enc.EncodeRecord == nil || enc.EncodeBatch == nil ||
		enc.StampOffset == nil || enc.UnitCount == nil || enc.Slice == nil ||
		enc.Decode == nil || enc.MaxWireSize == nil || enc.RetainedSize == nil {
		return nil, fmt.Errorf("stream: all encoder hooks are required")
	}
	if acks.Classify == nil {
		return nil, fmt.Errorf("stream: acknowledgment Classify hook is required")
	}

	var ackMdl ackModel[Resp] = hookAckModel[Resp]{hooks: acks}
	if acks.Resolve != nil {
		ackMdl = resolvingHookAckModel[Resp]{hookAckModel: hookAckModel[Resp]{hooks: acks}}
	}
	return NewCoreStream[Req, Resp](
		openingCtx,
		params,
		cfg,
		hookOpener[Req, Resp]{open: open},
		hookEncoder[Req]{hooks: enc},
		ackMdl,
		callback,
	), nil
}
