package stream

import (
	"context"

	"github.com/databricks/zerobus-sdk/purego/internal/transport"
)

// wireStream abstracts an open bidirectional transport stream.
// The core uses one sender goroutine.
type wireStream[Req, Resp any] interface {
	// ServerID returns the identifier assigned when this connection opened.
	ServerID() string
	// Send writes one request to the server.
	Send(req Req) error
	// Recv returns io.EOF when the server ends the stream.
	Recv() (Resp, error)
	// CloseSend half-closes sending while Recv drains.
	CloseSend() error
	// Close aborts the stream and releases resources. Idempotent.
	Close()
}

// opener creates transport streams.
type opener[Req, Resp any] interface {
	Open(ctx context.Context, p StreamParams) (wireStream[Req, Resp], error)
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
		params, cfg, NewEphemeralOpener(conn), enc, ackMdl, callback,
	), nil
}
