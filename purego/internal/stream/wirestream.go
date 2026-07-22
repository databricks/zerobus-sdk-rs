package stream

import (
	"context"

	"github.com/databricks/zerobus-sdk/purego/internal/transport"
)

// wireStream abstracts the transport so the core drives Send/Recv/teardown
// without naming a concrete RPC. Generic over the same Req/Resp the encoder and
// ackModel use: proto/JSON instantiate wireStream[encodedMsg, ephemeralResp].
//
// A wireStream is already past its handshake when opener returns it. It is not
// safe for concurrent Send; the core uses a single sender goroutine.
//
// TODO(arrow): the Arrow path instantiates wireStream over Flight.
type wireStream[Req, Resp any] interface {
	// Send writes one request to the server.
	Send(req Req) error
	// Recv blocks for the next response, returning io.EOF (unwrapped) once the
	// server ends the stream cleanly.
	Recv() (Resp, error)
	// CloseSend half-closes the send side, leaving Recv open to drain remaining
	// responses (used for graceful teardown).
	CloseSend() error
	// Close aborts the stream and releases resources. Idempotent.
	Close()
}

// opener opens a new wireStream, injected so the supervisor can reconnect and
// tests can supply an in-process fake. Generic over the same Req/Resp as
// wireStream.
type opener[Req, Resp any] interface {
	Open(ctx context.Context, p StreamParams) (wireStream[Req, Resp], error)
}

// StreamParams mirrors transport.StreamParams but lives here so upper layers
// can use the stream package without importing transport directly.
type StreamParams = transport.StreamParams

// ephemeralOpener adapts a *transport.Conn to opener[encodedMsg, ephemeralResp]
// for the proto/JSON wire path. *transport.Stream already satisfies wireStream,
// so this is a thin wrapper that just names the concrete types.
type ephemeralOpener struct{ conn *transport.Conn }

// NewEphemeralOpener returns the proto/JSON opener backed by conn. The public
// zerobus package uses it to build a proto/JSON CoreStream via NewProtoJSONStream.
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

// Compile-time proof that the transport's proto/JSON Stream satisfies the
// generic wireStream the core drives.
var _ wireStream[encodedMsg, ephemeralResp] = (*transport.Stream)(nil)

// NewProtoJSONStream builds a proto/JSON ingestion core over the EphemeralStream
// wire path: it wires the offset ack model, the proto or JSON encoder (chosen by
// params.RecordType), and the ephemeral opener. It is the constructor the public
// zerobus package calls for proto and JSON streams.
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
