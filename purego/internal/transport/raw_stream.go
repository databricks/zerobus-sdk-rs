package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// defaultHeadersTimeout bounds header resolution during Open when the caller's
// context has no deadline. A var so tests can shrink it via export_test.go;
// tests that override it therefore must not call t.Parallel().
var defaultHeadersTimeout = 15 * time.Second

// defaultHandshakeTimeout bounds the create-stream handshake during Open when
// the caller's context has no deadline, so Open can't hang if the server
// half-opens the stream. A var so tests can shrink it via export_test.go;
// tests that override it therefore must not call t.Parallel().
var defaultHandshakeTimeout = 15 * time.Second

// defaultDrainTimeout bounds gracefulClose's drain-to-EOF when the caller's
// context has no deadline, so it can't hang on an unresponsive server. This caps
// only the clean-close wait (letting the server send END_STREAM rather than an
// abrupt reset), not any ack wait — ack handling lands in a later layer. A var so
// tests can shrink it via export_test.go; tests that override it therefore must
// not call t.Parallel().
var defaultDrainTimeout = 500 * time.Millisecond

// bidiRPC is the subset of a generated gRPC bidirectional streaming client that
// rawStream needs. EphemeralStream satisfies it, as will Arrow Flight's DoPut.
type bidiRPC[Req, Resp any] interface {
	Send(*Req) error
	Recv() (*Resp, error)
	CloseSend() error
}

// rawStream is the protocol-agnostic half of an open ingestion stream: the
// send/receive plumbing, teardown, and handshake over a bidirectional RPC. It
// knows nothing about record framing or the setup message; concrete streams
// embed it and supply those via wire types and handshake hooks. Stream
// (proto/JSON over EphemeralStream) is the current implementation.
//
// Not safe for concurrent send; pair with a single writer goroutine. Sending
// and receiving from separate goroutines is fine.
type rawStream[Req, Resp any] struct {
	rpc bidiRPC[Req, Resp]
	// id is the error-message label, set once by the handshake and read by
	// send/recv/closeSend/ID. Atomic so the label read never races the write,
	// without coupling every accessor to a mutex. Nil means unset.
	id     atomic.Pointer[string]
	cancel context.CancelFunc
	once   sync.Once
}

// name returns the stream label for error messages: the recorded ID, or a
// neutral placeholder before one is set.
func (s *rawStream[Req, Resp]) name() string {
	if p := s.id.Load(); p != nil {
		return *p
	}
	return "stream"
}

// setID records the stream identifier. Safe to call while send/recv/ID read the
// label concurrently.
func (s *rawStream[Req, Resp]) setID(id string) {
	s.id.Store(&id)
}

// send writes one request to the server. It is not safe for concurrent use.
func (s *rawStream[Req, Resp]) send(req *Req) error {
	if err := s.rpc.Send(req); err != nil {
		return fmt.Errorf("transport: send on stream %s: %w", s.name(), err)
	}
	return nil
}

// recv blocks for the next response. It returns io.EOF unwrapped once the
// server closes the stream cleanly, so callers can compare against it directly.
func (s *rawStream[Req, Resp]) recv() (*Resp, error) {
	resp, err := s.rpc.Recv()
	switch {
	case err == io.EOF:
		return nil, io.EOF
	case err != nil:
		return nil, fmt.Errorf("transport: recv on stream %s: %w", s.name(), err)
	}
	return resp, nil
}

// closeSend half-closes the stream: no more requests are sent, but recv stays
// open to drain remaining responses.
func (s *rawStream[Req, Resp]) closeSend() error {
	if err := s.rpc.CloseSend(); err != nil {
		return fmt.Errorf("transport: close-send on stream %s: %w", s.name(), err)
	}
	return nil
}

// close aborts the stream and releases its resources. It is idempotent; any
// in-flight send or recv is unblocked with a cancellation error.
func (s *rawStream[Req, Resp]) close() {
	s.once.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
	})
}

// gracefulClose half-closes the send side, drains remaining responses to io.EOF,
// then releases resources. Draining to EOF lets the server see an orderly close;
// a bare close cancels the context and the server sees an abrupt reset instead.
//
// ctx bounds the drain: on ctx expiry or a non-EOF error it hard-aborts and
// returns the cause (ctx error preferred); a clean drain returns nil. Not safe to
// call concurrently with recv, and no send may follow (the send side is
// half-closed). Every return path calls close first, so a later close is a no-op.
func (s *rawStream[Req, Resp]) gracefulClose(ctx context.Context) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultDrainTimeout)
		defer cancel()
	}

	if err := s.closeSend(); err != nil {
		s.close()
		return err
	}

	// Bridge ctx to close so a caller deadline unblocks the recv below, which
	// otherwise waits on the stream's Close-only context.
	stop := context.AfterFunc(ctx, s.close)
	defer stop()

	for {
		_, err := s.recv()
		switch {
		case err == io.EOF:
			s.close()
			return nil
		case err != nil:
			s.close()
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return err
		}
		// Response arrived before EOF; discard and keep draining.
	}
}

// handshake runs the create-stream exchange shared by every ingestion protocol:
// send a setup message, await the readiness response, and validate it. Blocking
// here surfaces setup failures (auth, schema, table access) at open time.
//
// hctx bounds the exchange. recv runs on the stream's Close-only context, so it
// is bounded off-goroutine via a select on hctx; when hctx fires, teardown
// cancels that context and handshake waits for the goroutine, so it never
// outlives the call. teardown must cancel the RPC's context and is safe to call
// more than once. Reusers (e.g. a future Arrow/Flight wireStream) must supply it.
//
// The hooks are protocol-specific: sendSetup writes the first message;
// confirmReady validates the first response and returns the stream ID ("" if
// none).
func (s *rawStream[Req, Resp]) handshake(
	hctx context.Context,
	teardown context.CancelFunc,
	sendSetup func(rpc bidiRPC[Req, Resp]) error,
	confirmReady func(resp *Resp) (id string, err error),
) error {
	// A gRPC Send that fails with io.EOF means the stream aborted server-side; the
	// real status (e.g. an auth rejection) is on Recv, so fall through to it rather
	// than returning the opaque EOF and losing the code isAuthRejection keys on.
	if err := sendSetup(s.rpc); err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	type recvResult struct {
		resp *Resp
		err  error
	}
	done := make(chan recvResult, 1)
	go func() {
		resp, err := s.recv()
		done <- recvResult{resp, err}
	}()

	var resp *Resp
	select {
	case <-hctx.Done():
		// Unblock recv, then wait so the goroutine can't outlive this call.
		teardown()
		<-done
		return fmt.Errorf("await ready response: %w", hctx.Err())
	case r := <-done:
		if r.err != nil {
			return fmt.Errorf("await ready response: %w", r.err)
		}
		resp = r.resp
	}

	id, err := confirmReady(resp)
	if err != nil {
		return err
	}
	if id != "" {
		s.setID(id)
	}
	return nil
}
