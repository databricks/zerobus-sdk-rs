package transport

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// defaultHandshakeTimeout bounds the readiness wait when the caller's context
// has no deadline, so Open can't hang if the server half-opens the stream.
const defaultHandshakeTimeout = 30 * time.Second

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

// handshake runs the create-stream exchange shared by every ingestion protocol:
// send a setup message, await the readiness response, and validate it. Blocking
// here surfaces setup failures (auth, schema, table access) at open time.
//
// hctx (the caller's Open context) bounds only the readiness wait, defaulting to
// defaultHandshakeTimeout when it has no deadline; the live stream runs on a
// separate Close-only context, so this never tears down an established stream.
//
// The two hooks are protocol-specific: sendSetup writes the first message, and
// confirmReady validates the first response and returns the stream ID ("" if the
// protocol assigns none). Errors carry a concise cause for the caller to
// annotate.
func (s *rawStream[Req, Resp]) handshake(
	hctx context.Context,
	sendSetup func(rpc bidiRPC[Req, Resp]) error,
	confirmReady func(resp *Resp) (id string, err error),
) error {
	if err := sendSetup(s.rpc); err != nil {
		return err
	}

	if _, ok := hctx.Deadline(); !ok {
		var cancel context.CancelFunc
		hctx, cancel = context.WithTimeout(hctx, defaultHandshakeTimeout)
		defer cancel()
	}

	// Recv runs on the Close-only stream context, not hctx, so bound it by
	// receiving off-goroutine and selecting on hctx. On timeout/cancel Open tears
	// the stream down, which unblocks the goroutine.
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
