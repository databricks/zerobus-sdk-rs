package transport

import (
	"context"
	"fmt"
	"io"
	"sync"
)

// bidiRPC is the subset of a generated gRPC bidirectional streaming client that
// rawStream needs. The Zerobus EphemeralStream client satisfies it today, and
// the Arrow Flight DoPut client (which also exposes Send/Recv/CloseSend) will
// satisfy it too — which is what lets the two ingestion protocols share the
// plumbing below instead of duplicating it.
type bidiRPC[Req, Resp any] interface {
	Send(*Req) error
	Recv() (*Resp, error)
	CloseSend() error
}

// rawStream is the protocol-agnostic half of an open ingestion stream: the
// send/receive plumbing, teardown, and the handshake flow over a bidirectional
// RPC. It knows nothing about how records are framed or which setup message
// opens the stream — concrete streams embed it and supply those via wire types
// and handshake hooks. Today Stream (proto/JSON over EphemeralStream) is the
// only implementer; the Arrow path will embed the same rawStream over Flight's
// DoPut, so only its record framing and its setup/readiness hooks differ.
//
// Like the underlying gRPC stream, a rawStream is not safe for concurrent send;
// pair it with a single writer goroutine. Calling send and recv from separate
// goroutines is fine.
type rawStream[Req, Resp any] struct {
	rpc    bidiRPC[Req, Resp]
	name   string // identifies the stream in error messages
	cancel context.CancelFunc
	once   sync.Once
}

// send writes one request to the server. It is not safe for concurrent use.
func (s *rawStream[Req, Resp]) send(req *Req) error {
	if err := s.rpc.Send(req); err != nil {
		return fmt.Errorf("transport: send on stream %s: %w", s.name, err)
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
		return nil, fmt.Errorf("transport: recv on stream %s: %w", s.name, err)
	}
	return resp, nil
}

// closeSend half-closes the stream: no more requests are sent, but recv stays
// open to drain remaining responses.
func (s *rawStream[Req, Resp]) closeSend() error {
	if err := s.rpc.CloseSend(); err != nil {
		return fmt.Errorf("transport: close-send on stream %s: %w", s.name, err)
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

// handshake performs the create-stream exchange shared by every ingestion
// protocol: send a setup message, block for the server's readiness response,
// and validate it before any data flows. Blocking here surfaces setup failures
// (auth, schema/descriptor validation, table access) at open time rather than
// mid-ingest.
//
// Only the two hooks are protocol-specific: sendSetup writes the first message
// (create-stream for proto, schema for Arrow), and confirmReady validates the
// first response and returns the stream's identifier ("" for protocols that
// don't assign one, such as Arrow's ready sentinel). The send/await/validate
// flow, and recording the identifier for error messages, are shared. On success
// the stream is live for data.
//
// Errors carry a concise cause (from a hook, or the await step); the caller is
// expected to annotate them with its operation context (e.g. the table name).
func (s *rawStream[Req, Resp]) handshake(
	sendSetup func(rpc bidiRPC[Req, Resp]) error,
	confirmReady func(resp *Resp) (id string, err error),
) error {
	if err := sendSetup(s.rpc); err != nil {
		return err
	}
	resp, err := s.rpc.Recv()
	if err != nil {
		return fmt.Errorf("await ready response: %w", err)
	}
	id, err := confirmReady(resp)
	if err != nil {
		return err
	}
	s.name = id
	return nil
}
