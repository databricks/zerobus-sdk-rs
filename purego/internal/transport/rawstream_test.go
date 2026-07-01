package transport

import (
	"errors"
	"io"
	"strings"
	"testing"
)

type fakeBidiRPC struct {
	sendErr  error
	recvErr  error
	closeErr error
	resp     *string
}

func (f *fakeBidiRPC) Send(_ *string) error { return f.sendErr }

func (f *fakeBidiRPC) Recv() (*string, error) {
	if f.recvErr != nil {
		return nil, f.recvErr
	}
	return f.resp, nil
}

func (f *fakeBidiRPC) CloseSend() error { return f.closeErr }

func TestRawStreamHandshakeAssignsID(t *testing.T) {
	rpc := &fakeBidiRPC{resp: strPtr("ready")}
	s := rawStream[string, string]{rpc: rpc, name: "fallback"}

	if err := s.handshake(
		func(_ bidiRPC[string, string]) error { return nil },
		func(_ *string) (string, error) { return "stream-123", nil },
	); err != nil {
		t.Fatalf("handshake failed: %v", err)
	}

	if s.name != "stream-123" {
		t.Fatalf("name = %q, want stream-123", s.name)
	}
}

func TestRawStreamHandshakeKeepsFallbackWhenIDEmpty(t *testing.T) {
	rpc := &fakeBidiRPC{resp: strPtr("ready")}
	s := rawStream[string, string]{rpc: rpc, name: "fallback"}

	if err := s.handshake(
		func(_ bidiRPC[string, string]) error { return nil },
		func(_ *string) (string, error) { return "", nil },
	); err != nil {
		t.Fatalf("handshake failed: %v", err)
	}

	if s.name != "fallback" {
		t.Fatalf("name = %q, want fallback", s.name)
	}
}

func TestRawStreamSendRecvAndCloseSendWrapErrorsWithName(t *testing.T) {
	sendBoom := errors.New("send boom")
	recvBoom := errors.New("recv boom")
	closeBoom := errors.New("close boom")

	s := rawStream[string, string]{
		rpc:  &fakeBidiRPC{sendErr: sendBoom, recvErr: recvBoom, closeErr: closeBoom},
		name: "test-stream",
	}

	if err := s.send(strPtr("x")); err == nil || !strings.Contains(err.Error(), "test-stream") {
		t.Fatalf("send err = %v, expected stream name in wrapped error", err)
	}
	if _, err := s.recv(); err == nil || !strings.Contains(err.Error(), "test-stream") {
		t.Fatalf("recv err = %v, expected stream name in wrapped error", err)
	}
	if err := s.closeSend(); err == nil || !strings.Contains(err.Error(), "test-stream") {
		t.Fatalf("closeSend err = %v, expected stream name in wrapped error", err)
	}
}

func TestRawStreamRecvReturnsEOFUnwrapped(t *testing.T) {
	s := rawStream[string, string]{rpc: &fakeBidiRPC{recvErr: io.EOF}, name: "test-stream"}
	_, err := s.recv()
	if err != io.EOF {
		t.Fatalf("recv err = %v, want io.EOF", err)
	}
}

func strPtr(s string) *string { return &s }
