package transport

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	s := &rawStream[string, string]{rpc: rpc}
	s.setID("fallback")

	if err := s.handshake(
		context.Background(),
		func() {},
		func(_ bidiRPC[string, string]) error { return nil },
		func(_ *string) (string, error) { return "stream-123", nil },
	); err != nil {
		t.Fatalf("handshake failed: %v", err)
	}

	if s.name() != "stream-123" {
		t.Fatalf("name = %q, want stream-123", s.name())
	}
}

// TestRawStreamHandshakeSendEOFFallsThroughToRecv verifies the gRPC contract
// handling: when sendSetup fails with io.EOF (the stream aborted server-side),
// handshake ignores it and surfaces the real status via recv rather than
// returning the opaque EOF.
func TestRawStreamHandshakeSendEOFFallsThroughToRecv(t *testing.T) {
	rejected := status.Error(codes.Unauthenticated, "bad credentials")
	rpc := &fakeBidiRPC{recvErr: rejected}
	s := &rawStream[string, string]{rpc: rpc}

	err := s.handshake(
		context.Background(),
		func() {},
		func(_ bidiRPC[string, string]) error { return io.EOF },
		func(_ *string) (string, error) { return "", nil },
	)
	if err == nil {
		t.Fatal("handshake with Send io.EOF: got nil error, want the recv status")
	}
	if !isAuthRejection(err) {
		t.Fatalf("handshake error = %v, want an auth rejection recovered from recv", err)
	}
}

func TestRawStreamHandshakeKeepsFallbackWhenIDEmpty(t *testing.T) {
	rpc := &fakeBidiRPC{resp: strPtr("ready")}
	s := &rawStream[string, string]{rpc: rpc}
	s.setID("fallback")

	if err := s.handshake(
		context.Background(),
		func() {},
		func(_ bidiRPC[string, string]) error { return nil },
		func(_ *string) (string, error) { return "", nil },
	); err != nil {
		t.Fatalf("handshake failed: %v", err)
	}

	if s.name() != "fallback" {
		t.Fatalf("name = %q, want fallback", s.name())
	}
}

func TestRawStreamNameFallsBackWhenUnset(t *testing.T) {
	s := &rawStream[string, string]{rpc: &fakeBidiRPC{}}
	if got := s.name(); got != "stream" {
		t.Fatalf("name = %q, want the neutral placeholder %q", got, "stream")
	}
}

func TestRawStreamSendRecvAndCloseSendWrapErrorsWithName(t *testing.T) {
	sendBoom := errors.New("send boom")
	recvBoom := errors.New("recv boom")
	closeBoom := errors.New("close boom")

	s := &rawStream[string, string]{
		rpc: &fakeBidiRPC{sendErr: sendBoom, recvErr: recvBoom, closeErr: closeBoom},
	}
	s.setID("test-stream")

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

// blockingRPC blocks in Recv until teardown unblocks it, modeling a server that
// accepts the setup message but withholds the readiness response.
type blockingRPC struct {
	release  chan struct{}
	recvDone chan struct{}
}

func (b *blockingRPC) Send(_ *string) error { return nil }

func (b *blockingRPC) Recv() (*string, error) {
	<-b.release
	close(b.recvDone)
	return nil, context.Canceled
}

func (b *blockingRPC) CloseSend() error { return nil }

// TestRawStreamHandshakeReapsGoroutineOnCancel verifies that on context cancel,
// handshake calls teardown and waits for its recv goroutine before returning.
func TestRawStreamHandshakeReapsGoroutineOnCancel(t *testing.T) {
	rpc := &blockingRPC{release: make(chan struct{}), recvDone: make(chan struct{})}
	s := &rawStream[string, string]{rpc: rpc}

	hctx, cancel := context.WithCancel(context.Background())
	var teardownCalls int
	teardown := func() { // stands in for cancelling streamCtx; unblocks Recv
		teardownCalls++
		select {
		case <-rpc.release:
		default:
			close(rpc.release)
		}
	}

	cancel()
	err := s.handshake(hctx, teardown, func(_ bidiRPC[string, string]) error { return nil },
		func(_ *string) (string, error) { return "", nil })

	if err == nil {
		t.Fatal("handshake with cancelled context: got nil error, want cancellation")
	}
	if teardownCalls == 0 {
		t.Fatal("handshake did not call teardown on cancellation")
	}
	select {
	case <-rpc.recvDone:
	default:
		t.Fatal("handshake returned before its recv goroutine exited")
	}
}

// rejectingAfterTeardownRPC blocks Recv until teardown then returns the server's
// rejection status. This models the race where the handshake deadline fires as
// the server response becomes available.
type rejectingAfterTeardownRPC struct {
	release chan struct{}
	err     error
}

func (r *rejectingAfterTeardownRPC) Send(_ *string) error { return nil }

func (r *rejectingAfterTeardownRPC) Recv() (*string, error) {
	<-r.release
	return nil, r.err
}

func (r *rejectingAfterTeardownRPC) CloseSend() error { return nil }

// TestRawStreamHandshakeTimeoutPrefersServerRejection verifies that when the
// handshake deadline fires but the reaped Recv result carries a real terminal
// server status, handshake returns that status instead of DeadlineExceeded.
func TestRawStreamHandshakeTimeoutPrefersServerRejection(t *testing.T) {
	rejected := status.Error(codes.Unauthenticated, "bad credentials")
	rpc := &rejectingAfterTeardownRPC{
		release: make(chan struct{}),
		err:     rejected,
	}
	s := &rawStream[string, string]{rpc: rpc}

	hctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	teardown := func() {
		select {
		case <-rpc.release:
		default:
			close(rpc.release)
		}
	}

	err := s.handshake(
		hctx,
		teardown,
		func(_ bidiRPC[string, string]) error { return nil },
		func(_ *string) (string, error) { return "", nil },
	)
	if err == nil {
		t.Fatal("handshake with expired deadline: got nil error, want rejection")
	}
	if !isAuthRejection(err) {
		t.Fatalf("handshake error = %v, want auth rejection recovered from reaped recv", err)
	}
}

func TestRawStreamRecvReturnsEOFUnwrapped(t *testing.T) {
	s := &rawStream[string, string]{rpc: &fakeBidiRPC{recvErr: io.EOF}}
	s.setID("test-stream")
	_, err := s.recv()
	if err != io.EOF {
		t.Fatalf("recv err = %v, want io.EOF", err)
	}
}

// TestGracefulCloseCloseSendFailure: when CloseSend itself fails (e.g. the
// stream is already broken), gracefulClose must hard-abort and return the error
// rather than proceeding to drain.
func TestGracefulCloseCloseSendFailure(t *testing.T) {
	closeSendBoom := errors.New("close-send boom")
	var cancelled bool
	s := &rawStream[string, string]{
		rpc:    &fakeBidiRPC{closeErr: closeSendBoom},
		cancel: func() { cancelled = true },
	}
	s.setID("test-stream")

	err := s.gracefulClose(context.Background())
	if err == nil {
		t.Fatal("gracefulClose with failing CloseSend: got nil error, want failure")
	}
	if !strings.Contains(err.Error(), "test-stream") {
		t.Errorf("error %q should contain stream name", err.Error())
	}
	if !cancelled {
		t.Error("gracefulClose did not call close (cancel) after CloseSend failure")
	}
}

func strPtr(s string) *string { return &s }
