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
	if !IsAuthRejection(err) {
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
// accepts the setup message but withholds the readiness response. recvErr is what
// Recv reports once teardown lands: either the cancellation teardown provoked, or
// a status the server sent as the deadline fired.
type blockingRPC struct {
	release  chan struct{}
	recvDone chan struct{}
	recvErr  error
}

func newBlockingRPC(recvErr error) *blockingRPC {
	return &blockingRPC{
		release:  make(chan struct{}),
		recvDone: make(chan struct{}),
		recvErr:  recvErr,
	}
}

// releaseOnce unblocks Recv. It stands in for cancelling streamCtx and, like the
// real teardown, tolerates repeated calls.
func (b *blockingRPC) releaseOnce() {
	select {
	case <-b.release:
	default:
		close(b.release)
	}
}

func (b *blockingRPC) Send(_ *string) error { return nil }

func (b *blockingRPC) Recv() (*string, error) {
	<-b.release
	close(b.recvDone)
	return nil, b.recvErr
}

func (b *blockingRPC) CloseSend() error { return nil }

// TestRawStreamHandshakeReapsGoroutineOnCancel verifies that on context cancel,
// handshake calls teardown and waits for its recv goroutine before returning.
func TestRawStreamHandshakeReapsGoroutineOnCancel(t *testing.T) {
	rpc := newBlockingRPC(context.Canceled)
	s := &rawStream[string, string]{rpc: rpc}

	hctx, cancel := context.WithCancel(context.Background())
	var teardownCalls int
	teardown := func() {
		teardownCalls++
		rpc.releaseOnce()
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

// expiredContext returns a context whose deadline has already passed, so
// handshake is guaranteed to take its hctx-expiry branch.
func expiredContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	t.Cleanup(cancel)
	return ctx
}

// TestRawStreamHandshakeTimeoutPrefersServerRejection verifies that when the
// handshake deadline fires but the reaped Recv result carries a real terminal
// server status, handshake reports that status rather than the deadline, so
// Open can still recognise it as an auth rejection.
func TestRawStreamHandshakeTimeoutPrefersServerRejection(t *testing.T) {
	rpc := newBlockingRPC(status.Error(codes.Unauthenticated, "bad credentials"))
	s := &rawStream[string, string]{rpc: rpc}

	err := s.handshake(
		expiredContext(t),
		rpc.releaseOnce,
		func(_ bidiRPC[string, string]) error { return nil },
		func(_ *string) (string, error) { return "", nil },
	)
	if err == nil {
		t.Fatal("handshake with expired deadline: got nil error, want rejection")
	}
	if !IsAuthRejection(err) {
		t.Fatalf("handshake error = %v, want auth rejection recovered from reaped recv", err)
	}
	// The point of preferring the status is that the deadline no longer shadows it:
	// the stream layer treats a context error as terminal and skips reconnecting.
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("handshake error = %v, want the rejection to replace DeadlineExceeded", err)
	}
}

// TestRawStreamHandshakeTimeoutKeepsDeadlineForTeardownArtifacts verifies the
// other half of the rule: outcomes the handshake's own teardown provokes carry no
// server intent, so the deadline stays the reported cause.
func TestRawStreamHandshakeTimeoutKeepsDeadlineForTeardownArtifacts(t *testing.T) {
	for _, tc := range []struct {
		name    string
		recvErr error
	}{
		{"grpc cancelled", status.Error(codes.Canceled, "context canceled")},
		{"grpc deadline exceeded", status.Error(codes.DeadlineExceeded, "deadline")},
		{"bare context canceled", context.Canceled},
		{"bare context deadline exceeded", context.DeadlineExceeded},
		{"clean server close", io.EOF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rpc := newBlockingRPC(tc.recvErr)
			s := &rawStream[string, string]{rpc: rpc}

			err := s.handshake(
				expiredContext(t),
				rpc.releaseOnce,
				func(_ bidiRPC[string, string]) error { return nil },
				func(_ *string) (string, error) { return "", nil },
			)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("handshake error = %v, want DeadlineExceeded for a teardown artifact", err)
			}
		})
	}
}

// TestRawStreamHandshakeTimeoutRejectionIsDeterministic verifies that a rejection
// available at the same moment as the deadline always wins. handshake selects on
// hctx and the recv result, and Go picks randomly when both are ready, so this
// asserts across enough iterations to hit either branch.
func TestRawStreamHandshakeTimeoutRejectionIsDeterministic(t *testing.T) {
	for i := 0; i < 200; i++ {
		// Recv returns immediately, so the result may land before, during, or after
		// the select on the already-expired deadline.
		rpc := &fakeBidiRPC{recvErr: status.Error(codes.Unauthenticated, "bad credentials")}
		s := &rawStream[string, string]{rpc: rpc}

		err := s.handshake(
			expiredContext(t),
			func() {},
			func(_ bidiRPC[string, string]) error { return nil },
			func(_ *string) (string, error) { return "", nil },
		)
		if !IsAuthRejection(err) {
			t.Fatalf("iteration %d: handshake error = %v, want auth rejection every time", i, err)
		}
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
