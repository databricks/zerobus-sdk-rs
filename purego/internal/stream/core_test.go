package stream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ---- tests -----------------------------------------------------------------
func TestCoreStreamIngestAndFlush(t *testing.T) {
	rpc := newFakeRPC()
	cs := newTestStream(t, newFakeOpener(rpc))

	offset, err := cs.Ingest(context.Background(), []byte(`{"k":"v"}`))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if offset != 0 {
		t.Fatalf("want offset 0, got %d", offset)
	}

	// Drain the send channel and ack.
	waitCondition(t, func() bool { return len(rpc.sends) > 0 }, time.Second)
	<-rpc.sends
	rpc.ack(0)

	if err := cs.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestCoreStreamOffsetMonotonic(t *testing.T) {
	rpc := newFakeRPC()
	cs := newTestStream(t, newFakeOpener(rpc))

	for i := int64(0); i < 5; i++ {
		off, err := cs.Ingest(context.Background(), []byte(`{}`))
		if err != nil {
			t.Fatalf("Ingest %d: %v", i, err)
		}
		if off != i {
			t.Fatalf("want offset %d, got %d", i, off)
		}
	}
}

func TestCoreStreamWaitForOffset(t *testing.T) {
	rpc := newFakeRPC()
	cs := newTestStream(t, newFakeOpener(rpc))

	for i := range 3 {
		if _, err := cs.Ingest(context.Background(), []byte(`{}`)); err != nil {
			t.Fatalf("Ingest %d: %v", i, err)
		}
	}

	// Drain all three sends then ack offset 2 (covers 0, 1, 2).
	waitCondition(t, func() bool { return len(rpc.sends) == 3 }, time.Second)
	for range 3 {
		<-rpc.sends
	}
	rpc.ack(2)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := cs.WaitForOffset(ctx, 2); err != nil {
		t.Fatalf("WaitForOffset: %v", err)
	}
}

func TestCoreStreamFlushNothingIngested(t *testing.T) {
	rpc := newFakeRPC()
	cs := newTestStream(t, newFakeOpener(rpc))
	if err := cs.Flush(context.Background()); err != nil {
		t.Fatalf("Flush with no ingests: %v", err)
	}
}

func TestCoreStreamFlushContextExpires(t *testing.T) {
	rpc := newFakeRPC()
	// Intentionally never ack.
	_ = rpc
	cs := newTestStream(t, newFakeOpener(rpc))

	if _, err := cs.Ingest(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := cs.Flush(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
}

func TestCoreStreamCloseIsIdempotent(t *testing.T) {
	rpc := newFakeRPC()
	cs := newTestStream(t, newFakeOpener(rpc))
	cs.Close()
	cs.Close() // must not panic or block
}

// TestCoreStreamCloseDrainsGracefully verifies that a clean Close half-closes
// the send side (CloseSend) and keeps the receiver draining acks to io.EOF,
// rather than abruptly aborting the stream. An ack delivered after the
// half-close must still advance the watermark; an abrupt teardown would drop it.
func TestCoreStreamCloseDrainsGracefully(t *testing.T) {
	rpc := newGracefulFakeRPC()
	cb := &recordingCallback{}
	cs := newCoreForTest(testParams(), testConfig(), &gracefulOpener{rpc: rpc}, cb)

	off, err := cs.Ingest(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc.sends) > 0 }, time.Second)
	<-rpc.sends

	// Close in the background; it blocks until the graceful drain completes.
	done := make(chan struct{})
	go func() { cs.Close(); close(done) }()

	// Close must half-close the send side rather than hard-abort.
	select {
	case <-rpc.closeSent:
	case <-time.After(time.Second):
		t.Fatal("Close did not half-close the send side (CloseSend) — it hard-aborted")
	}

	// Deliver a late ack after the half-close, then end the stream. A graceful
	// drain observes this ack; an abrupt teardown would have dropped it.
	rpc.ack(off)
	rpc.serverEnd()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after the server ended the stream")
	}

	if got := cb.ackCount(); got != 1 {
		t.Fatalf("want the post-half-close ack drained (ackCount=1), got %d", got)
	}
}

func TestCoreStreamCloseAbortsBlockedGracefulRecv(t *testing.T) {
	rpc := newGracefulFakeRPC()
	cfg := testConfig()
	cfg.DrainTimeout = 25 * time.Millisecond
	cs := newCoreForTest(testParams(), cfg, &gracefulOpener{rpc: rpc}, nil)

	if _, err := cs.Ingest(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc.sends) == 1 }, time.Second)
	<-rpc.sends

	done := make(chan struct{})
	go func() {
		cs.Close()
		close(done)
	}()

	select {
	case <-rpc.closeSent:
	case <-time.After(time.Second):
		t.Fatal("Close did not half-close the send side")
	}
	select {
	case <-rpc.aborted:
	case <-time.After(time.Second):
		t.Fatal("Close did not abort Recv after DrainTimeout")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close remained blocked after aborting Recv")
	}
}

func TestCoreStreamIsClosed(t *testing.T) {
	rpc := newFakeRPC()
	cs := newTestStream(t, newFakeOpener(rpc))
	if cs.IsClosed() {
		t.Fatal("want not closed before Close()")
	}
	cs.Close()
	if !cs.IsClosed() {
		t.Fatal("want closed after Close()")
	}
}

func TestCoreStreamGetUnackedReturnsItems(t *testing.T) {
	rpc := newFakeRPC()
	cs := newCoreForTest(testParams(), testConfig(), newFakeOpener(rpc), nil)
	record := []byte(`{"a":1}`)
	if _, err := cs.Ingest(context.Background(), record); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	unacked := cs.GetUnacked()
	if len(unacked) != 1 || !bytes.Equal(unacked[0], record) {
		t.Fatalf("want exact unacked record %q, got %q", record, unacked)
	}
	if !cs.IsClosed() {
		t.Fatal("GetUnacked should close the stream")
	}
}

// GetUnacked must be a repeatable, non-destructive read: a diagnostic call must
// not empty the retained set, and mutating the result must not corrupt it.
func TestCoreStreamGetUnackedRepeatable(t *testing.T) {
	rpc := newFakeRPC()
	cs := newCoreForTest(testParams(), testConfig(), newFakeOpener(rpc), nil)
	record := []byte(`{"a":1}`)
	if _, err := cs.Ingest(context.Background(), record); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	first := cs.GetUnacked()
	if len(first) != 1 || !bytes.Equal(first[0], record) {
		t.Fatalf("first GetUnacked: want %q, got %q", record, first)
	}
	// Mutate the first result; it must not be an alias of the retained payload.
	for i := range first[0] {
		first[0][i] = 'X'
	}

	second := cs.GetUnacked()
	if len(second) != 1 || !bytes.Equal(second[0], record) {
		t.Fatalf("second GetUnacked: want original %q, got %q", record, second)
	}
}

func TestCoreStreamConcurrentIngest(t *testing.T) {
	rpc := newFakeRPC()
	cs := newTestStream(t, newFakeOpener(rpc))

	const n = 50
	var wg sync.WaitGroup
	offsets := make([]int64, n)
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			record := fmt.Appendf(nil, `{"record":%d}`, i)
			off, err := cs.Ingest(context.Background(), record)
			if err != nil {
				t.Errorf("goroutine %d Ingest: %v", i, err)
				return
			}
			offsets[i] = off
		}(i)
	}
	wg.Wait()

	// Verify all offsets are unique.
	seen := make(map[int64]bool)
	for _, off := range offsets {
		if seen[off] {
			t.Fatalf("duplicate offset %d", off)
		}
		seen[off] = true
	}

	waitCondition(t, func() bool { return len(rpc.sends) == n }, time.Second)
	sentRecords := make(map[string]bool, n)
	for range n {
		msg := <-rpc.sends
		sentRecords[msg.GetIngestRecord().GetJsonRecord()] = true
	}
	for i := range n {
		record := fmt.Sprintf(`{"record":%d}`, i)
		if !sentRecords[record] {
			t.Fatalf("record %q was not sent", record)
		}
	}
	rpc.ack(n - 1)
	if err := cs.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestCoreStreamEncodesConcurrentIngestsOutsideOffsetLock(t *testing.T) {
	rpc := newFakeRPC()
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseAll()

	enc := &concurrentEncoder{entered: entered, release: release}
	cs := NewCoreStream[encodedMsg, ephemeralResp](
		testParams(),
		testConfig(),
		newFakeOpener(rpc),
		enc,
		offsetAckModel{},
		nil,
	)
	t.Cleanup(func() { cs.Close() })

	errs := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := cs.Ingest(context.Background(), []byte(`{}`))
			errs <- err
		}()
	}
	for i := range 2 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatalf("only %d encode call(s) entered concurrently", i)
		}
	}
	releaseAll()
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}
}

func TestCoreStreamRecoveryRequeuesUnacked(t *testing.T) {
	rpc1 := newFakeRPC()
	rpc2 := newFakeRPC()
	fo := newFakeOpener(rpc1, rpc2)

	cfg := testConfig()
	cfg.RecoveryRetries = 1
	cs := newCoreForTest(testParams(), cfg, fo, nil)
	t.Cleanup(func() { cs.Close() })

	first, err := cs.Ingest(context.Background(), []byte(`{"first":true}`))
	if err != nil {
		t.Fatalf("first Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc1.sends) == 1 }, time.Second)
	firstSend := <-rpc1.sends
	if got := firstSend.GetIngestRecord().GetOffsetId(); got != 0 {
		t.Fatalf("first physical offset: want 0, got %d", got)
	}
	rpc1.ack(0)
	if err := cs.WaitForOffset(context.Background(), first); err != nil {
		t.Fatalf("first WaitForOffset: %v", err)
	}

	replayed, err := cs.Ingest(context.Background(), []byte(`{"replayed":true}`))
	if err != nil {
		t.Fatalf("second Ingest: %v", err)
	}
	last, err := cs.Ingest(context.Background(), []byte(`{"last":true}`))
	if err != nil {
		t.Fatalf("third Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc1.sends) == 2 }, time.Second)
	secondSend := <-rpc1.sends
	if got := secondSend.GetIngestRecord().GetOffsetId(); got != 1 {
		t.Fatalf("second physical offset: want 1, got %d", got)
	}
	thirdSend := <-rpc1.sends
	if got := thirdSend.GetIngestRecord().GetOffsetId(); got != 2 {
		t.Fatalf("third physical offset: want 2, got %d", got)
	}
	rpc1.close()

	waitCondition(t, func() bool { return len(rpc2.sends) == 2 }, 2*time.Second)
	replayedSend := <-rpc2.sends
	if got := replayedSend.GetIngestRecord().GetOffsetId(); got != 0 {
		t.Fatalf("replayed physical offset: want 0, got %d", got)
	}
	lastSend := <-rpc2.sends
	if got := lastSend.GetIngestRecord().GetOffsetId(); got != 1 {
		t.Fatalf("last physical offset: want 1, got %d", got)
	}
	rpc2.ack(1)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cs.WaitForOffset(ctx, replayed); err != nil {
		t.Fatalf("replayed WaitForOffset: %v", err)
	}
	if err := cs.WaitForOffset(ctx, last); err != nil {
		t.Fatalf("last WaitForOffset: %v", err)
	}
}

func TestCoreStreamImmediateEOFExhaustsRecovery(t *testing.T) {
	rpcs := []*fakeRPC{newFakeRPC(), newFakeRPC(), newFakeRPC()}
	for _, rpc := range rpcs {
		rpc.close()
	}
	fo := newFakeOpener(rpcs...)
	cfg := testConfig()
	cfg.RecoveryRetries = 2
	cfg.RecoveryBackoff = time.Millisecond
	cs := newCoreForTest(testParams(), cfg, fo, nil)
	t.Cleanup(func() { cs.Close() })

	waitCondition(t, cs.IsClosed, time.Second)
	if got := fo.openCount(); got != 3 {
		t.Fatalf("want 3 Open attempts, got %d", got)
	}
}

func TestCoreStreamStableIdleConnectionResetsRecovery(t *testing.T) {
	rpc1 := newFakeRPC()
	rpc1.close()
	rpc2 := newFakeRPC()
	rpc3 := newFakeRPC()
	fo := newFakeOpener(rpc1, rpc2, rpc3)
	cfg := testConfig()
	cfg.RecoveryRetries = 1
	cfg.RecoveryBackoff = time.Millisecond
	cfg.RecoveryResetAfter = 25 * time.Millisecond
	cs := newCoreForTest(testParams(), cfg, fo, nil)
	t.Cleanup(func() { cs.Close() })

	waitCondition(t, func() bool { return fo.openCount() == 2 }, time.Second)
	time.Sleep(50 * time.Millisecond)
	rpc2.close()
	waitCondition(t, func() bool { return fo.openCount() == 3 }, time.Second)
}

func TestCoreStreamDefersAckUntilSendSucceeds(t *testing.T) {
	rpc := newControlledSendRPC()
	cs := newCoreForTest(testParams(), testConfig(), &controlledSendOpener{rpc}, nil)
	t.Cleanup(func() { cs.Close() })

	offset, err := cs.Ingest(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	<-rpc.started
	rpc.ack(offset)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := cs.WaitForOffset(ctx, offset); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ack completed before Send: %v", err)
	}

	rpc.result <- nil
	<-rpc.sends
	rpc.close()
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := cs.WaitForOffset(ctx, offset); err != nil {
		t.Fatalf("deferred ack after Send and EOF: %v", err)
	}
}

func TestCoreStreamDefersTranslatedAckAfterReconnect(t *testing.T) {
	firstRPC := newFakeRPC()
	secondRPC := newControlledSendRPC()
	opener := &reconnectControlledOpener{first: firstRPC, second: secondRPC}
	cfg := testConfig()
	cfg.RecoveryRetries = 1
	cs := newCoreForTest(testParams(), cfg, opener, nil)
	t.Cleanup(func() { cs.Close() })

	first, err := cs.Ingest(context.Background(), []byte(`{"first":true}`))
	if err != nil {
		t.Fatalf("first Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(firstRPC.sends) == 1 }, time.Second)
	<-firstRPC.sends
	firstRPC.ack(0)
	if err := cs.WaitForOffset(context.Background(), first); err != nil {
		t.Fatalf("first WaitForOffset: %v", err)
	}

	replayed, err := cs.Ingest(context.Background(), []byte(`{"replayed":true}`))
	if err != nil {
		t.Fatalf("second Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(firstRPC.sends) == 1 }, time.Second)
	<-firstRPC.sends
	firstRPC.close()
	<-secondRPC.started
	secondRPC.ack(0)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := cs.WaitForOffset(ctx, replayed); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("translated ack completed before Send: %v", err)
	}

	secondRPC.result <- nil
	sent := <-secondRPC.sends
	if got := sent.GetIngestRecord().GetOffsetId(); got != 0 {
		t.Fatalf("replayed physical offset: want 0, got %d", got)
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := cs.WaitForOffset(ctx, replayed); err != nil {
		t.Fatalf("translated deferred ack: %v", err)
	}
}

func TestCoreStreamRejectsAckWhenSendFails(t *testing.T) {
	rpc := newControlledSendRPC()
	cfg := testConfig()
	cfg.Recovery = RecoveryDisabled
	cs := newCoreForTest(testParams(), cfg, &controlledSendOpener{rpc}, nil)
	t.Cleanup(func() { cs.Close() })

	offset, err := cs.Ingest(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	<-rpc.started
	rpc.ack(offset)
	rpc.result <- errors.New("send failed")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := cs.WaitForOffset(ctx, offset); err == nil {
		t.Fatal("ack from failed Send established durability")
	}
}

// TestCoreStreamMalformedAckTearsDownAndRecovers verifies that an
// uninterpretable server response (an ack missing its offset) tears the stream
// down instead of being silently ignored, and the supervisor reconnects.
func TestCoreStreamMalformedAckTearsDownAndRecovers(t *testing.T) {
	rpc1 := newFakeRPC()
	rpc2 := newFakeRPC()
	fo := newFakeOpener(rpc1, rpc2)

	cfg := testConfig()
	cfg.RecoveryRetries = 1
	cs := newCoreForTest(testParams(), cfg, fo, nil)
	t.Cleanup(func() { cs.Close() })

	off, err := cs.Ingest(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	// Wait for the send to land on rpc1, then feed a malformed ack. The receiver
	// must fail the stream, so the supervisor recovers to rpc2 and re-sends.
	waitCondition(t, func() bool { return len(rpc1.sends) > 0 }, time.Second)
	rpc1.malformedAck()

	waitCondition(t, func() bool { return len(rpc2.sends) > 0 }, 2*time.Second)
	<-rpc2.sends
	rpc2.ack(off)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cs.WaitForOffset(ctx, off); err != nil {
		t.Fatalf("WaitForOffset after recovery: %v", err)
	}
}

// TestCoreStreamIngestOnClosedStreamErrors verifies that Ingest on a cleanly
// closed stream returns an error, not (0, nil).
func TestCoreStreamIngestOnClosedStreamErrors(t *testing.T) {
	rpc := newFakeRPC()
	cs := newTestStream(t, newFakeOpener(rpc))
	cs.Close()

	_, err := cs.Ingest(context.Background(), []byte(`{}`))
	if err == nil {
		t.Fatal("want error from Ingest on closed stream, got nil")
	}
}

// TestCoreStreamFailedIngestDoesNotAdvanceFlushTarget verifies that a
// ctx-cancelled Ingest does not leave a gap that blocks Flush forever.
func TestCoreStreamFailedIngestDoesNotAdvanceFlushTarget(t *testing.T) {
	rpc := newFakeRPC()
	cs := newTestStream(t, newFakeOpener(rpc))

	// First Ingest succeeds.
	off, err := cs.Ingest(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatalf("first Ingest: %v", err)
	}

	// Second Ingest fails (cancelled context) — must not consume an offset.
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = cs.Ingest(cancelledCtx, []byte(`{}`))
	if err == nil {
		t.Fatal("want error from cancelled Ingest, got nil")
	}

	// Ack the first offset and flush — must not block waiting for offset 1.
	waitCondition(t, func() bool { return len(rpc.sends) > 0 }, time.Second)
	<-rpc.sends
	rpc.ack(off)

	ctx, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if err := cs.Flush(ctx); err != nil {
		t.Fatalf("Flush blocked on gap from failed Ingest: %v", err)
	}
}

// TestCoreStreamCloseUnblocksEnqueueAtCapacity verifies that a caller blocked
// in Ingest (buffer at capacity) unblocks when the stream is closed, rather
// than hanging forever on the semaphore.
func TestCoreStreamCloseUnblocksEnqueueAtCapacity(t *testing.T) {
	rpc := newFakeRPC()
	cfg := testConfig()
	cfg.MaxInflight = 1
	cs := newCoreForTest(testParams(), cfg, newFakeOpener(rpc), nil)
	t.Cleanup(func() { cs.Close() })

	// Fill the single slot.
	if _, err := cs.Ingest(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("first Ingest: %v", err)
	}

	// A second Ingest should block; close the stream from another goroutine.
	errCh := make(chan error, 1)
	go func() {
		_, err := cs.Ingest(context.Background(), []byte(`{}`))
		errCh <- err
	}()

	time.Sleep(20 * time.Millisecond)
	cs.Close()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("want error from blocked Ingest after Close, got nil")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Ingest did not unblock after Close")
	}
}

func TestCoreStreamGetUnackedWithoutCallback(t *testing.T) {
	rpc := newFakeRPC()
	cfg := testConfig()
	cfg.Recovery = RecoveryDisabled
	cs := newCoreForTest(testParams(), cfg, newFakeOpener(rpc), nil)

	records := [][]byte{[]byte(`{"first":1}`), []byte(`{"second":2}`)}
	for _, record := range records {
		if _, err := cs.Ingest(context.Background(), record); err != nil {
			t.Fatalf("Ingest(%q): %v", record, err)
		}
	}
	waitCondition(t, func() bool { return len(rpc.sends) == len(records) }, time.Second)
	rpc.malformedAck()

	waitCondition(t, cs.IsClosed, 2*time.Second)
	unacked := cs.GetUnacked()
	if len(unacked) != len(records) {
		t.Fatalf("GetUnacked returned %d records, want %d", len(unacked), len(records))
	}
	for i := range records {
		if !bytes.Equal(unacked[i], records[i]) {
			t.Fatalf("GetUnacked[%d] = %q, want %q", i, unacked[i], records[i])
		}
	}
}

func TestIsRetryable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "validation", err: wrapValidation(errors.New("invalid")), want: false},
		{name: "closed", err: errClosed, want: false},
		{name: "classified retryable", err: fmt.Errorf("wrapped: %w", &classifiedError{retryable: true}), want: true},
		{name: "classified terminal", err: &classifiedError{retryable: false}, want: false},
		{name: "open budget", err: &openBudgetExceeded{cause: context.DeadlineExceeded}, want: true},
		{name: "context deadline", err: context.DeadlineExceeded, want: false},
		{name: "ordinary transport", err: errors.New("connection reset"), want: true},
		{name: "status invalid argument", err: status.Error(codes.InvalidArgument, "bad"), want: false},
		{name: "status unauthenticated", err: status.Error(codes.Unauthenticated, "auth"), want: false},
		{name: "status permission denied", err: status.Error(codes.PermissionDenied, "denied"), want: false},
		{name: "status out of range", err: status.Error(codes.OutOfRange, "range"), want: false},
		{name: "status unimplemented", err: status.Error(codes.Unimplemented, "nope"), want: false},
		{name: "status not found", err: status.Error(codes.NotFound, "gone"), want: false},
		{name: "status wrapped invalid argument", err: fmt.Errorf("recv: %w", status.Error(codes.InvalidArgument, "bad")), want: false},
		{name: "status unavailable", err: status.Error(codes.Unavailable, "try later"), want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isRetryable(tc.err); got != tc.want {
				t.Fatalf("isRetryable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestCoreStreamRetriesOpenTimeout(t *testing.T) {
	opener := &timeoutOpener{}
	cfg := testConfig()
	cfg.RecoveryRetries = 2
	cfg.RecoveryTimeout = 10 * time.Millisecond
	cfg.RecoveryBackoff = time.Millisecond
	cs := newCoreForTest(testParams(), cfg, opener, nil)

	waitCondition(t, cs.IsClosed, time.Second)
	if got := opener.attempts.Load(); got != 3 {
		t.Fatalf("Open attempts = %d, want 3", got)
	}
	if err := cs.terminalErr(); err == nil || !strings.Contains(err.Error(), "open budget exceeded") {
		t.Fatalf("terminal error = %v, want open budget exhaustion", err)
	}
}

func TestCoreStreamOpenAuthRejectionInvalidatesOnce(t *testing.T) {
	provider := &countingHeadersProvider{}
	params := testParams()
	params.HeadersProvider = provider
	cfg := testConfig()
	cfg.Recovery = RecoveryDisabled
	cs := newCoreForTest(params, cfg, &authRejectingOpener{provider: provider}, nil)

	waitCondition(t, cs.IsClosed, time.Second)
	if got := provider.invalidations.Load(); got != 1 {
		t.Fatalf("Invalidate calls = %d, want 1", got)
	}
}

func TestCoreStreamLiveAuthRejectionInvalidatesOnce(t *testing.T) {
	provider := &countingHeadersProvider{}
	params := testParams()
	params.HeadersProvider = provider
	rpc := newTerminalRecvRPC()
	cfg := testConfig()
	cfg.Recovery = RecoveryDisabled
	cs := newCoreForTest(params, cfg, &terminalRecvOpener{rpc: rpc}, nil)

	waitCondition(t, func() bool { return !cs.IsClosed() }, time.Second)
	rpc.recvErr <- status.Error(codes.Unauthenticated, "expired credentials")
	waitCondition(t, cs.IsClosed, time.Second)
	if got := provider.invalidations.Load(); got != 1 {
		t.Fatalf("Invalidate calls = %d, want 1", got)
	}
}

func TestCoreStreamIngestBatch(t *testing.T) {
	rpc := newFakeRPC()
	cs := newTestStream(t, newFakeOpener(rpc))

	off, err := cs.IngestBatch(context.Background(), [][]byte{[]byte(`{"a":1}`), []byte(`{"b":2}`)})
	if err != nil {
		t.Fatalf("IngestBatch: %v", err)
	}
	if off != 0 {
		t.Fatalf("want offset 0 for the batch, got %d", off)
	}

	waitCondition(t, func() bool { return len(rpc.sends) > 0 }, time.Second)
	sent := <-rpc.sends
	ib := sent.GetIngestRecordBatch()
	if ib == nil {
		t.Fatal("want an IngestRecordBatch on the wire, got a single-record message")
	}
	if got := len(ib.GetJsonBatch().GetRecords()); got != 2 {
		t.Fatalf("want 2 records in the batch, got %d", got)
	}
	rpc.ack(off)

	if err := cs.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestWatermarkAlreadyCancelledContextReturnsImmediately(t *testing.T) {
	w := newWatermark()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() { done <- w.waitFor(ctx, 0) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("waitFor missed context cancellation")
	}
}

func TestCoreStreamCloseUnblocksBlockedSend(t *testing.T) {
	rpc := newBlockedSendRPC()
	cfg := testConfig()
	cfg.DrainTimeout = 25 * time.Millisecond
	cs := newCoreForTest(testParams(), cfg, &blockedSendOpener{rpc: rpc}, nil)

	if _, err := cs.Ingest(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	select {
	case <-rpc.sendStarted:
	case <-time.After(time.Second):
		t.Fatal("Send did not start")
	}

	done := make(chan struct{})
	go func() {
		cs.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Close hung behind blocked Send")
	}
}

func TestCoreStreamCloseWakesOffsetWaiters(t *testing.T) {
	rpc := newFakeRPC()
	cs := newCoreForTest(testParams(), testConfig(), newFakeOpener(rpc), nil)
	offset, err := cs.Ingest(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- cs.WaitForOffset(context.Background(), offset)
	}()
	cs.Close()

	select {
	case err := <-waitDone:
		if !errors.Is(err, errWatermarkClosed) {
			t.Fatalf("want errWatermarkClosed, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("WaitForOffset remained blocked after Close")
	}
}

func TestCoreStreamAckCallbackCanClose(t *testing.T) {
	rpc := newFakeRPC()
	callbackDone := make(chan struct{})
	var cs *testStream
	cb := &callbackFuncs{
		onAck: func(int64) {
			cs.Close()
			close(callbackDone)
		},
	}
	cs = newCoreForTest(testParams(), testConfig(), newFakeOpener(rpc), cb)

	offset, err := cs.Ingest(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc.sends) == 1 }, time.Second)
	<-rpc.sends
	rpc.ack(offset)

	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("OnAck calling Close deadlocked")
	}
}

func TestCoreStreamCloseReportsAbandonedOffsets(t *testing.T) {
	rpc := newFakeRPC()
	cb := &recordingCallback{}
	cs := newCoreForTest(testParams(), testConfig(), newFakeOpener(rpc), cb)

	record := []byte(`{"retained":true}`)
	offset, err := cs.Ingest(context.Background(), record)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc.sends) == 1 }, time.Second)
	cs.Close()
	waitCondition(t, func() bool { return len(cb.errorOffsets()) == 1 }, time.Second)
	if got := cb.errorOffsets()[0]; got != offset {
		t.Fatalf("want OnError offset %d, got %d", offset, got)
	}
	unacked := cs.GetUnacked()
	if len(unacked) != 1 || !bytes.Equal(unacked[0], record) {
		t.Fatalf("callback drain lost GetUnacked payload: %q", unacked)
	}
}

func TestCoreStreamErrorCallbackCanClose(t *testing.T) {
	rpc := newFakeRPC()
	callbackDone := make(chan struct{})
	var cs *testStream
	cb := &callbackFuncs{
		onError: func(int64, error) {
			cs.Close()
			close(callbackDone)
		},
	}
	cs = newCoreForTest(testParams(), testConfig(), newFakeOpener(rpc), cb)
	if _, err := cs.Ingest(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc.sends) == 1 }, time.Second)

	closeDone := make(chan struct{})
	go func() {
		cs.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Close blocked while dispatching OnError")
	}
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("OnError calling Close deadlocked")
	}
}

func TestCoreStreamRejectsAckBeyondInFlight(t *testing.T) {
	rpc1 := newFakeRPC()
	rpc2 := newFakeRPC()
	cfg := testConfig()
	cfg.RecoveryRetries = 1
	cs := newCoreForTest(testParams(), cfg, newFakeOpener(rpc1, rpc2), nil)
	t.Cleanup(func() { cs.Close() })

	offset, err := cs.Ingest(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc1.sends) == 1 }, time.Second)
	<-rpc1.sends
	rpc1.ack(offset + 100)

	waitCondition(t, func() bool { return len(rpc2.sends) == 1 }, time.Second)
	<-rpc2.sends
	shortCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := cs.WaitForOffset(shortCtx, offset); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("future ack falsely advanced durability watermark: %v", err)
	}

	rpc2.ack(offset)
	ctx, cancelWait := context.WithTimeout(context.Background(), time.Second)
	defer cancelWait()
	if err := cs.WaitForOffset(ctx, offset); err != nil {
		t.Fatalf("WaitForOffset after valid ack: %v", err)
	}
}

func TestCoreStreamCumulativeAckDispatchesEveryOffset(t *testing.T) {
	rpc := newFakeRPC()
	cb := &recordingCallback{}
	cs := newCoreForTest(testParams(), testConfig(), newFakeOpener(rpc), cb)
	t.Cleanup(func() { cs.Close() })

	for range 3 {
		if _, err := cs.Ingest(context.Background(), []byte(`{}`)); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}
	waitCondition(t, func() bool { return len(rpc.sends) == 3 }, time.Second)
	for range 3 {
		<-rpc.sends
	}
	rpc.ack(2)
	waitCondition(t, func() bool { return len(cb.ackOffsets()) == 3 }, time.Second)
	for i, got := range cb.ackOffsets() {
		if got != int64(i) {
			t.Fatalf("callback %d: want offset %d, got %d", i, i, got)
		}
	}
}

func TestCallbackDispatcherDoesNotDropLargeAckRange(t *testing.T) {
	cb := &recordingCallback{}
	dispatcher := newCallbackDispatcher(cb)
	dispatcher.enqueueAcks(0, 2047)
	dispatcher.shutdown(time.Second)

	offsets := cb.ackOffsets()
	if len(offsets) != 2048 {
		t.Fatalf("want 2048 callbacks, got %d", len(offsets))
	}
	for i, offset := range offsets {
		if offset != int64(i) {
			t.Fatalf("callback %d: want offset %d, got %d", i, i, offset)
		}
	}
}

func TestCallbackDispatcherRecoversPanicAndContinues(t *testing.T) {
	delivered := make(chan int64, 1)
	panicPath := make(chan struct{}, 1)
	cb := &callbackFuncs{
		onAck: func(offset int64) {
			if offset == 0 {
				panicPath <- struct{}{}
				panic("callback failure")
			}
			delivered <- offset
		},
	}
	dispatcher := newCallbackDispatcher(cb)
	dispatcher.enqueueAcks(0, 1)
	dispatcher.shutdown(time.Second)

	select {
	case <-panicPath:
	default:
		t.Fatal("panicking callback was not invoked")
	}
	select {
	case got := <-delivered:
		if got != 1 {
			t.Fatalf("callback offset = %d, want 1", got)
		}
	default:
		t.Fatal("callback after panic was not delivered")
	}
}

func TestCallbackDispatcherShutdownIsBounded(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	cb := &callbackFuncs{
		onAck: func(int64) {
			close(entered)
			<-release
		},
	}
	dispatcher := newCallbackDispatcher(cb)
	dispatcher.enqueueAcks(0, 0)
	<-entered

	var releaseOnce sync.Once
	releaseCallback := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseCallback()
	shutdownDone := make(chan struct{})
	go func() {
		dispatcher.shutdown(20 * time.Millisecond)
		close(shutdownDone)
	}()
	select {
	case <-shutdownDone:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("shutdown did not honor CallbackTeardownTimeout")
	}
	releaseCallback()
	select {
	case <-dispatcher.done:
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not finish after callback returned")
	}
}

func TestCoreStreamPauseDrainsLateAck(t *testing.T) {
	rpc := newFakeRPC()
	rpc2 := newFakeRPC()
	fo := newFakeOpener(rpc, rpc2)
	cfg := testConfig()
	cfg.StreamPausedMaxWait = durationPtr(200 * time.Millisecond)
	cs := newCoreForTest(testParams(), cfg, fo, nil)
	t.Cleanup(func() { cs.Close() })

	offset, err := cs.Ingest(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc.sends) == 1 }, time.Second)
	<-rpc.sends
	rpc.closeSignalWithDuration(150 * time.Millisecond)
	rpc.ack(offset)

	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()
	if err := cs.WaitForOffset(ctx, offset); err != nil {
		t.Fatalf("late ack was not drained during pause: %v", err)
	}
	waitCondition(t, func() bool { return fo.openCount() == 2 }, time.Second)
}

// An explicit zero StreamPausedMaxWait must reconnect immediately, ignoring the
// server-requested pause duration. A nil cap (the zero value) cannot express
// this, which is why the field is a pointer.
func TestCoreStreamPauseZeroCapReconnectsImmediately(t *testing.T) {
	rpc := newFakeRPC()
	rpc2 := newFakeRPC()
	fo := newFakeOpener(rpc, rpc2)
	cfg := testConfig()
	cfg.StreamPausedMaxWait = durationPtr(0) // explicit zero: don't honor the pause
	cs := newCoreForTest(testParams(), cfg, fo, nil)
	t.Cleanup(func() { cs.Close() })

	offset, err := cs.Ingest(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc.sends) == 1 }, time.Second)
	<-rpc.sends
	// A record stays in flight (never acked) and the server asks for a long pause.
	// Without the explicit zero cap the supervisor would wait ~1h before reopening.
	rpc.closeSignalWithDuration(time.Hour)

	waitCondition(t, func() bool { return fo.openCount() == 2 }, time.Second)
	_ = offset
}

func TestCoreStreamPauseHonorsRecoveryDisabled(t *testing.T) {
	rpc := newFakeRPC()
	fo := newFakeOpener(rpc)
	cfg := testConfig()
	cfg.Recovery = RecoveryDisabled
	cs := newCoreForTest(testParams(), cfg, fo, nil)

	if _, err := cs.Ingest(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc.sends) == 1 }, time.Second)
	rpc.closeSignal()
	waitCondition(t, cs.IsClosed, time.Second)
	if got := fo.openCount(); got != 1 {
		t.Fatalf("RecoveryDisabled pause reopened stream %d times", got)
	}
}

func TestCoreStreamPauseRemainsStickyAcrossEOF(t *testing.T) {
	rpc := newFakeRPC()
	cfg := testConfig()
	cfg.Recovery = RecoveryDisabled
	cfg.StreamPausedMaxWait = durationPtr(time.Second)
	cs := newCoreForTest(testParams(), cfg, newFakeOpener(rpc), nil)

	if _, err := cs.Ingest(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc.sends) == 1 }, time.Second)
	<-rpc.sends

	rpc.closeSignalWithDuration(time.Second)
	rpc.close()
	waitCondition(t, cs.IsClosed, time.Second)
	if err := cs.terminalErr(); err == nil ||
		!strings.Contains(err.Error(), "server requested pause and recovery is disabled") {
		t.Fatalf("terminal error = %v, want recovery-disabled pause", err)
	}
}

func TestCoreStreamPauseRecoveryPreservesAckMapping(t *testing.T) {
	rpc1 := newFakeRPC()
	rpc2 := newFakeRPC()
	fo := newFakeOpener(rpc1, rpc2)
	cb := &recordingCallback{}
	cfg := testConfig()
	cfg.StreamPausedMaxWait = durationPtr(30 * time.Millisecond)
	cs := newCoreForTest(testParams(), cfg, fo, cb)
	t.Cleanup(func() { cs.Close() })

	for i := range 3 {
		if _, err := cs.Ingest(context.Background(), fmt.Appendf(nil, `{"record":%d}`, i)); err != nil {
			t.Fatalf("Ingest %d: %v", i, err)
		}
	}
	waitCondition(t, func() bool { return len(rpc1.sends) == 3 }, time.Second)
	for range 3 {
		<-rpc1.sends
	}

	rpc1.closeSignalWithDuration(30 * time.Millisecond)
	rpc1.ack(0)
	waitCondition(t, func() bool { return cb.ackCount() == 1 }, time.Second)
	waitCondition(t, func() bool { return fo.openCount() == 2 }, time.Second)

	waitCondition(t, func() bool { return len(rpc2.sends) == 2 }, time.Second)
	firstReplay := <-rpc2.sends
	secondReplay := <-rpc2.sends
	if got := firstReplay.GetIngestRecord().GetOffsetId(); got != 0 {
		t.Fatalf("first replay physical offset = %d, want 0", got)
	}
	if got := secondReplay.GetIngestRecord().GetOffsetId(); got != 1 {
		t.Fatalf("second replay physical offset = %d, want 1", got)
	}
	rpc2.ack(1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := cs.WaitForOffset(ctx, 2); err != nil {
		t.Fatalf("WaitForOffset(2): %v", err)
	}
	waitCondition(t, func() bool { return cb.ackCount() == 3 }, time.Second)
	if got := cb.ackOffsets(); !slices.Equal(got, []int64{0, 1, 2}) {
		t.Fatalf("callback offsets = %v, want [0 1 2]", got)
	}
}

func TestCoreStreamIdlePauseRecoversImmediately(t *testing.T) {
	rpc1 := newFakeRPC()
	rpc2 := newFakeRPC()
	fo := newFakeOpener(rpc1, rpc2)
	cs := newCoreForTest(testParams(), testConfig(), fo, nil)
	t.Cleanup(func() { cs.Close() })

	waitCondition(t, func() bool { return fo.openCount() == 1 }, time.Second)
	rpc1.closeSignalWithDuration(time.Second)
	waitCondition(t, func() bool { return fo.openCount() == 2 }, 200*time.Millisecond)
}

func TestCoreStreamLackOfAckBudgetStartsWithFlight(t *testing.T) {
	rpc := newFakeRPC()
	fo := newFakeOpener(rpc)
	cfg := testConfig()
	cfg.LackOfAckTimeout = 200 * time.Millisecond
	cs := newCoreForTest(testParams(), cfg, fo, nil)
	t.Cleanup(func() { cs.Close() })

	waitCondition(t, func() bool { return fo.openCount() == 1 }, time.Second)
	time.Sleep(175 * time.Millisecond)
	offset, err := cs.Ingest(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc.sends) == 1 }, time.Second)
	<-rpc.sends
	time.Sleep(50 * time.Millisecond)
	if cs.IsClosed() || fo.openCount() != 1 {
		t.Fatal("record inherited an almost-expired idle ack timer")
	}
	rpc.ack(offset)
}

func TestCoreStreamRejectsOversizedPayloadWithoutConsumingOffset(t *testing.T) {
	rpc := newFakeRPC()
	cfg := testConfig()
	cfg.MaxPayloadBytes = 64
	cs := newCoreForTest(testParams(), cfg, newFakeOpener(rpc), nil)
	t.Cleanup(func() { cs.Close() })

	if _, err := cs.Ingest(context.Background(), bytes.Repeat([]byte("x"), 65)); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("want ErrPayloadTooLarge for record, got %v", err)
	}
	if _, err := cs.IngestBatch(context.Background(), [][]byte{
		bytes.Repeat([]byte("x"), 40),
		bytes.Repeat([]byte("y"), 30),
	}); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("want ErrPayloadTooLarge for batch, got %v", err)
	}
	cs.cfg.MaxBatchRecords = 1
	if _, err := cs.IngestBatch(context.Background(), [][]byte{
		[]byte(`{}`), []byte(`{}`),
	}); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("want ErrPayloadTooLarge for batch record count, got %v", err)
	}
	cs.cfg.MaxBatchRecords = DefaultMaxBatchRecords
	offset, err := cs.Ingest(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatalf("small Ingest: %v", err)
	}
	if offset != 0 {
		t.Fatalf("oversized inputs consumed offsets; small record got %d", offset)
	}

	wireCfg := testConfig()
	wireCfg.MaxPayloadBytes = len([]byte(`{}`))
	wireCS := newCoreForTest(testParams(), wireCfg, newFakeOpener(newFakeRPC()), nil)
	t.Cleanup(func() { wireCS.Close() })
	if _, err := wireCS.Ingest(context.Background(), []byte(`{}`)); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("want encoded wire-size rejection, got %v", err)
	}
}

func TestCoreStreamAcceptsPayloadAndBatchAtLimits(t *testing.T) {
	t.Run("encoded payload", func(t *testing.T) {
		record := []byte(`{"at":"limit"}`)
		enc := jsonEncoder{}
		msg, err := enc.encode(math.MaxInt64, record)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}

		rpc := newFakeRPC()
		cfg := testConfig()
		cfg.MaxPayloadBytes = enc.wireSize(msg)
		cs := newCoreForTest(testParams(), cfg, newFakeOpener(rpc), nil)
		t.Cleanup(func() { cs.Close() })

		offset, err := cs.Ingest(context.Background(), record)
		if err != nil {
			t.Fatalf("Ingest at MaxPayloadBytes: %v", err)
		}
		waitCondition(t, func() bool { return len(rpc.sends) == 1 }, time.Second)
		<-rpc.sends
		rpc.ack(offset)
	})

	t.Run("batch count", func(t *testing.T) {
		rpc := newFakeRPC()
		cfg := testConfig()
		cfg.MaxBatchRecords = 2
		cs := newCoreForTest(testParams(), cfg, newFakeOpener(rpc), nil)
		t.Cleanup(func() { cs.Close() })

		if _, err := cs.IngestBatch(
			context.Background(),
			[][]byte{[]byte(`{"first":1}`), []byte(`{"second":2}`)},
		); err != nil {
			t.Fatalf("IngestBatch at MaxBatchRecords: %v", err)
		}
	})
}
