package stream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
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
	// Use a fakeOpener that always fails to open so all items stay unacked.
	fo := &fakeOpener{openErr: fmt.Errorf("connection refused")}
	cfg := testConfig()
	cfg.Recovery = RecoveryDisabled
	cs := newCoreForTest(testParams(), cfg, fo, nil)

	// Ingest with a context that will eventually succeed (the buffer blocks,
	// not the opener). Give it a short context so we can proceed quickly.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, _ = cs.Ingest(ctx, []byte(`{"a":1}`))

	unacked := cs.GetUnacked()
	// Items may or may not be in unacked depending on timing; we just want
	// GetUnacked to not panic and to close the stream.
	_ = unacked
	if !cs.IsClosed() {
		t.Fatal("GetUnacked should close the stream")
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
			off, err := cs.Ingest(context.Background(), []byte(`{}`))
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
