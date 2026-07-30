package stream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/databricks/zerobus-sdk/purego/internal/transport"
)

// ---- tests -----------------------------------------------------------------
func TestCoreStreamIDsAcrossRecovery(t *testing.T) {
	first := newFakeRPC()
	second := newFakeRPC()
	opener := newFakeOpener(first, second)
	opener.serverIDs = []string{"server-1", "server-2"}
	cs := newTestStream(t, opener)

	logicalID := cs.ID()
	if logicalID == "" {
		t.Fatal("ID returned an empty logical stream ID")
	}
	waitCondition(t, func() bool {
		return cs.ServerID() == "server-1"
	}, time.Second)

	stopReads := make(chan struct{})
	readErr := make(chan string, 1)
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-stopReads:
				return
			default:
				if got := cs.ID(); got != logicalID {
					select {
					case readErr <- got:
					default:
					}
					return
				}
				_ = cs.ServerID()
			}
		}
	}()

	first.close()
	waitCondition(t, func() bool {
		return cs.ServerID() == "server-2"
	}, time.Second)
	close(stopReads)
	readers.Wait()

	select {
	case got := <-readErr:
		t.Fatalf("ID changed during recovery: got %q, want %q", got, logicalID)
	default:
	}
	if got := cs.ID(); got != logicalID {
		t.Fatalf("ID after recovery = %q, want %q", got, logicalID)
	}
}

func TestCoreStreamIDsAreUnique(t *testing.T) {
	cfg := testConfig()
	cfg.Recovery = RecoveryDisabled
	first := newCoreForTest(testParams(), cfg, &fakeOpener{openErr: errors.New("open failed")}, nil)
	second := newCoreForTest(testParams(), cfg, &fakeOpener{openErr: errors.New("open failed")}, nil)
	t.Cleanup(func() { first.Close() })
	t.Cleanup(func() { second.Close() })

	if first.ID() == second.ID() {
		t.Fatalf("two streams received the same ID %q", first.ID())
	}
}

func TestCoreStreamFailedRecoveryPreservesServerID(t *testing.T) {
	rpc := newFakeRPC()
	opener := newFakeOpener(rpc)
	opener.serverIDs = []string{"server-1"}
	cs := newTestStream(t, opener)

	waitCondition(t, func() bool {
		return cs.ServerID() == "server-1"
	}, time.Second)
	rpc.close()
	waitCondition(t, func() bool {
		return opener.openCount() >= 2
	}, time.Second)

	if got := cs.ServerID(); got != "server-1" {
		t.Fatalf("ServerID after failed recovery = %q, want %q", got, "server-1")
	}
}

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

func TestCoreStreamWaitForOffsetNoDeadlineStillTimesOut(t *testing.T) {
	rpc := newFakeRPC()
	cfg := testConfig()
	cfg.FlushTimeout = 25 * time.Millisecond
	cs := newCoreForTest(testParams(), cfg, newFakeOpener(rpc), nil)
	t.Cleanup(func() { cs.Close() })

	start := time.Now()
	err := cs.WaitForOffset(context.Background(), 42)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatalf("WaitForOffset exceeded FlushTimeout hard cap: %v", time.Since(start))
	}
}

func TestCoreStreamCloseIsIdempotent(t *testing.T) {
	rpc := newFakeRPC()
	cs := newTestStream(t, newFakeOpener(rpc))
	if err := cs.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := cs.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestCoreStreamCloseMemoizesFlushError(t *testing.T) {
	rpc := newFakeRPC()
	cfg := testConfig()
	cfg.FlushTimeout = 25 * time.Millisecond
	cs := newCoreForTest(testParams(), cfg, newFakeOpener(rpc), nil)

	if _, err := cs.Ingest(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc.sends) == 1 }, time.Second)

	firstErr := cs.Close()
	if !errors.Is(firstErr, context.DeadlineExceeded) {
		t.Fatalf("first Close error = %v, want DeadlineExceeded", firstErr)
	}
	if secondErr := cs.Close(); secondErr != firstErr {
		t.Fatalf("second Close error = %v, want memoized %v", secondErr, firstErr)
	}
}

func TestCoreStreamCloseLinearizesWithIngest(t *testing.T) {
	rpc := newFakeRPC()
	release := make(chan struct{})
	enc := &blockingNthEncoder{
		blockAt: 2,
		entered: make(chan struct{}),
		release: release,
	}
	cs := NewCoreStream[encodedMsg, ephemeralResp](
		testParams(),
		testConfig(),
		newFakeOpener(rpc),
		enc,
		offsetAckModel{},
		nil,
	)

	firstOffset, err := cs.Ingest(context.Background(), []byte(`{"first":true}`))
	if err != nil {
		t.Fatalf("first Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc.sends) == 1 }, time.Second)
	<-rpc.sends

	ingestErr := make(chan error, 1)
	go func() {
		_, err := cs.Ingest(context.Background(), []byte(`{"second":true}`))
		ingestErr <- err
	}()
	select {
	case <-enc.entered:
	case <-time.After(time.Second):
		t.Fatal("second Ingest did not enter encoder")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- cs.Close() }()
	waitCondition(t, func() bool {
		cs.offsetMu.Lock()
		defer cs.offsetMu.Unlock()
		return cs.closing
	}, time.Second)

	close(release)
	if err := <-ingestErr; !errors.Is(err, errClosed) {
		t.Fatalf("concurrent Ingest error = %v, want errClosed", err)
	}

	rpc.ack(firstOffset)
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not return after its captured target was acknowledged")
	}
}

func TestCoreStreamCloseWaitsForFlushBeforeShutdown(t *testing.T) {
	rpc := newFakeRPC()
	cfg := testConfig()
	cfg.FlushTimeout = time.Second
	cs := newCoreForTest(testParams(), cfg, newFakeOpener(rpc), nil)
	t.Cleanup(func() { cs.Close() })

	offset, err := cs.Ingest(context.Background(), []byte(`{}`))
	if err != nil {
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
	case <-done:
		t.Fatal("Close returned before flush ack")
	case <-time.After(50 * time.Millisecond):
		// expected: still waiting for durability
	}

	rpc.ack(offset)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close did not return after ack")
	}
}

// TestCoreStreamCloseDrainsGracefully verifies that Close flushes first, then
// half-closes the send side and drains Recv to EOF.
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

	// Close in the background; it must wait for flush before teardown.
	done := make(chan struct{})
	go func() { cs.Close(); close(done) }()

	select {
	case <-done:
		t.Fatal("Close returned before flush ack")
	case <-time.After(50 * time.Millisecond):
		// expected: waiting for ack
	}

	// Ack to unblock the flush phase, then expect graceful half-close.
	rpc.ack(off)
	select {
	case <-rpc.closeSent:
	case <-time.After(time.Second):
		t.Fatal("Close did not half-close the send side after flush")
	}

	// End the stream so Recv returns EOF and graceful drain completes.
	rpc.serverEnd()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after the server ended the stream")
	}

	waitCondition(t, func() bool { return cb.ackCount() == 1 }, time.Second)
}

func TestCoreStreamCloseAbortsBlockedGracefulRecv(t *testing.T) {
	rpc := newGracefulFakeRPC()
	cfg := testConfig()
	cfg.DrainTimeout = 25 * time.Millisecond
	cfg.FlushTimeout = time.Second
	cs := newCoreForTest(testParams(), cfg, &gracefulOpener{rpc: rpc}, nil)

	offset, err := cs.Ingest(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc.sends) == 1 }, time.Second)
	<-rpc.sends
	rpc.ack(offset) // make flush succeed before graceful teardown

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
	if _, err := cs.GetUnacked(); !errors.Is(err, ErrStreamStillActive) {
		t.Fatalf("want ErrStreamStillActive on active stream, got %v", err)
	}
	cs.Close()
	unacked, err := cs.GetUnacked()
	if err != nil {
		t.Fatalf("GetUnacked: %v", err)
	}
	if len(unacked) != 1 || !bytes.Equal(unacked[0], record) {
		t.Fatalf("want exact unacked record %q, got %q", record, unacked)
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
	cs.Close()

	first, err := cs.GetUnacked()
	if err != nil {
		t.Fatalf("first GetUnacked: %v", err)
	}
	if len(first) != 1 || !bytes.Equal(first[0], record) {
		t.Fatalf("first GetUnacked: want %q, got %q", record, first)
	}
	// Mutate the first result; it must not be an alias of the retained payload.
	for i := range first[0] {
		first[0][i] = 'X'
	}

	second, err := cs.GetUnacked()
	if err != nil {
		t.Fatalf("second GetUnacked: %v", err)
	}
	if len(second) != 1 || !bytes.Equal(second[0], record) {
		t.Fatalf("second GetUnacked: want original %q, got %q", record, second)
	}
}

// Replaying unacked records requires knowing which of them shared an offset,
// since the server acks a batch atomically. GetUnackedBatches keeps that
// grouping; GetUnacked is its flattening.
func TestCoreStreamGetUnackedBatchesPreservesGrouping(t *testing.T) {
	rpc := newFakeRPC()
	cs := newCoreForTest(testParams(), testConfig(), newFakeOpener(rpc), nil)
	single := []byte(`{"a":1}`)
	batch := [][]byte{[]byte(`{"b":2}`), []byte(`{"c":3}`)}
	if _, err := cs.Ingest(context.Background(), single); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if _, err := cs.IngestBatch(context.Background(), batch); err != nil {
		t.Fatalf("IngestBatch: %v", err)
	}
	cs.Close()

	groups, err := cs.GetUnackedBatches()
	if err != nil {
		t.Fatalf("GetUnackedBatches: %v", err)
	}
	want := [][][]byte{{single}, batch}
	equalGroups := slices.EqualFunc(groups, want, func(got, exp [][]byte) bool {
		return slices.EqualFunc(got, exp, bytes.Equal)
	})
	if !equalGroups {
		t.Fatalf("GetUnackedBatches = %q, want %q", groups, want)
	}

	flat, err := cs.GetUnacked()
	if err != nil {
		t.Fatalf("GetUnacked: %v", err)
	}
	wantFlat := [][]byte{single, batch[0], batch[1]}
	if !slices.EqualFunc(flat, wantFlat, bytes.Equal) {
		t.Fatalf("GetUnacked = %q, want %q", flat, wantFlat)
	}

	// The grouped view clones too, so a caller mutating it cannot corrupt the
	// retained payloads that a later replay reads.
	for i := range groups[0][0] {
		groups[0][0][i] = 'X'
	}
	again, err := cs.GetUnackedBatches()
	if err != nil {
		t.Fatalf("second GetUnackedBatches: %v", err)
	}
	if !bytes.Equal(again[0][0], single) {
		t.Fatalf("second GetUnackedBatches = %q, want original %q", again[0][0], single)
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
	sentRecords := make(map[string]int64, n)
	for range n {
		msg := <-rpc.sends
		record := msg.GetIngestRecord()
		sentRecords[record.GetJsonRecord()] = record.GetOffsetId()
	}
	for i := range n {
		record := fmt.Sprintf(`{"record":%d}`, i)
		got, ok := sentRecords[record]
		if !ok {
			t.Fatalf("record %q was not sent", record)
		}
		if got != offsets[i] {
			t.Fatalf("record %q wire offset = %d, returned offset = %d", record, got, offsets[i])
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

func TestCoreStreamReusesSendCompletionSignal(t *testing.T) {
	rpc := newControlledSendRPC()
	cs := newCoreForTest(testParams(), testConfig(), &controlledSendOpener{rpc}, nil)
	t.Cleanup(func() { cs.Close() })

	first, err := cs.Ingest(context.Background(), []byte(`{"first":true}`))
	if err != nil {
		t.Fatalf("first Ingest: %v", err)
	}
	second, err := cs.Ingest(context.Background(), []byte(`{"second":true}`))
	if err != nil {
		t.Fatalf("second Ingest: %v", err)
	}
	select {
	case <-rpc.started:
	case <-time.After(time.Second):
		t.Fatal("first Send did not start")
	}
	rpc.ack(0)
	rpc.result <- nil
	waitCondition(t, func() bool { return len(rpc.sends) == 1 }, time.Second)

	rpc.result <- nil
	waitCondition(t, func() bool { return len(rpc.sends) == 2 }, time.Second)
	for range 2 {
		<-rpc.sends
	}
	rpc.ack(1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := cs.WaitForOffset(ctx, second); err != nil {
		t.Fatalf("second WaitForOffset: %v", err)
	}
	if err := cs.WaitForOffset(ctx, first); err != nil {
		t.Fatalf("first WaitForOffset: %v", err)
	}
}

func TestCoreStreamRecvPumpStaysOutstandingDuringClassification(t *testing.T) {
	rpc := newScriptedRPC()
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseModel := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseModel()
	model := &blockingAckModel{entered: make(chan struct{}), release: release}
	cs := NewCoreStream[encodedMsg, ephemeralResp](
		testParams(),
		testConfig(),
		&scriptedOpener{rpc: rpc},
		jsonEncoder{},
		model,
		nil,
	)
	t.Cleanup(func() { cs.Close() })

	select {
	case call := <-rpc.recvStarted:
		if call != 1 {
			t.Fatalf("first Recv call = %d", call)
		}
	case <-time.After(time.Second):
		t.Fatal("first Recv did not start")
	}
	offset, err := cs.Ingest(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc.sends) == 1 }, time.Second)
	<-rpc.sends
	rpc.ack(0)
	select {
	case <-model.entered:
	case <-time.After(time.Second):
		t.Fatal("ack classification did not start")
	}
	select {
	case call := <-rpc.recvStarted:
		if call != 2 {
			t.Fatalf("next Recv call = %d, want 2", call)
		}
	case <-time.After(time.Second):
		t.Fatal("next Recv was not outstanding during classification")
	}
	releaseModel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := cs.WaitForOffset(ctx, offset); err != nil {
		t.Fatalf("WaitForOffset: %v", err)
	}
}

func TestCoreStreamReceiverFailureWinsWhileSendIsBlocked(t *testing.T) {
	tests := []struct {
		name       string
		fail       func(*blockedSendTerminalRPC)
		wantText   string
		wantCode   codes.Code
		wantAuthIn int64
	}{
		{
			name: "authentication rejection",
			fail: func(rpc *blockedSendTerminalRPC) {
				rpc.recvErr <- status.Error(codes.Unauthenticated, "expired credentials")
			},
			wantText:   "expired credentials",
			wantCode:   codes.Unauthenticated,
			wantAuthIn: 1,
		},
		{
			name:     "malformed response",
			fail:     func(rpc *blockedSendTerminalRPC) { rpc.malformedAck() },
			wantText: "unusable server response",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			provider := &countingHeadersProvider{}
			params := testParams()
			params.HeadersProvider = provider
			rpc := newBlockedSendTerminalRPC()
			cfg := testConfig()
			cfg.Recovery = RecoveryDisabled
			cs := newCoreForTest(params, cfg, &blockedSendTerminalOpener{rpc: rpc}, nil)

			if _, err := cs.Ingest(context.Background(), []byte(`{}`)); err != nil {
				t.Fatalf("Ingest: %v", err)
			}
			select {
			case <-rpc.started:
			case <-time.After(time.Second):
				t.Fatal("Send did not block")
			}
			tc.fail(rpc)

			waitCondition(t, cs.IsClosed, time.Second)
			err := cs.terminalErr()
			if err == nil || !strings.Contains(err.Error(), tc.wantText) {
				t.Fatalf("terminal error = %v, want %q", err, tc.wantText)
			}
			if tc.wantCode != codes.OK && status.Code(err) != tc.wantCode {
				t.Fatalf("status code = %v, want %v", status.Code(err), tc.wantCode)
			}
			if got := provider.invalidations.Load(); got != tc.wantAuthIn {
				t.Fatalf("Invalidate calls = %d, want %d", got, tc.wantAuthIn)
			}
			cs.Close()
		})
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
	cfg.FlushTimeout = 25 * time.Millisecond
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

func TestCoreStreamByteBackpressureResumesAfterAck(t *testing.T) {
	rpc := newFakeRPC()
	cfg := testConfig()
	record := []byte(`{"value":1}`)
	cfg.MaxInflight = 2
	cfg.MaxBufferedPayloadBytes = jsonEncoder{}.retainedSize(len(record), 1)
	cs := newCoreForTest(testParams(), cfg, newFakeOpener(rpc), nil)
	t.Cleanup(func() { cs.Close() })

	first, err := cs.Ingest(context.Background(), record)
	if err != nil {
		t.Fatalf("first Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc.sends) == 1 }, time.Second)
	<-rpc.sends

	secondResult := make(chan struct {
		offset int64
		err    error
	}, 1)
	go func() {
		offset, ingestErr := cs.Ingest(context.Background(), record)
		secondResult <- struct {
			offset int64
			err    error
		}{offset: offset, err: ingestErr}
	}()
	waitCondition(t, func() bool { return cs.buf.waiterCount() == 1 }, time.Second)

	rpc.ack(first)
	var second int64
	select {
	case result := <-secondResult:
		if result.err != nil {
			t.Fatalf("second Ingest: %v", result.err)
		}
		second = result.offset
	case <-time.After(time.Second):
		t.Fatal("second Ingest did not resume after ack released byte capacity")
	}
	waitCondition(t, func() bool { return len(rpc.sends) == 1 }, time.Second)
	<-rpc.sends
	rpc.ack(second)
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
	unacked, err := cs.GetUnacked()
	if err != nil {
		t.Fatalf("GetUnacked: %v", err)
	}
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
		// Terminal here but retryable in the Rust core; pinned so the divergence
		// cannot change silently.
		{name: "status failed precondition", err: status.Error(codes.FailedPrecondition, "schema changed"), want: false},
		// A server-sent Canceled status is not the caller's context.Canceled.
		{name: "status canceled", err: status.Error(codes.Canceled, "server canceled"), want: true},
		{name: "status resource exhausted", err: status.Error(codes.ResourceExhausted, "throttled"), want: true},
		// The outermost self-classifying error outranks both the status code and any
		// classification beneath it, which is why runOnce must not wrap a cause that
		// is already known to be permanent. TestCoreStreamOpenTimeoutKeepsTerminalStatus
		// covers that for a terminal status and
		// TestCoreStreamOpenTimeoutKeepsNonRetryableClassification for an error that
		// carries no status at all.
		{name: "open budget outranks wrapped status", err: &openBudgetExceeded{
			cause: &openFailure{cause: status.Error(codes.InvalidArgument, "bad table")},
		}, want: true},
		{name: "open budget outranks wrapped classification", err: &openBudgetExceeded{
			cause: &openFailure{cause: &classifiedError{retryable: false}},
		}, want: true},
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

// An Open that returns a permanent rejection just as its deadline expires must
// stay terminal. Wrapping it as openBudgetExceeded made it self-classify as
// retryable, so a bad table name burned the whole recovery budget.
func TestCoreStreamOpenTimeoutKeepsTerminalStatus(t *testing.T) {
	opener := &terminalTimeoutOpener{}
	cfg := testConfig()
	cfg.RecoveryRetries = 2
	cfg.RecoveryTimeout = 10 * time.Millisecond
	cfg.RecoveryBackoff = time.Millisecond
	cs := newCoreForTest(testParams(), cfg, opener, nil)
	t.Cleanup(func() { cs.Close() })

	waitCondition(t, cs.IsClosed, time.Second)
	if got := opener.attempts.Load(); got != 1 {
		t.Fatalf("Open attempts = %d, want 1 (terminal status must not be retried)", got)
	}
	if err := cs.terminalErr(); err == nil ||
		!strings.Contains(err.Error(), "bad table name") {
		t.Fatalf("terminal error = %v, want the InvalidArgument rejection", err)
	}
}

// The same masking applies to a rejection that carries no gRPC status at all: an
// OAuth TokenError is codes.Unknown, so IsTerminalStatus cannot see it and the
// openBudgetExceeded wrapper outranked its own non-retryable classification,
// re-requesting a rejected credential for the whole recovery budget.
func TestCoreStreamOpenTimeoutKeepsNonRetryableClassification(t *testing.T) {
	opener := &classifiedTimeoutOpener{err: &classifiedError{retryable: false}}
	cfg := testConfig()
	cfg.RecoveryRetries = 2
	cfg.RecoveryTimeout = 10 * time.Millisecond
	cfg.RecoveryBackoff = time.Millisecond
	cs := newCoreForTest(testParams(), cfg, opener, nil)
	t.Cleanup(func() { cs.Close() })

	waitCondition(t, cs.IsClosed, time.Second)
	if got := opener.attempts.Load(); got != 1 {
		t.Fatalf("Open attempts = %d, want 1 (a self-classified rejection must not be retried)", got)
	}
	err := cs.terminalErr()
	if err == nil || !strings.Contains(err.Error(), "classified error") {
		t.Fatalf("terminal error = %v, want the provider rejection", err)
	}
	if strings.Contains(err.Error(), "open budget exceeded") {
		t.Fatalf("terminal error = %v, want no budget wrapper over a classified rejection", err)
	}
}

// The converse must keep working: an error that reports itself non-retryable only
// because the open budget cut it short is the timeout openBudgetExceeded exists
// for, so it must still be retried rather than killing the stream on one slow
// token fetch.
func TestCoreStreamOpenTimeoutRetriesDeadlineClassifiedError(t *testing.T) {
	opener := &classifiedTimeoutOpener{err: &deadlineClassifiedError{}}
	cfg := testConfig()
	cfg.RecoveryRetries = 2
	cfg.RecoveryTimeout = 10 * time.Millisecond
	cfg.RecoveryBackoff = time.Millisecond
	cs := newCoreForTest(testParams(), cfg, opener, nil)
	t.Cleanup(func() { cs.Close() })

	waitCondition(t, cs.IsClosed, time.Second)
	if got := opener.attempts.Load(); got != 3 {
		t.Fatalf("Open attempts = %d, want the initial attempt plus two retries", got)
	}
	if err := cs.terminalErr(); err == nil ||
		!strings.Contains(err.Error(), "open budget exceeded") {
		t.Fatalf("terminal error = %v, want open budget exhaustion", err)
	}
}

func TestDeniesRetry(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "unclassified", err: errors.New("connection reset"), want: false},
		{name: "classified retryable", err: &classifiedError{retryable: true}, want: false},
		{name: "classified terminal", err: &classifiedError{retryable: false}, want: true},
		{
			name: "classified terminal when wrapped",
			err:  fmt.Errorf("open: %w", &classifiedError{retryable: false}),
			want: true,
		},
		// Non-retryable only because a deadline cut it short: still a timeout.
		{name: "classified deadline", err: &deadlineClassifiedError{}, want: false},
		{name: "bare deadline", err: context.DeadlineExceeded, want: false},
		{name: "status rejection carries no classification", err: status.Error(codes.InvalidArgument, "bad"), want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := deniesRetry(tc.err); got != tc.want {
				t.Fatalf("deniesRetry(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestCoreStreamOpenAuthRejectionInvalidatesOnce(t *testing.T) {
	provider := &countingHeadersProvider{}
	params := testParams()
	params.HeadersProvider = provider
	cfg := testConfig()
	cfg.Recovery = RecoveryDisabled
	cs := newCoreForTest(params, cfg, &authRejectingOpener{}, nil)

	waitCondition(t, cs.IsClosed, time.Second)
	if got := provider.invalidations.Load(); got != 1 {
		t.Fatalf("Invalidate calls = %d, want 1", got)
	}
}

func TestCoreStreamCloseRacingAuthRejectionStillInvalidates(t *testing.T) {
	provider := &countingHeadersProvider{}
	params := testParams()
	params.HeadersProvider = provider
	opener := &cancelThenAuthOpener{started: make(chan struct{})}
	cfg := testConfig()
	cfg.Recovery = RecoveryDisabled
	cs := newCoreForTest(params, cfg, opener, nil)

	select {
	case <-opener.started:
	case <-time.After(time.Second):
		t.Fatal("Open did not start")
	}
	if err := cs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := provider.invalidations.Load(); got != 1 {
		t.Fatalf("Invalidate calls = %d, want 1", got)
	}
}

func TestCoreStreamCloseRacingLiveAuthRejectionStillInvalidates(t *testing.T) {
	provider := &countingHeadersProvider{}
	params := testParams()
	params.HeadersProvider = provider
	stream := newAuthOnCloseStream()
	cs := newCoreForTest(
		params, testConfig(), &authOnCloseOpener{stream: stream}, nil,
	)

	select {
	case <-stream.recvStarted:
	case <-time.After(time.Second):
		t.Fatal("Recv did not start")
	}
	if err := cs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := provider.invalidations.Load(); got != 1 {
		t.Fatalf("Invalidate calls = %d, want 1", got)
	}
}

func TestCoreStreamInitialAuthRejectionRefreshesOnce(t *testing.T) {
	for _, code := range []codes.Code{codes.Unauthenticated, codes.PermissionDenied} {
		t.Run(code.String(), func(t *testing.T) {
			provider := &countingHeadersProvider{}
			params := testParams()
			params.HeadersProvider = provider
			opener := &authRefreshOpener{
				rpc:        newFakeRPC(),
				rejections: 1,
				code:       code,
			}
			cfg := testConfig()
			cfg.Recovery = RecoveryEnabled
			cfg.RecoveryRetries = 3
			cfg.RecoveryBackoff = time.Millisecond
			cs := newCoreForTest(params, cfg, opener, nil)
			t.Cleanup(func() { cs.Close() })

			waitCondition(t, func() bool { return opener.openCount() == 2 }, time.Second)
			if cs.IsClosed() {
				t.Fatal("stream closed instead of retrying with refreshed credentials")
			}
			if got := provider.invalidations.Load(); got != 1 {
				t.Fatalf("Invalidate calls = %d, want 1", got)
			}
		})
	}
}

func TestCoreStreamSecondInitialAuthRejectionIsTerminal(t *testing.T) {
	for _, code := range []codes.Code{codes.Unauthenticated, codes.PermissionDenied} {
		t.Run(code.String(), func(t *testing.T) {
			provider := &countingHeadersProvider{}
			params := testParams()
			params.HeadersProvider = provider
			opener := &authRefreshOpener{
				rpc:        newFakeRPC(),
				rejections: 2,
				code:       code,
			}
			cfg := testConfig()
			cfg.Recovery = RecoveryEnabled
			cfg.RecoveryRetries = 3
			cfg.RecoveryBackoff = time.Millisecond
			cs := newCoreForTest(params, cfg, opener, nil)

			waitCondition(t, cs.IsClosed, time.Second)
			if got := opener.openCount(); got != 2 {
				t.Fatalf("Open attempts = %d, want 2", got)
			}
			if got := provider.invalidations.Load(); got != 2 {
				t.Fatalf("Invalidate calls = %d, want one per rejection", got)
			}
			cs.Close()
		})
	}
}

func TestCoreStreamInitialAuthRefreshAfterTransientOpenFailure(t *testing.T) {
	provider := &countingHeadersProvider{}
	params := testParams()
	params.HeadersProvider = provider
	opener := &scriptedOpenOpener{
		steps: []openStep{
			{err: errors.New("temporary connection failure")},
			{err: status.Error(codes.Unauthenticated, "stale credentials")},
			{rpc: newFakeRPC()},
		},
	}
	cfg := testConfig()
	cfg.RecoveryRetries = 3
	cfg.RecoveryBackoff = time.Millisecond
	cs := newCoreForTest(params, cfg, opener, nil)
	t.Cleanup(func() { cs.Close() })

	waitCondition(t, func() bool { return opener.openCount() == 3 }, time.Second)
	if cs.IsClosed() {
		t.Fatal("stream closed instead of trying refreshed credentials")
	}
	if got := provider.invalidations.Load(); got != 1 {
		t.Fatalf("Invalidate calls = %d, want 1", got)
	}
}

func TestCoreStreamInitialAuthRefreshConsumesRetryBudget(t *testing.T) {
	provider := &countingHeadersProvider{}
	params := testParams()
	params.HeadersProvider = provider
	opener := &scriptedOpenOpener{
		steps: []openStep{
			{err: status.Error(codes.Unauthenticated, "stale credentials")},
			{err: errors.New("first transient failure")},
			{err: errors.New("second transient failure")},
			{rpc: newFakeRPC()},
		},
	}
	cfg := testConfig()
	cfg.RecoveryRetries = 2
	cfg.RecoveryBackoff = time.Millisecond
	cs := newCoreForTest(params, cfg, opener, nil)

	waitCondition(t, cs.IsClosed, time.Second)
	if got := opener.openCount(); got != 3 {
		t.Fatalf("Open attempts = %d, want initial attempt plus two retries", got)
	}
	if got := provider.invalidations.Load(); got != 1 {
		t.Fatalf("Invalidate calls = %d, want 1", got)
	}
	cs.Close()
}

func TestCoreStreamAuthRejectionWithoutProviderIsTerminal(t *testing.T) {
	opener := &scriptedOpenOpener{
		steps: []openStep{
			{err: status.Error(codes.Unauthenticated, "credentials rejected")},
			{rpc: newFakeRPC()},
		},
	}
	cfg := testConfig()
	cfg.RecoveryRetries = 3
	cfg.RecoveryBackoff = time.Millisecond
	cs := newCoreForTest(testParams(), cfg, opener, nil)

	waitCondition(t, cs.IsClosed, time.Second)
	if got := opener.openCount(); got != 1 {
		t.Fatalf("Open attempts = %d, want 1 without a refreshable provider", got)
	}
	cs.Close()
}

func TestCoreStreamAuthRejectionAfterPauseIsTerminal(t *testing.T) {
	provider := &countingHeadersProvider{}
	params := testParams()
	params.HeadersProvider = provider
	rpc := newFakeRPC()
	opener := &scriptedOpenOpener{
		steps: []openStep{
			{rpc: rpc},
			{err: status.Error(codes.Unauthenticated, "reconnect credentials rejected")},
			{rpc: newFakeRPC()},
		},
	}
	cfg := testConfig()
	cfg.RecoveryRetries = 3
	cfg.RecoveryBackoff = time.Millisecond
	cs := newCoreForTest(params, cfg, opener, nil)

	waitCondition(t, func() bool { return opener.openCount() == 1 }, time.Second)
	rpc.closeSignal()
	waitCondition(t, cs.IsClosed, time.Second)
	if got := opener.openCount(); got != 2 {
		t.Fatalf("Open attempts = %d, want pause reconnect plus terminal auth rejection", got)
	}
	if got := provider.invalidations.Load(); got != 1 {
		t.Fatalf("Invalidate calls = %d, want 1", got)
	}
	cs.Close()
}

func TestCoreStreamHeadersAuthRejectionInvalidatesOnce(t *testing.T) {
	provider := &countingHeadersProvider{
		getErr: status.Error(codes.PermissionDenied, "header credentials rejected"),
	}
	params := testParams()
	params.TableName = "  c.s.t  "
	params.HeadersProvider = provider
	cfg := testConfig()
	cfg.Recovery = RecoveryDisabled
	cs := newCoreForTest(params, cfg, &headersResolvingOpener{}, nil)

	waitCondition(t, cs.IsClosed, time.Second)
	if got := provider.invalidations.Load(); got != 1 {
		t.Fatalf("Invalidate calls = %d, want 1", got)
	}
	if got, _ := provider.lastGet.Load().(string); got != "c.s.t" {
		t.Fatalf("GetHeaders table = %q, want normalized table", got)
	}
	if got, _ := provider.lastInvalidate.Load().(string); got != "c.s.t" {
		t.Fatalf("Invalidate table = %q, want normalized table", got)
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

// gRPC reports a server-side abort to Send as an opaque io.EOF and puts the real
// status on Recv. Taking the send error as the cause and discarding the receiver's
// result made a permanent rejection look like a generic disconnect: the stream
// reconnected with rejected credentials and resent the records.
func TestCoreStreamSendEOFYieldsToReceiverStatus(t *testing.T) {
	provider := &countingHeadersProvider{}
	params := testParams()
	params.HeadersProvider = provider
	rpc := newEOFSendRPC()
	opener := &eofSendOpener{rpc: rpc}
	cfg := testConfig()
	cfg.RecoveryRetries = 3
	cfg.RecoveryBackoff = time.Millisecond
	cs := newCoreForTest(params, cfg, opener, nil)
	t.Cleanup(func() { cs.Close() })

	if _, err := cs.Ingest(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	// Recv stays parked until the status below, so the sender's io.EOF is
	// necessarily the cause that ends the run — the case that used to be misread.
	select {
	case <-rpc.sendStarted:
	case <-time.After(time.Second):
		t.Fatal("Send did not run")
	}
	rpc.recvErr <- status.Error(codes.Unauthenticated, "credentials rejected")

	waitCondition(t, cs.IsClosed, 2*time.Second)
	err := cs.terminalErr()
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("terminal error = %v, want the Unauthenticated status rather than the send EOF", err)
	}
	if got := provider.invalidations.Load(); got != 1 {
		t.Fatalf("Invalidate calls = %d, want 1", got)
	}
	if got := opener.opens.Load(); got != 1 {
		t.Fatalf("Open attempts = %d, want no reconnect after a permanent rejection", got)
	}
}

func TestCoreStreamLiveAuthRejectionIsTerminalEvenWithRecoveryEnabled(t *testing.T) {
	provider := &countingHeadersProvider{}
	params := testParams()
	params.HeadersProvider = provider
	rpc := newTerminalRecvRPC()
	opener := &terminalRecvCountingOpener{rpc: rpc}
	cfg := testConfig()
	cfg.Recovery = RecoveryEnabled
	cfg.RecoveryRetries = 3
	cs := newCoreForTest(params, cfg, opener, nil)

	waitCondition(t, func() bool { return opener.attempts.Load() == 1 }, time.Second)
	rpc.recvErr <- status.Error(codes.Unauthenticated, "expired credentials")
	waitCondition(t, cs.IsClosed, time.Second)
	if got := provider.invalidations.Load(); got != 1 {
		t.Fatalf("Invalidate calls = %d, want 1", got)
	}
	if got := opener.attempts.Load(); got != 1 {
		t.Fatalf("unexpected recovery attempt count = %d, want 1", got)
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

func TestCoreStreamEmptyBatchRecordsConsumeBufferedByteBudget(t *testing.T) {
	cfg := testConfig()
	cfg.MaxBufferedPayloadBytes = 1024
	cs := newCoreForTest(testParams(), cfg, newFakeOpener(newFakeRPC()), nil)
	t.Cleanup(func() { cs.Close() })

	records := make([][]byte, 100)
	if _, err := cs.IngestBatch(context.Background(), records); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("IngestBatch error = %v, want ErrPayloadTooLarge", err)
	}
	if items, bytes := cs.buf.usage(); items != 0 || bytes != 0 {
		t.Fatalf("buffer usage after rejection = (%d, %d), want (0, 0)", items, bytes)
	}
}

func TestCoreStreamIngestBatchEmptyIsNoOp(t *testing.T) {
	rpc := newFakeRPC()
	cs := newTestStream(t, newFakeOpener(rpc))

	offset, err := cs.IngestBatch(context.Background(), nil)
	if err != nil {
		t.Fatalf("IngestBatch(nil): %v", err)
	}
	if offset != -1 {
		t.Fatalf("empty batch offset = %d, want -1 sentinel", offset)
	}
	if len(rpc.sends) != 0 {
		t.Fatalf("empty batch should not send, got %d sends", len(rpc.sends))
	}

	nextOffset, err := cs.Ingest(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatalf("Ingest after empty batch: %v", err)
	}
	if nextOffset != 0 {
		t.Fatalf("empty batch consumed offset; next offset = %d, want 0", nextOffset)
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
	cfg.FlushTimeout = 25 * time.Millisecond
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

// A waiter on an offset the stream will never reach must be woken by Close with
// errWatermarkClosed. FlushTimeout stays long on purpose: if the wait could time
// out on its own, the test would pass without the wakeup happening at all.
func TestCoreStreamCloseWakesOffsetWaiters(t *testing.T) {
	rpc := newFakeRPC()
	cfg := testConfig()
	cfg.FlushTimeout = 30 * time.Second
	cs := newCoreForTest(testParams(), cfg, newFakeOpener(rpc), nil)
	offset, err := cs.Ingest(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc.sends) == 1 }, time.Second)
	<-rpc.sends
	// Ack what was ingested so Close's own flush completes and cannot be the
	// thing that fails the watermark: only the clean-close path is left.
	rpc.ack(offset)
	waitCondition(t, func() bool { return cs.wm.current() == offset }, time.Second)

	waitDone := make(chan error, 1)
	go func() {
		// One past the close boundary, so the target is unreachable by design.
		waitDone <- cs.WaitForOffset(context.Background(), offset+1)
	}()
	cs.Close()

	select {
	case err := <-waitDone:
		if !errors.Is(err, errWatermarkClosed) {
			t.Fatalf("WaitForOffset = %v, want errWatermarkClosed", err)
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
	cfg := testConfig()
	cfg.FlushTimeout = 25 * time.Millisecond
	cs := newCoreForTest(testParams(), cfg, newFakeOpener(rpc), cb)

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
	unacked, err := cs.GetUnacked()
	if err != nil {
		t.Fatalf("GetUnacked: %v", err)
	}
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
	cfg := testConfig()
	cfg.FlushTimeout = 25 * time.Millisecond
	cs = newCoreForTest(testParams(), cfg, newFakeOpener(rpc), cb)
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

func TestCoreStreamCallbacksPreserveAckThenErrorOrder(t *testing.T) {
	events := make(chan string, 4)
	cb := &callbackFuncs{
		onAck: func(offset int64) {
			events <- fmt.Sprintf("ack:%d", offset)
		},
		onError: func(offset int64, _ error) {
			events <- fmt.Sprintf("error:%d", offset)
		},
	}
	rpc := newFakeRPC()
	cfg := testConfig()
	cfg.Recovery = RecoveryDisabled
	cs := newCoreForTest(testParams(), cfg, newFakeOpener(rpc), cb)

	for range 4 {
		if _, err := cs.Ingest(context.Background(), []byte(`{}`)); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}
	waitCondition(t, func() bool { return len(rpc.sends) == 4 }, time.Second)
	for range 4 {
		<-rpc.sends
	}
	rpc.ack(1)
	rpc.malformedAck()
	waitCondition(t, cs.IsClosed, time.Second)

	var got []string
	for range 4 {
		select {
		case event := <-events:
			got = append(got, event)
		case <-time.After(time.Second):
			t.Fatalf("callback events = %v, want four events", got)
		}
	}
	want := []string{"ack:0", "ack:1", "error:2", "error:3"}
	if !slices.Equal(got, want) {
		t.Fatalf("callback events = %v, want %v", got, want)
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

// StreamPausedMaxWait is an upper bound — min(cap, server duration) — so a pause
// carrying no duration must reconnect immediately rather than wait out the cap.
func TestCoreStreamPauseCapDoesNotExtendUnspecifiedDuration(t *testing.T) {
	rpc := newFakeRPC()
	fo := newFakeOpener(rpc, newFakeRPC())
	cfg := testConfig()
	cfg.StreamPausedMaxWait = durationPtr(time.Hour)
	cfg.FlushTimeout = 25 * time.Millisecond // the record is never acked
	cs := newCoreForTest(testParams(), cfg, fo, nil)
	t.Cleanup(func() { cs.Close() })

	if _, err := cs.Ingest(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc.sends) == 1 }, time.Second)
	<-rpc.sends
	// A record stays in flight, so the pause is not short-circuited as idle.
	rpc.closeSignal() // no duration

	waitCondition(t, func() bool { return fo.openCount() == 2 }, time.Second)
}

func TestEffectivePauseWaitCapsWithoutExtending(t *testing.T) {
	hour := time.Hour
	tests := []struct {
		name    string
		maxWait *time.Duration
		server  time.Duration
		want    time.Duration
	}{
		{name: "no cap honors server", server: time.Minute, want: time.Minute},
		{name: "no cap unspecified", server: 0, want: 0},
		{name: "cap shortens server", maxWait: durationPtr(time.Second), server: time.Minute, want: time.Second},
		{name: "cap above server", maxWait: &hour, server: time.Minute, want: time.Minute},
		{name: "cap does not extend unspecified", maxWait: &hour, server: 0, want: 0},
		{name: "explicit zero cap", maxWait: durationPtr(0), server: time.Minute, want: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cs := &testStream{cfg: Config{StreamPausedMaxWait: tc.maxWait}}
			if got := cs.effectivePauseWait(tc.server); got != tc.want {
				t.Fatalf("effectivePauseWait(%v) = %v, want %v", tc.server, got, tc.want)
			}
		})
	}
}

func TestCoreStreamCopiesPauseCap(t *testing.T) {
	rpc := newFakeRPC()
	rpc2 := newFakeRPC()
	fo := newFakeOpener(rpc, rpc2)
	pauseCap := time.Duration(0)
	cfg := testConfig()
	cfg.FlushTimeout = 25 * time.Millisecond
	cfg.StreamPausedMaxWait = &pauseCap
	cs := newCoreForTest(testParams(), cfg, fo, nil)
	t.Cleanup(func() { cs.Close() })

	// Mutating the caller-owned config after construction must not affect the
	// live stream's explicit-zero pause cap.
	pauseCap = time.Hour

	if _, err := cs.Ingest(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc.sends) == 1 }, time.Second)
	<-rpc.sends
	rpc.closeSignalWithDuration(time.Hour)

	waitCondition(t, func() bool { return fo.openCount() == 2 }, time.Second)
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

func TestCoreStreamPauseDoesNotMaskReceiverFailure(t *testing.T) {
	tests := []struct {
		name      string
		queueFail func(*scriptedRPC)
		want      string
		wantCode  codes.Code
		wantErr   error
	}{
		{
			name:      "malformed response",
			queueFail: func(rpc *scriptedRPC) { rpc.malformed() },
			want:      "unusable server response",
		},
		{
			name: "auth rejection",
			queueFail: func(rpc *scriptedRPC) {
				rpc.fail(status.Error(codes.Unauthenticated, "expired credentials"))
			},
			want:     "expired credentials",
			wantCode: codes.Unauthenticated,
		},
		{
			name:      "transport error",
			queueFail: func(rpc *scriptedRPC) { rpc.fail(io.ErrUnexpectedEOF) },
			want:      "unexpected EOF",
			wantErr:   io.ErrUnexpectedEOF,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rpc := newScriptedRPC()
			cfg := testConfig()
			cfg.Recovery = RecoveryDisabled
			cs := newCoreForTest(testParams(), cfg, &scriptedOpener{rpc: rpc}, nil)

			if _, err := cs.Ingest(context.Background(), []byte(`{}`)); err != nil {
				t.Fatalf("Ingest: %v", err)
			}
			waitCondition(t, func() bool { return len(rpc.sends) == 1 }, time.Second)
			<-rpc.sends
			rpc.pause(time.Second)
			tc.queueFail(rpc)

			waitCondition(t, cs.IsClosed, time.Second)
			err := cs.terminalErr()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("terminal error = %v, want %q", err, tc.want)
			}
			var ps pauseSignal
			if errors.As(err, &ps) {
				t.Fatalf("terminal error = %v, want receiver failure rather than pause", err)
			}
			if tc.wantCode != codes.OK && status.Code(err) != tc.wantCode {
				t.Fatalf("status code = %v, want %v", status.Code(err), tc.wantCode)
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("terminal error = %v, want errors.Is(%v)", err, tc.wantErr)
			}
		})
	}
}

func TestCoreStreamPauseOwnsConcurrentSendFailure(t *testing.T) {
	rpc := newControlledSendRPC()
	cfg := testConfig()
	cfg.Recovery = RecoveryDisabled
	cfg.StreamPausedMaxWait = durationPtr(20 * time.Millisecond)
	cs := newCoreForTest(testParams(), cfg, &controlledSendOpener{rpc: rpc}, nil)
	pauseObserved := make(chan struct{})
	cs.onPauseObserved = func() { close(pauseObserved) }

	if _, err := cs.Ingest(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	select {
	case <-rpc.started:
	case <-time.After(time.Second):
		t.Fatal("Send did not start")
	}
	rpc.closeSignalWithDuration(20 * time.Millisecond)
	select {
	case <-pauseObserved:
	case <-time.After(time.Second):
		t.Fatal("pause was not observed")
	}
	rpc.result <- io.ErrClosedPipe

	waitCondition(t, cs.IsClosed, time.Second)
	if err := cs.terminalErr(); err == nil ||
		!strings.Contains(err.Error(), "server requested pause and recovery is disabled") {
		t.Fatalf("terminal error = %v, want recovery-disabled pause", err)
	}
}

// A real receiver failure after a pause must not be reported as the pause: it has
// to surface to the caller and consume the recovery budget.
func TestCoreStreamReceiverErrorAfterPauseWins(t *testing.T) {
	rpc := newFakeRPC()
	cfg := testConfig()
	cfg.Recovery = RecoveryDisabled
	cfg.StreamPausedMaxWait = durationPtr(2 * time.Second)
	cs := newCoreForTest(testParams(), cfg, newFakeOpener(rpc), nil)
	t.Cleanup(func() { cs.Close() })

	if _, err := cs.Ingest(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc.sends) == 1 }, time.Second)
	<-rpc.sends

	rpc.closeSignalWithDuration(2 * time.Second)
	rpc.malformedAck()

	waitCondition(t, cs.IsClosed, 3*time.Second)
	if err := cs.terminalErr(); err == nil ||
		!strings.Contains(err.Error(), "unusable server response") {
		t.Fatalf("terminal error = %v, want the protocol violation, not the pause", err)
	}
}

// The same masking also bypassed credential invalidation, so a rejected cached
// token could be reused indefinitely.
func TestCoreStreamAuthRejectionAfterPauseInvalidates(t *testing.T) {
	provider := &countingHeadersProvider{}
	params := testParams()
	params.HeadersProvider = provider
	rpc := newTerminalRecvRPC()
	cfg := testConfig()
	cfg.StreamPausedMaxWait = durationPtr(2 * time.Second)
	cs := newCoreForTest(params, cfg, &terminalRecvOpener{rpc: rpc}, nil)
	t.Cleanup(func() { cs.Close() })

	if _, err := cs.Ingest(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc.sends) == 1 }, time.Second)
	<-rpc.sends

	// Let the pause be consumed before injecting the rejection, so Recv does not
	// pick between the two.
	rpc.closeSignalWithDuration(2 * time.Second)
	waitCondition(t, func() bool { return len(rpc.recvs) == 0 }, time.Second)
	rpc.recvErr <- status.Error(codes.Unauthenticated, "credentials rejected")

	waitCondition(t, cs.IsClosed, 2*time.Second)
	if got := provider.invalidations.Load(); got != 1 {
		t.Fatalf("Invalidate calls = %d, want 1", got)
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

func TestCoreStreamIdlePauseHonorsDeadline(t *testing.T) {
	rpc1 := newScriptedRPC()
	rpc2 := newFakeRPC()
	fo := &streamSequenceOpener{streams: []wireStream[encodedMsg, ephemeralResp]{
		transport.NewFakeStreamForTesting(rpc1),
		transport.NewFakeStreamForTesting(rpc2),
	}}
	cs := newCoreForTest(testParams(), testConfig(), fo, nil)
	t.Cleanup(func() { cs.Close() })
	waitStarted := make(chan time.Time, 1)
	releaseWait := make(chan struct{})
	cs.pauseWait = func(ctx context.Context, deadline time.Time) bool {
		waitStarted <- deadline
		select {
		case <-releaseWait:
			return true
		case <-ctx.Done():
			return false
		}
	}

	waitCondition(t, func() bool { return fo.openCount() == 1 }, time.Second)
	rpc1.pause(time.Second)
	var deadline time.Time
	select {
	case deadline = <-waitStarted:
	case <-time.After(time.Second):
		t.Fatal("supervisor did not begin pause wait")
	}
	if time.Until(deadline) <= 0 {
		t.Fatalf("pause deadline %v is not in the future", deadline)
	}
	if got := fo.openCount(); got != 1 {
		t.Fatalf("reopened before pause deadline: %d opens", got)
	}
	close(releaseWait)
	waitCondition(t, func() bool { return fo.openCount() == 2 }, time.Second)
}

func TestCoreStreamCloseInterruptsPauseDeadline(t *testing.T) {
	rpc := newScriptedRPC()
	cs := newCoreForTest(testParams(), testConfig(), &scriptedOpener{rpc: rpc}, nil)

	rpc.pause(time.Hour)
	select {
	case <-rpc.aborted:
	case <-time.After(time.Second):
		t.Fatal("paused connection was not closed")
	}
	done := make(chan struct{})
	go func() {
		cs.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close did not interrupt pause deadline")
	}
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

// A cumulative ack legitimately repeats, so a stale one must not refresh the
// ack-silence budget: otherwise a server re-sending one offset could postpone
// recovery indefinitely while later offsets stay unacknowledged.
func TestCoreStreamStaleAckDoesNotPostponeLackOfAck(t *testing.T) {
	rpc := newGracefulFakeRPC()
	cfg := testConfig()
	cfg.Recovery = RecoveryDisabled
	cfg.LackOfAckTimeout = 150 * time.Millisecond
	cs := newCoreForTest(testParams(), cfg, &gracefulOpener{rpc: rpc}, nil)
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

	// Ack offset 0, then keep re-sending that same ack for longer than the wait
	// below, so the teardown cannot be an artifact of the acks stopping. Offsets 1
	// and 2 stay unacknowledged, so the budget must expire while acks still
	// arrive. ack is a no-op once the stream has torn down.
	rpc.ack(0)
	stopAcks := make(chan struct{})
	acksDone := make(chan struct{})
	go func() {
		defer close(acksDone)
		for {
			select {
			case <-stopAcks:
				return
			case <-time.After(20 * time.Millisecond):
				rpc.ack(0)
			}
		}
	}()
	t.Cleanup(func() {
		close(stopAcks)
		<-acksDone
	})

	waitCondition(t, cs.IsClosed, time.Second)
	if err := cs.terminalErr(); err == nil ||
		!strings.Contains(err.Error(), "no ack from server") {
		t.Fatalf("terminal error = %v, want lack-of-ack teardown", err)
	}
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
	if items, bytes := wireCS.buf.usage(); items != 0 || bytes != 0 {
		t.Fatalf("buffer usage after rejection = (%d, %d), want (0, 0)", items, bytes)
	}
}

func TestCoreStreamPreservesExplicitBufferedByteLimit(t *testing.T) {
	cfg := testConfig()
	cfg.MaxPayloadBytes = 64
	cfg.MaxBufferedPayloadBytes = 1
	cs := newCoreForTest(testParams(), cfg, newFakeOpener(newFakeRPC()), nil)
	t.Cleanup(func() { cs.Close() })

	if cs.cfg.MaxBufferedPayloadBytes != 1 {
		t.Fatalf("buffered byte limit = %d, want 1", cs.cfg.MaxBufferedPayloadBytes)
	}
	if _, err := cs.Ingest(context.Background(), []byte(`{}`)); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("Ingest error = %v, want ErrPayloadTooLarge", err)
	}
}

func TestCoreStreamEncoderErrorReleasesCapacity(t *testing.T) {
	rpc := newFakeRPC()
	cfg := testConfig()
	cfg.MaxInflight = 1
	cs := NewCoreStream[encodedMsg, ephemeralResp](
		testParams(),
		cfg,
		newFakeOpener(rpc),
		failingEncoder{err: errors.New("encode failed")},
		offsetAckModel{},
		nil,
	)
	t.Cleanup(func() { cs.Close() })

	if _, err := cs.Ingest(context.Background(), []byte(`{}`)); err == nil {
		t.Fatal("Ingest succeeded with failing encoder")
	}
	if items, bytes := cs.buf.usage(); items != 0 || bytes != 0 {
		t.Fatalf("usage after encode error = (%d, %d), want (0, 0)", items, bytes)
	}
	// Capacity stays available even after repeated encode failures.
	if _, err := cs.Ingest(context.Background(), []byte(`{}`)); err == nil {
		t.Fatal("second Ingest unexpectedly succeeded with failing encoder")
	}
}

func TestCoreStreamAcceptsPayloadAndBatchAtLimits(t *testing.T) {
	t.Run("encoded payload", func(t *testing.T) {
		record := []byte(`{"at":"limit"}`)
		enc := jsonEncoder{}
		msg, err := enc.encode(record)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}

		rpc := newFakeRPC()
		cfg := testConfig()
		cfg.MaxPayloadBytes = enc.maxWireSize(msg)
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

func TestCoreStreamLogicalOffsetExhaustion(t *testing.T) {
	rpc := newFakeRPC()
	cs := newCoreForTest(testParams(), testConfig(), newFakeOpener(rpc), nil)
	t.Cleanup(func() { cs.Close() })

	cs.offsetMu.Lock()
	cs.nextOffset = math.MaxInt64
	cs.offsetMu.Unlock()

	offset, err := cs.Ingest(context.Background(), []byte(`{"last":true}`))
	if err != nil {
		t.Fatalf("last Ingest: %v", err)
	}
	if offset != math.MaxInt64 {
		t.Fatalf("last offset = %d, want MaxInt64", offset)
	}
	if _, err := cs.Ingest(context.Background(), []byte(`{"overflow":true}`)); !errors.Is(err, ErrOffsetExhausted) {
		t.Fatalf("post-exhaustion Ingest error = %v, want ErrOffsetExhausted", err)
	}
	if got := cs.lastEnqueued.Load(); got != math.MaxInt64 {
		t.Fatalf("lastEnqueued = %d, want MaxInt64", got)
	}

	waitCondition(t, func() bool { return len(rpc.sends) == 1 }, time.Second)
	<-rpc.sends
	rpc.ack(0)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := cs.Flush(ctx); err != nil {
		t.Fatalf("Flush at MaxInt64: %v", err)
	}
}

func TestCallbackDispatcherDoesNotCoalescePastMaxOffset(t *testing.T) {
	cb := &recordingCallback{}
	dispatcher := newCallbackDispatcher(cb)
	dispatcher.enqueueAcks(math.MaxInt64, math.MaxInt64)
	dispatcher.enqueueAcks(math.MinInt64, math.MinInt64)
	dispatcher.shutdown(time.Second)

	if got := cb.ackOffsets(); !slices.Equal(got, []int64{math.MaxInt64, math.MinInt64}) {
		t.Fatalf("callback offsets = %v", got)
	}
}
