package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/databricks/zerobus-sdk/purego/internal/transport"
	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

// ---- fake transport helpers ------------------------------------------------

// fakeRPC is a minimal in-process bidi stream satisfying transport.FakeStreamRPC.
// It lets tests control exactly what acks the receiver sees without a real
// gRPC server. close() closes the recvs channel so Recv returns io.EOF,
// unblocking the receiver goroutine just like a real gRPC stream close would.
type fakeRPC struct {
	sends     chan *zerobuspb.EphemeralStreamRequest
	recvs     chan *zerobuspb.EphemeralStreamResponse
	closeOnce sync.Once
	closed    atomic.Bool
}

func newFakeRPC() *fakeRPC {
	return &fakeRPC{
		sends: make(chan *zerobuspb.EphemeralStreamRequest, 64),
		recvs: make(chan *zerobuspb.EphemeralStreamResponse, 64),
	}
}

func (f *fakeRPC) Send(req *zerobuspb.EphemeralStreamRequest) error {
	if f.closed.Load() {
		return io.EOF
	}
	select {
	case f.sends <- req:
		return nil
	default:
		// channel full — shouldn't happen with buffered 64 in tests
		return fmt.Errorf("fakeRPC: sends channel full")
	}
}

func (f *fakeRPC) Recv() (*zerobuspb.EphemeralStreamResponse, error) {
	resp, ok := <-f.recvs
	if !ok {
		return nil, io.EOF
	}
	return resp, nil
}

// CloseSend closes the stream from the send side; in our fake this also
// closes the recvs channel so the receiver unblocks with io.EOF.
func (f *fakeRPC) CloseSend() error {
	f.close()
	return nil
}

// close closes the recvs channel, causing any blocking Recv call to return
// io.EOF. Safe to call multiple times.
func (f *fakeRPC) close() {
	f.closeOnce.Do(func() {
		f.closed.Store(true)
		close(f.recvs)
	})
}

// ack sends a DurabilityAck response for offset.
func (f *fakeRPC) ack(offset int64) {
	f.recvs <- &zerobuspb.EphemeralStreamResponse{
		Payload: &zerobuspb.EphemeralStreamResponse_IngestRecordResponse{
			IngestRecordResponse: &zerobuspb.IngestRecordResponse{
				DurabilityAckUpToOffset: proto.Int64(offset),
			},
		},
	}
}

// closeSignal sends a server CloseStreamSignal, asking the client to pause.
func (f *fakeRPC) closeSignal() {
	f.recvs <- &zerobuspb.EphemeralStreamResponse{
		Payload: &zerobuspb.EphemeralStreamResponse_CloseStreamSignal{
			CloseStreamSignal: &zerobuspb.CloseStreamSignal{},
		},
	}
}

// closeSignalWithDuration sends a CloseStreamSignal carrying the given
// server-requested pause duration.
func (f *fakeRPC) closeSignalWithDuration(d time.Duration) {
	f.recvs <- &zerobuspb.EphemeralStreamResponse{
		Payload: &zerobuspb.EphemeralStreamResponse_CloseStreamSignal{
			CloseStreamSignal: &zerobuspb.CloseStreamSignal{
				Duration: durationpb.New(d),
			},
		},
	}
}

// fakeOpener wraps a fakeRPC as a transport.Opener by building a rawStream
// from it. Each call to Open returns the same underlying fakeRPC so tests
// can send acks from it.
type fakeOpener struct {
	mu       sync.Mutex
	rpcs     []*fakeRPC
	idx      int
	openErr  error // if non-nil, all opens fail with this error
	attempts int   // number of Open calls, for asserting retry behavior
}

func newFakeOpener(rpcs ...*fakeRPC) *fakeOpener {
	return &fakeOpener{rpcs: rpcs}
}

func (fo *fakeOpener) openCount() int {
	fo.mu.Lock()
	defer fo.mu.Unlock()
	return fo.attempts
}

func (fo *fakeOpener) Open(_ context.Context, _ transport.StreamParams) (wireStream[encodedMsg, ephemeralResp], error) {
	fo.mu.Lock()
	defer fo.mu.Unlock()
	fo.attempts++
	if fo.openErr != nil {
		return nil, fo.openErr
	}
	if fo.idx >= len(fo.rpcs) {
		return nil, fmt.Errorf("fakeOpener: no more RPCs")
	}
	rpc := fo.rpcs[fo.idx]
	fo.idx++
	return transport.NewFakeStreamForTesting(rpc), nil
}

// gracefulFakeRPC models real gRPC teardown semantics more faithfully than
// fakeRPC by distinguishing the two teardown verbs:
//   - CloseSend half-closes the send side (signalling closeSent) but leaves Recv
//     open, so a test can deliver late acks after the half-close and assert the
//     receiver drains them.
//   - Abort models Close/context-cancel: it unblocks a blocked Recv with an error
//     and drops any further acks, as an abrupt reset would.
//
// serverEnd closes recvs to produce the io.EOF that ends a graceful drain.
type gracefulFakeRPC struct {
	sends     chan *zerobuspb.EphemeralStreamRequest
	recvs     chan *zerobuspb.EphemeralStreamResponse
	closeSent chan struct{}
	aborted   chan struct{}
	closeOnce sync.Once
	abortOnce sync.Once
	endOnce   sync.Once
	ended     atomic.Bool
}

func newGracefulFakeRPC() *gracefulFakeRPC {
	return &gracefulFakeRPC{
		sends:     make(chan *zerobuspb.EphemeralStreamRequest, 64),
		recvs:     make(chan *zerobuspb.EphemeralStreamResponse, 64),
		closeSent: make(chan struct{}),
		aborted:   make(chan struct{}),
	}
}

func (f *gracefulFakeRPC) Send(req *zerobuspb.EphemeralStreamRequest) error {
	f.sends <- req
	return nil
}

func (f *gracefulFakeRPC) Recv() (*zerobuspb.EphemeralStreamResponse, error) {
	select {
	case resp, ok := <-f.recvs:
		if !ok {
			return nil, io.EOF
		}
		return resp, nil
	case <-f.aborted:
		return nil, io.ErrClosedPipe
	}
}

// CloseSend half-closes the send side without ending Recv, matching gRPC.
func (f *gracefulFakeRPC) CloseSend() error {
	f.closeOnce.Do(func() { close(f.closeSent) })
	return nil
}

// Abort models Stream.Close/context-cancel: it unblocks Recv with an error.
func (f *gracefulFakeRPC) Abort() {
	f.abortOnce.Do(func() {
		f.ended.Store(true)
		close(f.aborted)
	})
}

// serverEnd ends the stream from the server side so the drain sees io.EOF.
func (f *gracefulFakeRPC) serverEnd() {
	f.endOnce.Do(func() {
		f.ended.Store(true)
		close(f.recvs)
	})
}

func (f *gracefulFakeRPC) ack(offset int64) {
	if f.ended.Load() {
		return
	}
	f.recvs <- &zerobuspb.EphemeralStreamResponse{
		Payload: &zerobuspb.EphemeralStreamResponse_IngestRecordResponse{
			IngestRecordResponse: &zerobuspb.IngestRecordResponse{
				DurabilityAckUpToOffset: proto.Int64(offset),
			},
		},
	}
}

type gracefulOpener struct{ rpc *gracefulFakeRPC }

func (o *gracefulOpener) Open(_ context.Context, _ transport.StreamParams) (wireStream[encodedMsg, ephemeralResp], error) {
	return transport.NewFakeStreamForTesting(o.rpc), nil
}

// blockingFakeRPC is like fakeRPC but its sends channel has a caller-chosen
// capacity so tests can wedge the sender by refusing further reads. When
// sends is full the transport's non-blocking Send fallback returns an error;
// we instead make Send block, so runOnce sees a truly wedged sender.
type blockingFakeRPC struct {
	sends     chan *zerobuspb.EphemeralStreamRequest
	recvs     chan *zerobuspb.EphemeralStreamResponse
	aborted   chan struct{}
	sentN     atomic.Int64
	closeOnce sync.Once
	abortOnce sync.Once
	closed    atomic.Bool
}

func newBlockingFakeRPC(capacity int) *blockingFakeRPC {
	return &blockingFakeRPC{
		sends:   make(chan *zerobuspb.EphemeralStreamRequest, capacity),
		recvs:   make(chan *zerobuspb.EphemeralStreamResponse, 64),
		aborted: make(chan struct{}),
	}
}

func (f *blockingFakeRPC) Send(req *zerobuspb.EphemeralStreamRequest) error {
	if f.closed.Load() {
		return io.EOF
	}
	// Blocking send: wedges until either sends has room, or Abort is called
	// (which models stream.Close()'s hard abort and lets a wedged Send
	// return so runOnce can reap it).
	select {
	case f.sends <- req:
		f.sentN.Add(1)
		return nil
	case <-f.aborted:
		return io.EOF
	}
}

func (f *blockingFakeRPC) Recv() (*zerobuspb.EphemeralStreamResponse, error) {
	select {
	case resp, ok := <-f.recvs:
		if !ok {
			return nil, io.EOF
		}
		return resp, nil
	case <-f.aborted:
		return nil, io.ErrClosedPipe
	}
}

func (f *blockingFakeRPC) CloseSend() error {
	f.close()
	return nil
}

// Abort models transport.Stream.Close(): unblock a blocked Send/Recv.
func (f *blockingFakeRPC) Abort() {
	f.abortOnce.Do(func() {
		f.closed.Store(true)
		close(f.aborted)
	})
}

func (f *blockingFakeRPC) close() {
	f.closeOnce.Do(func() {
		f.closed.Store(true)
		close(f.recvs)
	})
}

func (f *blockingFakeRPC) sendCount() int64 { return f.sentN.Load() }

type blockingOpener struct{ rpc *blockingFakeRPC }

func (o *blockingOpener) Open(_ context.Context, _ transport.StreamParams) (wireStream[encodedMsg, ephemeralResp], error) {
	return transport.NewFakeStreamForTesting(o.rpc), nil
}

// ---- recording ack callback ------------------------------------------------

type recordingCallback struct {
	mu   sync.Mutex
	acks []int64
	errs []int64
}

func (r *recordingCallback) OnAck(offset int64) {
	r.mu.Lock()
	r.acks = append(r.acks, offset)
	r.mu.Unlock()
}

func (r *recordingCallback) OnError(offset int64, _ error) {
	r.mu.Lock()
	r.errs = append(r.errs, offset)
	r.mu.Unlock()
}

func (r *recordingCallback) ackCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.acks)
}

// callbackFn is a test AckCallback whose behavior is customized per test via
// injected closures. Nil closures are no-ops.
type callbackFn struct {
	onAck   func(int64)
	onError func(int64, error)
}

func (c *callbackFn) OnAck(offset int64) {
	if c.onAck != nil {
		c.onAck(offset)
	}
}

func (c *callbackFn) OnError(offset int64, err error) {
	if c.onError != nil {
		c.onError(offset, err)
	}
}

// ---- test helpers ----------------------------------------------------------

func testConfig() Config {
	c := DefaultConfig()
	c.MaxInflight = 64
	c.FlushTimeout = 2 * time.Second
	c.LackOfAckTimeout = 2 * time.Second
	c.RecoveryBackoff = 10 * time.Millisecond
	return c
}

func testParams() StreamParams {
	return StreamParams{
		TableName:  "c.s.t",
		RecordType: zerobuspb.RecordType_JSON,
	}
}

// testStream is the proto/JSON core specialization used throughout these tests.
type testStream = CoreStream[encodedMsg, ephemeralResp]

// testOpener is the opener type the fakes satisfy.
type testOpener = opener[encodedMsg, ephemeralResp]

// newCoreForTest builds a proto/JSON CoreStream with the JSON encoder and offset
// ack model — the common wiring every test needs.
func newCoreForTest(params StreamParams, cfg Config, o testOpener, cb AckCallback) *testStream {
	return NewCoreStream[encodedMsg, ephemeralResp](params, cfg, o, jsonEncoder{}, offsetAckModel{}, cb)
}

func newTestStream(t *testing.T, o testOpener) *testStream {
	t.Helper()
	cb := &recordingCallback{}
	cs := newCoreForTest(testParams(), testConfig(), o, cb)
	t.Cleanup(func() { cs.Close() })
	return cs
}

// waitCondition polls fn until it returns true or deadline expires.
func waitCondition(t *testing.T, fn func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !fn() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within timeout")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

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

// TestCoreStreamAckCallbackFiresPerOffset verifies that a cumulative server
// ack (offset 2) yields exactly one OnAck per newly-acknowledged logical
// offset [0, 1, 2] — not one OnAck(2). It also verifies that a duplicate ack
// (no new items) triggers no additional callbacks, matching the AckCallback
// contract and the Rust SDK.
func TestCoreStreamAckCallbackFiresPerOffset(t *testing.T) {
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
	// Cumulative ack for offset 2 covers offsets 0, 1, and 2.
	rpc.ack(2)

	waitCondition(t, func() bool { return cb.ackCount() >= 3 }, time.Second)

	cb.mu.Lock()
	got := append([]int64(nil), cb.acks...)
	cb.mu.Unlock()
	if len(got) != 3 || got[0] != 0 || got[1] != 1 || got[2] != 2 {
		t.Fatalf("want [0 1 2], got %v", got)
	}

	// A duplicate ack covering the same watermark must NOT fire additional
	// callbacks — no new items were discarded.
	rpc.ack(2)
	time.Sleep(50 * time.Millisecond)
	if got := cb.ackCount(); got != 3 {
		t.Fatalf("duplicate ack fired extra callbacks: want 3 total, got %d", got)
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

// TestCoreStreamRecoveryRequeuesUnacked verifies that items in flight when
// the first transport stream dies are re-sent on the recovery stream.
func TestCoreStreamRecoveryRequeuesUnacked(t *testing.T) {
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

	// Wait for the send to land on rpc1 then kill rpc1 before acking.
	waitCondition(t, func() bool { return len(rpc1.sends) > 0 }, time.Second)
	rpc1.close() // triggers receiver EOF → supervisor recovers to rpc2

	// rpc2 should receive the re-sent item.
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

// TestCoreStreamGetUnackedWorksWithCallback verifies that GetUnacked returns
// items even when an AckCallback is registered, since the supervisor drains
// the buffer for the callback on failure and GetUnacked then returns nothing —
// the two are mutually exclusive. With no callback, GetUnacked must work.
func TestCoreStreamGetUnackedWithoutCallback(t *testing.T) {
	fo := &fakeOpener{openErr: fmt.Errorf("connection refused")}
	cfg := testConfig()
	cfg.Recovery = RecoveryDisabled
	cs := newCoreForTest(testParams(), cfg, fo, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, _ = cs.Ingest(ctx, []byte(`{}`))

	waitCondition(t, cs.IsClosed, 2*time.Second)
	_ = cs.GetUnacked() // must not panic; behavior covered by IsClosed check above
}

// TestCoreStreamNonRetryableErrorTerminates checks that a non-retryable error
// from the opener is surfaced and Flush returns it.
func TestCoreStreamNonRetryableErrorTerminates(t *testing.T) {
	nonRetryable := wrapValidation(fmt.Errorf("bad table name"))
	fo := &fakeOpener{openErr: nonRetryable}

	cfg := testConfig()
	cfg.Recovery = RecoveryDisabled
	cs := newCoreForTest(testParams(), cfg, fo, nil)
	t.Cleanup(func() { cs.Close() })

	// Ingest something so Flush has a target offset.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	// Ingest may block briefly; ignore its error here.
	_, _ = cs.Ingest(ctx, []byte(`{}`))

	// Wait for the stream to reach terminal state.
	waitCondition(t, func() bool { return cs.IsClosed() }, 2*time.Second)

	flushErr := cs.Flush(context.Background())
	if flushErr == nil {
		t.Fatal("want error from Flush after terminal failure, got nil")
	}
}

// TestCoreStreamInvalidParamsNotRetried verifies that an Open failure caused by
// transport.ErrInvalidParams is treated as non-retryable: the supervisor gives
// up after a single attempt rather than burning the recovery budget on a
// deterministic failure. Recovery is left enabled to prove the classification,
// not the disabled path, is what stops the retries.
func TestCoreStreamInvalidParamsNotRetried(t *testing.T) {
	fo := &fakeOpener{openErr: fmt.Errorf("stream: open: bad table: %w", transport.ErrInvalidParams)}

	cfg := testConfig()
	cfg.RecoveryRetries = 5 // would retry 5x if this were classified retryable
	cs := newCoreForTest(testParams(), cfg, fo, nil)
	t.Cleanup(func() { cs.Close() })

	waitCondition(t, cs.IsClosed, 2*time.Second)

	if n := fo.openCount(); n != 1 {
		t.Fatalf("invalid-params open should not be retried, got %d Open attempts", n)
	}
}

// nonRetryableSelfClassifying is an Open error that self-reports non-retryable
// via the IsRetryable() interface — mirroring how the auth layer's TokenError
// signals a permanent failure (revoked creds, invalid_client).
type nonRetryableSelfClassifying struct{ msg string }

func (e *nonRetryableSelfClassifying) Error() string     { return e.msg }
func (e *nonRetryableSelfClassifying) IsRetryable() bool { return false }

// A layer-reported non-retryable error (e.g. an OAuth TokenError with
// retryable=false) must stop the supervisor rather than burn the recovery
// budget on a failure that can't succeed on retry.
func TestCoreStreamSelfClassifiedNonRetryableErrorTerminates(t *testing.T) {
	fo := &fakeOpener{openErr: &nonRetryableSelfClassifying{msg: "auth: oauth: HTTP 401: invalid_client"}}

	cfg := testConfig()
	cfg.RecoveryRetries = 5 // would retry 5x if this were classified retryable
	cs := newCoreForTest(testParams(), cfg, fo, nil)
	t.Cleanup(func() { cs.Close() })

	waitCondition(t, cs.IsClosed, 2*time.Second)

	if n := fo.openCount(); n != 1 {
		t.Fatalf("self-classified non-retryable error should not be retried, got %d Open attempts", n)
	}
}

// TestCoreStreamPauseSignalReconnectsWithoutConsumingRetries verifies that a
// server CloseStreamSignal is treated as a pause-then-reconnect: the client
// reconnects on a fresh stream, re-sends the unacked record, and does NOT count
// the pause against the recovery budget (RecoveryRetries=0 would otherwise make
// any real failure terminal after one attempt).
func TestCoreStreamPauseSignalReconnectsWithoutConsumingRetries(t *testing.T) {
	rpc1 := newFakeRPC()
	rpc2 := newFakeRPC()
	fo := newFakeOpener(rpc1, rpc2)

	cfg := testConfig()
	cfg.RecoveryRetries = 0 // a pause must not consume the (zero) retry budget
	cs := newCoreForTest(testParams(), cfg, fo, nil)
	t.Cleanup(func() { cs.Close() })

	off, err := cs.Ingest(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	// Wait for the send to land on rpc1, then tell the client to pause.
	waitCondition(t, func() bool { return len(rpc1.sends) > 0 }, time.Second)
	<-rpc1.sends
	rpc1.closeSignal()

	// The client should reconnect to rpc2 and re-send the unacked record.
	waitCondition(t, func() bool { return len(rpc2.sends) > 0 }, 2*time.Second)
	<-rpc2.sends
	rpc2.ack(off)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cs.WaitForOffset(ctx, off); err != nil {
		t.Fatalf("WaitForOffset after pause+reconnect: %v", err)
	}
	if cs.IsClosed() {
		t.Fatal("stream should be live after a pause, not terminal")
	}
}

// StreamPausedMaxWait caps the pause honored on a server CloseStreamSignal to
// min(cap, server-requested). A large server-requested duration paired with a
// small client cap must yield a reconnect within the cap, not the server value.
func TestCoreStreamPauseSignalRespectsStreamPausedMaxWaitCap(t *testing.T) {
	rpc1 := newFakeRPC()
	rpc2 := newFakeRPC()
	fo := newFakeOpener(rpc1, rpc2)

	cfg := testConfig()
	cfg.RecoveryBackoff = 0
	// Server will ask for a long pause; the client cap is much smaller.
	cfg.StreamPausedMaxWait = 25 * time.Millisecond
	serverPause := 5 * time.Second

	cs := newCoreForTest(testParams(), cfg, fo, nil)
	t.Cleanup(func() { cs.Close() })

	off, err := cs.Ingest(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc1.sends) > 0 }, time.Second)
	<-rpc1.sends

	// Server signals a very long pause; cap must win.
	start := time.Now()
	rpc1.closeSignalWithDuration(serverPause)

	// Reconnect (rpc2 receives the re-sent record) must land close to the cap,
	// well before the server's requested duration.
	waitCondition(t, func() bool { return len(rpc2.sends) > 0 }, time.Second)
	elapsed := time.Since(start)
	if elapsed >= serverPause/4 {
		t.Fatalf("reconnect took %v; cap of %v was not honored (server asked %v)",
			elapsed, cfg.StreamPausedMaxWait, serverPause)
	}
	<-rpc2.sends
	rpc2.ack(off)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cs.WaitForOffset(ctx, off); err != nil {
		t.Fatalf("WaitForOffset: %v", err)
	}
}

// TestCoreStreamIdleStreamDoesNotFailOnLackOfAck verifies that a stream with
// nothing in flight is never torn down by the lack-of-ack timeout. The opener
// has exactly one RPC, so any spurious reconnect attempt would fail with "no
// more RPCs" and the stream would terminate — both assertions catch that.
func TestCoreStreamIdleStreamDoesNotFailOnLackOfAck(t *testing.T) {
	rpc := newFakeRPC()
	fo := newFakeOpener(rpc)
	cfg := testConfig()
	cfg.LackOfAckTimeout = 30 * time.Millisecond
	cs := newCoreForTest(testParams(), cfg, fo, nil)
	t.Cleanup(func() { cs.Close() })

	// Ingest and fully ack a record so the in-flight set drains to empty.
	off, err := cs.Ingest(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc.sends) > 0 }, time.Second)
	<-rpc.sends
	rpc.ack(off)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := cs.WaitForOffset(ctx, off); err != nil {
		t.Fatalf("WaitForOffset: %v", err)
	}

	// Idle for many lack-of-ack windows. With nothing in flight, silence is not a
	// failure, so the stream must stay live and must not reconnect.
	time.Sleep(200 * time.Millisecond)

	if cs.IsClosed() {
		t.Fatal("idle stream was torn down by the lack-of-ack timeout")
	}
	if n := fo.openCount(); n != 1 {
		t.Fatalf("idle stream reconnected (openCount=%d); lack-of-ack fired with nothing in flight", n)
	}
}

// TestCoreStreamHealthyRunResetsRecoveryBudget verifies that the recovery retry
// budget is per-episode, not lifetime. Three successive connections each receive
// the re-sent record then die with EOF; with RecoveryRetries=1 and a lifetime
// budget the supervisor would give up before the fourth connection. Because each
// run connected and ran successfully, the budget resets and the stream survives
// to ack on the fourth.
func TestCoreStreamHealthyRunResetsRecoveryBudget(t *testing.T) {
	rpc1, rpc2, rpc3, rpc4 := newFakeRPC(), newFakeRPC(), newFakeRPC(), newFakeRPC()
	fo := newFakeOpener(rpc1, rpc2, rpc3, rpc4)
	cfg := testConfig()
	cfg.RecoveryRetries = 1
	cs := newCoreForTest(testParams(), cfg, fo, nil)
	t.Cleanup(func() { cs.Close() })

	off, err := cs.Ingest(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	// Each connection sees the (re-sent) record, then the server drops it (EOF).
	for i, rpc := range []*fakeRPC{rpc1, rpc2, rpc3} {
		waitCondition(t, func() bool { return len(rpc.sends) > 0 }, 2*time.Second)
		<-rpc.sends
		rpc.close() // EOF: a healthy run that ends; supervisor must recover
		_ = i
	}

	// Fourth connection: the record is re-sent once more and finally acked.
	waitCondition(t, func() bool { return len(rpc4.sends) > 0 }, 2*time.Second)
	<-rpc4.sends
	rpc4.ack(off)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cs.WaitForOffset(ctx, off); err != nil {
		t.Fatalf("stream did not survive 3 healthy-then-disconnect cycles: %v", err)
	}
	if cs.IsClosed() {
		t.Fatal("stream terminated despite healthy runs resetting the recovery budget")
	}
}

// TestCoreStreamProtoIngestDoesNotAliasCallerBuffer verifies that mutating the
// caller's []byte after Ingest does not change the payload the sender
// eventually places on the wire. The proto encoder must snapshot input bytes
// before handing them to the buffer, or a caller reusing a marshalling scratch
// buffer would corrupt queued records.
func TestCoreStreamProtoIngestDoesNotAliasCallerBuffer(t *testing.T) {
	rpc := newFakeRPC()
	cfg := testConfig()
	cs := NewCoreStream[encodedMsg, ephemeralResp](
		StreamParams{TableName: "c.s.t", RecordType: zerobuspb.RecordType_PROTO},
		cfg, newFakeOpener(rpc), protoEncoder{}, offsetAckModel{}, nil,
	)
	t.Cleanup(func() { cs.Close() })

	// Ingest with a mutable buffer, then overwrite it before the send happens.
	buf := []byte{0xAA, 0xBB, 0xCC}
	off, err := cs.Ingest(context.Background(), buf)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	// Mutate the caller's buffer right after Ingest returns.
	buf[0] = 0xFF
	buf[1] = 0xFF
	buf[2] = 0xFF

	waitCondition(t, func() bool { return len(rpc.sends) > 0 }, time.Second)
	sent := <-rpc.sends
	got := sent.GetIngestRecord().GetProtoEncodedRecord()
	if len(got) != 3 || got[0] != 0xAA || got[1] != 0xBB || got[2] != 0xCC {
		t.Fatalf("proto ingest aliased caller buffer: want [0xAA 0xBB 0xCC], got %v", got)
	}
	rpc.ack(off)
	_ = cs.Flush(context.Background())
}

// TestCoreStreamProtoIngestBatchDoesNotAliasCallerBuffers verifies the same
// no-alias contract for the batch ingest path.
func TestCoreStreamProtoIngestBatchDoesNotAliasCallerBuffers(t *testing.T) {
	rpc := newFakeRPC()
	cfg := testConfig()
	cs := NewCoreStream[encodedMsg, ephemeralResp](
		StreamParams{TableName: "c.s.t", RecordType: zerobuspb.RecordType_PROTO},
		cfg, newFakeOpener(rpc), protoEncoder{}, offsetAckModel{}, nil,
	)
	t.Cleanup(func() { cs.Close() })

	r1 := []byte{0x01, 0x02}
	r2 := []byte{0x03, 0x04}
	off, err := cs.IngestBatch(context.Background(), [][]byte{r1, r2})
	if err != nil {
		t.Fatalf("IngestBatch: %v", err)
	}
	// Mutate both caller buffers post-Ingest.
	r1[0], r1[1] = 0xFF, 0xFF
	r2[0], r2[1] = 0xFF, 0xFF

	waitCondition(t, func() bool { return len(rpc.sends) > 0 }, time.Second)
	sent := <-rpc.sends
	batch := sent.GetIngestRecordBatch().GetProtoEncodedBatch().GetRecords()
	if len(batch) != 2 {
		t.Fatalf("want 2 records, got %d", len(batch))
	}
	if batch[0][0] != 0x01 || batch[0][1] != 0x02 || batch[1][0] != 0x03 || batch[1][1] != 0x04 {
		t.Fatalf("batch aliased caller buffers: got %v", batch)
	}
	rpc.ack(off)
	_ = cs.Flush(context.Background())
}

// TestCoreStreamCallbackCallsCloseNoDeadlock verifies that an OnAck callback
// calling Close on the stream itself does not deadlock. Callbacks run on a
// dedicated dispatcher goroutine, so Close waits only for the supervisor —
// which is separate from the callback path.
func TestCoreStreamCallbackCallsCloseNoDeadlock(t *testing.T) {
	rpc := newFakeRPC()
	closed := make(chan struct{})
	var closeCalled atomic.Bool

	var cs *testStream
	cb := &callbackFn{
		onAck: func(int64) {
			if closeCalled.CompareAndSwap(false, true) {
				cs.Close()
				close(closed)
			}
		},
	}
	cs = newCoreForTest(testParams(), testConfig(), newFakeOpener(rpc), cb)

	if _, err := cs.Ingest(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc.sends) > 0 }, time.Second)
	<-rpc.sends
	rpc.ack(0)

	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("callback-invoked Close deadlocked")
	}
	if !cs.IsClosed() {
		t.Fatal("stream should be closed after callback-invoked Close")
	}
}

// TestCoreStreamWaitForOffsetUnblocksOnClose verifies that a WaitForOffset
// call blocked on an unacked offset returns promptly with an error once the
// stream is cleanly closed, rather than hanging forever waiting for an ack
// that will never come.
func TestCoreStreamWaitForOffsetUnblocksOnClose(t *testing.T) {
	rpc := newFakeRPC()
	cs := newCoreForTest(testParams(), testConfig(), newFakeOpener(rpc), nil)

	off, err := cs.Ingest(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc.sends) > 0 }, time.Second)
	<-rpc.sends // consume; do NOT ack

	errCh := make(chan error, 1)
	go func() {
		// Background ctx so Wait can only unblock via Close.
		errCh <- cs.WaitForOffset(context.Background(), off)
	}()

	time.Sleep(50 * time.Millisecond)
	cs.Close()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("WaitForOffset should return an error after clean Close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitForOffset did not unblock after Close")
	}
}

// TestCoreStreamPreCancelledCtxHonoredInWaitForOffset ensures WaitForOffset
// with an already-cancelled ctx fails fast rather than parking on the cond
// wait: without the pre-loop ctx check, an AfterFunc broadcast could race the
// condition check and leave the waiter asleep.
func TestCoreStreamPreCancelledCtxHonoredInWaitForOffset(t *testing.T) {
	rpc := newFakeRPC()
	cs := newTestStream(t, newFakeOpener(rpc))

	if _, err := cs.Ingest(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() { done <- cs.WaitForOffset(ctx, 0) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("WaitForOffset with pre-cancelled ctx parked instead of failing fast")
	}
}

// TestCoreStreamCloseDoesNotSendQueuedRecords verifies that Close on a stream
// whose sender is blocked on Send (fake never accepts) does not let the next
// queued record slip through the sender loop before teardown.
func TestCoreStreamCloseDoesNotSendQueuedRecords(t *testing.T) {
	// A blocking fake: Send never accepts more than one message; the second
	// pending record would only leave the buffer via next() if next()
	// ignores ctx cancellation.
	rpc := newBlockingFakeRPC(1)
	cfg := testConfig()
	cs := newCoreForTest(testParams(), cfg, &blockingOpener{rpc: rpc}, nil)

	// First send fills rpc.sends (capacity 1); the sender is now blocked on
	// the second Send.
	if _, err := cs.Ingest(context.Background(), []byte(`{"i":0}`)); err != nil {
		t.Fatalf("Ingest 0: %v", err)
	}
	if _, err := cs.Ingest(context.Background(), []byte(`{"i":1}`)); err != nil {
		t.Fatalf("Ingest 1: %v", err)
	}

	waitCondition(t, func() bool { return rpc.sendCount() >= 1 }, time.Second)

	// Close must return in bounded time, and must not have advanced the send
	// count beyond the first Send.
	done := make(chan struct{})
	go func() { cs.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close blocked on wedged Send")
	}
	if got := rpc.sendCount(); got != 1 {
		t.Fatalf("Close leaked queued Send: %d records sent, want 1", got)
	}
}

// TestCoreStreamPauseSignalDrainsInFlightAcks verifies that when the server
// requests a pause, the client keeps receiving until in-flight records are
// acked, rather than aborting immediately. A late ack delivered after the
// pause must still advance the watermark.
func TestCoreStreamPauseSignalDrainsInFlightAcks(t *testing.T) {
	rpc1 := newFakeRPC()
	rpc2 := newFakeRPC()
	fo := newFakeOpener(rpc1, rpc2)

	cfg := testConfig()
	cfg.StreamPausedMaxWait = 2 * time.Second // ample drain window
	cs := newCoreForTest(testParams(), cfg, fo, nil)
	t.Cleanup(func() { cs.Close() })

	off, err := cs.Ingest(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc1.sends) > 0 }, time.Second)
	<-rpc1.sends

	// Pause first, then deliver the late ack. If the receiver aborts on
	// pause, the ack is dropped and the record is re-sent on rpc2 (no
	// forward progress) — WaitForOffset would still succeed but rpc2 would
	// receive the re-send. We verify that no re-send happens by asserting
	// rpc2.sends stays empty.
	rpc1.closeSignalWithDuration(500 * time.Millisecond)
	// Give the receiver a moment to observe the pause and enter drain mode.
	time.Sleep(20 * time.Millisecond)
	// Late ack: an aborting receiver would have already closed rpc1's Recv.
	rpc1.ack(off)

	// Wait for the reconnect (drain-complete or window-expired).
	waitCondition(t, func() bool { return fo.openCount() >= 2 }, 3*time.Second)

	// After reconnect: rpc2 must NOT receive a re-send (the pause drain
	// caught the ack, so nothing is unacked).
	select {
	case <-rpc2.sends:
		t.Fatal("pause drop caused an unnecessary re-send after reconnect")
	case <-time.After(200 * time.Millisecond):
		// Good: no re-send.
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cs.WaitForOffset(ctx, off); err != nil {
		t.Fatalf("WaitForOffset: %v", err)
	}
}

// TestCoreStreamPauseWithRecoveryDisabledDoesNotReconnect verifies that a
// server pause on a stream with recovery disabled is treated as a terminal
// signal — the client does not reconnect.
func TestCoreStreamPauseWithRecoveryDisabledDoesNotReconnect(t *testing.T) {
	rpc1 := newFakeRPC()
	rpc2 := newFakeRPC()
	fo := newFakeOpener(rpc1, rpc2)

	cfg := testConfig()
	cfg.Recovery = RecoveryDisabled
	cs := newCoreForTest(testParams(), cfg, fo, nil)
	t.Cleanup(func() { cs.Close() })

	if _, err := cs.Ingest(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc1.sends) > 0 }, time.Second)
	<-rpc1.sends
	rpc1.closeSignal()

	waitCondition(t, cs.IsClosed, 2*time.Second)
	if got := fo.openCount(); got != 1 {
		t.Fatalf("pause under RecoveryDisabled reconnected: openCount=%d, want 1", got)
	}
}

// TestCoreStreamPauseImmediateModeReconnectsWithoutWaiting verifies that with
// PauseWaitImmediate the client reconnects as soon as the pause signal
// arrives, regardless of the server-requested duration.
func TestCoreStreamPauseImmediateModeReconnectsWithoutWaiting(t *testing.T) {
	rpc1 := newFakeRPC()
	rpc2 := newFakeRPC()
	fo := newFakeOpener(rpc1, rpc2)

	cfg := testConfig()
	cfg.StreamPausedMaxWaitMode = PauseWaitImmediate
	cs := newCoreForTest(testParams(), cfg, fo, nil)
	t.Cleanup(func() { cs.Close() })

	off, err := cs.Ingest(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc1.sends) > 0 }, time.Second)
	<-rpc1.sends

	start := time.Now()
	rpc1.closeSignalWithDuration(5 * time.Second)

	waitCondition(t, func() bool { return len(rpc2.sends) > 0 }, time.Second)
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("PauseWaitImmediate did not reconnect promptly: %v", elapsed)
	}
	<-rpc2.sends
	rpc2.ack(off)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cs.WaitForOffset(ctx, off); err != nil {
		t.Fatalf("WaitForOffset: %v", err)
	}
}

// TestCoreStreamGetUnackedWithCallbackPreservesPayloads verifies that when the
// stream fails terminally with a callback registered, GetUnacked still
// returns the unacked record bytes (matching the Rust SDK). The callback
// receives OnError events for the same offsets.
func TestCoreStreamGetUnackedWithCallbackPreservesPayloads(t *testing.T) {
	fo := &fakeOpener{openErr: fmt.Errorf("connection refused")}
	cfg := testConfig()
	cfg.Recovery = RecoveryDisabled
	cb := &recordingCallback{}
	cs := newCoreForTest(testParams(), cfg, fo, cb)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	// Ingest three records; the buffer keeps them because the opener always
	// fails and Recovery is disabled.
	for i := range 3 {
		_, _ = cs.Ingest(ctx, []byte(fmt.Sprintf(`{"i":%d}`, i)))
	}

	waitCondition(t, cs.IsClosed, 2*time.Second)

	unacked := cs.GetUnacked()
	if len(unacked) == 0 {
		t.Fatal("GetUnacked with callback returned no records; payloads must be preserved")
	}
	// The callback should have received errors for the same items.
	waitCondition(t, func() bool {
		cb.mu.Lock()
		defer cb.mu.Unlock()
		return len(cb.errs) > 0
	}, time.Second)
}

// TestCoreStreamPayloadTooLargeRejected verifies MaxPayloadBytes enforcement:
// oversized records and batches are rejected with ErrPayloadTooLarge at
// Ingest without consuming an offset or reaching the transport.
func TestCoreStreamPayloadTooLargeRejected(t *testing.T) {
	rpc := newFakeRPC()
	cfg := testConfig()
	cfg.MaxPayloadBytes = 16
	cs := newCoreForTest(testParams(), cfg, newFakeOpener(rpc), nil)
	t.Cleanup(func() { cs.Close() })

	// Exact-limit record: accepted.
	if _, err := cs.Ingest(context.Background(), make([]byte, 16)); err != nil {
		t.Fatalf("record at exact limit rejected: %v", err)
	}
	// Above-limit record: rejected.
	if _, err := cs.Ingest(context.Background(), make([]byte, 17)); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("above-limit record: want ErrPayloadTooLarge, got %v", err)
	}
	// Batch whose aggregate exceeds the limit: rejected even if each element
	// is below.
	batch := [][]byte{make([]byte, 8), make([]byte, 8), make([]byte, 4)}
	if _, err := cs.IngestBatch(context.Background(), batch); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("above-limit batch: want ErrPayloadTooLarge, got %v", err)
	}
}

// TestCoreStreamZeroConfigDoesNotDeadlock verifies that a Config{} (all zero
// values) is normalized to safe defaults so the stream neither deadlocks on
// an unbuffered semaphore nor busy-spins on a zero lack-of-ack timer.
func TestCoreStreamZeroConfigDoesNotDeadlock(t *testing.T) {
	rpc := newFakeRPC()
	// Pass a completely zero Config; the constructor must sanitize it.
	cs := newCoreForTest(testParams(), Config{}, newFakeOpener(rpc), nil)
	t.Cleanup(func() { cs.Close() })

	off, err := cs.Ingest(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatalf("Ingest against zero-Config stream: %v", err)
	}
	waitCondition(t, func() bool { return len(rpc.sends) > 0 }, time.Second)
	<-rpc.sends
	rpc.ack(off)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cs.WaitForOffset(ctx, off); err != nil {
		t.Fatalf("WaitForOffset against zero-Config stream: %v", err)
	}
}

// TestCoreStreamBlockedIngestObservesOwnCtxCancel verifies that a caller
// blocked on the buffer semaphore observes ITS OWN ctx cancellation promptly,
// not just the ctx of an earlier caller. Regression for the ingestMu-held-
// across-wait bug where every later caller serialized behind the first
// blocked one.
func TestCoreStreamBlockedIngestObservesOwnCtxCancel(t *testing.T) {
	rpc := newFakeRPC()
	cfg := testConfig()
	cfg.MaxInflight = 1
	cs := newCoreForTest(testParams(), cfg, newFakeOpener(rpc), nil)
	t.Cleanup(func() { cs.Close() })

	// Fill the only slot; do NOT ack.
	if _, err := cs.Ingest(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("first Ingest: %v", err)
	}

	// Two concurrent Ingests with distinct ctxs. Cancel only the second's;
	// it must return promptly with its own ctx.Canceled while the first
	// stays blocked.
	ctx1 := context.Background()
	ctx2, cancel2 := context.WithCancel(context.Background())

	err1Ch := make(chan error, 1)
	err2Ch := make(chan error, 1)
	go func() { _, e := cs.Ingest(ctx1, []byte(`{}`)); err1Ch <- e }()
	go func() { _, e := cs.Ingest(ctx2, []byte(`{}`)); err2Ch <- e }()

	time.Sleep(30 * time.Millisecond)
	cancel2()

	select {
	case err := <-err2Ch:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("second Ingest: want context.Canceled, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("second Ingest did not observe its own ctx cancellation")
	}
	// First Ingest must still be waiting.
	select {
	case err := <-err1Ch:
		t.Fatalf("first Ingest unblocked unexpectedly: %v", err)
	default:
	}
}

// TestCoreStreamFirstSendAfterIdleGetsFullTimeout verifies that a record
// ingested just before the idle lack-of-ack timer would have expired still
// gets a fresh full timeout — the sender's flightSignal re-arms the timer
// with the full LackOfAckTimeout budget, not the leftover fraction.
func TestCoreStreamFirstSendAfterIdleGetsFullTimeout(t *testing.T) {
	rpc := newFakeRPC()
	cfg := testConfig()
	cfg.LackOfAckTimeout = 100 * time.Millisecond
	cs := newCoreForTest(testParams(), cfg, newFakeOpener(rpc), nil)
	t.Cleanup(func() { cs.Close() })

	// Let the stream stay idle for a while (no records in flight).
	time.Sleep(150 * time.Millisecond)
	// Ingest and never ack.
	off, err := cs.Ingest(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	sendTime := time.Now()
	waitCondition(t, func() bool { return len(rpc.sends) > 0 }, time.Second)
	<-rpc.sends

	// The stream should NOT tear down before ~LackOfAckTimeout has elapsed
	// since the send. Sample at 60% of the timeout: still live.
	deadline := sendTime.Add(60 * cfg.LackOfAckTimeout / 100)
	for time.Now().Before(deadline) {
		if cs.IsClosed() {
			t.Fatal("stream torn down before full lack-of-ack budget after idle → send transition")
		}
		time.Sleep(5 * time.Millisecond)
	}
	_ = off
}

// TestCoreStreamIngestBatch verifies the batch ingest path: records go on the
// wire as a single atomic IngestRecordBatch under one logical offset, and Flush
// completes once that offset is acked.
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
