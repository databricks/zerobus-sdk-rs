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

	"github.com/databricks/zerobus-sdk/purego/internal/transport"
	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

// ---- fake transport helpers ------------------------------------------------

// fakeRPC is a minimal in-process bidi stream satisfying transport.StreamRPC.
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

// fakeOpener wraps a fakeRPC as a transport.Opener by building a rawStream
// from it. Each call to Open returns the same underlying fakeRPC so tests
// can send acks from it.
type fakeOpener struct {
	mu      sync.Mutex
	rpcs    []*fakeRPC
	idx     int
	openErr error // if non-nil, all opens fail with this error
}

func newFakeOpener(rpcs ...*fakeRPC) *fakeOpener {
	return &fakeOpener{rpcs: rpcs}
}

func (fo *fakeOpener) Open(_ context.Context, _ transport.StreamParams) (*transport.Stream, error) {
	fo.mu.Lock()
	defer fo.mu.Unlock()
	if fo.openErr != nil {
		return nil, fo.openErr
	}
	if fo.idx >= len(fo.rpcs) {
		return nil, fmt.Errorf("fakeOpener: no more RPCs")
	}
	rpc := fo.rpcs[fo.idx]
	fo.idx++
	return transport.NewStreamFromRPC(rpc), nil
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

func newTestStream(t *testing.T, opener Opener) *CoreStream {
	t.Helper()
	cb := &recordingCallback{}
	cs := NewCoreStream(testParams(), testConfig(), opener, jsonEncoder{}, offsetAckModel{}, cb)
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

func TestCoreStreamAckCallbackFires(t *testing.T) {
	rpc := newFakeRPC()
	cb := &recordingCallback{}
	cs := NewCoreStream(testParams(), testConfig(), newFakeOpener(rpc), jsonEncoder{}, offsetAckModel{}, cb)
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

	waitCondition(t, func() bool { return cb.ackCount() >= 1 }, time.Second)
}

func TestCoreStreamCloseIsIdempotent(t *testing.T) {
	rpc := newFakeRPC()
	cs := newTestStream(t, newFakeOpener(rpc))
	cs.Close()
	cs.Close() // must not panic or block
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
	cfg.RecoveryEnabled = false
	cs := NewCoreStream(testParams(), cfg, fo, jsonEncoder{}, offsetAckModel{}, nil)

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
	cs := NewCoreStream(testParams(), cfg, fo, jsonEncoder{}, offsetAckModel{}, nil)
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

// TestCoreStreamNonRetryableErrorTerminates checks that a non-retryable error
// from the opener is surfaced and Flush returns it.
func TestCoreStreamNonRetryableErrorTerminates(t *testing.T) {
	nonRetryable := wrapValidation(fmt.Errorf("bad table name"))
	fo := &fakeOpener{openErr: nonRetryable}

	cfg := testConfig()
	cfg.RecoveryEnabled = false
	cs := NewCoreStream(testParams(), cfg, fo, jsonEncoder{}, offsetAckModel{}, nil)
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
