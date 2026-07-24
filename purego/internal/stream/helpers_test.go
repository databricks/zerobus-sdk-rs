package stream

import (
	"context"
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

// fakeRPC is a controllable in-process stream.
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

func (f *fakeRPC) CloseSend() error {
	f.close()
	return nil
}

func (f *fakeRPC) close() {
	f.closeOnce.Do(func() {
		f.closed.Store(true)
		close(f.recvs)
	})
}

func (f *fakeRPC) ack(offset int64) {
	f.recvs <- &zerobuspb.EphemeralStreamResponse{
		Payload: &zerobuspb.EphemeralStreamResponse_IngestRecordResponse{
			IngestRecordResponse: &zerobuspb.IngestRecordResponse{
				DurabilityAckUpToOffset: proto.Int64(offset),
			},
		},
	}
}

type controlledSendRPC struct {
	*fakeRPC
	started   chan struct{}
	result    chan error
	aborted   chan struct{}
	startOnce sync.Once
	abortOnce sync.Once
}

func newControlledSendRPC() *controlledSendRPC {
	return &controlledSendRPC{
		fakeRPC: newFakeRPC(),
		started: make(chan struct{}),
		result:  make(chan error, 1),
		aborted: make(chan struct{}),
	}
}

func (f *controlledSendRPC) Send(req *zerobuspb.EphemeralStreamRequest) error {
	f.startOnce.Do(func() { close(f.started) })
	select {
	case err := <-f.result:
		if err != nil {
			return err
		}
		return f.fakeRPC.Send(req)
	case <-f.aborted:
		return io.ErrClosedPipe
	}
}

func (f *controlledSendRPC) Abort() {
	f.abortOnce.Do(func() {
		close(f.aborted)
		f.fakeRPC.close()
	})
}

type controlledSendOpener struct{ rpc *controlledSendRPC }

func (o *controlledSendOpener) Open(_ context.Context, _ transport.StreamParams) (wireStream[encodedMsg, ephemeralResp], error) {
	return transport.NewFakeStreamForTesting(o.rpc), nil
}

// fakeOpener returns configured streams in order.
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

// gracefulFakeRPC distinguishes graceful and abrupt teardown.
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
