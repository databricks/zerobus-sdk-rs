package stream

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

type controlledSendRPC struct {
	*fakeRPC
	started   chan struct{}
	result    chan error
	aborted   chan struct{}
	pauseRead chan struct{}
	startOnce sync.Once
	abortOnce sync.Once
}

func newControlledSendRPC() *controlledSendRPC {
	return &controlledSendRPC{
		fakeRPC:   newFakeRPC(),
		started:   make(chan struct{}),
		result:    make(chan error, 1),
		aborted:   make(chan struct{}),
		pauseRead: make(chan struct{}, 1),
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

func (f *controlledSendRPC) Recv() (*zerobuspb.EphemeralStreamResponse, error) {
	resp, err := f.fakeRPC.Recv()
	if resp != nil && resp.GetCloseStreamSignal() != nil {
		select {
		case f.pauseRead <- struct{}{}:
		default:
		}
	}
	return resp, err
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

// blockedSendTerminalRPC keeps Send blocked until Abort while allowing Recv to
// report an authoritative server failure first.
type blockedSendTerminalRPC struct {
	*controlledSendRPC
	recvErr chan error
}

func newBlockedSendTerminalRPC() *blockedSendTerminalRPC {
	return &blockedSendTerminalRPC{
		controlledSendRPC: newControlledSendRPC(),
		recvErr:           make(chan error, 1),
	}
}

func (f *blockedSendTerminalRPC) Recv() (*zerobuspb.EphemeralStreamResponse, error) {
	select {
	case resp, ok := <-f.recvs:
		if !ok {
			return nil, io.EOF
		}
		return resp, nil
	case err := <-f.recvErr:
		return nil, err
	case <-f.aborted:
		return nil, io.ErrClosedPipe
	}
}

type blockedSendTerminalOpener struct{ rpc *blockedSendTerminalRPC }

func (o *blockedSendTerminalOpener) Open(_ context.Context, _ transport.StreamParams) (wireStream[encodedMsg, ephemeralResp], error) {
	return transport.NewFakeStreamForTesting(o.rpc), nil
}

// authOnCloseStream models a live Recv whose authentication rejection becomes
// observable while Close is tearing the connection down.
type authOnCloseStream struct {
	recvStarted chan struct{}
	closed      chan struct{}
	startOnce   sync.Once
	closeOnce   sync.Once
}

func newAuthOnCloseStream() *authOnCloseStream {
	return &authOnCloseStream{
		recvStarted: make(chan struct{}),
		closed:      make(chan struct{}),
	}
}

func (*authOnCloseStream) ServerID() string { return "auth-on-close" }

func (*authOnCloseStream) Send(encodedMsg) error { return nil }

func (s *authOnCloseStream) Recv() (ephemeralResp, error) {
	s.startOnce.Do(func() { close(s.recvStarted) })
	<-s.closed
	return nil, status.Error(codes.Unauthenticated, "expired credentials")
}

func (*authOnCloseStream) CloseSend() error { return io.ErrClosedPipe }

func (s *authOnCloseStream) Close() {
	s.closeOnce.Do(func() { close(s.closed) })
}

type authOnCloseOpener struct{ stream *authOnCloseStream }

func (o *authOnCloseOpener) Open(_ context.Context, _ transport.StreamParams) (wireStream[encodedMsg, ephemeralResp], error) {
	return o.stream, nil
}

type reconnectControlledOpener struct {
	mu     sync.Mutex
	first  *fakeRPC
	second *controlledSendRPC
	opens  int
}

func (o *reconnectControlledOpener) Open(_ context.Context, _ transport.StreamParams) (wireStream[encodedMsg, ephemeralResp], error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.opens++
	if o.opens == 1 {
		return transport.NewFakeStreamForTesting(o.first), nil
	}
	if o.opens == 2 {
		return transport.NewFakeStreamForTesting(o.second), nil
	}
	return nil, fmt.Errorf("reconnectControlledOpener: no more RPCs")
}

// malformedAck sends an IngestRecordResponse with no offset field set — a
// protocol violation the ack model classifies as malformedResponse.
func (f *fakeRPC) malformedAck() {
	f.recvs <- &zerobuspb.EphemeralStreamResponse{
		Payload: &zerobuspb.EphemeralStreamResponse_IngestRecordResponse{
			IngestRecordResponse: &zerobuspb.IngestRecordResponse{},
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
	mu        sync.Mutex
	rpcs      []*fakeRPC
	serverIDs []string
	idx       int
	openErr   error // if non-nil, all opens fail with this error
	attempts  int   // number of Open calls, for asserting retry behavior
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
	serverID := "fake-stream"
	if fo.idx < len(fo.serverIDs) {
		serverID = fo.serverIDs[fo.idx]
	}
	fo.idx++
	return &serverIDStream{
		wireStream: transport.NewFakeStreamForTesting(rpc),
		serverID:   serverID,
	}, nil
}

type serverIDStream struct {
	wireStream[encodedMsg, ephemeralResp]
	serverID string
}

func (s *serverIDStream) ServerID() string {
	return s.serverID
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

// blockedSendRPC models a transport Send that cannot return until Stream.Close
// cancels the RPC. It verifies that Close observes its own context cancellation
// instead of waiting forever for a worker to report on errCh.
type blockedSendRPC struct {
	sendStarted chan struct{}
	aborted     chan struct{}
	startOnce   sync.Once
	abortOnce   sync.Once
}

func newBlockedSendRPC() *blockedSendRPC {
	return &blockedSendRPC{
		sendStarted: make(chan struct{}),
		aborted:     make(chan struct{}),
	}
}

func (f *blockedSendRPC) Send(*zerobuspb.EphemeralStreamRequest) error {
	f.startOnce.Do(func() { close(f.sendStarted) })
	<-f.aborted
	return io.ErrClosedPipe
}

func (f *blockedSendRPC) Recv() (*zerobuspb.EphemeralStreamResponse, error) {
	<-f.aborted
	return nil, io.ErrClosedPipe
}

func (f *blockedSendRPC) CloseSend() error { return nil }

func (f *blockedSendRPC) Abort() {
	f.abortOnce.Do(func() { close(f.aborted) })
}

type blockedSendOpener struct{ rpc *blockedSendRPC }

func (o *blockedSendOpener) Open(_ context.Context, _ transport.StreamParams) (wireStream[encodedMsg, ephemeralResp], error) {
	return transport.NewFakeStreamForTesting(o.rpc), nil
}

type countingHeadersProvider struct {
	invalidations  atomic.Int64
	getErr         error
	lastGet        atomic.Value
	lastInvalidate atomic.Value
}

func (p *countingHeadersProvider) GetHeaders(_ context.Context, tableName string) (map[string]string, error) {
	p.lastGet.Store(tableName)
	return nil, p.getErr
}

func (p *countingHeadersProvider) Invalidate(_ context.Context, tableName string) {
	p.invalidations.Add(1)
	p.lastInvalidate.Store(tableName)
}

type authRejectingOpener struct{}

func (*authRejectingOpener) Open(context.Context, transport.StreamParams) (wireStream[encodedMsg, ephemeralResp], error) {
	return nil, status.Error(codes.Unauthenticated, "stale credentials")
}

type cancelThenAuthOpener struct {
	started chan struct{}
	once    sync.Once
}

func (o *cancelThenAuthOpener) Open(ctx context.Context, _ transport.StreamParams) (wireStream[encodedMsg, ephemeralResp], error) {
	o.once.Do(func() { close(o.started) })
	<-ctx.Done()
	return nil, status.Error(codes.Unauthenticated, "stale credentials")
}

// authRefreshOpener rejects a configured number of initial Open attempts,
// then succeeds. The supervisor owns credential invalidation.
type authRefreshOpener struct {
	mu         sync.Mutex
	rpc        *fakeRPC
	rejections int
	attempts   int
	code       codes.Code
}

func (o *authRefreshOpener) Open(context.Context, transport.StreamParams) (wireStream[encodedMsg, ephemeralResp], error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.attempts++
	if o.attempts <= o.rejections {
		code := o.code
		if code == codes.OK {
			code = codes.Unauthenticated
		}
		return nil, status.Error(code, "stale credentials")
	}
	return transport.NewFakeStreamForTesting(o.rpc), nil
}

func (o *authRefreshOpener) openCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.attempts
}

type headersResolvingOpener struct{}

func (*headersResolvingOpener) Open(ctx context.Context, params transport.StreamParams) (wireStream[encodedMsg, ephemeralResp], error) {
	_, err := params.HeadersProvider.GetHeaders(ctx, params.TableName)
	return nil, err
}

type openStep struct {
	rpc *fakeRPC
	err error
}

// scriptedOpenOpener returns one result per Open. Credential invalidation is the
// supervisor's job, so a rejecting step only returns the status.
type scriptedOpenOpener struct {
	mu       sync.Mutex
	steps    []openStep
	attempts int
}

func (o *scriptedOpenOpener) Open(context.Context, transport.StreamParams) (wireStream[encodedMsg, ephemeralResp], error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.attempts++
	if o.attempts > len(o.steps) {
		return nil, fmt.Errorf("scriptedOpenOpener: no result for attempt %d", o.attempts)
	}
	step := o.steps[o.attempts-1]
	if step.err != nil {
		return nil, step.err
	}
	return transport.NewFakeStreamForTesting(step.rpc), nil
}

func (o *scriptedOpenOpener) openCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.attempts
}

type terminalRecvRPC struct {
	*fakeRPC
	recvErr chan error
}

func newTerminalRecvRPC() *terminalRecvRPC {
	return &terminalRecvRPC{
		fakeRPC: newFakeRPC(),
		recvErr: make(chan error, 1),
	}
}

func (f *terminalRecvRPC) Recv() (*zerobuspb.EphemeralStreamResponse, error) {
	select {
	case resp, ok := <-f.recvs:
		if !ok {
			return nil, io.EOF
		}
		return resp, nil
	case err := <-f.recvErr:
		return nil, err
	}
}

type terminalRecvOpener struct{ rpc *terminalRecvRPC }

func (o *terminalRecvOpener) Open(_ context.Context, _ transport.StreamParams) (wireStream[encodedMsg, ephemeralResp], error) {
	return transport.NewFakeStreamForTesting(o.rpc), nil
}

type terminalRecvCountingOpener struct {
	rpc      *terminalRecvRPC
	attempts atomic.Int64
}

func (o *terminalRecvCountingOpener) Open(_ context.Context, _ transport.StreamParams) (wireStream[encodedMsg, ephemeralResp], error) {
	o.attempts.Add(1)
	return transport.NewFakeStreamForTesting(o.rpc), nil
}

// eofSendRPC models the stream a server terminated mid-send: Send fails with the
// opaque io.EOF gRPC uses for a server-side abort, while the authoritative status
// is delivered through Recv (via the embedded recvErr channel). sendStarted lets
// a test order the two so the sender's exit is the one that ends the run.
type eofSendRPC struct {
	*terminalRecvRPC
	sendStarted chan struct{}
	startOnce   sync.Once
}

func newEOFSendRPC() *eofSendRPC {
	return &eofSendRPC{
		terminalRecvRPC: newTerminalRecvRPC(),
		sendStarted:     make(chan struct{}),
	}
}

func (f *eofSendRPC) Send(*zerobuspb.EphemeralStreamRequest) error {
	f.startOnce.Do(func() { close(f.sendStarted) })
	return io.EOF
}

type eofSendOpener struct {
	rpc   *eofSendRPC
	opens atomic.Int64
}

func (o *eofSendOpener) Open(_ context.Context, _ transport.StreamParams) (wireStream[encodedMsg, ephemeralResp], error) {
	if o.opens.Add(1) > 1 {
		return nil, fmt.Errorf("eofSendOpener: unexpected reconnect")
	}
	return transport.NewFakeStreamForTesting(o.rpc), nil
}

func durationPtr(d time.Duration) *time.Duration { return &d }

type recvStep struct {
	resp *zerobuspb.EphemeralStreamResponse
	err  error
}

type scriptedRPC struct {
	*fakeRPC
	steps       chan recvStep
	recvStarted chan int64
	recvCalls   atomic.Int64
	aborted     chan struct{}
	abortOnce   sync.Once
}

func newScriptedRPC() *scriptedRPC {
	return &scriptedRPC{
		fakeRPC:     newFakeRPC(),
		steps:       make(chan recvStep, 16),
		recvStarted: make(chan int64, 16),
		aborted:     make(chan struct{}),
	}
}

func (f *scriptedRPC) Recv() (*zerobuspb.EphemeralStreamResponse, error) {
	f.recvStarted <- f.recvCalls.Add(1)
	select {
	case step := <-f.steps:
		return step.resp, step.err
	case <-f.aborted:
		return nil, io.ErrClosedPipe
	}
}

type blockingAckModel struct {
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (m *blockingAckModel) classify(resp ephemeralResp) (respKind, int64, pauseSignal) {
	m.once.Do(func() { close(m.entered) })
	<-m.release
	return (offsetAckModel{}).classify(resp)
}

func (f *scriptedRPC) Abort() {
	f.abortOnce.Do(func() {
		close(f.aborted)
		f.fakeRPC.close()
	})
}

func (f *scriptedRPC) pause(d time.Duration) {
	f.steps <- recvStep{resp: &zerobuspb.EphemeralStreamResponse{
		Payload: &zerobuspb.EphemeralStreamResponse_CloseStreamSignal{
			CloseStreamSignal: &zerobuspb.CloseStreamSignal{Duration: durationpb.New(d)},
		},
	}}
}

func (f *scriptedRPC) malformed() {
	f.steps <- recvStep{resp: &zerobuspb.EphemeralStreamResponse{
		Payload: &zerobuspb.EphemeralStreamResponse_IngestRecordResponse{
			IngestRecordResponse: &zerobuspb.IngestRecordResponse{},
		},
	}}
}

func (f *scriptedRPC) ack(offset int64) {
	f.steps <- recvStep{resp: &zerobuspb.EphemeralStreamResponse{
		Payload: &zerobuspb.EphemeralStreamResponse_IngestRecordResponse{
			IngestRecordResponse: &zerobuspb.IngestRecordResponse{
				DurabilityAckUpToOffset: proto.Int64(offset),
			},
		},
	}}
}

func (f *scriptedRPC) fail(err error) {
	f.steps <- recvStep{err: err}
}

type scriptedOpener struct{ rpc *scriptedRPC }

func (o *scriptedOpener) Open(_ context.Context, _ transport.StreamParams) (wireStream[encodedMsg, ephemeralResp], error) {
	return transport.NewFakeStreamForTesting(o.rpc), nil
}

type streamSequenceOpener struct {
	mu       sync.Mutex
	streams  []wireStream[encodedMsg, ephemeralResp]
	attempts int
}

func (o *streamSequenceOpener) Open(_ context.Context, _ transport.StreamParams) (wireStream[encodedMsg, ephemeralResp], error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.attempts++
	if len(o.streams) == 0 {
		return nil, fmt.Errorf("streamSequenceOpener: no more streams")
	}
	stream := o.streams[0]
	o.streams = o.streams[1:]
	return stream, nil
}

func (o *streamSequenceOpener) openCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.attempts
}

type classifiedError struct {
	retryable bool
}

func (e *classifiedError) Error() string     { return "classified error" }
func (e *classifiedError) IsRetryable() bool { return e.retryable }

// deadlineClassifiedError models a provider error that reports itself
// non-retryable only because a deadline cut it short — the shape an OAuth mint
// takes when the open budget expires mid-request. It must still be retried.
type deadlineClassifiedError struct{}

func (*deadlineClassifiedError) Error() string     { return "classified deadline error" }
func (*deadlineClassifiedError) IsRetryable() bool { return false }
func (*deadlineClassifiedError) Unwrap() error     { return context.DeadlineExceeded }

type concurrentEncoder struct {
	jsonEncoder
	entered chan struct{}
	release <-chan struct{}
}

func (e *concurrentEncoder) encode(record []byte) (encodedMsg, error) {
	e.entered <- struct{}{}
	<-e.release
	return e.jsonEncoder.encode(record)
}

type failingEncoder struct {
	jsonEncoder
	err error
}

func (e failingEncoder) encode(record []byte) (encodedMsg, error) {
	return nil, e.err
}

func (e failingEncoder) encodeBatch(records [][]byte) (encodedMsg, error) {
	return nil, e.err
}

type blockingNthEncoder struct {
	jsonEncoder
	blockAt int64
	calls   atomic.Int64
	entered chan struct{}
	release <-chan struct{}
}

func (e *blockingNthEncoder) encode(record []byte) (encodedMsg, error) {
	if e.calls.Add(1) == e.blockAt {
		close(e.entered)
		<-e.release
	}
	return e.jsonEncoder.encode(record)
}

type timeoutOpener struct {
	attempts atomic.Int64
}

func (o *timeoutOpener) Open(ctx context.Context, _ transport.StreamParams) (wireStream[encodedMsg, ephemeralResp], error) {
	o.attempts.Add(1)
	<-ctx.Done()
	return nil, ctx.Err()
}

// terminalTimeoutOpener reports a permanent rejection at the moment the open
// deadline expires, so both the timeout and the status describe the same failure.
type terminalTimeoutOpener struct {
	attempts atomic.Int64
}

func (o *terminalTimeoutOpener) Open(ctx context.Context, _ transport.StreamParams) (wireStream[encodedMsg, ephemeralResp], error) {
	o.attempts.Add(1)
	<-ctx.Done()
	return nil, status.Error(codes.InvalidArgument, "bad table name")
}

// classifiedTimeoutOpener reports a self-classifying failure at the moment the
// open deadline expires, modelling a HeadersProvider rejection that carries no
// gRPC status (an OAuth TokenError is codes.Unknown).
type classifiedTimeoutOpener struct {
	err      error
	attempts atomic.Int64
}

func (o *classifiedTimeoutOpener) Open(ctx context.Context, _ transport.StreamParams) (wireStream[encodedMsg, ephemeralResp], error) {
	o.attempts.Add(1)
	<-ctx.Done()
	return nil, o.err
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

func (r *recordingCallback) ackOffsets() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int64(nil), r.acks...)
}

func (r *recordingCallback) errorOffsets() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int64(nil), r.errs...)
}

type callbackFuncs struct {
	onAck   func(int64)
	onError func(int64, error)
}

func (c *callbackFuncs) OnAck(offset int64) {
	if c.onAck != nil {
		c.onAck(offset)
	}
}

func (c *callbackFuncs) OnError(offset int64, err error) {
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
