package stream

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/databricks/zerobus-sdk/purego/internal/transport"
)

// Default config constants for the ingestion core.
const (
	DefaultMaxInflight        = 1_000_000
	DefaultRecoveryRetries    = 4
	DefaultRecoveryBackoff    = 2 * time.Second
	DefaultRecoveryResetAfter = 15 * time.Second
	DefaultFlushTimeout       = 5 * time.Minute
	DefaultLackOfAckTimeout   = 60 * time.Second
	// DefaultDrainTimeout bounds graceful shutdown.
	DefaultDrainTimeout = 500 * time.Millisecond
)

// RecoverySetting controls stream reconnection and defaults to enabled.
type RecoverySetting int

const (
	// RecoveryEnabled reconnects on failure.
	RecoveryEnabled RecoverySetting = iota
	// RecoveryDisabled prevents reconnection.
	RecoveryDisabled
)

// enabled reports whether recovery should be attempted.
func (r RecoverySetting) enabled() bool { return r == RecoveryEnabled }

// Config holds per-stream configuration.
type Config struct {
	// MaxInflight is the maximum number of unacknowledged records in the buffer.
	MaxInflight int
	// Recovery controls reconnection and defaults to enabled.
	Recovery RecoverySetting
	// RecoveryRetries limits consecutive reconnect failures.
	RecoveryRetries int
	// RecoveryBackoff is the fixed wait between reconnect attempts.
	RecoveryBackoff time.Duration
	// RecoveryResetAfter marks a connection healthy after this duration.
	RecoveryResetAfter time.Duration
	// FlushTimeout bounds Flush when the caller's context has no deadline.
	FlushTimeout time.Duration
	// LackOfAckTimeout bounds ack silence while records are in flight.
	LackOfAckTimeout time.Duration
	// DrainTimeout bounds graceful shutdown.
	DrainTimeout time.Duration
	// StreamPausedMaxWait caps a server-requested pause.
	// Non-positive values apply no client cap.
	StreamPausedMaxWait time.Duration
}

// DefaultConfig returns a Config with SDK-standard defaults.
func DefaultConfig() Config {
	return Config{
		MaxInflight: DefaultMaxInflight,
		// Recovery left as its zero value, RecoveryEnabled.
		RecoveryRetries:    DefaultRecoveryRetries,
		RecoveryBackoff:    DefaultRecoveryBackoff,
		RecoveryResetAfter: DefaultRecoveryResetAfter,
		FlushTimeout:       DefaultFlushTimeout,
		LackOfAckTimeout:   DefaultLackOfAckTimeout,
		DrainTimeout:       DefaultDrainTimeout,
	}
}

// AckCallback is called once per acknowledged record/batch offset.
type AckCallback interface {
	OnAck(offset int64)
	OnError(offset int64, err error)
}

// watermark tracks the highest acknowledged offset.
type watermark struct {
	mu       sync.Mutex
	cond     *sync.Cond
	offset   int64 // highest fully-acked offset; -1 means none yet
	err      error // terminal error set once and never cleared
	terminal bool
}

func newWatermark() *watermark {
	w := &watermark{offset: -1}
	w.cond = sync.NewCond(&w.mu)
	return w
}

// advance sets the watermark to max(current, offset) and wakes waiters.
func (w *watermark) advance(offset int64) {
	w.mu.Lock()
	if offset > w.offset {
		w.offset = offset
	}
	w.mu.Unlock()
	w.cond.Broadcast()
}

// fail marks the watermark terminal with err and wakes all waiters.
func (w *watermark) fail(err error) {
	w.mu.Lock()
	if !w.terminal {
		w.err = err
		w.terminal = true
	}
	w.mu.Unlock()
	w.cond.Broadcast()
}

func (w *watermark) current() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.offset
}

// waitFor blocks until target, cancellation, or terminal failure.
func (w *watermark) waitFor(ctx context.Context, target int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for w.offset < target && !w.terminal {
		// Wake up on ctx cancellation too.
		stop := context.AfterFunc(ctx, func() { w.cond.Broadcast() })
		w.cond.Wait()
		stop()
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	if w.offset >= target {
		return nil
	}
	return w.err
}

// CoreStream is the protocol-independent ingestion pipeline.
// It coordinates buffering, sending, acknowledgements, and recovery.
type CoreStream[Req, Resp any] struct {
	params   StreamParams
	cfg      Config
	opener   opener[Req, Resp]
	enc      encoder[Req]
	ackMdl   ackModel[Resp]
	buf      *buffer[Req]
	wm       *watermark
	callback AckCallback

	clientID string
	serverID atomic.Pointer[string]

	// ingestMu serializes offset assignment and enqueue.
	ingestMu sync.Mutex
	// nextOffset is protected by ingestMu.
	nextOffset int64
	// lastEnqueued is the highest successfully queued offset.
	lastEnqueued int64

	// done is closed when the supervisor exits (terminal state).
	done chan struct{}
	// termErr holds the first terminal error after done is closed.
	termErr error
	termMu  sync.Mutex

	closeOnce sync.Once
	// cancelSupervisor cancels the supervisor's context so Close unblocks it.
	cancelSupervisor context.CancelFunc
}

var fallbackStreamIDCounter atomic.Uint64

func newClientStreamID() string {
	var id [16]byte
	if _, err := rand.Read(id[:]); err == nil {
		return "client-stream-" + hex.EncodeToString(id[:])
	}
	return fmt.Sprintf(
		"client-stream-%d-%d-%d",
		os.Getpid(),
		time.Now().UnixNano(),
		fallbackStreamIDCounter.Add(1),
	)
}

// NewCoreStream constructs a stream and starts its supervisor.
func NewCoreStream[Req, Resp any](
	params StreamParams,
	cfg Config,
	opener opener[Req, Resp],
	enc encoder[Req],
	ackMdl ackModel[Resp],
	callback AckCallback,
) *CoreStream[Req, Resp] {
	ctx, cancel := context.WithCancel(context.Background())
	cs := &CoreStream[Req, Resp]{
		params:           params,
		cfg:              cfg,
		opener:           opener,
		enc:              enc,
		ackMdl:           ackMdl,
		buf:              newBuffer[Req](cfg.MaxInflight),
		wm:               newWatermark(),
		callback:         callback,
		clientID:         newClientStreamID(),
		lastEnqueued:     -1,
		done:             make(chan struct{}),
		cancelSupervisor: cancel,
	}
	go cs.supervise(ctx)
	return cs
}

// ID returns the stable client-generated logical stream ID.
func (cs *CoreStream[Req, Resp]) ID() string {
	return cs.clientID
}

// ServerID returns the most recently opened server-assigned stream ID.
func (cs *CoreStream[Req, Resp]) ServerID() string {
	id := cs.serverID.Load()
	if id == nil {
		return ""
	}
	return *id
}

func (cs *CoreStream[Req, Resp]) setServerID(id string) {
	cs.serverID.Store(&id)
}

// Ingest queues one record and returns its durability offset.
func (cs *CoreStream[Req, Resp]) Ingest(ctx context.Context, record []byte) (int64, error) {
	return cs.enqueueEncoded(ctx, func(offset int64) (Req, error) {
		return cs.enc.encode(offset, record)
	})
}

// IngestBatch queues one atomic batch under one offset.
// Prefer it in hot paths to reduce per-message overhead.
func (cs *CoreStream[Req, Resp]) IngestBatch(ctx context.Context, records [][]byte) (int64, error) {
	return cs.enqueueEncoded(ctx, func(offset int64) (Req, error) {
		return cs.enc.encodeBatch(offset, records)
	})
}

// enqueueEncoded assigns an offset only after a successful enqueue.
func (cs *CoreStream[Req, Resp]) enqueueEncoded(ctx context.Context, encodeFn func(offset int64) (Req, error)) (int64, error) {
	if cs.isClosed() {
		if err := cs.terminalErr(); err != nil {
			return 0, err
		}
		return 0, errClosed
	}
	cs.ingestMu.Lock()
	defer cs.ingestMu.Unlock()

	offset := cs.nextOffset
	msg, err := encodeFn(offset)
	if err != nil {
		return 0, err
	}
	if err := cs.buf.enqueue(ctx, offset, msg); err != nil {
		return 0, err
	}
	cs.nextOffset++
	cs.lastEnqueued = offset
	return offset, nil
}

// Flush waits for all currently queued records.
// It applies DefaultFlushTimeout when ctx has no deadline.
func (cs *CoreStream[Req, Resp]) Flush(ctx context.Context) error {
	cs.ingestMu.Lock()
	target := cs.lastEnqueued
	cs.ingestMu.Unlock()
	if target < 0 {
		return nil // nothing successfully ingested yet
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cs.cfg.FlushTimeout)
		defer cancel()
	}
	return cs.WaitForOffset(ctx, target)
}

// WaitForOffset waits until offset is durable.
// The offset must come from a successful ingest call.
func (cs *CoreStream[Req, Resp]) WaitForOffset(ctx context.Context, offset int64) error {
	return cs.wm.waitFor(ctx, offset)
}

// GetUnacked closes the stream and returns unacknowledged records.
// Batch items expand into their original records.
func (cs *CoreStream[Req, Resp]) GetUnacked() [][]byte {
	cs.Close()
	items := cs.buf.drain()
	out := make([][]byte, 0, len(items))
	for _, it := range items {
		// Re-extract the raw record bytes from the encoded message so callers get
		// back the original record content; a batch yields all its records.
		out = append(out, cs.enc.decode(it.payload)...)
	}
	return out
}

// Close gracefully terminates the stream and is idempotent.
// It does not wait for durability; call Flush first when required.
func (cs *CoreStream[Req, Resp]) Close() {
	cs.closeOnce.Do(func() {
		cs.cancelSupervisor()
		cs.buf.close()
		<-cs.done // wait for the supervisor to exit
	})
}

// IsClosed reports whether the stream has been closed or failed terminally.
func (cs *CoreStream[Req, Resp]) IsClosed() bool {
	return cs.isClosed()
}

func (cs *CoreStream[Req, Resp]) isClosed() bool {
	select {
	case <-cs.done:
		return true
	default:
		return false
	}
}

func (cs *CoreStream[Req, Resp]) terminalErr() error {
	cs.termMu.Lock()
	defer cs.termMu.Unlock()
	return cs.termErr
}

func (cs *CoreStream[Req, Resp]) setTerminalErr(err error) {
	cs.termMu.Lock()
	if cs.termErr == nil {
		cs.termErr = err
	}
	cs.termMu.Unlock()
}

type sendEvent struct {
	logicalOffset  int64
	physicalOffset int64
	completed      bool
	err            error
	consumed       chan struct{}
}

// runOnce operates one transport stream until a worker exits.
// resetRecoveryBudget reports durable progress or a stable connection.
func (cs *CoreStream[Req, Resp]) runOnce(ctx context.Context) (cause error, resetRecoveryBudget bool) {
	stream, err := cs.opener.Open(ctx, cs.params)
	if err != nil {
		// Invalid parameters cannot be fixed by reconnecting.
		if errors.Is(err, transport.ErrInvalidParams) {
			return wrapValidation(fmt.Errorf("stream: open: %w", err)), false
		}
		return fmt.Errorf("stream: open: %w", err), false
	}
	cs.setServerID(stream.ServerID())
	openedAt := time.Now()
	startAck := cs.wm.current()

	// Resend unacknowledged items on the new connection.
	cs.buf.requeue()

	// senderCtx controls only this connection's sender.
	senderCtx, cancelSender := context.WithCancel(ctx)
	defer cancelSender()

	errCh := make(chan error, 2)
	sendEvents := make(chan sendEvent, 2)

	go cs.sender(senderCtx, stream, errCh, sendEvents)
	go cs.receiver(stream, errCh, sendEvents)

	// Tear down after the first worker exits.
	cause = <-errCh
	cancelSender() // unblocks sender waiting on buf.next()
	var ps pauseSignal
	switch {
	case ctx.Err() != nil:
		cs.gracefulTeardown(stream, errCh)
	case errors.As(cause, &ps):
		// Requeue after the pause.
		stream.Close()
		<-errCh
	default:
		// Abort a broken stream.
		stream.Close()
		<-errCh // drain second exit
	}
	resetAfter := cs.cfg.RecoveryResetAfter
	if resetAfter <= 0 {
		resetAfter = DefaultRecoveryResetAfter
	}
	resetRecoveryBudget = cs.wm.current() > startAck ||
		time.Since(openedAt) >= resetAfter
	return cause, resetRecoveryBudget
}

// gracefulTeardown drains responses within DrainTimeout.
func (cs *CoreStream[Req, Resp]) gracefulTeardown(stream wireStream[Req, Resp], errCh <-chan error) {
	if err := stream.CloseSend(); err != nil {
		stream.Close()
		<-errCh
		return
	}
	timer := time.NewTimer(cs.cfg.DrainTimeout)
	defer timer.Stop()
	select {
	case <-errCh:
		stream.Close()
	case <-timer.C:
		stream.Close()
		<-errCh
	}
}

// sender writes queued items until cancellation or failure.
func (cs *CoreStream[Req, Resp]) sender(
	senderCtx context.Context,
	stream wireStream[Req, Resp],
	errCh chan<- error,
	sendEvents chan<- sendEvent,
) {
	publish := func(event sendEvent) bool {
		select {
		case sendEvents <- event:
			return true
		case <-senderCtx.Done():
			return false
		}
	}
	physicalOffset := int64(0)
	for {
		it, err := cs.buf.next(senderCtx)
		if err != nil {
			errCh <- nil // ctx cancelled or buffer closed — clean exit
			return
		}
		cs.enc.stampOffset(it.payload, physicalOffset)
		if !publish(sendEvent{
			logicalOffset: it.offset, physicalOffset: physicalOffset,
		}) {
			errCh <- nil
			return
		}
		err = stream.Send(it.payload)
		consumed := make(chan struct{})
		sendEvents <- sendEvent{
			logicalOffset:  it.offset,
			physicalOffset: physicalOffset,
			completed:      true,
			err:            err,
			consumed:       consumed,
		}
		if err != nil {
			errCh <- fmt.Errorf("stream: send offset %d: %w", it.offset, err)
			return
		}
		physicalOffset++
		select {
		case <-consumed:
		case <-senderCtx.Done():
			errCh <- nil
			return
		}
	}
}

// receiver processes responses and advances the ack watermark.
// Each Recv runs separately so the ack timer remains responsive.
func (cs *CoreStream[Req, Resp]) receiver(
	stream wireStream[Req, Resp],
	errCh chan<- error,
	sendEvents <-chan sendEvent,
) {
	type recvResult struct {
		resp Resp
		err  error
	}

	// Keep exactly one Recv active.
	recvCh := make(chan recvResult, 1)
	spawnRecv := func() {
		go func() {
			resp, err := stream.Recv()
			recvCh <- recvResult{resp, err}
		}()
	}
	spawnRecv()

	lackTimer := time.NewTimer(cs.cfg.LackOfAckTimeout)
	defer lackTimer.Stop()

	lastAckedPhysical := int64(-1)
	sendingPhysical := int64(-1)
	sendingLogical := int64(-1)
	pendingPhysicalAck := int64(-1)
	sentBasePhysical := int64(0)
	var sentLogical []int64

	resetLackTimer := func() {
		if !lackTimer.Stop() {
			select {
			case <-lackTimer.C:
			default:
			}
		}
		lackTimer.Reset(cs.cfg.LackOfAckTimeout)
	}
	applyLogicalAck := func(offset int64) {
		resetLackTimer()
		cs.wm.advance(offset)
		cs.buf.discardThrough(offset)
		if cs.callback != nil {
			cs.callback.OnAck(offset)
		}
	}
	applyPhysicalAck := func(offset int64) error {
		resetLackTimer()
		if offset <= lastAckedPhysical {
			return nil
		}
		index := offset - sentBasePhysical
		if index < 0 || index >= int64(len(sentLogical)) {
			return fmt.Errorf(
				"stream: server ack offset %d exceeds highest completed physical offset %d",
				offset, sentBasePhysical+int64(len(sentLogical))-1,
			)
		}
		logical := sentLogical[index]
		applyLogicalAck(logical)
		lastAckedPhysical = offset
		sentLogical = sentLogical[index+1:]
		sentBasePhysical = offset + 1
		if len(sentLogical) == 0 {
			sentLogical = nil
		}
		return nil
	}
	handleSendEvent := func(event sendEvent) error {
		if !event.completed {
			sendingPhysical = event.physicalOffset
			sendingLogical = event.logicalOffset
			return nil
		}
		close(event.consumed)
		if event.err == nil {
			expected := sentBasePhysical + int64(len(sentLogical))
			if event.physicalOffset != expected {
				return fmt.Errorf(
					"stream: physical send offset %d is not contiguous after %d",
					event.physicalOffset, expected-1,
				)
			}
			sentLogical = append(sentLogical, event.logicalOffset)
		}
		sendingPhysical = -1
		sendingLogical = -1
		var ackErr error
		if event.err == nil && pendingPhysicalAck >= 0 {
			ackErr = applyPhysicalAck(pendingPhysicalAck)
		}
		pendingPhysicalAck = -1
		return ackErr
	}
	resolvePendingOnExit := func() error {
		if pendingPhysicalAck < 0 || sendingPhysical < 0 {
			return nil
		}
		if sendingLogical < 0 {
			return fmt.Errorf("stream: missing logical offset for active send")
		}
		stream.Close()
		return handleSendEvent(<-sendEvents)
	}
	handleTerminal := func(err error) {
		if pendingErr := resolvePendingOnExit(); pendingErr != nil && err == nil {
			err = pendingErr
		}
		errCh <- err
	}

	for {
		var r recvResult
		select {
		case event := <-sendEvents:
			if err := handleSendEvent(event); err != nil {
				handleTerminal(err)
				return
			}
			continue
		case <-lackTimer.C:
			// Silence is only a failure while records are actually awaiting an ack.
			// An idle stream (nothing in flight) legitimately receives no acks, so
			// re-arm the timer and keep waiting on the same outstanding Recv rather
			// than tearing the stream down.
			if cs.buf.inFlight() == 0 {
				lackTimer.Reset(cs.cfg.LackOfAckTimeout)
				continue
			}
			stream.Close()
			<-recvCh // reap the outstanding Recv goroutine
			handleTerminal(fmt.Errorf(
				"stream: no ack from server for %s", cs.cfg.LackOfAckTimeout,
			))
			return
		case r = <-recvCh:
		}

		if r.err == io.EOF {
			handleTerminal(nil)
			return
		}
		if r.err != nil {
			handleTerminal(fmt.Errorf("stream: recv: %w", r.err))
			return
		}

		kind, offset, pause := cs.ackMdl.classify(r.resp)
		if kind == unknownResponse || kind == malformedResponse {
			// Reject protocol violations.
			handleTerminal(fmt.Errorf("stream: unusable server response (kind %d)", kind))
			return
		}
		if kind == pauseResponse {
			// Let the supervisor apply the requested pause.
			handleTerminal(pause)
			return
		}

		// Keep one Recv outstanding.
		spawnRecv()

		if kind == ackResponse {
			// Observe send-start events queued before a fast response.
			for {
				select {
				case event := <-sendEvents:
					if err := handleSendEvent(event); err != nil {
						handleTerminal(err)
						return
					}
				default:
					goto sendsDrained
				}
			}
		sendsDrained:
			resetLackTimer()
			if sendingPhysical >= 0 && offset == sendingPhysical {
				if offset > pendingPhysicalAck {
					pendingPhysicalAck = offset
				}
				continue
			}
			if err := applyPhysicalAck(offset); err != nil {
				handleTerminal(err)
				return
			}
		}
		// Non-ack, non-pause kinds already returned above; the next Recv is
		// already outstanding.
	}
}
