package stream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
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
	DefaultRecoveryTimeout    = 15 * time.Second
	DefaultRecoveryResetAfter = 15 * time.Second
	DefaultFlushTimeout       = 5 * time.Minute
	DefaultLackOfAckTimeout   = 60 * time.Second
	// DefaultMaxPayloadBytes leaves room below the 10 MiB service limit.
	DefaultMaxPayloadBytes = 10*1024*1024 - 64*1024
	// DefaultMaxBatchRecords bounds per-batch allocation.
	DefaultMaxBatchRecords = 100_000
	// DefaultCallbackTeardownTimeout bounds callback shutdown.
	DefaultCallbackTeardownTimeout = 5 * time.Second
	// DefaultDrainTimeout bounds graceful shutdown.
	DefaultDrainTimeout = 500 * time.Millisecond
)

// RecoverySetting controls stream reconnection.
// Its zero value enables recovery.
type RecoverySetting int

const (
	// RecoveryEnabled reconnects on failure. It is the zero value, so a Config
	// that never sets Recovery still recovers.
	RecoveryEnabled RecoverySetting = iota
	// RecoveryDisabled fails the stream on the first unrecoverable error without
	// attempting to reconnect.
	RecoveryDisabled
)

// enabled reports whether recovery should be attempted.
func (r RecoverySetting) enabled() bool { return r == RecoveryEnabled }

// Config holds per-stream configuration. All fields have sane defaults via
// DefaultConfig(); override individual fields before passing to NewCoreStream.
type Config struct {
	// MaxInflight is the maximum number of unacknowledged records in the buffer.
	MaxInflight int
	// Recovery controls whether stream reconnection is attempted on failure. The
	// zero value (RecoveryEnabled) recovers; set RecoveryDisabled to opt out.
	Recovery RecoverySetting
	// RecoveryRetries limits consecutive reconnect failures.
	RecoveryRetries int
	// RecoveryBackoff is the fixed wait between reconnect attempts.
	RecoveryBackoff time.Duration
	// RecoveryTimeout bounds each Open attempt.
	RecoveryTimeout time.Duration
	// RecoveryResetAfter marks a connection healthy after this duration.
	RecoveryResetAfter time.Duration
	// FlushTimeout is the maximum wait budget for Flush and WaitForOffset.
	// The caller's context may shorten this budget, but cannot extend it.
	FlushTimeout time.Duration
	// LackOfAckTimeout bounds ack silence while records are in flight.
	LackOfAckTimeout time.Duration
	// DrainTimeout bounds the orderly drain-to-EOF performed on a clean Close so
	// the server observes an orderly END_STREAM rather than an abrupt reset.
	DrainTimeout time.Duration
	// MaxPayloadBytes caps one encoded ingest request. Non-positive values use
	// DefaultMaxPayloadBytes.
	MaxPayloadBytes int
	// MaxBatchRecords caps the number of records in one batch independently of
	// byte size. Non-positive values use DefaultMaxBatchRecords.
	MaxBatchRecords int
	// CallbackTeardownTimeout bounds asynchronous callback worker teardown.
	CallbackTeardownTimeout time.Duration
	// StreamPausedMaxWait caps how long the client honors a server-requested
	// pause before reconnecting. Nil means no client cap (wait the full
	// server-requested duration); a non-nil value caps the wait, and an explicit
	// zero means reconnect immediately without honoring the pause duration.
	StreamPausedMaxWait *time.Duration
}

// DefaultConfig returns a Config with SDK-standard defaults.
func DefaultConfig() Config {
	return Config{
		MaxInflight: DefaultMaxInflight,
		// Recovery left as its zero value, RecoveryEnabled.
		RecoveryRetries:         DefaultRecoveryRetries,
		RecoveryBackoff:         DefaultRecoveryBackoff,
		RecoveryTimeout:         DefaultRecoveryTimeout,
		RecoveryResetAfter:      DefaultRecoveryResetAfter,
		FlushTimeout:            DefaultFlushTimeout,
		LackOfAckTimeout:        DefaultLackOfAckTimeout,
		DrainTimeout:            DefaultDrainTimeout,
		MaxPayloadBytes:         DefaultMaxPayloadBytes,
		MaxBatchRecords:         DefaultMaxBatchRecords,
		CallbackTeardownTimeout: DefaultCallbackTeardownTimeout,
	}
}

// sanitizeConfig replaces invalid values with defaults.
func sanitizeConfig(c Config) Config {
	if c.MaxInflight <= 0 {
		c.MaxInflight = DefaultMaxInflight
	}
	if c.RecoveryRetries <= 0 {
		c.RecoveryRetries = DefaultRecoveryRetries
	}
	if c.RecoveryBackoff <= 0 {
		c.RecoveryBackoff = DefaultRecoveryBackoff
	}
	if c.RecoveryTimeout <= 0 {
		c.RecoveryTimeout = DefaultRecoveryTimeout
	}
	if c.RecoveryResetAfter <= 0 {
		c.RecoveryResetAfter = DefaultRecoveryResetAfter
	}
	if c.FlushTimeout <= 0 {
		c.FlushTimeout = DefaultFlushTimeout
	}
	if c.LackOfAckTimeout <= 0 {
		c.LackOfAckTimeout = DefaultLackOfAckTimeout
	}
	if c.DrainTimeout <= 0 {
		c.DrainTimeout = DefaultDrainTimeout
	}
	if c.MaxPayloadBytes <= 0 {
		c.MaxPayloadBytes = DefaultMaxPayloadBytes
	}
	if c.MaxBatchRecords <= 0 {
		c.MaxBatchRecords = DefaultMaxBatchRecords
	}
	if c.CallbackTeardownTimeout <= 0 {
		c.CallbackTeardownTimeout = DefaultCallbackTeardownTimeout
	}
	return c
}

// AckCallback is called asynchronously once per acknowledged record/batch
// offset. Implementations must return promptly; callbacks run on a dedicated
// worker so they cannot block ingestion or stream shutdown.
type AckCallback interface {
	OnAck(offset int64)
	OnError(offset int64, err error)
}

// watermark is the monotonic ack offset shared between the receiver goroutine
// (writer) and Flush/WaitForOffset callers (readers).
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

func (w *watermark) closeForClean() {
	w.fail(errWatermarkClosed)
}

// waitFor blocks until the watermark reaches target or the watermark becomes
// terminal. Returns nil if the target is reached, or the terminal error.
func (w *watermark) waitFor(ctx context.Context, target int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	for w.offset < target && !w.terminal {
		// Wake up on ctx cancellation too.
		stop := context.AfterFunc(ctx, func() {
			w.mu.Lock()
			w.cond.Broadcast()
			w.mu.Unlock()
		})
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

// CoreStream is the protocol-agnostic ingestion core. It owns the buffer,
// sender goroutine, receiver goroutine, ack watermark, and the supervisor
// that reconnects on failure. It is generic over the wire request/response
// types (Req/Resp): proto/JSON instantiate it over EphemeralStream, Arrow over
// Flight. The three specialization points — encoder, ackModel, and the
// wireStream returned by opener — are injected, so this core is written once
// and never names a concrete proto type.
//
// The three goroutines:
//
//	sender   — pulls items from the buffer, writes them to the wire stream.
//	receiver — reads acks from the wire stream, advances the watermark.
//	supervisor — create → run → recover loop; wires sender+receiver together.
//
// Callers interact only through Ingest, IngestBatch, Flush, WaitForOffset,
// GetUnacked, and Close.
type CoreStream[Req, Resp any] struct {
	params     StreamParams
	cfg        Config
	opener     opener[Req, Resp]
	enc        encoder[Req]
	ackMdl     ackModel[Resp]
	buf        *buffer[Req]
	wm         *watermark
	dispatcher *callbackDispatcher

	// offsetMu protects offset assignment.
	offsetMu sync.Mutex
	// nextOffset is the next logical offset to assign.
	nextOffset int64
	// lastEnqueued is the highest queued offset.
	lastEnqueued atomic.Int64

	// done is closed when the supervisor exits (terminal state).
	done chan struct{}
	// termErr holds the first terminal error after done is closed.
	termErr         error
	retainedUnacked []item[Req]
	termMu          sync.Mutex

	closeOnce sync.Once
	// cancelSupervisor cancels the supervisor's context so Close unblocks it.
	cancelSupervisor context.CancelFunc
}

// NewCoreStream constructs a CoreStream and starts the supervisor goroutine.
// The stream is immediately ready for Ingest calls; the supervisor opens the
// first transport stream in the background. Prefer the per-protocol
// constructors (NewProtoJSONStream) over calling this directly.
func NewCoreStream[Req, Resp any](
	params StreamParams,
	cfg Config,
	opener opener[Req, Resp],
	enc encoder[Req],
	ackMdl ackModel[Resp],
	callback AckCallback,
) *CoreStream[Req, Resp] {
	cfg = sanitizeConfig(cfg)
	if len(params.DescriptorProto) > 0 {
		params.DescriptorProto = bytes.Clone(params.DescriptorProto)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cs := &CoreStream[Req, Resp]{
		params:           params,
		cfg:              cfg,
		opener:           opener,
		enc:              enc,
		ackMdl:           ackMdl,
		buf:              newBuffer[Req](cfg.MaxInflight),
		wm:               newWatermark(),
		dispatcher:       newCallbackDispatcher(callback),
		done:             make(chan struct{}),
		cancelSupervisor: cancel,
	}
	cs.lastEnqueued.Store(-1)
	go cs.supervise(ctx)
	return cs
}

// Ingest encodes record and enqueues it in the buffer, blocking if the buffer
// is at capacity (backpressure). Returns the logical offset assigned to this
// record; pass it to WaitForOffset to confirm durability.
func (cs *CoreStream[Req, Resp]) Ingest(ctx context.Context, record []byte) (int64, error) {
	if err := cs.checkRawSize(len(record)); err != nil {
		return 0, err
	}
	return cs.enqueueEncoded(ctx, func() (Req, error) {
		return cs.enc.encode(math.MaxInt64, record)
	})
}

// IngestBatch encodes records as a single atomic batch and enqueues it, blocking
// if the buffer is at capacity. The whole batch occupies one logical offset and
// the server acks it atomically. Returns that offset. Prefer this over Ingest in
// hot paths: it amortizes per-message overhead across the batch.
func (cs *CoreStream[Req, Resp]) IngestBatch(ctx context.Context, records [][]byte) (int64, error) {
	if len(records) == 0 {
		// Rust returns Ok(None) for empty batches; in Go we model that as a
		// no-op with sentinel offset -1 and no queueing.
		return -1, nil
	}
	if len(records) > cs.cfg.MaxBatchRecords {
		return 0, fmt.Errorf("%w: %d records exceeds MaxBatchRecords=%d",
			ErrPayloadTooLarge, len(records), cs.cfg.MaxBatchRecords)
	}
	total := 0
	for _, record := range records {
		if len(record) > cs.cfg.MaxPayloadBytes || total > cs.cfg.MaxPayloadBytes-len(record) {
			return 0, fmt.Errorf("%w: raw batch exceeds MaxPayloadBytes=%d",
				ErrPayloadTooLarge, cs.cfg.MaxPayloadBytes)
		}
		total += len(record)
	}
	if err := cs.checkRawSize(total); err != nil {
		return 0, err
	}
	return cs.enqueueEncoded(ctx, func() (Req, error) {
		return cs.enc.encodeBatch(math.MaxInt64, records)
	})
}

func (cs *CoreStream[Req, Resp]) checkRawSize(size int) error {
	if size > cs.cfg.MaxPayloadBytes {
		return fmt.Errorf("%w: %d bytes exceeds MaxPayloadBytes=%d",
			ErrPayloadTooLarge, size, cs.cfg.MaxPayloadBytes)
	}
	return nil
}

// enqueueEncoded reserves capacity and encodes before assigning an offset.
func (cs *CoreStream[Req, Resp]) enqueueEncoded(ctx context.Context, encodeFn func() (Req, error)) (int64, error) {
	if cs.isClosed() {
		if err := cs.terminalErr(); err != nil {
			return 0, err
		}
		return 0, errClosed
	}
	if err := cs.buf.reserve(ctx); err != nil {
		return 0, err
	}

	// Encode with the largest possible wire offset so the size check remains
	// valid after the sender stamps any connection-local physical offset.
	msg, err := encodeFn()
	if err != nil {
		cs.buf.release()
		return 0, err
	}
	if size := cs.enc.wireSize(msg); size > cs.cfg.MaxPayloadBytes {
		cs.buf.release()
		return 0, fmt.Errorf("%w: encoded size %d exceeds MaxPayloadBytes=%d",
			ErrPayloadTooLarge, size, cs.cfg.MaxPayloadBytes)
	}

	cs.offsetMu.Lock()
	offset := cs.nextOffset
	cs.enc.stampOffset(msg, offset)
	if err := cs.buf.append(offset, msg); err != nil {
		cs.offsetMu.Unlock()
		return 0, err
	}
	cs.nextOffset++
	cs.lastEnqueued.Store(offset)
	cs.offsetMu.Unlock()
	return offset, nil
}

// Flush blocks until every record ingested so far is acknowledged by the
// server. Returns nil once all are durable, or an error if the stream fails
// or the wait budget expires.
//
// Flush always enforces Config.FlushTimeout as an upper bound, even when the
// caller context has no deadline.
func (cs *CoreStream[Req, Resp]) Flush(ctx context.Context) error {
	target := cs.lastEnqueued.Load()
	if target < 0 {
		return nil // nothing successfully ingested yet
	}
	return cs.WaitForOffset(ctx, target)
}

// WaitForOffset blocks until the server has acknowledged all records up to and
// including offset, or until the stream fails terminally.
//
// offset must be one returned by a successful Ingest. Passing an offset that was
// never enqueued (e.g. a value above the last assigned offset) is a caller error:
// since the watermark can never reach it, the call blocks until either the
// caller context expires, FlushTimeout expires, or the stream fails. Prefer
// Flush, which waits for exactly the records ingested so far.
func (cs *CoreStream[Req, Resp]) WaitForOffset(ctx context.Context, offset int64) error {
	boundedCtx, cancel := context.WithTimeout(ctx, cs.cfg.FlushTimeout)
	defer cancel()
	return cs.wm.waitFor(boundedCtx, offset)
}

// GetUnacked returns records that were ingested but never acknowledged, one
// entry per record. A batched buffer item expands to all of its records (not
// just the first), so no unacked record is silently dropped.
//
// The result is a fresh copy on every call: the unacked set is consolidated
// into retained storage once, and each call decodes a clone. A diagnostic read
// therefore never removes the records a later retry or persistence path needs,
// and mutating the returned bytes never corrupts the retained payloads.
//
// Calling GetUnacked on an active stream returns ErrStreamStillActive; callers
// must close or wait for terminal failure first.
func (cs *CoreStream[Req, Resp]) GetUnacked() ([][]byte, error) {
	if !cs.isClosed() {
		return nil, ErrStreamStillActive
	}
	items := cs.consolidateUnacked()
	out := make([][]byte, 0, len(items))
	for _, it := range items {
		// Re-extract the raw record bytes from the encoded message so callers get
		// back the original record content; a batch yields all its records. decode
		// clones, so the retained payload is never aliased.
		out = append(out, cs.enc.decode(it.payload)...)
	}
	return out, nil
}

// Close flushes pending records first, then terminates the stream and releases
// its transport and buffer resources. It is idempotent and blocks until
// lifecycle teardown completes.
// AckCallback delivery is asynchronous and may finish after Close returns.
//
// If flush cannot complete within FlushTimeout, Close proceeds with teardown and
// any remaining records are abandoned (retrievable via GetUnacked, or reported
// through the AckCallback's OnError). On a clean shutdown the live stream is
// torn down gracefully (half-close, then drain any straggling acks to EOF,
// bounded by DrainTimeout) so the server observes an orderly END_STREAM rather
// than an abrupt reset; see gracefulTeardown.
func (cs *CoreStream[Req, Resp]) Close() {
	cs.closeOnce.Do(func() {
		_ = cs.Flush(context.Background())
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

func (cs *CoreStream[Req, Resp]) retainUnacked(items []item[Req]) {
	if len(items) == 0 {
		return
	}
	cs.termMu.Lock()
	cs.retainedUnacked = items
	cs.termMu.Unlock()
}

// consolidateUnacked folds any still-buffered items into retained storage once,
// then returns a clone of the retained set without clearing it. Call only after
// Close, so the buffer is drained and no new items can arrive. Repeated or
// concurrent calls all observe the same snapshot rather than racing to empty it.
func (cs *CoreStream[Req, Resp]) consolidateUnacked() []item[Req] {
	cs.termMu.Lock()
	defer cs.termMu.Unlock()
	if items := cs.buf.drain(); len(items) > 0 {
		cs.retainedUnacked = append(cs.retainedUnacked, items...)
	}
	return slices.Clone(cs.retainedUnacked)
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
	openCtx, cancelOpen := context.WithTimeout(ctx, cs.cfg.RecoveryTimeout)
	stream, err := cs.opener.Open(openCtx, cs.params)
	openTimedOut := errors.Is(openCtx.Err(), context.DeadlineExceeded)
	cancelOpen()
	if err != nil {
		openErr := &openFailure{cause: fmt.Errorf("stream: open: %w", err)}
		// Invalid parameters cannot be fixed by reconnecting.
		if errors.Is(err, transport.ErrInvalidParams) {
			return wrapValidation(openErr), false
		}
		if openTimedOut && ctx.Err() == nil {
			return &openBudgetExceeded{cause: openErr}, false
		}
		return openErr, false
	}
	openedAt := time.Now()
	startAck := cs.wm.current()

	// Resend unacknowledged items on the new connection.
	cs.buf.requeue()

	// senderCtx controls only this connection's sender.
	senderCtx, cancelSender := context.WithCancel(ctx)
	defer cancelSender()

	senderExitCh := make(chan error, 1)
	receiverExitCh := make(chan error, 1)
	pauseCh := make(chan pauseSignal, 1)
	flightSignal := make(chan struct{}, 1)
	sendEvents := make(chan sendEvent, 2)

	go cs.sender(senderCtx, stream, senderExitCh, flightSignal, sendEvents)
	go cs.receiver(stream, receiverExitCh, pauseCh, flightSignal, sendEvents)

	var senderParked bool
	var senderExited bool
	var receiverExited bool
	var observedPause *pauseSignal
	recordPause := func(ps pauseSignal) {
		if observedPause == nil {
			pauseCopy := ps
			observedPause = &pauseCopy
		}
		if !senderParked {
			cancelSender()
			senderParked = true
		}
	}
	drainPause := func() {
		if observedPause != nil {
			return
		}
		select {
		case ps := <-pauseCh:
			recordPause(ps)
		default:
		}
	}
waitLoop:
	for {
		select {
		case senderErr := <-senderExitCh:
			senderExited = true
			drainPause()
			if !senderParked {
				cause = senderErr
				break waitLoop
			}
		case receiverErr := <-receiverExitCh:
			receiverExited = true
			drainPause()
			if observedPause != nil {
				cause = *observedPause
			} else {
				cause = receiverErr
			}
			break waitLoop
		case ps := <-pauseCh:
			recordPause(ps)
		case <-ctx.Done():
			break waitLoop
		}
	}

	cancelSender()
	var ps pauseSignal
	switch {
	case ctx.Err() != nil:
		cs.gracefulTeardown(
			stream, senderExited, receiverExited, senderExitCh, receiverExitCh,
		)
	case errors.As(cause, &ps):
		stream.Close()
		if !senderExited {
			<-senderExitCh
		}
		if !receiverExited {
			<-receiverExitCh
		}
	default:
		// Failure/recovery path: the stream is already broken, so hard-abort to
		// unblock the receiver's Recv immediately.
		stream.Close()
		if !senderExited {
			<-senderExitCh
		}
		if !receiverExited {
			<-receiverExitCh
		}
	}
	resetRecoveryBudget = cs.wm.current() > startAck ||
		time.Since(openedAt) >= cs.cfg.RecoveryResetAfter
	return cause, resetRecoveryBudget
}

// gracefulTeardown stops sending and drains responses to EOF.
// DrainTimeout prevents shutdown from hanging.
func (cs *CoreStream[Req, Resp]) gracefulTeardown(
	stream wireStream[Req, Resp],
	senderExited, receiverExited bool,
	senderExitCh, receiverExitCh <-chan error,
) {
	// One shared deadline bounds the whole graceful shutdown, so the sender-exit
	// wait and the receiver-drain wait can't each consume a full DrainTimeout.
	timer := time.NewTimer(cs.cfg.DrainTimeout)
	defer timer.Stop()

	if !senderExited {
		select {
		case <-senderExitCh:
		case <-timer.C:
			// Abort a blocked Send so Close can finish.
			stream.Close()
			<-senderExitCh
			if !receiverExited {
				<-receiverExitCh
			}
			return
		}
	}
	if receiverExited {
		stream.Close()
		return
	}
	if err := stream.CloseSend(); err != nil {
		// Send side already broken; fall back to a hard abort.
		stream.Close()
		<-receiverExitCh
		return
	}
	select {
	case <-receiverExitCh:
		// Receiver drained to EOF; release resources (Close is idempotent).
		stream.Close()
	case <-timer.C:
		// Drain budget exceeded; force the receiver out and reap it.
		stream.Close()
		<-receiverExitCh
	}
}

// sender writes queued items until cancellation or failure.
func (cs *CoreStream[Req, Resp]) sender(
	senderCtx context.Context,
	stream wireStream[Req, Resp],
	errCh chan<- error,
	flightSignal chan<- struct{},
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
		// Arm the ack timeout before Send can block.
		select {
		case flightSignal <- struct{}{}:
		default:
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
	pauseCh chan<- pauseSignal,
	flightSignal <-chan struct{},
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
	if !lackTimer.Stop() {
		<-lackTimer.C
	}
	lackTimerArmed := false
	armLackTimer := func() {
		if lackTimerArmed || cs.buf.inFlight() == 0 {
			return
		}
		lackTimer.Reset(cs.cfg.LackOfAckTimeout)
		lackTimerArmed = true
	}
	disarmLackTimer := func() {
		if !lackTimerArmed {
			return
		}
		if !lackTimer.Stop() {
			select {
			case <-lackTimer.C:
			default:
			}
		}
		lackTimerArmed = false
	}
	resetLackTimer := func() {
		disarmLackTimer()
		armLackTimer()
	}
	defer disarmLackTimer()

	var pauseState *pauseSignal
	var pauseTimer *time.Timer
	stopPauseTimer := func() {
		if pauseTimer != nil {
			pauseTimer.Stop()
			pauseTimer = nil
		}
	}
	defer stopPauseTimer()

	lastAckedPhysical := int64(-1)
	sendingPhysical := int64(-1)
	sendingLogical := int64(-1)
	pendingPhysicalAck := int64(-1)
	sentBasePhysical := int64(0)
	var sentLogical []int64

	applyLogicalAck := func(offset int64) error {
		current := cs.wm.current()
		if offset <= current {
			if cs.buf.inFlight() == 0 {
				disarmLackTimer()
			} else {
				resetLackTimer()
			}
			return nil
		}
		highest, ok := cs.buf.highestInFlight()
		if !ok || offset > highest {
			return fmt.Errorf(
				"stream: server ack offset %d exceeds highest in-flight offset %d",
				offset, highest,
			)
		}
		discarded := cs.buf.discardThrough(offset)
		if discarded.count > 0 {
			cs.wm.advance(discarded.last)
			cs.dispatcher.enqueueAcks(discarded.first, discarded.last)
		}
		if cs.buf.inFlight() == 0 {
			disarmLackTimer()
			if pauseState != nil {
				stopPauseTimer()
				return *pauseState
			}
		} else {
			resetLackTimer()
		}
		return nil
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
		if err := applyLogicalAck(logical); err != nil {
			return err
		}
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
		var lackC <-chan time.Time
		var pauseC <-chan time.Time
		if lackTimerArmed {
			lackC = lackTimer.C
		}
		if pauseTimer != nil {
			pauseC = pauseTimer.C
		}
		select {
		case event := <-sendEvents:
			if err := handleSendEvent(event); err != nil {
				handleTerminal(err)
				return
			}
			continue
		case <-flightSignal:
			armLackTimer()
			continue
		case <-lackC:
			lackTimerArmed = false
			if pauseState != nil {
				continue
			}
			if cs.buf.inFlight() == 0 {
				continue
			}
			stream.Close()
			<-recvCh // reap the outstanding Recv goroutine
			handleTerminal(fmt.Errorf(
				"stream: no ack from server for %s", cs.cfg.LackOfAckTimeout,
			))
			return
		case <-pauseC:
			pauseTimer = nil
			handleTerminal(*pauseState)
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
			// A response the ack model can't interpret (unrecognized type, or an
			// ack missing/with a negative offset) is a protocol violation. Tear the
			// stream down rather than silently dropping it; the supervisor decides
			// whether to reconnect. The current Recv already delivered and no new
			// one is outstanding, so there's nothing to reap.
			handleTerminal(fmt.Errorf("stream: unusable server response (kind %d)", kind))
			return
		}
		if kind == pauseResponse {
			if pauseState == nil {
				pauseCopy := pause
				pauseState = &pauseCopy
				select {
				case pauseCh <- pause:
				default:
				}
				if cs.buf.inFlight() == 0 {
					// Recover immediately when nothing needs draining.
					handleTerminal(pause)
					return
				}
				wait := cs.effectivePauseWait(pause.duration)
				if wait <= 0 {
					handleTerminal(pause)
					return
				}
				pauseTimer = time.NewTimer(wait)
				disarmLackTimer()
			}
			// Drain late acks during the pause.
			spawnRecv()
			continue
		}

		// Not a terminal response — keep reading. Spawn the next Recv before
		// handling this one so the loop always has exactly one outstanding.
		spawnRecv()

		if kind == ackResponse {
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

func (cs *CoreStream[Req, Resp]) effectivePauseWait(serverDuration time.Duration) time.Duration {
	wait := serverDuration
	// An explicit cap (including zero) overrides the server duration when it is
	// shorter; nil leaves the server-requested wait untouched.
	if cap := cs.cfg.StreamPausedMaxWait; cap != nil && (wait <= 0 || *cap < wait) {
		wait = *cap
	}
	return wait
}
