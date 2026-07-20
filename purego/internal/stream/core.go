package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/databricks/zerobus-sdk/purego/internal/transport"
	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

// Default config constants matching the Rust SDK (stream_options.rs).
const (
	DefaultMaxInflight      = 1_000_000
	DefaultRecoveryRetries  = 4
	DefaultRecoveryBackoff  = 2 * time.Second
	DefaultFlushTimeout     = 5 * time.Minute
	DefaultLackOfAckTimeout = 60 * time.Second
	// DefaultDrainTimeout bounds the orderly drain-to-EOF on a clean Close so a
	// wedged sender or an unresponsive server can't hang teardown. Matches the
	// transport's teardown drain budget; the long ack wait lives in Flush.
	DefaultDrainTimeout = 500 * time.Millisecond
)

// RecoverySetting controls whether a stream reconnects on failure. It is an enum
// rather than a bool so its zero value is the safe default (recovery enabled): a
// zero-valued Config recovers, and disabling is an explicit opt-out. This avoids
// the bool zero-value footgun where Config{...} silently disables recovery.
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
	// RecoveryRetries is the maximum number of reconnect attempts before giving up.
	RecoveryRetries int
	// RecoveryBackoff is the fixed wait between reconnect attempts.
	RecoveryBackoff time.Duration
	// FlushTimeout bounds Flush when the caller's context has no deadline.
	FlushTimeout time.Duration
	// LackOfAckTimeout is how long the receiver waits for a server ack before
	// treating silence as a stream failure.
	LackOfAckTimeout time.Duration
	// DrainTimeout bounds the orderly drain-to-EOF performed on a clean Close so
	// the server observes an orderly END_STREAM rather than an abrupt reset.
	DrainTimeout time.Duration
	// StreamPausedMaxWait caps how long the client waits after a server
	// CloseStreamSignal before reconnecting. The effective wait is
	// min(StreamPausedMaxWait, server-requested duration). A non-positive value
	// means "no client cap": wait the full server-requested duration. This lets a
	// caller trade graceful drain (wait longer) against faster recovery.
	StreamPausedMaxWait time.Duration
}

// DefaultConfig returns a Config with SDK-standard defaults.
func DefaultConfig() Config {
	return Config{
		MaxInflight: DefaultMaxInflight,
		// Recovery left as its zero value, RecoveryEnabled.
		RecoveryRetries:  DefaultRecoveryRetries,
		RecoveryBackoff:  DefaultRecoveryBackoff,
		FlushTimeout:     DefaultFlushTimeout,
		LackOfAckTimeout: DefaultLackOfAckTimeout,
		DrainTimeout:     DefaultDrainTimeout,
	}
}

// StreamParams mirrors transport.StreamParams but lives here so upper layers
// can use the stream package without importing transport directly.
type StreamParams = transport.StreamParams

// Opener opens a new transport stream. Injected so tests can supply a fake.
type Opener interface {
	Open(ctx context.Context, p StreamParams) (*transport.Stream, error)
}

// AckCallback is called once per acknowledged record/batch offset.
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

// waitFor blocks until the watermark reaches target or the watermark becomes
// terminal. Returns nil if the target is reached, or the terminal error.
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

// CoreStream is the protocol-agnostic ingestion core. It owns the buffer,
// sender goroutine, receiver goroutine, ack watermark, and the supervisor
// that reconnects on failure. Encoding and ack parsing are injected via the
// encoder and ackModel interfaces.
//
// The three goroutines:
//
//	sender   — pulls items from the buffer, writes them to the wire stream.
//	receiver — reads acks from the wire stream, advances the watermark.
//	supervisor — create → run → recover loop; wires sender+receiver together.
//
// Callers interact only through Ingest, Flush, WaitForOffset, GetUnacked,
// and Close.
type CoreStream struct {
	params   StreamParams
	cfg      Config
	opener   Opener
	enc      encoder
	ackMdl   ackModel
	buf      *buffer
	wm       *watermark
	callback AckCallback

	// ingestMu serializes offset assignment + enqueue so that nextOffset only
	// advances for records that were successfully queued. Without this, a failed
	// enqueue (e.g. ctx-cancelled backpressure) would consume an offset that is
	// never sent, making Flush wait forever for an offset the server never sees.
	ingestMu sync.Mutex
	// nextOffset is the next logical offset to assign. Protected by ingestMu.
	nextOffset int64
	// lastEnqueued is the highest offset successfully enqueued; -1 until the
	// first successful Ingest. Flush targets this, not nextOffset-1, so a
	// failed Ingest can't create a permanent gap.
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

// NewCoreStream constructs a CoreStream and starts the supervisor goroutine.
// The stream is immediately ready for Ingest calls; the supervisor opens the
// first transport stream in the background.
func NewCoreStream(
	params StreamParams,
	cfg Config,
	opener Opener,
	enc encoder,
	ackMdl ackModel,
	callback AckCallback,
) *CoreStream {
	ctx, cancel := context.WithCancel(context.Background())
	cs := &CoreStream{
		params:           params,
		cfg:              cfg,
		opener:           opener,
		enc:              enc,
		ackMdl:           ackMdl,
		buf:              newBuffer(cfg.MaxInflight),
		wm:               newWatermark(),
		callback:         callback,
		lastEnqueued:     -1,
		done:             make(chan struct{}),
		cancelSupervisor: cancel,
	}
	go cs.supervise(ctx)
	return cs
}

// Ingest encodes record and enqueues it in the buffer, blocking if the buffer
// is at capacity (backpressure). Returns the logical offset assigned to this
// record; pass it to WaitForOffset to confirm durability.
func (cs *CoreStream) Ingest(ctx context.Context, record []byte) (int64, error) {
	if cs.isClosed() {
		if err := cs.terminalErr(); err != nil {
			return 0, err
		}
		return 0, errClosed
	}
	// ingestMu serializes offset assignment + enqueue so that nextOffset only
	// advances for records that actually make it into the buffer. A failed
	// enqueue (encode error or ctx-cancelled backpressure) must not consume an
	// offset; otherwise Flush would wait for an offset the server never sees.
	cs.ingestMu.Lock()
	defer cs.ingestMu.Unlock()

	offset := cs.nextOffset
	msg, err := cs.enc.encode(offset, record)
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

// Flush blocks until every record ingested so far is acknowledged by the
// server. Returns nil once all are durable, or an error if the stream fails
// or ctx expires.
//
// When ctx has no deadline, Flush applies DefaultFlushTimeout.
func (cs *CoreStream) Flush(ctx context.Context) error {
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

// WaitForOffset blocks until the server has acknowledged all records up to and
// including offset, or until ctx expires or the stream fails terminally.
//
// offset must be one returned by a successful Ingest. Passing an offset that was
// never enqueued (e.g. a value above the last assigned offset) is a caller error:
// since the watermark can never reach it, the call blocks until ctx expires (or
// the stream fails). Prefer Flush, which waits for exactly the records ingested
// so far.
func (cs *CoreStream) WaitForOffset(ctx context.Context, offset int64) error {
	return cs.wm.waitFor(ctx, offset)
}

// GetUnacked returns records that were ingested but never acknowledged, one
// entry per record. A batched buffer item expands to all of its records (not
// just the first), so no unacked record is silently dropped. It closes the
// stream first (idempotent) to ensure the buffer is fully drained and no new
// items are added.
func (cs *CoreStream) GetUnacked() [][]byte {
	cs.Close()
	items := cs.buf.drain()
	out := make([][]byte, 0, len(items))
	for _, it := range items {
		// Re-extract the raw record bytes from the encoded message so callers get
		// back the original record content; a batch yields all its records.
		out = append(out, extractRecords(it.payload)...)
	}
	return out
}

// Close terminates the stream and releases its resources. It is idempotent and
// blocks until teardown completes.
//
// Close does NOT wait for pending records to be acknowledged: any records still
// in flight when Close is called are abandoned (retrievable via GetUnacked, or
// reported through the AckCallback's OnError). Callers that need durability must
// Flush first, then Close — the standard loop-then-Flush pattern. On this clean
// shutdown the live stream is torn down gracefully (half-close, then drain any
// straggling acks to EOF, bounded by DrainTimeout) so the server observes an
// orderly END_STREAM rather than an abrupt reset; see gracefulTeardown.
func (cs *CoreStream) Close() {
	cs.closeOnce.Do(func() {
		cs.cancelSupervisor()
		cs.buf.close()
		<-cs.done // wait for the supervisor to exit
	})
}

// IsClosed reports whether the stream has been closed or failed terminally.
func (cs *CoreStream) IsClosed() bool {
	return cs.isClosed()
}

func (cs *CoreStream) isClosed() bool {
	select {
	case <-cs.done:
		return true
	default:
		return false
	}
}

func (cs *CoreStream) terminalErr() error {
	cs.termMu.Lock()
	defer cs.termMu.Unlock()
	return cs.termErr
}

func (cs *CoreStream) setTerminalErr(err error) {
	cs.termMu.Lock()
	if cs.termErr == nil {
		cs.termErr = err
	}
	cs.termMu.Unlock()
}

// runOnce opens one transport stream and runs sender+receiver until one of
// them exits. Returns the cause of the exit so the supervisor can decide
// whether to recover.
func (cs *CoreStream) runOnce(ctx context.Context) error {
	stream, err := cs.opener.Open(ctx, cs.params)
	if err != nil {
		// Invalid params (bad table name, unsupported record type, missing
		// descriptor) are deterministic — reconnecting reproduces them — so mark
		// them non-retryable rather than burning the recovery budget on a failure
		// that can't succeed.
		if errors.Is(err, transport.ErrInvalidParams) {
			return wrapValidation(fmt.Errorf("stream: open: %w", err))
		}
		return fmt.Errorf("stream: open: %w", err)
	}

	// Move all in-flight items back to the pending queue so the sender
	// re-sends them on this new connection.
	cs.buf.requeue()

	// senderCtx is cancelled when we want the sender to stop for THIS stream
	// only, without cancelling the supervisor's outer ctx (which would signal Close).
	senderCtx, cancelSender := context.WithCancel(ctx)
	defer cancelSender()

	errCh := make(chan error, 2)

	go cs.sender(senderCtx, stream, errCh)
	go cs.receiver(stream, errCh)

	// Wait for the first goroutine to signal, then tear both down. On clean
	// shutdown (Close cancelled ctx) only the sender exits first — the receiver
	// keeps draining acks — so we half-close and let it drain to EOF for an
	// orderly server-observed END_STREAM. On failure we hard-abort instead.
	cause := <-errCh
	cancelSender() // unblocks sender waiting on buf.next()
	var ps pauseSignal
	switch {
	case ctx.Err() != nil:
		cs.gracefulTeardown(stream, errCh)
	case errors.As(cause, &ps):
		// Server-requested pause: the receiver already closed the stream to
		// unblock its Recv, so just reap the sender. Requeue happens on reconnect.
		stream.Close()
		<-errCh
	default:
		// Failure/recovery path: the stream is already broken, so hard-abort to
		// unblock the receiver's Recv immediately.
		stream.Close()
		<-errCh // drain second exit
	}
	return cause
}

// gracefulTeardown closes a healthy stream in an orderly fashion after the
// sender has stopped: half-close the send side so the server flushes any
// remaining acks and ends the stream, let the still-running receiver drain to
// io.EOF (advancing the watermark for late acks), then release resources.
// DrainTimeout bounds the wait so a wedged server can't hang Close; on expiry we
// hard-abort. This mirrors transport.GracefulClose, done cooperatively because
// the receiver goroutine — not this one — owns Recv.
func (cs *CoreStream) gracefulTeardown(stream *transport.Stream, errCh <-chan error) {
	if err := stream.CloseSend(); err != nil {
		// Send side already broken; fall back to a hard abort.
		stream.Close()
		<-errCh
		return
	}
	timer := time.NewTimer(cs.cfg.DrainTimeout)
	defer timer.Stop()
	select {
	case <-errCh:
		// Receiver drained to EOF; release resources (Close is idempotent).
		stream.Close()
	case <-timer.C:
		// Drain budget exceeded; force the receiver out and reap it.
		stream.Close()
		<-errCh
	}
}

// sender pulls items from the buffer and writes them to stream. Exits when
// senderCtx is cancelled (per-stream teardown), the buffer is closed, or
// Send fails. senderCtx is derived from a per-runOnce cancel so the supervisor
// can stop this sender without cancelling the outer supervisor context.
func (cs *CoreStream) sender(senderCtx context.Context, stream *transport.Stream, errCh chan<- error) {
	for {
		it, err := cs.buf.next(senderCtx)
		if err != nil {
			errCh <- nil // ctx cancelled or buffer closed — clean exit
			return
		}
		if err := stream.Send(it.payload); err != nil {
			errCh <- fmt.Errorf("stream: send offset %d: %w", it.offset, err)
			return
		}
	}
}

// receiver reads ack responses from stream and advances the watermark. It runs
// until io.EOF (server closed cleanly, including after a graceful half-close),
// a receive error, the lack-of-ack timeout, or the stream being Closed out from
// under it (which surfaces as a Recv error). Teardown is coordinated by runOnce,
// so the receiver does not watch the supervisor context itself.
//
// Each Recv call runs on its own goroutine so we can race it against the
// lack-of-ack timer even when the underlying transport Recv is blocking (e.g. a
// fake in tests, or a stalled server). This mirrors Rust's tokio::select!
// pattern in the receiver task.
func (cs *CoreStream) receiver(stream *transport.Stream, errCh chan<- error) {
	type recvResult struct {
		resp *zerobuspb.EphemeralStreamResponse
		err  error
	}

	lackTimer := time.NewTimer(cs.cfg.LackOfAckTimeout)
	defer lackTimer.Stop()

	for {
		ch := make(chan recvResult, 1)
		go func() {
			resp, err := stream.Recv()
			ch <- recvResult{resp, err}
		}()

		var r recvResult
		select {
		case <-lackTimer.C:
			stream.Close()
			<-ch
			errCh <- fmt.Errorf("stream: no ack from server for %s", cs.cfg.LackOfAckTimeout)
			return
		case r = <-ch:
		}

		if r.err == io.EOF {
			errCh <- nil
			return
		}
		if r.err != nil {
			errCh <- fmt.Errorf("stream: recv: %w", r.err)
			return
		}
		resp := r.resp

		// A server CloseStreamSignal is a flow-control request, not noise: the
		// server is about to close this stream and wants the client to pause
		// (stop sending, keep buffering and draining acks) then reconnect. Report
		// it as a pause so the supervisor waits the requested window and
		// reconnects without counting it against the recovery budget. This
		// iteration's Recv goroutine already delivered (via r above), so there is
		// nothing to drain; runOnce owns tearing the stream down.
		if sig := resp.GetCloseStreamSignal(); sig != nil {
			errCh <- pauseSignal{duration: sig.GetDuration().AsDuration()}
			return
		}

		offset, ok := cs.ackMdl.parse(resp)
		if !ok {
			// Some other non-ack response; ignore.
			continue
		}

		// Reset the lack-of-ack timer on every ack received.
		if !lackTimer.Stop() {
			select {
			case <-lackTimer.C:
			default:
			}
		}
		lackTimer.Reset(cs.cfg.LackOfAckTimeout)

		cs.wm.advance(offset)
		// Discard all buffer items that are now acknowledged, freeing their
		// backpressure slots for waiting enqueue callers.
		cs.buf.discardThrough(offset)

		if cs.callback != nil {
			cs.callback.OnAck(offset)
		}
	}
}

// extractRecords recovers the raw record bytes from an encoded wire message.
// A single-record message yields one entry; a batch yields all of its records
// so GetUnacked never silently drops the tail of an unacked batch. Used by
// GetUnacked to return original content to the caller.
func extractRecords(msg encodedMsg) [][]byte {
	if msg == nil {
		return nil
	}
	if ir := msg.GetIngestRecord(); ir != nil {
		if b := ir.GetProtoEncodedRecord(); b != nil {
			return [][]byte{b}
		}
		return [][]byte{[]byte(ir.GetJsonRecord())}
	}
	if ib := msg.GetIngestRecordBatch(); ib != nil {
		if pb := ib.GetProtoEncodedBatch(); pb != nil {
			return pb.GetRecords()
		}
		if jb := ib.GetJsonBatch(); jb != nil {
			recs := jb.GetRecords()
			out := make([][]byte, len(recs))
			for i, r := range recs {
				out[i] = []byte(r)
			}
			return out
		}
	}
	return nil
}
