package stream

import (
	"context"
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
)

// Config holds per-stream configuration. All fields have sane defaults via
// DefaultConfig(); override individual fields before passing to NewCoreStream.
type Config struct {
	// MaxInflight is the maximum number of unacknowledged records in the buffer.
	MaxInflight int
	// RecoveryEnabled controls whether stream reconnection is attempted on failure.
	RecoveryEnabled bool
	// RecoveryRetries is the maximum number of reconnect attempts before giving up.
	RecoveryRetries int
	// RecoveryBackoff is the fixed wait between reconnect attempts.
	RecoveryBackoff time.Duration
	// FlushTimeout bounds Flush when the caller's context has no deadline.
	FlushTimeout time.Duration
	// LackOfAckTimeout is how long the receiver waits for a server ack before
	// treating silence as a stream failure.
	LackOfAckTimeout time.Duration
}

// DefaultConfig returns a Config with SDK-standard defaults.
func DefaultConfig() Config {
	return Config{
		MaxInflight:      DefaultMaxInflight,
		RecoveryEnabled:  true,
		RecoveryRetries:  DefaultRecoveryRetries,
		RecoveryBackoff:  DefaultRecoveryBackoff,
		FlushTimeout:     DefaultFlushTimeout,
		LackOfAckTimeout: DefaultLackOfAckTimeout,
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
func (cs *CoreStream) WaitForOffset(ctx context.Context, offset int64) error {
	return cs.wm.waitFor(ctx, offset)
}

// GetUnacked returns records that were ingested but never acknowledged.
// It closes the stream first (idempotent) to ensure the buffer is fully
// drained and no new items are added.
func (cs *CoreStream) GetUnacked() [][]byte {
	cs.Close()
	items := cs.buf.drain()
	out := make([][]byte, len(items))
	for i, it := range items {
		// Re-extract the raw bytes from the encoded message so callers get
		// back the original record content.
		out[i] = extractBytes(it.payload)
	}
	return out
}

// Close flushes and then terminates the stream. It is idempotent.
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
	go cs.receiver(ctx, stream, errCh)

	// Wait for the first goroutine to signal; then tear both down.
	cause := <-errCh
	cancelSender() // unblocks sender waiting on buf.next()
	stream.Close() // unblocks receiver waiting on Recv()
	<-errCh        // drain second exit
	return cause
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

// receiver reads ack responses from stream and advances the watermark.
// Exits on io.EOF (server closed cleanly), a receive error, or ctx cancel.
//
// Each Recv call runs on its own goroutine so we can race it against
// ctx.Done(). This mirrors Rust's tokio::select! pattern in the receiver task
// and ensures the receiver unblocks promptly on Close/recovery even when the
// underlying transport Recv is blocking (e.g. a fake in tests, or a stalled
// server before the lack-of-ack timer fires).
func (cs *CoreStream) receiver(ctx context.Context, stream *transport.Stream, errCh chan<- error) {
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
		case <-ctx.Done():
			stream.Close() // unblock the recv goroutine above
			<-ch
			errCh <- nil
			return
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

		offset, ok := cs.ackMdl.parse(resp)
		if !ok {
			// Non-ack response (e.g. close signal); ignore.
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
		// Discard all buffer items that are now acknowledged.
		cs.buf.mu.Lock()
		for len(cs.buf.flight) > 0 && cs.buf.flight[0].offset <= offset {
			cs.buf.flight = cs.buf.flight[1:]
			cs.buf.mu.Unlock()
			<-cs.buf.sem
			cs.buf.mu.Lock()
		}
		cs.buf.mu.Unlock()

		if cs.callback != nil {
			cs.callback.OnAck(offset)
		}
	}
}

// extractBytes recovers the raw record bytes from an encoded wire message.
// Used by GetUnacked to return original content to the caller.
func extractBytes(msg encodedMsg) []byte {
	if msg == nil {
		return nil
	}
	if ir := msg.GetIngestRecord(); ir != nil {
		if b := ir.GetProtoEncodedRecord(); b != nil {
			return b
		}
		return []byte(ir.GetJsonRecord())
	}
	if ib := msg.GetIngestRecordBatch(); ib != nil {
		if pb := ib.GetProtoEncodedBatch(); pb != nil && len(pb.GetRecords()) > 0 {
			return pb.GetRecords()[0]
		}
		if jb := ib.GetJsonBatch(); jb != nil && len(jb.GetRecords()) > 0 {
			return []byte(jb.GetRecords()[0])
		}
	}
	return nil
}

// newAckModelForParams constructs the ackModel matching the stream's RecordType.
func newAckModelForParams(p StreamParams) (ackModel, error) {
	return newAckModel(p.RecordType)
}

// newEncoderForParams constructs the encoder matching the stream's RecordType.
func newEncoderForParams(p StreamParams) (encoder, error) {
	switch p.RecordType {
	case zerobuspb.RecordType_PROTO:
		return protoEncoder{}, nil
	case zerobuspb.RecordType_JSON:
		return jsonEncoder{}, nil
	default:
		return nil, fmt.Errorf("stream: unsupported record type %v", p.RecordType)
	}
}
