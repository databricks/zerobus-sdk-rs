package stream

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strings"
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
	// DefaultMaxBufferedPayloadBytes bounds estimated encoded memory retained by
	// queued and in-flight payloads.
	DefaultMaxBufferedPayloadBytes = 64 * 1024 * 1024
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
	// MaxInflight is the maximum number of unacknowledged buffer entries, not
	// records: one Ingest or IngestBatch call occupies one entry regardless of
	// how many records it carries. MaxBufferedPayloadBytes bounds the same set
	// by estimated retained size; whichever limit binds first applies
	// backpressure.
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
	// RecoveryResetAfter resets the retry budget once a connection stays open
	// this long without failing.
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
	// MaxBufferedPayloadBytes caps estimated encoded memory retained by queued
	// and in-flight payloads, including per-record and request-object overhead.
	MaxBufferedPayloadBytes int64
	// CallbackTeardownTimeout bounds asynchronous callback worker teardown.
	CallbackTeardownTimeout time.Duration
	// StreamPausedMaxWait caps how long the client honors a server-requested
	// pause before reconnecting: the wait is min(StreamPausedMaxWait, server
	// duration). Nil means no client cap (wait the full server-requested
	// duration); an explicit zero means reconnect immediately. A pause carrying
	// no duration reconnects immediately either way.
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
		MaxBufferedPayloadBytes: DefaultMaxBufferedPayloadBytes,
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
	if c.MaxBufferedPayloadBytes <= 0 {
		c.MaxBufferedPayloadBytes = DefaultMaxBufferedPayloadBytes
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
// The per-stream goroutines:
//
//	sender   — pulls items from the buffer, writes them to the wire stream.
//	receiver — reads acks from the wire stream, advances the watermark.
//	supervisor — create → run → recover loop; wires sender+receiver together.
//	dispatcher — delivers AckCallback events off the receiver's path.
//
// The receiver owns a persistent pump that keeps one Recv outstanding so ack
// silence stays observable while a Recv is blocked.
//
// Callers interact only through Ingest, IngestBatch, Flush, WaitForOffset,
// GetUnacked, GetUnackedBatches, and Close.
type CoreStream[Req, Resp any] struct {
	params     StreamParams
	cfg        Config
	opener     opener[Req, Resp]
	enc        encoder[Req]
	ackMdl     ackModel[Resp]
	buf        *buffer[Req]
	wm         *watermark
	dispatcher *callbackDispatcher
	pauseWait  func(context.Context, time.Time) bool

	clientID string
	serverID atomic.Pointer[string]

	// offsetMu protects offset assignment.
	offsetMu sync.Mutex
	// nextOffset is the next logical offset to assign.
	nextOffset int64
	// closing prevents new items from being admitted once Close snapshots its
	// durability target. It is protected by offsetMu.
	closing bool
	// offsetExhausted is set after assigning math.MaxInt64.
	offsetExhausted atomic.Bool
	// lastEnqueued is the highest queued offset.
	lastEnqueued atomic.Int64

	// done is closed when the supervisor exits (terminal state).
	done chan struct{}
	// readyCh closes once the first-open result is known. A nil readyErr means
	// at least one open succeeded; a non-nil readyErr means the first-open
	// process failed terminally or was cancelled before succeeding.
	readyCh  chan struct{}
	readyMu  sync.Mutex
	readyErr error
	readySet bool
	// termErr holds the first terminal error after done is closed.
	termErr         error
	retainedUnacked []item[Req]
	termMu          sync.Mutex

	closeOnce sync.Once
	closeErr  error
	// cancelSupervisor cancels the supervisor's context so Close unblocks it.
	cancelSupervisor context.CancelFunc
}

// newClientStreamID mints the client-side stream identifier sent at handshake.
// crypto/rand.Read never returns an error: it fills the buffer completely or
// crashes the program, so there is no degraded-entropy path to fall back to.
func newClientStreamID() string {
	var id [16]byte
	rand.Read(id[:])
	return "client-stream-" + hex.EncodeToString(id[:])
}

// NewCoreStream constructs a CoreStream and starts the supervisor goroutine.
// The stream is immediately ready for Ingest calls; the supervisor opens the
// first transport stream in the background. openingCtx supplies values for the
// stream lifetime and bounds the complete first-open process. After the first
// successful open, caller cancellation is detached and Close owns lifecycle
// cancellation. Prefer the per-protocol constructors (NewProtoJSONStream) over
// calling this directly.
func NewCoreStream[Req, Resp any](
	openingCtx context.Context,
	params StreamParams,
	cfg Config,
	opener opener[Req, Resp],
	enc encoder[Req],
	ackMdl ackModel[Resp],
	callback AckCallback,
) *CoreStream[Req, Resp] {
	cfg = sanitizeConfig(cfg)
	if cfg.StreamPausedMaxWait != nil {
		pauseCap := *cfg.StreamPausedMaxWait
		cfg.StreamPausedMaxWait = &pauseCap
	}
	params.TableName = strings.TrimSpace(params.TableName)
	if len(params.DescriptorProto) > 0 {
		params.DescriptorProto = bytes.Clone(params.DescriptorProto)
	}
	lifecycleCtx, cancel := context.WithCancel(context.WithoutCancel(openingCtx))
	cs := &CoreStream[Req, Resp]{
		params:           params,
		cfg:              cfg,
		opener:           opener,
		enc:              enc,
		ackMdl:           ackMdl,
		buf:              newBuffer[Req](cfg.MaxInflight, cfg.MaxBufferedPayloadBytes),
		wm:               newWatermark(),
		dispatcher:       newCallbackDispatcher(callback),
		clientID:         newClientStreamID(),
		pauseWait:        waitUntil,
		done:             make(chan struct{}),
		readyCh:          make(chan struct{}),
		cancelSupervisor: cancel,
	}
	cs.lastEnqueued.Store(-1)
	go cs.supervise(lifecycleCtx, openingCtx)
	return cs
}

// WaitReady waits for the first-open result. It returns nil once the stream has
// opened at least once, or an error if first-open fails terminally or the
// stream is closed before first-open succeeds.
func (cs *CoreStream[Req, Resp]) WaitReady(ctx context.Context) error {
	readyResult := func() error {
		cs.readyMu.Lock()
		defer cs.readyMu.Unlock()
		return cs.readyErr
	}

	select {
	case <-cs.readyCh:
		return readyResult()
	case <-ctx.Done():
		// If both become ready together, prefer the published first-open result
		// over the caller's context so a completed open outcome is never masked.
		select {
		case <-cs.readyCh:
			return readyResult()
		default:
			return ctx.Err()
		}
	}
}

func (cs *CoreStream[Req, Resp]) signalReady(err error) {
	cs.readyMu.Lock()
	if cs.readySet {
		cs.readyMu.Unlock()
		return
	}
	cs.readySet = true
	cs.readyErr = err
	close(cs.readyCh)
	cs.readyMu.Unlock()
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

func waitUntil(ctx context.Context, deadline time.Time) bool {
	if wait := time.Until(deadline); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-timer.C:
			return true
		case <-ctx.Done():
			return false
		}
	}
	return true
}

// Ingest encodes record and enqueues it in the buffer, blocking if the buffer
// is at capacity (backpressure). Returns the logical offset assigned to this
// record, or -1 with an error when the record is not queued. For throughput,
// queue records in a loop and call Flush once; waiting for every offset
// serializes ingestion on server round trips.
func (cs *CoreStream[Req, Resp]) Ingest(ctx context.Context, record []byte) (int64, error) {
	if err := cs.checkRawSize(len(record)); err != nil {
		return -1, err
	}
	weight := cs.enc.retainedSize(len(record), 1)
	return cs.enqueueEncoded(ctx, weight, func() (Req, error) {
		return cs.enc.encode(record)
	})
}

// IngestBatch encodes records as a single atomic batch and enqueues it, blocking
// if the buffer is at capacity. The whole batch occupies one logical offset and
// the server acks it atomically. Returns that offset, -1 for an empty batch
// (a no-op), or -1 with an error when the batch is not queued. Prefer this over
// Ingest in hot paths: it amortizes per-message overhead across the batch.
func (cs *CoreStream[Req, Resp]) IngestBatch(ctx context.Context, records [][]byte) (int64, error) {
	if len(records) == 0 {
		// Rust returns Ok(None) for empty batches; in Go we model that as a
		// no-op with sentinel offset -1 and no queueing.
		return -1, nil
	}
	if len(records) > cs.cfg.MaxBatchRecords {
		return -1, fmt.Errorf("%w: %d records exceeds MaxBatchRecords=%d",
			ErrPayloadTooLarge, len(records), cs.cfg.MaxBatchRecords)
	}
	total := 0
	for _, record := range records {
		if len(record) > cs.cfg.MaxPayloadBytes || total > cs.cfg.MaxPayloadBytes-len(record) {
			return -1, fmt.Errorf("%w: raw batch exceeds MaxPayloadBytes=%d",
				ErrPayloadTooLarge, cs.cfg.MaxPayloadBytes)
		}
		total += len(record)
	}
	if err := cs.checkRawSize(total); err != nil {
		return -1, err
	}
	weight := cs.enc.retainedSize(total, len(records))
	return cs.enqueueEncoded(ctx, weight, func() (Req, error) {
		return cs.enc.encodeBatch(records)
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
// Failed ingest returns -1 to match the original Go SDK (offsets start at 0).
func (cs *CoreStream[Req, Resp]) enqueueEncoded(
	ctx context.Context,
	weight int64,
	encodeFn func() (Req, error),
) (int64, error) {
	if cs.isClosed() {
		if err := cs.terminalErr(); err != nil {
			return -1, err
		}
		return -1, errClosed
	}
	cs.offsetMu.Lock()
	closing := cs.closing
	cs.offsetMu.Unlock()
	if closing {
		return -1, errClosed
	}
	if cs.offsetExhausted.Load() {
		return -1, ErrOffsetExhausted
	}
	if err := cs.buf.reserve(ctx, weight); err != nil {
		return -1, err
	}

	msg, err := encodeFn()
	if err != nil {
		cs.buf.release(weight)
		return -1, err
	}
	if size := cs.enc.maxWireSize(msg); size > cs.cfg.MaxPayloadBytes {
		cs.buf.release(weight)
		return -1, fmt.Errorf("%w: encoded size %d exceeds MaxPayloadBytes=%d",
			ErrPayloadTooLarge, size, cs.cfg.MaxPayloadBytes)
	}

	cs.offsetMu.Lock()
	if cs.closing {
		cs.offsetMu.Unlock()
		cs.buf.release(weight)
		return -1, errClosed
	}
	if cs.offsetExhausted.Load() {
		cs.offsetMu.Unlock()
		cs.buf.release(weight)
		return -1, ErrOffsetExhausted
	}
	offset := cs.nextOffset
	cs.enc.stampOffset(msg, offset)
	if err := cs.buf.append(offset, msg, weight); err != nil {
		cs.offsetMu.Unlock()
		return -1, err
	}
	if offset == math.MaxInt64 {
		cs.offsetExhausted.Store(true)
	} else {
		cs.nextOffset++
	}
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
	return cs.flushThrough(ctx, target)
}

func (cs *CoreStream[Req, Resp]) flushThrough(ctx context.Context, target int64) error {
	if target < 0 {
		// Nothing was successfully ingested, so there is no watermark to wait
		// for. A stream that already failed terminally — an unknown table or a
		// rejected credential on the background open — must still report that
		// failure here rather than a bare success, since Flush is where such a
		// failure is documented to surface. A clean Close sets no terminal
		// error, so it still reports nil.
		return cs.terminalErr()
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

// GetUnackedBatches returns records that were ingested but never acknowledged,
// grouped as they were submitted: one entry per Ingest or IngestBatch call, in
// offset order. The grouping is the unit the server acks atomically, so a caller
// replaying after a failure can resubmit each group as one batch and reproduce
// the original durability boundaries. Recovering a partial batch requires
// knowing which records shared an offset, which a flat list cannot express.
//
// The result is a fresh copy on every call: the unacked set is consolidated into
// retained storage once, and each call decodes a clone. A diagnostic read
// therefore never removes the records a later retry or persistence path needs,
// and mutating the returned bytes never corrupts the retained payloads.
//
// Calling this on an active stream returns ErrStreamStillActive; callers must
// close or wait for terminal failure first.
func (cs *CoreStream[Req, Resp]) GetUnackedBatches() ([][][]byte, error) {
	if !cs.isClosed() {
		return nil, ErrStreamStillActive
	}
	items := cs.consolidateUnacked()
	out := make([][][]byte, 0, len(items))
	for _, it := range items {
		// Re-extract the raw record bytes from the encoded message so callers get
		// back the original record content. decode clones, so the retained payload
		// is never aliased.
		out = append(out, cs.enc.decode(it.payload))
	}
	return out, nil
}

// GetUnacked is the flattened form of GetUnackedBatches: one entry per record,
// batch boundaries discarded. A batched item expands to all of its records (not
// just the first), so no unacked record is silently dropped. Prefer
// GetUnackedBatches when the records will be replayed rather than only inspected.
func (cs *CoreStream[Req, Resp]) GetUnacked() ([][]byte, error) {
	batches, err := cs.GetUnackedBatches()
	if err != nil {
		return nil, err
	}
	out := make([][]byte, 0, len(batches))
	for _, batch := range batches {
		out = append(out, batch...)
	}
	return out, nil
}

// Close stops admission, flushes every record admitted before the close
// boundary, then terminates the stream and releases its transport and buffer
// resources. It is idempotent, blocks until lifecycle teardown completes, and
// returns the same durability result to every caller.
// AckCallback delivery is asynchronous and may finish after Close returns.
//
// If flush cannot complete within FlushTimeout, Close proceeds with teardown and
// any remaining records are abandoned (retrievable via GetUnacked, or reported
// through the AckCallback's OnError). On a clean shutdown the live stream is
// torn down gracefully (half-close, then drain any straggling acks to EOF,
// bounded by DrainTimeout) so the server observes an orderly END_STREAM rather
// than an abrupt reset; see gracefulTeardown.
func (cs *CoreStream[Req, Resp]) Close() error {
	cs.closeOnce.Do(func() {
		// Publish closing under the same lock used for the final admission
		// check. Every successful Ingest is therefore either included in target
		// or linearizes after this boundary and returns errClosed.
		cs.offsetMu.Lock()
		cs.closing = true
		target := cs.lastEnqueued.Load()
		cs.offsetMu.Unlock()

		cs.closeErr = cs.flushThrough(context.Background(), target)
		cs.cancelSupervisor()
		cs.buf.close()
		<-cs.done // wait for the supervisor to exit
	})
	return cs.closeErr
}

// Terminate stops admission and tears the stream down immediately, without the
// final flush Close performs: records queued but not yet acknowledged are
// abandoned rather than waited on (still retrievable via GetUnacked, and
// reported through the AckCallback's OnError). Use it when the underlying
// connection is going away and waiting for acknowledgements cannot succeed.
//
// It is idempotent, blocks until lifecycle teardown completes, and shares its
// once-guard with Close, so whichever call runs first decides the result both
// report. It returns any terminal stream failure, nil otherwise.
func (cs *CoreStream[Req, Resp]) Terminate() error {
	cs.closeOnce.Do(func() {
		cs.offsetMu.Lock()
		cs.closing = true
		cs.offsetMu.Unlock()

		cs.cancelSupervisor()
		cs.buf.close()
		<-cs.done // wait for the supervisor to exit
		cs.closeErr = cs.terminalErr()
	})
	return cs.closeErr
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
}

// runOnce operates one transport stream until a worker exits. lifecycleCtx owns
// the live connection after open; openingCtx bounds only this open attempt.
// resetRecoveryBudget reports durable progress or a stable connection.
func (cs *CoreStream[Req, Resp]) runOnce(
	lifecycleCtx, openingCtx context.Context,
) (cause error, resetRecoveryBudget bool) {
	openCtx, cancelOpen := context.WithTimeout(openingCtx, cs.cfg.RecoveryTimeout)
	// SDK/Stream Close owns lifecycleCtx. Link it into the bounded open attempt
	// without making caller cancellation own the post-handshake connection.
	stopLifecycleCancel := context.AfterFunc(lifecycleCtx, cancelOpen)
	stream, err := cs.opener.Open(openCtx, cs.params)
	openTimedOut := errors.Is(openCtx.Err(), context.DeadlineExceeded)
	openingCtxDone := openingCtx.Err() != nil
	stopLifecycleCancel()
	cancelOpen()
	if err != nil {
		openErr := &openFailure{cause: fmt.Errorf("stream: open: %w", err)}
		// Invalid parameters cannot be fixed by reconnecting.
		if errors.Is(err, transport.ErrInvalidParams) {
			return wrapValidation(openErr), false
		}
		// A deadline that happens to expire alongside a permanent rejection must
		// not promote it to retryable: the status, or the error's own
		// classification, still decides.
		if openTimedOut && lifecycleCtx.Err() == nil && !openingCtxDone &&
			!transport.IsTerminalStatus(err) && !deniesRetry(err) {
			return &openBudgetExceeded{cause: openErr}, false
		}
		return openErr, false
	}
	cs.setServerID(stream.ServerID())
	cs.signalReady(nil)
	openedAt := time.Now()
	startAck := cs.wm.current()

	// Resend unacknowledged items on the new connection.
	cs.buf.requeue()

	// senderCtx controls only this connection's sender.
	senderCtx, cancelSender := context.WithCancel(lifecycleCtx)
	defer cancelSender()

	senderExitCh := make(chan error, 1)
	receiverExitCh := make(chan error, 1)
	receiverDone := make(chan struct{})
	pauseCh := make(chan pauseSignal, 1)
	flightSignal := make(chan struct{}, 1)
	// cap 2: at most a {start, completed} pair for the one record in flight.
	sendEvents := make(chan sendEvent, 2)
	sendConsumed := make(chan struct{}, 1)

	go cs.sender(senderCtx, stream, senderExitCh, flightSignal, sendEvents, sendConsumed)
	go func() {
		defer close(receiverDone)
		cs.receiver(stream, receiverExitCh, pauseCh, flightSignal, sendEvents, sendConsumed)
	}()

	var senderParked bool
	var senderExited bool
	var receiverReported bool
	var pauseObserved bool
	recordPause := func() {
		pauseObserved = true
		if !senderParked {
			cancelSender()
			senderParked = true
		}
	}
	drainPause := func() {
		if pauseObserved {
			return
		}
		select {
		case <-pauseCh:
			recordPause()
		default:
		}
	}
waitLoop:
	for {
		select {
		case senderErr := <-senderExitCh:
			senderExited = true
			drainPause()
			if senderParked {
				continue
			}
			cause = senderErr
			// A receiver-triggered teardown publishes its authoritative cause
			// before closing the stream. If that close unblocked Send, the
			// receiver cause is already available and must win arbitration.
			select {
			case receiverErr := <-receiverExitCh:
				receiverReported = true
				if receiverErr != nil && !errors.Is(receiverErr, context.Canceled) {
					cause = receiverErr
				}
			default:
			}
			break waitLoop
		case receiverErr := <-receiverExitCh:
			receiverReported = true
			drainPause()
			// The receiver surfaces a pause as its own exit error, so whatever it
			// reports is authoritative: a real failure still consumes the recovery
			// budget and reaches credential invalidation rather than being masked
			// by an earlier pause.
			cause = receiverErr
			break waitLoop
		case <-pauseCh:
			recordPause()
		case <-lifecycleCtx.Done():
			break waitLoop
		}
	}

	cancelSender()
	var ps pauseSignal
	switch {
	case lifecycleCtx.Err() != nil:
		cs.gracefulTeardown(
			stream, senderExited, senderExitCh, receiverDone,
		)
		// gracefulTeardown may have unblocked a Recv that was already
		// returning a definitive rejection as Close cancelled the lifecycle.
		// Preserve that result so the supervisor still invalidates credentials.
		select {
		case receiverErr := <-receiverExitCh:
			if transport.IsAuthRejection(receiverErr) {
				cause = receiverErr
			}
		default:
		}
	case errors.As(cause, &ps):
		stream.Close()
		if !senderExited {
			<-senderExitCh
		}
		<-receiverDone
	default:
		// gRPC reports a server-side abort to Send as an opaque io.EOF and carries
		// the real status (auth, schema, protocol) on Recv, so an EOF cause is not
		// authoritative yet. Let the receiver report first, bounded by DrainTimeout:
		// the Close below would otherwise replace that status with a cancellation,
		// leaving recovery to retry a permanent rejection and skip credential
		// invalidation. A failed send does not end the receiver, so it is still
		// reading and the server's status is what unblocks it.
		if !receiverReported && errors.Is(cause, io.EOF) {
			timer := time.NewTimer(cs.cfg.DrainTimeout)
			select {
			case receiverErr := <-receiverExitCh:
				receiverReported = true
				if receiverErr != nil && !errors.Is(receiverErr, context.Canceled) {
					cause = receiverErr
				}
			case <-timer.C:
			}
			timer.Stop()
		}
		// Failure/recovery path: the stream is already broken, so hard-abort to
		// unblock the receiver's Recv immediately.
		stream.Close()
		if !senderExited {
			<-senderExitCh
		}
		<-receiverDone
	}
	resetRecoveryBudget = cs.wm.current() > startAck ||
		time.Since(openedAt) >= cs.cfg.RecoveryResetAfter
	return cause, resetRecoveryBudget
}

// gracefulTeardown stops sending and drains responses to EOF.
// DrainTimeout prevents shutdown from hanging.
func (cs *CoreStream[Req, Resp]) gracefulTeardown(
	stream wireStream[Req, Resp],
	senderExited bool,
	senderExitCh <-chan error,
	receiverDone <-chan struct{},
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
			<-receiverDone
			return
		}
	}
	select {
	case <-receiverDone:
		stream.Close()
		return
	default:
	}
	if err := stream.CloseSend(); err != nil {
		// Send side already broken; fall back to a hard abort.
		stream.Close()
		<-receiverDone
		return
	}
	select {
	case <-receiverDone:
		// Receiver drained to EOF; release resources (Close is idempotent).
		stream.Close()
	case <-timer.C:
		// Drain budget exceeded; force the receiver out and reap it.
		stream.Close()
		<-receiverDone
	}
}

// sender writes queued items until cancellation or failure.
func (cs *CoreStream[Req, Resp]) sender(
	senderCtx context.Context,
	stream wireStream[Req, Resp],
	errCh chan<- error,
	flightSignal chan<- struct{},
	sendEvents chan<- sendEvent,
	sendConsumed <-chan struct{},
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
	physicalExhausted := false
	for {
		if physicalExhausted {
			errCh <- fmt.Errorf("stream: physical offset space exhausted")
			return
		}
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
		sendEvents <- sendEvent{
			logicalOffset:  it.offset,
			physicalOffset: physicalOffset,
			completed:      true,
			err:            err,
		}
		if err != nil {
			errCh <- fmt.Errorf("stream: send offset %d: %w", it.offset, err)
			return
		}
		if physicalOffset == math.MaxInt64 {
			physicalExhausted = true
		} else {
			physicalOffset++
		}
		select {
		case <-sendConsumed:
		case <-senderCtx.Done():
			errCh <- nil
			return
		}
	}
}

// receiver processes responses and advances the ack watermark.
func (cs *CoreStream[Req, Resp]) receiver(
	stream wireStream[Req, Resp],
	errCh chan<- error,
	pauseCh chan<- pauseSignal,
	flightSignal <-chan struct{},
	sendEvents <-chan sendEvent,
	sendConsumed chan<- struct{},
) {
	type recvResult struct {
		resp Resp
		err  error
	}

	recvCh := make(chan recvResult)
	recvStop := make(chan struct{})
	recvDone := make(chan struct{})
	go func() {
		defer close(recvDone)
		for {
			resp, err := stream.Recv()
			select {
			case recvCh <- recvResult{resp, err}:
			case <-recvStop:
				return
			}
			if err != nil {
				return
			}
		}
	}()
	var stopRecvOnce sync.Once
	stopRecv := func() {
		stopRecvOnce.Do(func() {
			close(recvStop)
			stream.Close()
		})
		<-recvDone
	}

	lackTimer := time.NewTimer(cs.cfg.LackOfAckTimeout)
	lackTimer.Stop()
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
		// Go 1.23+ guarantees that Stop prevents stale timer values.
		lackTimer.Stop()
		lackTimerArmed = false
	}
	// syncLackTimer realigns the ack-silence budget with the in-flight set. Only
	// durable progress restarts it: a stale or duplicate cumulative ack must not
	// extend the budget, or a server repeating one offset could postpone recovery
	// indefinitely while later offsets stay unacknowledged.
	syncLackTimer := func(progressed bool) {
		if cs.buf.inFlight() == 0 {
			disarmLackTimer()
			return
		}
		if progressed {
			disarmLackTimer()
		}
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
			syncLackTimer(false)
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
		syncLackTimer(discarded.count > 0)
		if cs.buf.inFlight() == 0 && pauseState != nil {
			stopPauseTimer()
			return *pauseState
		}
		return nil
	}
	applyPhysicalAck := func(offset int64) error {
		if offset <= lastAckedPhysical {
			syncLackTimer(false)
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
		var result error
		if event.err == nil {
			expected := sentBasePhysical + int64(len(sentLogical))
			if event.physicalOffset != expected {
				result = fmt.Errorf(
					"stream: physical send offset %d is not contiguous after %d",
					event.physicalOffset, expected-1,
				)
			} else {
				sentLogical = append(sentLogical, event.logicalOffset)
			}
		}
		sendingPhysical = -1
		sendingLogical = -1
		var ackErr error
		if event.err == nil && pendingPhysicalAck >= 0 {
			ackErr = applyPhysicalAck(pendingPhysicalAck)
		}
		pendingPhysicalAck = -1
		if result == nil {
			result = ackErr
		}
		sendConsumed <- struct{}{}
		return result
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
		var ps pauseSignal
		if err != nil && !errors.As(err, &ps) {
			// Real receiver failures are authoritative. Publish before any
			// operation that can close the transport and unblock Send.
			errCh <- err
			stopRecv()
			return
		}
		if pendingErr := resolvePendingOnExit(); pendingErr != nil {
			if err == nil || errors.As(err, &ps) {
				err = pendingErr
			}
		}
		// Publish the resolved clean/pause outcome before final pump teardown.
		errCh <- err
		stopRecv()
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
			if pauseState != nil {
				handleTerminal(*pauseState)
			} else {
				handleTerminal(nil)
			}
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
			// whether to reconnect. handleTerminal reaps the pump's next Recv.
			handleTerminal(fmt.Errorf("stream: unusable server response (kind %d)", kind))
			return
		}
		if kind == pauseResponse {
			if pauseState == nil {
				pauseCopy := pause
				wait := cs.effectivePauseWait(pause.duration)
				pauseCopy.resumeAt = time.Now().Add(wait)
				pauseState = &pauseCopy
				select {
				case pauseCh <- pauseCopy:
				default:
				}
				if cs.buf.inFlight() == 0 {
					handleTerminal(pauseCopy)
					return
				}
				if wait <= 0 {
					handleTerminal(pauseCopy)
					return
				}
				pauseTimer = time.NewTimer(wait)
				disarmLackTimer()
			}
			// Drain late acks during the pause.
			continue
		}

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
	// The cap is an upper bound only: min(cap, server duration). An unspecified
	// server duration stays zero so the stream reconnects immediately, the same as
	// with no cap set.
	if maxWait := cs.cfg.StreamPausedMaxWait; maxWait != nil && *maxWait < wait {
		wait = *maxWait
	}
	return wait
}
