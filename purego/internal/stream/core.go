package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/databricks/zerobus-sdk/purego/internal/transport"
)

// Default config constants for the ingestion core.
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
	// DefaultRecoveryTimeout bounds each Open attempt so a stalled dial cannot
	// hold recovery indefinitely.
	DefaultRecoveryTimeout = 15 * time.Second
	// DefaultCallbackTeardownTimeout bounds how long a terminal-state teardown
	// waits for pending user callbacks (OnAck/OnError) to drain before giving
	// up.
	DefaultCallbackTeardownTimeout = 5 * time.Second
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
// Non-positive values are normalized to their defaults by sanitizeConfig on
// stream construction so a caller-passed Config{} does not deadlock, spin, or
// abort teardown immediately.
type Config struct {
	// MaxInflight is the maximum number of unacknowledged buffer entries: a
	// single Ingest is one entry and an IngestBatch is also one entry, so
	// the memory bound this expresses is (MaxInflight × max encoded message
	// size). A future revision may add an explicit byte budget for finer
	// control; today MaxPayloadBytes caps per-message wire size.
	MaxInflight int
	// Recovery controls whether stream reconnection is attempted on failure. The
	// zero value (RecoveryEnabled) recovers; set RecoveryDisabled to opt out.
	Recovery RecoverySetting
	// RecoveryRetries is the maximum number of consecutive failed reconnect
	// attempts before giving up. The budget is per recovery episode: a stream
	// that connects and runs successfully resets it, so a long-lived stream that
	// disconnects occasionally is not doomed after RecoveryRetries lifetime
	// disconnects.
	RecoveryRetries int
	// RecoveryBackoff is the fixed wait between reconnect attempts.
	RecoveryBackoff time.Duration
	// RecoveryTimeout bounds each Open attempt so a stalled dial cannot pin
	// recovery; a per-attempt failure that hits this budget is retried like any
	// other transient error, subject to RecoveryRetries.
	RecoveryTimeout time.Duration
	// FlushTimeout bounds Flush when the caller's context has no deadline.
	FlushTimeout time.Duration
	// LackOfAckTimeout is how long the receiver waits for a server ack, while
	// records are in flight, before treating silence as a stream failure. An idle
	// stream with nothing in flight is never failed for silence.
	LackOfAckTimeout time.Duration
	// DrainTimeout bounds the orderly drain-to-EOF performed on a clean Close so
	// the server observes an orderly END_STREAM rather than an abrupt reset.
	DrainTimeout time.Duration
	// CallbackTeardownTimeout bounds how long terminal teardown waits for
	// pending OnAck/OnError callbacks to drain before giving up.
	CallbackTeardownTimeout time.Duration
	// MaxPayloadBytes caps the aggregate size of a single ingest call (one
	// record or the whole batch). Oversized payloads are rejected at Ingest
	// with ErrPayloadTooLarge so a deterministic input error does not turn into
	// a transport/recovery failure. Non-positive means "use DefaultMaxPayloadBytes".
	MaxPayloadBytes int
	// StreamPausedMaxWait caps how long the client waits after a server
	// CloseStreamSignal before reconnecting. The effective wait is
	// min(StreamPausedMaxWait, server-requested duration). See
	// StreamPausedMaxWaitMode for the sentinel-value semantics: the zero value
	// means "no client cap"; use StreamPausedImmediate to reconnect immediately.
	StreamPausedMaxWait time.Duration
	// StreamPausedMaxWaitMode selects between the pause-wait sentinels; zero =
	// use StreamPausedMaxWait verbatim (or no cap if that is also zero).
	StreamPausedMaxWaitMode PauseWaitMode
}

// PauseWaitMode selects the interpretation of a server-requested pause. The
// default (PauseWaitServer) waits the server-requested duration, capped by
// StreamPausedMaxWait if positive. PauseWaitImmediate reconnects immediately
// on any pause signal regardless of the server-requested duration, matching
// the "immediate reconnect on pause" configuration used elsewhere in the
// ingestion SDK family.
type PauseWaitMode int

const (
	// PauseWaitServer waits the server-requested duration (optionally capped by
	// StreamPausedMaxWait). Zero value.
	PauseWaitServer PauseWaitMode = iota
	// PauseWaitImmediate reconnects as soon as the server asks for a pause,
	// ignoring the server-requested duration entirely.
	PauseWaitImmediate
)

// DefaultConfig returns a Config with SDK-standard defaults.
func DefaultConfig() Config {
	return Config{
		MaxInflight: DefaultMaxInflight,
		// Recovery left as its zero value, RecoveryEnabled.
		RecoveryRetries:         DefaultRecoveryRetries,
		RecoveryBackoff:         DefaultRecoveryBackoff,
		RecoveryTimeout:         DefaultRecoveryTimeout,
		FlushTimeout:            DefaultFlushTimeout,
		LackOfAckTimeout:        DefaultLackOfAckTimeout,
		DrainTimeout:            DefaultDrainTimeout,
		CallbackTeardownTimeout: DefaultCallbackTeardownTimeout,
		MaxPayloadBytes:         DefaultMaxPayloadBytes,
	}
}

// sanitizeConfig normalizes non-positive or absent values to their defaults so
// a caller-passed Config{} does not (a) block every Ingest on an unbuffered
// semaphore (MaxInflight==0), (b) spin the receiver on a zero lack-of-ack
// timer, or (c) hard-abort teardown immediately (DrainTimeout==0). Kept
// separate from DefaultConfig so callers can still see which fields they
// explicitly set; only the fields that would break liveness or contradict
// the documented default behaviour are rewritten. Recovery,
// StreamPausedMaxWait, and StreamPausedMaxWaitMode are left as-is because
// their zero values have well-defined meaning.
//
// RecoveryRetries treats 0 and negative both as "unset → use default": a
// Config{} whose zero-valued Recovery says "recovery enabled" would
// otherwise reconnect zero times, contradicting the documented behaviour
// that a zero Config is safe. Callers who want a single-attempt stream
// use RecoverySetting=RecoveryDisabled instead (that path bypasses the
// retry loop entirely); if per-episode budget tuning is required beyond
// on/off, this is the config field a future revision would migrate to an
// explicit optional value.
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
	if c.FlushTimeout <= 0 {
		c.FlushTimeout = DefaultFlushTimeout
	}
	if c.LackOfAckTimeout <= 0 {
		c.LackOfAckTimeout = DefaultLackOfAckTimeout
	}
	if c.DrainTimeout <= 0 {
		c.DrainTimeout = DefaultDrainTimeout
	}
	if c.CallbackTeardownTimeout <= 0 {
		c.CallbackTeardownTimeout = DefaultCallbackTeardownTimeout
	}
	if c.MaxPayloadBytes <= 0 {
		c.MaxPayloadBytes = DefaultMaxPayloadBytes
	}
	return c
}

// AckCallback receives durability notifications for records that were
// Ingested successfully.
//
// # Delivery semantics
//
// Callbacks are invoked serially on a dedicated dispatcher goroutine, so a
// slow callback does not stall the receiver/supervisor and it is safe to
// call CoreStream methods (including Close) from inside a callback — the
// caller won't self-deadlock.
//
// OnAck is best-effort per newly-acknowledged logical offset. Under normal
// load one OnAck fires per offset covered by the ack (a cumulative server
// ack for offset 5 following an ack at offset 2 yields three OnAcks for
// offsets 3, 4, and 5); a repeated ack that discards no new items yields
// zero OnAcks. If the user's OnAck implementation stalls and the internal
// dispatch queue fills, further OnAcks are dropped rather than back-
// pressuring the receiver — the record's durability is unaffected (Flush /
// WaitForOffset remain accurate), only the observability signal is lost.
// Callers that require strict per-offset delivery must keep OnAck fast, or
// rely on WaitForOffset for durability confirmation.
//
// OnError fires on terminal failure, once per drained buffer item (a batch
// occupies a single item, so its OnError fires once with the batch's
// assigned offset). Delivery is bounded: if the user callback is stalled
// and the dispatch queue fills, later OnErrors during terminal drain may be
// dropped so the supervisor can complete teardown promptly; the unacked
// records remain retrievable via GetUnacked regardless.
type AckCallback interface {
	OnAck(offset int64)
	OnError(offset int64, err error)
}

// cbEvent is one callback dispatch unit; err==nil means OnAck.
type cbEvent struct {
	offset int64
	err    error
}

// callbackDispatcher owns the goroutine that invokes user AckCallback methods.
// It exists so the receiver and supervisor goroutines are never blocked by a
// slow user callback, and so callbacks that call CoreStream.Close() cannot
// self-deadlock (Close waits for the supervisor; the supervisor would
// otherwise wait for the callback that is waiting for Close).
type callbackDispatcher struct {
	cb        AckCallback
	events    chan cbEvent
	done      chan struct{}
	closeOnce sync.Once
	// inCallback is set to 1 while the dispatcher goroutine is inside a user
	// callback; shutdown() checks it to detect reentrant calls (a callback
	// invoked Close) and skip the synchronous wait that would otherwise
	// self-deadlock.
	inCallback atomic.Bool
}

// newCallbackDispatcher spawns the dispatcher goroutine. Returns nil (a no-op
// dispatcher, safely usable via nil checks at call sites) if cb is nil.
func newCallbackDispatcher(cb AckCallback) *callbackDispatcher {
	if cb == nil {
		return nil
	}
	d := &callbackDispatcher{
		cb:     cb,
		events: make(chan cbEvent, 1024),
		done:   make(chan struct{}),
	}
	go d.run()
	return d
}

func (d *callbackDispatcher) run() {
	defer close(d.done)
	for e := range d.events {
		d.inCallback.Store(true)
		d.safeInvoke(e)
		d.inCallback.Store(false)
	}
}

// safeInvoke calls the user callback with a panic guard so a misbehaving
// callback does not crash the process. The recovered panic is silently
// dropped: the stream itself is unaffected (durability is tracked by the
// watermark, not the callback), and there is no user-visible channel to
// surface the panic on. Callbacks that need to surface panics should
// recover themselves and route via their own error channel.
func (d *callbackDispatcher) safeInvoke(e cbEvent) {
	defer func() { _ = recover() }()
	if e.err == nil {
		d.cb.OnAck(e.offset)
	} else {
		d.cb.OnError(e.offset, e.err)
	}
}

// enqueueAck posts an OnAck event; non-blocking under normal load (buffered
// channel). If the channel is full (a callback is stalled), the ack is dropped
// with the guarantee that watermark advancement itself is unaffected — the
// callback is best-effort observability, not a durability signal.
func (d *callbackDispatcher) enqueueAck(offset int64) {
	if d == nil {
		return
	}
	select {
	case d.events <- cbEvent{offset: offset}:
	default:
		// Callback is stalled and would otherwise block the receiver. Dropping
		// the event is preferable to stalling ack processing; callers relying
		// on strict per-offset delivery must ensure their callback keeps up.
	}
}

// enqueueError posts an OnError event, non-blocking. If the channel is full
// the event is dropped: terminal drain must not stall the supervisor even
// under a slow user callback (otherwise Close would hang, waiting for the
// supervisor which is waiting for the callback which is waiting for Close).
// GetUnacked retains the record so the data is not lost — only the
// observability signal.
//
// The whole terminal drain is bounded by CallbackTeardownTimeout at the
// supervisor level via drainErrorsBounded, not per event, so the total wait
// is O(1) rather than O(items).
func (d *callbackDispatcher) enqueueError(offset int64, err error) {
	if d == nil {
		return
	}
	select {
	case d.events <- cbEvent{offset: offset, err: err}:
	default:
		// Drop: best-effort observability. GetUnacked retains the payload.
	}
}

// dispatchErrorsBounded posts one OnError event per drained item within a
// single global deadline. Once the deadline expires further events are
// dropped so a slow callback cannot delay the supervisor from closing
// cs.done. The deadline is applied once (not per event) so the total wait
// scales O(1) with items, not O(items × per-event budget).
func (d *callbackDispatcher) dispatchErrorsBounded(items []cbEvent, budget time.Duration) {
	if d == nil || len(items) == 0 {
		return
	}
	deadline := time.Now().Add(budget)
	for _, e := range items {
		select {
		case d.events <- e:
		default:
			// Queue is full. Wait up to the remaining budget for room; on
			// deadline, drop this and every subsequent event.
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return
			}
			t := time.NewTimer(remaining)
			select {
			case d.events <- e:
				t.Stop()
			case <-t.C:
				return
			}
		}
	}
}

// shutdown closes the event channel and waits up to timeout for the
// dispatcher to drain and exit. If a user callback is running when shutdown
// is called, the synchronous wait is skipped so a callback-invoked Close
// does not self-deadlock: the dispatcher goroutine can't drain further
// events while the callback is running, and the callback can't return
// until Close does. After a skipped or timed-out wait the dispatcher
// continues to drain in the background and exits on its own once events
// is empty.
//
// Note: the "callback running" check is process-state, not caller-identity.
// If an unrelated goroutine calls Close while another callback is in
// flight, that Close also skips the wait. This is a deliberate trade-off:
// Close is best-effort teardown, and the alternative (goroutine-ID
// detection) has no clean Go API. Callers that need a strict "all
// callbacks completed" barrier should synchronize externally.
func (d *callbackDispatcher) shutdown(timeout time.Duration) {
	if d == nil {
		return
	}
	d.closeOnce.Do(func() { close(d.events) })
	if d.inCallback.Load() {
		return
	}
	if timeout <= 0 {
		return
	}
	select {
	case <-d.done:
	case <-time.After(timeout):
	}
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

// errWatermarkClosed is returned by WaitForOffset when the stream was cleanly
// closed before the target offset became durable: the target can never be
// reached, so blocking further would be a permanent hang.
var errWatermarkClosed = errors.New("stream: closed before offset was acknowledged")

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

// closeForClean marks the watermark terminal with errWatermarkClosed unless it
// is already terminal with a real error. Called on clean Close so
// WaitForOffset cannot hang after the stream is torn down: the target is no
// longer reachable, and callers get an explicit error instead of blocking
// forever on a background ctx.
func (w *watermark) closeForClean() { w.fail(errWatermarkClosed) }

// waitFor blocks until the watermark reaches target or the watermark becomes
// terminal. Returns nil if the target is reached, or the terminal error.
func (w *watermark) waitFor(ctx context.Context, target int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	// Fail fast if ctx is already cancelled: without this an already-cancelled
	// ctx could still park at Wait (AfterFunc broadcast racing with the loop
	// condition check).
	if err := ctx.Err(); err != nil {
		return err
	}
	for w.offset < target && !w.terminal {
		// Take w.mu inside the AfterFunc so Broadcast can't be lost against a
		// racing Wait: without the lock, ctx could fire between the condition
		// check and Wait's park step, missing the wake-up.
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

	// offsetMu serializes offset assignment so nextOffset advances atomically
	// per accepted record. It is intentionally NOT held across the semaphore
	// wait: the caller reserves a buffer slot first (context-aware, may block),
	// then briefly takes offsetMu to assign the offset, then appends. That
	// keeps concurrent Ingest callers from serializing behind whoever is stuck
	// on backpressure, and lets each caller observe its own ctx cancellation
	// promptly.
	offsetMu sync.Mutex
	// nextOffset is the next logical offset to assign. Protected by offsetMu.
	nextOffset int64
	// lastEnqueued is the highest offset successfully enqueued (accessed as an
	// atomic int64 via sync/atomic operations); -1 until the first successful
	// Ingest. Flush targets this so a failed Ingest can't create a permanent
	// gap.
	lastEnqueued atomic.Int64
	// lastSent is the highest offset for which the sender has successfully
	// returned from stream.Send on the CURRENT connection (reset on each new
	// runOnce iteration since a fresh stream re-sends previously-observed
	// items). Ack validation checks against this value rather than
	// lastEnqueued: an enqueued-but-not-yet-sent item has never been shown
	// to the server, so an ack covering it is a protocol error even though
	// the offset is technically within the client's assigned range.
	// -1 while nothing has been sent on the current connection.
	lastSent atomic.Int64

	// done is closed when the supervisor exits (terminal state).
	done chan struct{}
	// termErr holds the first terminal error after done is closed.
	termErr error
	termMu  sync.Mutex

	closeOnce sync.Once
	// cancelSupervisor cancels the supervisor's context so Close unblocks it.
	cancelSupervisor context.CancelFunc

	// retainedFailed holds the encoded unacked items preserved on terminal
	// failure so GetUnacked can decode them lazily. Kept as encoded items
	// (rather than eagerly-decoded [][]byte) so terminal drain does not
	// duplicate the entire backlog in memory when the user may never call
	// GetUnacked. Protected by termMu. Payloads are also retained for
	// GetUnacked when an AckCallback is registered; the two signals converge
	// on the same records rather than being mutually exclusive.
	retainedFailed []item[Req]
}

// setRetainedFailed atomically stores the terminal-drain unacked items in
// their encoded form; decoding is deferred to GetUnacked so a terminal
// failure with a large in-flight set does not spike memory by holding both
// encoded and decoded copies at once.
func (cs *CoreStream[Req, Resp]) setRetainedFailed(items []item[Req]) {
	if len(items) == 0 {
		return
	}
	cs.termMu.Lock()
	cs.retainedFailed = items
	cs.termMu.Unlock()
}

// takeRetainedFailed returns the retained items and clears the slot so
// GetUnacked observes them exactly once.
func (cs *CoreStream[Req, Resp]) takeRetainedFailed() []item[Req] {
	cs.termMu.Lock()
	out := cs.retainedFailed
	cs.retainedFailed = nil
	cs.termMu.Unlock()
	return out
}

// NewCoreStream constructs a CoreStream and starts the supervisor goroutine.
// The stream is immediately ready for Ingest calls; the supervisor opens the
// first transport stream in the background. Prefer the per-protocol
// constructors (NewProtoJSONStream) over calling this directly.
//
// Construction is intentionally asynchronous at this internal layer:
// invalid credentials or bad StreamParams surface as a Flush / GetUnacked
// error once the supervisor's first Open attempts have exhausted
// RecoveryRetries. The public zerobus package is expected to wrap this
// with a readiness gate (initial-stream-open confirmation) before
// exposing the stream to users.
func NewCoreStream[Req, Resp any](
	params StreamParams,
	cfg Config,
	opener opener[Req, Resp],
	enc encoder[Req],
	ackMdl ackModel[Resp],
	callback AckCallback,
) *CoreStream[Req, Resp] {
	cfg = sanitizeConfig(cfg)
	// Snapshot the DescriptorProto so caller mutation cannot race with the
	// initial Open or with recovery-time Opens (which re-read StreamParams).
	// Other fields are value types or interface handles; only the byte
	// slice needs a deep copy.
	if len(params.DescriptorProto) > 0 {
		params.DescriptorProto = cloneBytes(params.DescriptorProto)
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
	cs.lastSent.Store(-1)
	go cs.supervise(ctx)
	return cs
}

// Ingest encodes record and enqueues it in the buffer, blocking if the buffer
// is at capacity (backpressure). Returns the logical offset assigned to this
// record; pass it to WaitForOffset to confirm durability.
//
// Records whose serialized wire size exceeds MaxPayloadBytes are rejected
// with ErrPayloadTooLarge before any offset is assigned so oversized input
// is a deterministic input error, not a transport failure that burns the
// recovery budget. Wire size (with proto framing) is what the server sees,
// so pre-encode input size alone is not sufficient — but a cheap raw-size
// precheck runs first so a genuinely huge input (e.g. accidental multi-GB
// buffer) is rejected before we copy it.
func (cs *CoreStream[Req, Resp]) Ingest(ctx context.Context, record []byte) (int64, error) {
	if err := cs.checkRawSize(len(record)); err != nil {
		return 0, err
	}
	return cs.enqueueEncoded(ctx, func(offset int64) (Req, error) {
		return cs.enc.encode(offset, record)
	})
}

// IngestBatch encodes records as a single atomic batch and enqueues it, blocking
// if the buffer is at capacity. The whole batch occupies one logical offset and
// the server acks it atomically. Returns that offset. Prefer this over Ingest in
// hot paths: it amortizes per-message overhead across the batch.
//
// The serialized wire size of the batch is checked against MaxPayloadBytes
// and rejected with ErrPayloadTooLarge before any offset is assigned. A cheap
// raw-size precheck runs first so oversized input is rejected before it is
// copied into the encoded message.
//
// A batch with zero records is a no-op (returns -1 without allocating an
// offset); a batch with any empty record is still rejected as invalid input.
func (cs *CoreStream[Req, Resp]) IngestBatch(ctx context.Context, records [][]byte) (int64, error) {
	if len(records) == 0 {
		return -1, nil
	}
	total := 0
	for _, r := range records {
		// Guard against int overflow — the sum can wrap when records is a
		// user-controlled slice of arbitrary sizes.
		if total > cs.cfg.MaxPayloadBytes || len(r) > cs.cfg.MaxPayloadBytes {
			return 0, fmt.Errorf("%w: raw batch size exceeds MaxPayloadBytes=%d",
				ErrPayloadTooLarge, cs.cfg.MaxPayloadBytes)
		}
		total += len(r)
	}
	if err := cs.checkRawSize(total); err != nil {
		return 0, err
	}
	return cs.enqueueEncoded(ctx, func(offset int64) (Req, error) {
		return cs.enc.encodeBatch(offset, records)
	})
}

// checkRawSize rejects clearly-oversized input before any encoding copy.
// The raw byte count is what a caller controls; the encoded wire size is
// then checked separately in enqueueEncoded to catch cases where framing
// pushes an otherwise-fine input over the cap.
func (cs *CoreStream[Req, Resp]) checkRawSize(n int) error {
	if n > cs.cfg.MaxPayloadBytes {
		return fmt.Errorf("%w: %d bytes exceeds MaxPayloadBytes=%d",
			ErrPayloadTooLarge, n, cs.cfg.MaxPayloadBytes)
	}
	return nil
}

// enqueueEncoded reserves a backpressure slot (context-aware, may block),
// pre-encodes the message OUTSIDE the offset-assignment lock, then briefly
// takes offsetMu to stamp the assigned offset and append. Doing the heavy
// encoding work (payload clones, proto.Size) without the lock means a large
// batch does not serialize concurrent small ingests behind it. A failed
// reserve/encode/append consumes no offset, so Flush never waits on an
// offset the server never sees.
func (cs *CoreStream[Req, Resp]) enqueueEncoded(ctx context.Context, encodeFn func(offset int64) (Req, error)) (int64, error) {
	if cs.isClosed() {
		if err := cs.terminalErr(); err != nil {
			return 0, err
		}
		return 0, errClosed
	}
	// Reserve a slot first (may block on backpressure or ctx). This is what
	// makes many-goroutine Ingest scale: the wait happens without any core
	// mutex held.
	if err := cs.buf.reserve(ctx); err != nil {
		return 0, err
	}
	// Encode with a placeholder offset (0) OUTSIDE the offset lock. The
	// stampOffset method will overwrite the offset field once we hold the
	// lock, so the encoded payload is offset-agnostic here.
	msg, err := encodeFn(0)
	if err != nil {
		cs.buf.release()
		return 0, err
	}
	// Wire-size validation against MaxPayloadBytes uses proto.Size, not the
	// raw input bytes: proto framing adds tags and varints on top of the
	// caller's payload, so a batch just under a byte-sum limit can still
	// exceed the server's message size limit and bounce back as a transport
	// failure. Reject deterministically here so recovery is not consumed by
	// an input-shape error.
	if size := cs.enc.wireSize(msg); size > cs.cfg.MaxPayloadBytes {
		cs.buf.release()
		return 0, fmt.Errorf("%w: %d bytes exceeds MaxPayloadBytes=%d",
			ErrPayloadTooLarge, size, cs.cfg.MaxPayloadBytes)
	}
	// Now take the offset lock, stamp offset, and append. All the expensive
	// per-call work is done; the critical section is just an offset++ plus
	// two mutating calls, so concurrent Ingests interleave freely.
	cs.offsetMu.Lock()
	offset := cs.nextOffset
	// Refuse offset exhaustion before it wraps into a negative value (which
	// is also the "nothing enqueued yet" sentinel and would corrupt Flush).
	if offset < 0 {
		cs.offsetMu.Unlock()
		cs.buf.release()
		return 0, fmt.Errorf("stream: logical offset space exhausted")
	}
	cs.enc.stampOffset(msg, offset)
	if err := cs.buf.append(offset, msg); err != nil {
		cs.offsetMu.Unlock()
		// append() released the slot on error.
		return 0, err
	}
	cs.nextOffset++
	// Store lastEnqueued while still holding offsetMu so it advances
	// monotonically alongside nextOffset. Storing after Unlock let two
	// concurrent Ingests interleave — the lower-offset caller's Store could
	// land last and mask a higher offset from Flush, which would then return
	// success while unacked records remained.
	cs.lastEnqueued.Store(offset)
	cs.offsetMu.Unlock()
	return offset, nil
}

// Flush blocks until every record ingested so far is acknowledged by the
// server. Returns nil once all are durable, or an error if the stream fails
// or ctx expires.
//
// When ctx has no deadline, Flush applies Config.FlushTimeout (defaulting
// to DefaultFlushTimeout when unset).
func (cs *CoreStream[Req, Resp]) Flush(ctx context.Context) error {
	target := cs.lastEnqueued.Load()
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
func (cs *CoreStream[Req, Resp]) WaitForOffset(ctx context.Context, offset int64) error {
	return cs.wm.waitFor(ctx, offset)
}

// GetUnacked returns records that were ingested but never acknowledged, one
// entry per record. A batched buffer item expands to all of its records (not
// just the first), so no unacked record is silently dropped.
//
// GetUnacked is a post-shutdown recovery accessor: the caller must Close the
// stream (or observe a terminal failure via IsClosed) BEFORE calling this.
// Calling it on a live stream returns ErrStreamStillActive and does not
// mutate any state; a destructive Close would race with the sender and
// receiver goroutines and reject records that were already durable.
//
// GetUnacked is compatible with a registered AckCallback: on terminal
// failure the supervisor also fires OnError for each unacked buffer item,
// but the encoded items are retained here so callers get both structured
// error events AND the raw bytes to persist or re-ingest. Records are
// decoded and cloned lazily here rather than at terminal drain so a large
// in-flight set does not double memory when the user never calls
// GetUnacked. The returned byte slices are independent copies — mutating
// them does not affect any internal state.
func (cs *CoreStream[Req, Resp]) GetUnacked() ([][]byte, error) {
	if !cs.isClosed() {
		return nil, ErrStreamStillActive
	}
	// If the supervisor already drained the buffer for OnError dispatch, the
	// items live on retainedFailed; otherwise drain the buffer now.
	items := cs.buf.drain()
	failed := cs.takeRetainedFailed()
	out := make([][]byte, 0, len(items)+len(failed))
	decodeAppend := func(it item[Req]) {
		for _, r := range cs.enc.decode(it.payload) {
			out = append(out, cloneBytes(r))
		}
	}
	for _, it := range items {
		decodeAppend(it)
	}
	for _, it := range failed {
		decodeAppend(it)
	}
	return out, nil
}

// Close terminates the stream and releases its resources. It is idempotent
// and blocks until teardown completes.
//
// Close does NOT wait for pending records to be acknowledged: any records
// still in flight when Close is called are abandoned and remain retrievable
// via GetUnacked. On a clean Close the AckCallback's OnError is NOT fired
// for those records — only terminal recovery failure fires OnError. Callers
// that need durability must Flush first, then Close (the standard
// loop-then-Flush pattern). On this clean shutdown the live stream is torn
// down gracefully (half-close, then drain any straggling acks to EOF,
// bounded by DrainTimeout) so the server observes an orderly END_STREAM
// rather than an abrupt reset; see gracefulTeardown.
//
// It is safe to call Close from inside an AckCallback: callbacks run on a
// dedicated dispatcher goroutine, not on the supervisor, so this method
// does not self-deadlock.
func (cs *CoreStream[Req, Resp]) Close() {
	cs.closeOnce.Do(func() {
		cs.cancelSupervisor()
		cs.buf.close()
		<-cs.done // wait for the supervisor to exit
		// Fail the watermark so any WaitForOffset callers waiting on offsets
		// that will never be acked (because we're shutting down) return an
		// explicit error instead of blocking forever. If the supervisor has
		// already marked the watermark terminal with a real error, that error
		// wins — fail() is one-shot.
		cs.wm.closeForClean()
		// Shut the callback dispatcher down last so any OnAck/OnError events
		// posted by the supervisor's drain (or by the receiver just before
		// exit) have a chance to run before we return.
		cs.dispatcher.shutdown(cs.cfg.CallbackTeardownTimeout)
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

// runOnce opens one transport stream and runs sender+receiver until one of
// them exits. Returns the cause of the exit so the supervisor can decide
// whether to recover, and healthy=true when the iteration counts as a
// distinct healthy episode. "Healthy" is either of:
//
//   - The receiver observed at least one server ack this iteration (real
//     durable progress), OR
//   - The connection stayed alive for at least RecoveryTimeout without
//     exiting (a stable idle stream: it may have carried no traffic, but
//     the connection itself was durable).
//
// Neither condition is met by an Open-then-immediate-EOF loop, so a
// permanent tight-loop failure still exhausts RecoveryRetries and does
// not hammer the server with no backoff. Meanwhile, an idle-but-stable
// stream that later disconnects starts a fresh recovery episode instead
// of inheriting old failedAttempts and prematurely terminating.
//
// The receiver drives pause handling: on a server CloseStreamSignal it sends
// on pauseCh and then keeps draining acks until either the flight set empties
// or the pause deadline expires. runOnce watches pauseCh and parks the sender
// (cancels senderCtx) as soon as the pause begins so no further records enter
// flight during the drain window.
func (cs *CoreStream[Req, Resp]) runOnce(ctx context.Context) (cause error, healthy bool) {
	// Bound the Open at RecoveryTimeout so a stalled dial cannot pin recovery
	// indefinitely — each attempt gets an explicit budget.
	openCtx, cancelOpen := context.WithTimeout(ctx, cs.cfg.RecoveryTimeout)
	stream, err := cs.opener.Open(openCtx, cs.params)
	openTimedOut := openCtx.Err() == context.DeadlineExceeded
	cancelOpen()
	if err != nil {
		if errors.Is(err, transport.ErrInvalidParams) {
			return wrapValidation(fmt.Errorf("stream: open: %w", err)), false
		}
		// Distinguish our internal per-attempt budget from a caller
		// cancellation of the outer supervisor ctx. The former should stay
		// retryable so RecoveryRetries actually governs stalled dials; the
		// latter (ctx cancelled by Close) is naturally non-retryable and
		// short-circuited by isRetryable's context checks. Same pattern the
		// auth layer applies to header-fetch budgets.
		if openTimedOut && ctx.Err() == nil {
			return &openBudgetExceeded{cause: fmt.Errorf("stream: open: %w", err)}, false
		}
		return fmt.Errorf("stream: open: %w", err), false
	}

	// Move all in-flight items back to the pending queue so the sender
	// re-sends them on this new connection.
	cs.buf.requeue()
	// Reset the sent watermark: requeue() has moved any previously-flighted
	// items back into the pending queue, so on this new connection nothing
	// has been shown to the server yet. Ack validation on this stream will
	// only accept offsets whose Send returned on THIS connection.
	cs.lastSent.Store(-1)

	// senderCtx is cancelled when we want the sender to stop for THIS stream
	// only, without cancelling the supervisor's outer ctx (which would signal
	// Close). It is also used to park the sender during a pause drain.
	senderCtx, cancelSender := context.WithCancel(ctx)
	defer cancelSender()

	// The sender and receiver report their exits on dedicated channels so
	// runOnce can distinguish which goroutine exited first. A sender exit
	// after we parked it (pause path) is NOT the cause; we want the
	// receiver's pauseSignal instead.
	senderExitCh := make(chan error, 1)
	receiverExitCh := make(chan error, 1)
	pauseCh := make(chan pauseSignal, 1)
	// flightSignal is a buffered 1-capacity channel the sender uses to tell
	// the receiver "at least one record has entered flight since you last
	// armed the timer." A dropped signal is fine (buffer full → receiver
	// already knows).
	flightSignal := make(chan struct{}, 1)
	// progressed is set by the receiver on the first ack it processes; it
	// tells the supervisor this iteration made durable progress and the
	// per-episode retry budget may be reset. Without it, a stream that
	// Opens successfully but immediately EOFs would reset the budget every
	// iteration and hammer the server with no backoff.
	var progressed atomic.Bool

	// runStart timestamps the moment we finished Open. A stream that stays
	// connected for >= RecoveryTimeout without exiting also counts as a
	// healthy episode even if it received no acks, so a stable idle stream
	// that later disconnects gets a fresh recovery budget.
	runStart := time.Now()

	go cs.sender(senderCtx, stream, senderExitCh, flightSignal)
	go cs.receiver(stream, receiverExitCh, pauseCh, flightSignal, &progressed)

	// Wait for the first meaningful exit. ctx.Done() is watched too so a
	// long server pause cannot pin teardown when Close is called mid-drain.
	var senderParked bool
	var senderExited bool
	var receiverExited bool
waitLoop:
	for {
		select {
		case sErr := <-senderExitCh:
			senderExited = true
			// A sender exit while parked (pause) is NOT the cause we
			// return; keep waiting on the receiver so it can finish
			// draining acks. A sender exit while unparked is either
			// ctx cancelled (Close) or a Send failure; both break out.
			if !senderParked {
				cause = sErr
				break waitLoop
			}
		case cause = <-receiverExitCh:
			receiverExited = true
			break waitLoop
		case <-pauseCh:
			// Pause drain has begun. Park the sender so no further records
			// enter flight; the receiver keeps reading acks. The receiver
			// will post its exit (pauseSignal cause) on receiverExitCh when
			// the drain window ends.
			if !senderParked {
				cancelSender()
				senderParked = true
			}
		case <-ctx.Done():
			// Close cancelled the supervisor ctx (possibly mid-pause).
			// Break out; the switch below picks the ctx-cancelled arm and
			// hard-aborts both goroutines, so a long server pause never
			// holds teardown.
			break waitLoop
		}
	}

	// Stop the sender (idempotent) so its teardown can proceed if it hasn't
	// exited yet.
	cancelSender()

	var ps pauseSignal
	switch {
	case ctx.Err() != nil:
		// Clean shutdown from Close: half-close the send side, let the
		// receiver drain to EOF for an orderly END_STREAM.
		cs.gracefulTeardown(stream, senderExited, receiverExited, senderExitCh, receiverExitCh)
	case errors.As(cause, &ps):
		// Pause drain ended. Hard-abort the stream to close the receiver's
		// blocking Recv (and any pending send).
		stream.Close()
		if !senderExited {
			<-senderExitCh
		}
		if !receiverExited {
			<-receiverExitCh
		}
	default:
		// Failure/recovery path: sender-side error or receiver-side error.
		// Hard-abort to unblock whichever goroutine hasn't exited yet.
		stream.Close()
		if !senderExited {
			<-senderExitCh
		}
		if !receiverExited {
			<-receiverExitCh
		}
	}
	// A run is "healthy" if it either processed a real ack, or the
	// connection stayed alive long enough (>= RecoveryTimeout) that we can
	// distinguish it from an immediate-EOF loop. Either signal resets the
	// per-episode retry budget in the supervisor.
	healthy = progressed.Load() || time.Since(runStart) >= cs.cfg.RecoveryTimeout
	return cause, healthy
}

// gracefulTeardown closes a healthy stream in an orderly fashion after the
// sender has stopped: half-close the send side so the server flushes any
// remaining acks and ends the stream, let the still-running receiver drain to
// io.EOF (advancing the watermark for late acks), then release resources.
// DrainTimeout bounds the wait so a wedged server can't hang Close; on expiry
// we hard-abort. This mirrors transport.GracefulClose, done cooperatively
// because the receiver goroutine — not this one — owns Recv.
func (cs *CoreStream[Req, Resp]) gracefulTeardown(
	stream wireStream[Req, Resp],
	senderExited, receiverExited bool,
	senderExitCh, receiverExitCh <-chan error,
) {
	// Reap the sender first (it was cancelled). Bound the wait: a wedged
	// Send won't observe ctx cancellation, so we may need to hard-abort the
	// stream to unblock it.
	if !senderExited {
		senderReapTimer := time.NewTimer(cs.cfg.DrainTimeout)
		select {
		case <-senderExitCh:
			senderReapTimer.Stop()
		case <-senderReapTimer.C:
			// Sender is wedged inside Send. Hard-abort the stream so Send
			// returns; the receiver will exit too. Skip the graceful
			// half-close path since Send is broken by definition.
			stream.Close()
			<-senderExitCh
			if !receiverExited {
				<-receiverExitCh
			}
			return
		}
	}
	if receiverExited {
		// Receiver already exited (e.g. EOF observed during waitLoop);
		// nothing more to drain.
		stream.Close()
		return
	}
	if err := stream.CloseSend(); err != nil {
		// Send side already broken; fall back to a hard abort.
		stream.Close()
		<-receiverExitCh
		return
	}
	timer := time.NewTimer(cs.cfg.DrainTimeout)
	defer timer.Stop()
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

// sender pulls items from the buffer and writes them to stream. Exits when
// senderCtx is cancelled (per-stream teardown), the buffer is closed, or Send
// fails. senderCtx is derived from a per-runOnce cancel so the supervisor can
// stop this sender without cancelling the outer supervisor context. After each
// successful Send it pings flightSignal so the receiver can arm the
// lack-of-ack timer with a fresh full budget as soon as work enters flight —
// preventing the "first send after idle can fail almost immediately" race
// where the record inherits a nearly-expired timer.
func (cs *CoreStream[Req, Resp]) sender(senderCtx context.Context, stream wireStream[Req, Resp], errCh chan<- error, flightSignal chan<- struct{}) {
	for {
		it, err := cs.buf.next(senderCtx)
		if err != nil {
			errCh <- nil // ctx cancelled or buffer closed — clean exit
			return
		}
		// Signal flight BEFORE calling Send: buf.next has already moved the
		// item into the in-flight set, so a stalled Send must not prevent
		// the receiver from arming the lack-of-ack timer. Signalling after
		// Send would let a wedged send leave the timer disarmed until the
		// Send returned (which, if the transport blocks, is never), so a
		// slow server would not be caught. A dropped signal (channel full)
		// is fine: the receiver only needs one wake-up per idle→work
		// transition.
		select {
		case flightSignal <- struct{}{}:
		default:
		}
		if err := stream.Send(it.payload); err != nil {
			errCh <- fmt.Errorf("stream: send offset %d: %w", it.offset, err)
			return
		}
		// Record the send-side watermark AFTER Send returns successfully so
		// ack validation can distinguish "on the wire" from "queued or
		// mid-Send." A server ack covering an offset past lastSent is a
		// protocol error even if lastEnqueued has advanced past it, because
		// the server has not yet been shown that offset. Offsets are
		// monotonic per stream so Store (not CAS) is enough — only the
		// single sender writes this.
		cs.lastSent.Store(it.offset)
	}
}

// receiver reads ack responses from stream and advances the watermark. It runs
// until io.EOF (server closed cleanly, including after a graceful half-close),
// a receive error, the lack-of-ack timeout (only while records are in flight),
// or the stream being Closed out from under it (which surfaces as a Recv error).
// Teardown is coordinated by runOnce, so the receiver does not watch the
// supervisor context itself.
//
// The pause path is cooperative with the sender via pauseCh: on pauseResponse
// the receiver signals pauseCh (so runOnce parks the sender) and keeps
// draining acks until either the buffer's inFlight goes to zero or the pause
// deadline expires.
//
// The lack-of-ack timer is armed only when records are actually in flight.
// The sender pings flightSignal BEFORE each Send (buffer.next has already
// moved the item into flight by that point) so the receiver arms the timer
// with a fresh full LackOfAckTimeout as soon as a record enters flight —
// even if Send subsequently blocks. Pinging after Send would let a wedged
// send leave the timer disarmed until the send returned (which, if the
// transport blocks, is never). The channel is buffered=1 so the sender
// never blocks on it.
//
// Each Recv call runs on its own goroutine so we can race it against the
// lack-of-ack timer even when the underlying transport Recv is blocking (e.g. a
// fake in tests, or a stalled server).
func (cs *CoreStream[Req, Resp]) receiver(stream wireStream[Req, Resp], errCh chan<- error, pauseCh chan<- pauseSignal, flightSignal <-chan struct{}, progressed *atomic.Bool) {
	type recvResult struct {
		resp Resp
		err  error
	}

	// A single outstanding Recv goroutine at a time feeds recvCh.
	recvCh := make(chan recvResult, 1)
	spawnRecv := func() {
		go func() {
			resp, err := stream.Recv()
			recvCh <- recvResult{resp, err}
		}()
	}
	spawnRecv()

	// lackTimer is started stopped; it is armed only after we know work has
	// entered flight (either from the sender's signal, or after a requeue on
	// reconnect).
	lackTimer := time.NewTimer(cs.cfg.LackOfAckTimeout)
	if !lackTimer.Stop() {
		<-lackTimer.C
	}
	lackTimerArmed := false

	armLack := func() {
		if lackTimerArmed || cs.buf.inFlight() == 0 {
			return
		}
		lackTimer.Reset(cs.cfg.LackOfAckTimeout)
		lackTimerArmed = true
	}
	disarmLack := func() {
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
	resetLack := func() {
		disarmLack()
		armLack()
	}

	// Arm on entry: after a reconnect the supervisor requeues in-flight items
	// so work may already be in the queue awaiting the sender.
	armLack()

	// pauseState is set when we've observed a server CloseStreamSignal; the
	// receiver then drains acks against pauseDeadline while the sender is
	// parked by runOnce.
	var pauseState *pauseSignal
	var pauseTimer *time.Timer
	stopPauseTimer := func() {
		if pauseTimer != nil {
			pauseTimer.Stop()
			pauseTimer = nil
		}
	}

	for {
		var r recvResult
		var pauseC <-chan time.Time
		var lackC <-chan time.Time
		if pauseTimer != nil {
			pauseC = pauseTimer.C
		}
		if lackTimerArmed {
			lackC = lackTimer.C
		}
		// One composite select drives all four signals. Cases we don't want
		// active (nil channels) are ignored, so we always block only on the
		// signals that make sense in the current state.
		select {
		case <-flightSignal:
			// Sender pushed something onto the wire; ensure the timer is
			// armed with a fresh full LackOfAckTimeout budget.
			armLack()
			continue
		case <-lackC:
			lackTimerArmed = false
			if pauseState != nil {
				// Ignore lack-of-ack during a server-requested pause: the
				// server explicitly told us it's about to close the stream
				// and asked us to wait its requested window. Truncating the
				// pause here would defeat the throttling and could cut
				// short a legitimate drain. The pause window is instead
				// bounded by pauseTimer; a genuinely silent server ends
				// the wait through pauseC. Do not tear the stream down.
				continue
			}
			// Recheck under the buffer's lock: an ack may have drained the
			// flight set between the timer firing and us noticing.
			if cs.buf.inFlight() == 0 {
				continue
			}
			stream.Close()
			<-recvCh
			errCh <- fmt.Errorf("stream: no ack from server for %s", cs.cfg.LackOfAckTimeout)
			return
		case <-pauseC:
			pauseTimer = nil
			errCh <- *pauseState
			return
		case r = <-recvCh:
		}

		if r.err == io.EOF {
			stopPauseTimer()
			errCh <- nil
			return
		}
		if r.err != nil {
			stopPauseTimer()
			errCh <- fmt.Errorf("stream: recv: %w", r.err)
			return
		}

		kind, offset, pause := cs.ackMdl.classify(r.resp)
		switch kind {
		case pauseResponse:
			// A server pause is a flow-control request: signal runOnce to
			// park the sender and enter pause-drain mode. Repeat pause
			// signals during an active pause window are no-ops.
			if pauseState == nil {
				ps := pause
				pauseState = &ps
				wait := effectivePauseWait(cs.cfg, pause.duration)
				// Signal runOnce so it stops the sender for THIS stream,
				// regardless of the effective wait: an immediate reconnect
				// still requires the sender to be parked so no further
				// records enter flight before we tear down.
				select {
				case pauseCh <- pause:
				default:
				}
				if wait <= 0 {
					// Configuration or server both asked for immediate
					// reconnect. End pause drain now — no records will enter
					// flight thanks to the pauseCh signal above.
					stopPauseTimer()
					errCh <- pause
					return
				}
				// Always honour the pause window: server-driven throttling
				// wants us to hold off reconnecting even when nothing is
				// currently in flight, or reconnect churn amplifies load.
				// The window is capped by StreamPausedMaxWait; the receiver
				// still keeps draining acks in case work enters flight
				// before it (buffered ingest is possible during pause).
				pauseTimer = time.NewTimer(wait)
			}
			spawnRecv()
			continue
		case malformedResponse:
			stopPauseTimer()
			errCh <- fmt.Errorf("stream: server ack missing durability offset")
			return
		case unknownResponse:
			// A response the ack model does not recognise is a protocol
			// error, not noise: fail the stream so the supervisor can
			// decide whether to recover.
			stopPauseTimer()
			errCh <- fmt.Errorf("stream: unexpected server response type")
			return
		}

		spawnRecv()

		// kind == ackResponse. Validate the offset against work actually
		// SENT on this connection, not just enqueued: buf.next moves an
		// item into flight before Send is called, so an enqueued item may
		// still be queued or blocked in mid-Send. An ack claiming durability
		// for an offset the server has not yet been shown is a protocol
		// error even if the offset is within the client's assigned range;
		// discarding it from flight would falsely satisfy Flush /
		// WaitForOffset for a never-sent record.
		//
		// lastSent is the highest offset for which Send returned successfully
		// on the current connection (reset per runOnce iteration by
		// cs.buf.requeue's caller). A load of -1 means "nothing sent yet on
		// this stream", so any ack is invalid.
		highestSent := cs.lastSent.Load()
		if offset > highestSent {
			stopPauseTimer()
			errCh <- fmt.Errorf("stream: server ack offset %d exceeds highest sent %d", offset, highestSent)
			return
		}
		// Advance the watermark only to the offset of items we actually
		// discarded from flight; ignore regressing acks (offset lower
		// than what was already discarded) so a duplicate/replay ack
		// does not fire callbacks or roll the watermark backwards.
		discarded := cs.buf.discardThrough(offset)
		if len(discarded) > 0 {
			cs.wm.advance(discarded[len(discarded)-1])
			// Mark the run as having made real progress so the supervisor
			// resets its per-episode retry budget only after actual acks.
			progressed.Store(true)
			// Per-offset callbacks: one OnAck per newly-acked logical offset.
			for _, o := range discarded {
				cs.dispatcher.enqueueAck(o)
			}
		}
		// Timer maintenance: reset the freshness budget while work
		// remains in flight; disarm when the flight set empties.
		if cs.buf.inFlight() == 0 {
			disarmLack()
		} else {
			resetLack()
		}
		// Pause windows are honoured even after the flight set empties,
		// so server-driven throttling still holds us off reconnecting for
		// the requested duration and does not churn under load. The
		// pauseTimer case above will end the wait when the window expires.
	}
}

// effectivePauseWait returns the effective wait duration for a server-requested
// pause given the client configuration. Return of 0 or negative means
// "reconnect immediately"; a positive value is the wait window.
//
//   - PauseWaitImmediate: always 0 (immediate reconnect).
//   - PauseWaitServer + StreamPausedMaxWait > 0: min(cap, server duration).
//   - PauseWaitServer + StreamPausedMaxWait <= 0: server duration (no cap).
func effectivePauseWait(cfg Config, serverDuration time.Duration) time.Duration {
	if cfg.StreamPausedMaxWaitMode == PauseWaitImmediate {
		return 0
	}
	wait := serverDuration
	if cap := cfg.StreamPausedMaxWait; cap > 0 && (wait <= 0 || cap < wait) {
		wait = cap
	}
	return wait
}
