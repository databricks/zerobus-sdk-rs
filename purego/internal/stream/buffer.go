// Package stream is the generic ingestion core: offset assignment, send/recv
// goroutines, ack watermark, Flush/WaitForOffset, and the recovery supervisor.
// Protocol-specific behaviour (encoding, ack parsing, wire transport) is
// injected through the encoder, ackModel, and wireStream interfaces, so the
// core is written once and instantiated per wire protocol (proto/JSON today,
// Arrow Flight later) without editing this package's core logic.
package stream

import (
	"context"
	"sync"
)

// item is one unit of work in the buffer: an already-encoded wire message
// paired with the logical offset that identifies it for acknowledgment. Req is
// the wire request type the core is instantiated with (encodedMsg for
// proto/JSON; a Flight frame for Arrow).
type item[Req any] struct {
	offset  int64
	payload Req
}

// buffer is the bounded, offset-assigning queue between the caller's Ingest
// calls and the sender goroutine. Callers enqueue already-encoded messages;
// the sender observes them (moves to in-flight); acks cause them to be
// discarded. It is generic over the wire request type so the core holds no
// concrete proto type.
//
// Concurrency model:
//   - Many goroutines may call enqueue concurrently.
//   - Exactly one sender goroutine calls next/requeue.
//   - Exactly one receiver goroutine calls discardThrough as acks arrive.
//   - Exactly one goroutine (the supervisor) calls drain.
//
// All state (queue, flight, sem, cond) is private; the sender and receiver
// interact only through these methods, never by touching the fields directly.
//
// The semaphore enforces the MaxInflight cap: enqueue blocks once the cap is
// reached and unblocks as acks arrive and discardThrough releases permits.
type buffer[Req any] struct {
	mu       sync.Mutex
	cond     *sync.Cond
	queue    []item[Req] // pending: enqueued but not yet observed by the sender
	flight   []item[Req] // in-flight: observed by the sender, waiting for ack
	closed   bool
	sem      chan struct{} // capacity = maxInflight; held while item is in queue or flight
	doneOnce sync.Once
	doneCh   chan struct{} // closed when the buffer is closed/drained; unblocks sem waiters
}

func newBuffer[Req any](maxInflight int) *buffer[Req] {
	// queue/flight grow on demand; don't preallocate to maxInflight, which with
	// the default 1M cap would reserve tens of MB per stream before a single
	// record is ingested. Only the semaphore is sized to the cap, since it is the
	// backpressure gate and its capacity defines the bound.
	b := &buffer[Req]{
		sem:    make(chan struct{}, maxInflight),
		doneCh: make(chan struct{}),
	}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *buffer[Req]) closeDone() {
	b.doneOnce.Do(func() { close(b.doneCh) })
}

// reserve blocks until a backpressure slot is available or ctx is cancelled.
// It is split from the append step so callers can hold the offset-assignment
// critical section for as short a time as possible: the semaphore wait happens
// outside the coreStream ingest mutex, so one blocked caller does not serialize
// every later caller behind it.
func (b *buffer[Req]) reserve(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case b.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-b.doneCh:
		return errClosed
	}
}

// release returns a previously-reserved slot to the semaphore. Used when a
// reserved slot cannot be consumed (e.g. the buffer was closed between reserve
// and append).
func (b *buffer[Req]) release() { <-b.sem }

// append records an already-reserved item in the pending queue. The caller must
// have called reserve() successfully first; a failed append (buffer closed
// concurrently) releases the reservation so the semaphore stays consistent.
func (b *buffer[Req]) append(offset int64, msg Req) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		b.release()
		return errClosed
	}
	b.queue = append(b.queue, item[Req]{offset: offset, payload: msg})
	b.mu.Unlock()
	b.cond.Signal()
	return nil
}

// enqueue adds an already-encoded message to the pending queue, blocking until
// a slot is available (backpressure) or ctx is cancelled. The offset must be
// monotonically increasing; the caller (coreStream) is responsible for that.
// Returns ctx.Err() if ctx fires before a slot opens.
func (b *buffer[Req]) enqueue(ctx context.Context, offset int64, msg Req) error {
	if err := b.reserve(ctx); err != nil {
		return err
	}
	return b.append(offset, msg)
}

// next blocks until a pending item is available and moves it to the in-flight
// list, returning the item. The sender must later call discard (on ack) or
// requeue (on reconnect) for every item returned by next.
//
// Returns errClosed when the buffer has been closed. Cancellation is honoured
// both while waiting for an item AND immediately before dequeuing one, so a
// Close that cancels the sender's context never lets one more queued record
// slip onto the wire.
//
// Returns ctx.Err() if ctx is cancelled.
func (b *buffer[Req]) next(ctx context.Context) (item[Req], error) {
	// Fail fast if ctx is already cancelled: the sender has been told to stop
	// (per-stream teardown or Close), so we must not consume a queued item.
	if err := ctx.Err(); err != nil {
		return item[Req]{}, err
	}
	b.mu.Lock()
	for len(b.queue) == 0 && !b.closed {
		// Take b.mu inside the AfterFunc so the broadcast is properly ordered
		// against Wait: without the lock, ctx could fire between the loop's
		// condition check and Wait's park step, missing the wake-up and
		// leaving the goroutine asleep on an already-cancelled ctx.
		stop := context.AfterFunc(ctx, func() {
			b.mu.Lock()
			b.cond.Broadcast()
			b.mu.Unlock()
		})
		b.cond.Wait()
		stop()
		if ctx.Err() != nil {
			b.mu.Unlock()
			return item[Req]{}, ctx.Err()
		}
	}
	if len(b.queue) == 0 {
		b.mu.Unlock()
		return item[Req]{}, errClosed
	}
	// Re-check ctx after acquiring an item: if teardown started while we were
	// waking, drop the item back and return so Close is not delayed by a Send
	// that will just be aborted anyway.
	if err := ctx.Err(); err != nil {
		b.mu.Unlock()
		return item[Req]{}, err
	}
	it := b.queue[0]
	// Zero the departed slot so the payload is collectible once the item moves
	// to flight (and later gets discarded), rather than being pinned by the
	// underlying array.
	var zero item[Req]
	b.queue[0] = zero
	b.queue = b.queue[1:]
	b.flight = append(b.flight, it)
	b.mu.Unlock()
	return it, nil
}

// discardThrough removes every in-flight item whose offset is <= offset (all
// now acknowledged by the server) and releases one semaphore slot per removed
// item so blocked enqueue callers can proceed. It is the sender/receiver's only
// hook for ack-driven eviction, keeping the buffer's internals (flight, sem,
// cond) private. Returns the offsets of the newly-discarded items in order so
// the receiver can fire a per-offset OnAck for each; the returned slice is nil
// if the ack covered no new items.
func (b *buffer[Req]) discardThrough(offset int64) []int64 {
	b.mu.Lock()
	var discarded []int64
	var zero item[Req]
	for len(b.flight) > 0 && b.flight[0].offset <= offset {
		discarded = append(discarded, b.flight[0].offset)
		// Zero the departed slot so the payload's backing memory is
		// collectible even while the underlying array is retained.
		b.flight[0] = zero
		b.flight = b.flight[1:]
	}
	b.mu.Unlock()
	for range discarded {
		<-b.sem
	}
	if len(discarded) > 0 {
		b.cond.Broadcast()
	}
	return discarded
}

// requeue moves all in-flight items back to the front of the pending queue so
// they are re-sent after a reconnect. Called by the supervisor on stream failure.
func (b *buffer[Req]) requeue() {
	b.mu.Lock()
	defer b.mu.Unlock()
	// Prepend in-flight items (in order) before any still-pending ones. Clear
	// the old flight slots so the backing array does not pin now-transferred
	// payloads (they're referenced by queue, not flight).
	var zero item[Req]
	newQueue := make([]item[Req], 0, len(b.flight)+len(b.queue))
	newQueue = append(newQueue, b.flight...)
	newQueue = append(newQueue, b.queue...)
	for i := range b.flight {
		b.flight[i] = zero
	}
	b.flight = b.flight[:0]
	// Zero the tail of the old queue slice before replacing it, so callers
	// blocked on the semaphore can't observe stale payloads via the old array.
	for i := range b.queue {
		b.queue[i] = zero
	}
	b.queue = newQueue
	b.cond.Broadcast()
}

// drain returns all items currently in the buffer (pending + in-flight) and
// closes it. Subsequent enqueue calls return errClosed. Called once the
// supervisor has given up and the caller wants unacked records.
func (b *buffer[Req]) drain() []item[Req] {
	b.mu.Lock()
	all := make([]item[Req], 0, len(b.flight)+len(b.queue))
	all = append(all, b.flight...)
	all = append(all, b.queue...)
	b.queue = nil
	b.flight = nil
	b.closed = true
	b.mu.Unlock()
	b.cond.Broadcast()
	b.closeDone()
	return all
}

// close marks the buffer closed and wakes any blocked next or enqueue calls.
// Pending items are not discarded — drain must be called to retrieve them.
func (b *buffer[Req]) close() {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	b.cond.Broadcast()
	b.closeDone()
}

// inFlight returns the number of items observed by the sender but not yet
// acknowledged. The receiver uses it to gate the lack-of-ack timeout: silence
// is only a failure while records are actually awaiting an ack; an idle stream
// (nothing in flight) legitimately receives no acks and must not be torn down.
func (b *buffer[Req]) inFlight() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.flight)
}
