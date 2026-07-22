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
//   - Exactly one sender goroutine calls next.
//   - Exactly one receiver goroutine calls discardThrough as acks arrive.
//   - The supervisor calls requeue and drain, but only while the sender is
//     stopped — so next never runs concurrently with requeue or drain.
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

// enqueue adds an already-encoded message to the pending queue, blocking until
// a slot is available (backpressure) or ctx is cancelled. The offset must be
// monotonically increasing; the caller (coreStream) is responsible for that.
// Returns ctx.Err() if ctx fires before a slot opens.
func (b *buffer[Req]) enqueue(ctx context.Context, offset int64, msg Req) error {
	// Honor an already-cancelled ctx first: select picks randomly among ready
	// cases, so a free slot could otherwise mask cancellation.
	if err := ctx.Err(); err != nil {
		return err
	}
	// Acquire a slot before touching the queue so callers block here rather than
	// inside the mutex. ctx cancellation wakes up the select immediately.
	select {
	case b.sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-b.doneCh:
		return errClosed
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		<-b.sem // release the slot we just took
		return errClosed
	}
	b.queue = append(b.queue, item[Req]{offset: offset, payload: msg})
	b.mu.Unlock()
	b.cond.Signal()
	return nil
}

// next blocks until a pending item is available and moves it to the in-flight
// list, returning the item. The sender must later call discard (on ack) or
// requeue (on reconnect) for every item returned by next.
//
// Returns errClosed when the buffer has been closed and drained.
// Returns ctx.Err() if ctx is cancelled while waiting.
func (b *buffer[Req]) next(ctx context.Context) (item[Req], error) {
	b.mu.Lock()
	for len(b.queue) == 0 && !b.closed {
		// Wake up on ctx cancellation too.
		stop := context.AfterFunc(ctx, func() { b.cond.Broadcast() })
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
	it := b.queue[0]
	b.queue = b.queue[1:]
	b.flight = append(b.flight, it)
	b.mu.Unlock()
	return it, nil
}

// discardThrough removes every in-flight item whose offset is <= offset (all
// now acknowledged by the server) and releases one semaphore slot per removed
// item so blocked enqueue callers can proceed. It is the sender/receiver's only
// hook for ack-driven eviction, keeping the buffer's internals (flight, sem,
// cond) private. Returns the number of items discarded.
func (b *buffer[Req]) discardThrough(offset int64) int {
	b.mu.Lock()
	n := 0
	for len(b.flight) > 0 && b.flight[0].offset <= offset {
		b.flight = b.flight[1:]
		n++
	}
	b.mu.Unlock()
	for range n {
		<-b.sem
	}
	if n > 0 {
		b.cond.Broadcast()
	}
	return n
}

// requeue moves all in-flight items back to the front of the pending queue so
// they are re-sent after a reconnect. Called by the supervisor on stream failure.
func (b *buffer[Req]) requeue() {
	b.mu.Lock()
	defer b.mu.Unlock()
	// Prepend in-flight items (in order) before any still-pending ones.
	b.queue = append(b.flight, b.queue...)
	b.flight = b.flight[:0]
	b.cond.Broadcast()
}

// drain returns all items currently in the buffer (pending + in-flight) and
// closes it. Subsequent enqueue calls return errClosed. Called once the
// supervisor has given up and the caller wants unacked records.
//
// The sender must be stopped before drain is called: an item returned by next
// but not yet discarded is still in flight, so draining it concurrently would
// report a record as unacked while the sender is putting it on the wire.
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

// len returns the total number of items in the buffer (pending + in-flight).
func (b *buffer[Req]) len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.queue) + len(b.flight)
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
