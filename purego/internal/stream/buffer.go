// Package stream is the generic ingestion core: offset assignment, send/recv
// goroutines, ack watermark, Flush/WaitForOffset, and the recovery supervisor.
// Protocol-specific behaviour (encoding, ack parsing, wire transport) is
// injected through the encoder, ackModel, and wireStream interfaces.
package stream

import (
	"context"
	"sync"
)

// item is one unit of work in the buffer: an already-encoded wire message
// paired with the logical offset that identifies it for acknowledgment.
type item struct {
	offset  int64
	payload encodedMsg
}

// buffer is the bounded, offset-assigning queue between the caller's Ingest
// calls and the sender goroutine. It is the Go equivalent of Rust's
// LandingZone: callers enqueue already-encoded messages; the sender observes
// them (moves to in-flight); acks cause the sender to discard them.
//
// Concurrency model:
//   - Many goroutines may call enqueue concurrently.
//   - Exactly one sender goroutine calls next/requeue/discard.
//   - Exactly one goroutine (the supervisor) calls drain.
//
// The semaphore enforces the MaxInflight cap: enqueue blocks once the cap is
// reached and unblocks as acks arrive and discard releases permits.
type buffer struct {
	mu     sync.Mutex
	cond   *sync.Cond
	queue  []item // pending: enqueued but not yet observed by the sender
	flight []item // in-flight: observed by the sender, waiting for ack
	closed bool
	sem    chan struct{} // capacity = maxInflight; held while item is in queue or flight
}

func newBuffer(maxInflight int) *buffer {
	b := &buffer{
		queue:  make([]item, 0, maxInflight),
		flight: make([]item, 0, maxInflight),
		sem:    make(chan struct{}, maxInflight),
	}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// enqueue adds an already-encoded message to the pending queue, blocking until
// a slot is available (backpressure) or ctx is cancelled. The offset must be
// monotonically increasing; the caller (coreStream) is responsible for that.
// Returns ctx.Err() if ctx fires before a slot opens.
func (b *buffer) enqueue(ctx context.Context, offset int64, msg encodedMsg) error {
	// Acquire a slot before touching the queue so callers block here rather than
	// inside the mutex. ctx cancellation wakes up the select immediately.
	select {
	case b.sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		<-b.sem // release the slot we just took
		return errClosed
	}
	b.queue = append(b.queue, item{offset: offset, payload: msg})
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
func (b *buffer) next(ctx context.Context) (item, error) {
	b.mu.Lock()
	for len(b.queue) == 0 && !b.closed {
		// Wake up on ctx cancellation too.
		stop := context.AfterFunc(ctx, func() { b.cond.Broadcast() })
		b.cond.Wait()
		stop()
		if ctx.Err() != nil {
			b.mu.Unlock()
			return item{}, ctx.Err()
		}
	}
	if len(b.queue) == 0 {
		b.mu.Unlock()
		return item{}, errClosed
	}
	it := b.queue[0]
	b.queue = b.queue[1:]
	b.flight = append(b.flight, it)
	b.mu.Unlock()
	return it, nil
}

// discard removes the oldest in-flight item (acknowledged by the server) and
// releases its semaphore slot so a new enqueue can proceed.
func (b *buffer) discard() {
	b.mu.Lock()
	if len(b.flight) > 0 {
		b.flight = b.flight[1:]
	}
	b.mu.Unlock()
	<-b.sem
	b.cond.Signal()
}

// requeue moves all in-flight items back to the front of the pending queue so
// they are re-sent after a reconnect. Called by the supervisor on stream failure.
func (b *buffer) requeue() {
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
func (b *buffer) drain() []item {
	b.mu.Lock()
	defer b.mu.Unlock()
	all := make([]item, 0, len(b.flight)+len(b.queue))
	all = append(all, b.flight...)
	all = append(all, b.queue...)
	b.queue = nil
	b.flight = nil
	b.closed = true
	b.cond.Broadcast()
	return all
}

// close marks the buffer closed and wakes any blocked next calls. Pending
// items are not discarded — drain must be called to retrieve them.
func (b *buffer) close() {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	b.cond.Broadcast()
}

// len returns the total number of items in the buffer (pending + in-flight).
func (b *buffer) len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.queue) + len(b.flight)
}
