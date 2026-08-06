// Package stream is the generic ingestion core: offset assignment, send/recv
// goroutines, ack watermark, Flush/WaitForOffset, and the recovery supervisor.
// Protocol-specific behaviour (encoding, ack parsing, payload slicing, and wire
// transport) is injected through narrow hooks, so atomic offset protocols and
// record-count protocols share one implementation.
package stream

import (
	"container/list"
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// defaultMaxInflight is the fallback backpressure cap used when a non-positive
// value reaches newBuffer.
const defaultMaxInflight = 1_000_000

// item is one logical unit of work in the buffer: an already-encoded payload
// paired with its SDK offset and protocol durability-unit count. Proto/JSON
// payloads are atomic and always carry one unit; a record-count protocol can
// carry multiple units and acknowledge a prefix.
type item[Req any] struct {
	offset     int64
	payload    Req
	units      uint64
	ackedUnits uint64
	weight     int64
	pendingAt  time.Time
}

type discardResult struct {
	first int64
	last  int64
	count int
}

type capacityWaiter struct {
	weight  int64
	ready   chan struct{}
	granted bool
	err     error
	elem    *list.Element
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
// All state (queue, flight, capacity, cond) is private; the sender and receiver
// interact only through these methods, never by touching the fields directly.
//
// Count and payload-byte limits apply to queued plus in-flight items.
type buffer[Req any] struct {
	mu               sync.Mutex
	cond             *sync.Cond
	queue            []item[Req] // pending: enqueued but not yet observed by the sender
	flight           []item[Req] // in-flight: observed by the sender, waiting for ack
	closed           bool
	maxInflight      int
	maxBufferedBytes int64
	usedItems        int
	usedBytes        int64
	accountingReset  bool
	waiters          *list.List
	flightRevision   uint64
}

func newBuffer[Req any](maxInflight int, byteLimit int64) *buffer[Req] {
	// Normalize a non-positive count cap, which would otherwise prevent every
	// reservation from being granted.
	if maxInflight <= 0 {
		maxInflight = defaultMaxInflight
	}
	maxBufferedBytes := int64(math.MaxInt64)
	if byteLimit > 0 {
		maxBufferedBytes = byteLimit
	}
	b := &buffer[Req]{
		maxInflight:      maxInflight,
		maxBufferedBytes: maxBufferedBytes,
		waiters:          list.New(),
	}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// reserve acquires one item slot and payload-byte weight.
func (b *buffer[Req]) reserve(ctx context.Context, weight int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if weight < 0 || weight > b.maxBufferedBytes {
		return fmt.Errorf("%w: buffered payload weight %d exceeds limit %d",
			ErrPayloadTooLarge, weight, b.maxBufferedBytes)
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return errClosed
	}
	if b.waiters.Len() == 0 && b.canReserveLocked(weight) {
		b.usedItems++
		b.usedBytes += weight
		b.mu.Unlock()
		return nil
	}
	waiter := &capacityWaiter{weight: weight, ready: make(chan struct{})}
	waiter.elem = b.waiters.PushBack(waiter)
	b.mu.Unlock()

	select {
	case <-waiter.ready:
		return waiter.err
	case <-ctx.Done():
		b.mu.Lock()
		if waiter.granted {
			// drain may have closed the buffer and reset accounting after this
			// reservation was granted but before cancellation won the select.
			if !b.accountingReset {
				b.usedItems--
				b.usedBytes -= weight
			}
		} else if waiter.err == nil {
			if waiter.elem != nil {
				b.waiters.Remove(waiter.elem)
				waiter.elem = nil
			}
		}
		b.grantWaitersLocked()
		b.mu.Unlock()
		return ctx.Err()
	}
}

func (b *buffer[Req]) canReserveLocked(weight int64) bool {
	return b.usedItems < b.maxInflight &&
		weight <= b.maxBufferedBytes-b.usedBytes
}

func (b *buffer[Req]) grantWaitersLocked() {
	for !b.closed && b.waiters.Len() > 0 {
		front := b.waiters.Front()
		waiter := front.Value.(*capacityWaiter)
		if !b.canReserveLocked(waiter.weight) {
			return
		}
		b.waiters.Remove(front)
		waiter.elem = nil
		b.usedItems++
		b.usedBytes += waiter.weight
		waiter.granted = true
		close(waiter.ready)
	}
}

func (b *buffer[Req]) failWaitersLocked(err error) {
	for element := b.waiters.Front(); element != nil; element = element.Next() {
		waiter := element.Value.(*capacityWaiter)
		waiter.err = err
		waiter.elem = nil
		close(waiter.ready)
	}
	b.waiters.Init()
}

func (b *buffer[Req]) release(weight int64) {
	b.mu.Lock()
	if !b.accountingReset {
		b.usedItems--
		b.usedBytes -= weight
		b.grantWaitersLocked()
	}
	b.mu.Unlock()
}

// reconcileReservation replaces an admission estimate with the actual retained
// payload size. Shrinks are always applied immediately. An underestimated
// reservation may grow only when the unreserved byte budget is already
// available; it never waits while holding an item slot, which would deadlock if
// several concurrent builders all underestimated at the count limit.
func (b *buffer[Req]) reconcileReservation(estimated, actual int64) error {
	if actual < 0 || actual > b.maxBufferedBytes {
		return fmt.Errorf("%w: buffered payload weight %d exceeds limit %d",
			ErrPayloadTooLarge, actual, b.maxBufferedBytes)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return errClosed
	}
	delta := actual - estimated
	if delta > b.maxBufferedBytes-b.usedBytes {
		return fmt.Errorf(
			"%w: actual retained payload weight %d exceeds reserved estimate %d and remaining byte capacity",
			ErrPayloadTooLarge,
			actual,
			estimated,
		)
	}
	b.usedBytes += delta
	if delta < 0 {
		b.grantWaitersLocked()
	}
	return nil
}

// append adds an item after reserve succeeds.
func (b *buffer[Req]) append(offset int64, msg Req, weight int64) error {
	return b.appendUnits(offset, msg, 1, weight)
}

// appendUnits adds an item with its protocol durability-unit count after reserve
// succeeds.
func (b *buffer[Req]) appendUnits(offset int64, msg Req, units uint64, weight int64) error {
	b.mu.Lock()
	if b.closed {
		if !b.accountingReset {
			b.usedItems--
			b.usedBytes -= weight
		}
		b.mu.Unlock()
		b.cond.Broadcast()
		return errClosed
	}
	b.queue = append(b.queue, item[Req]{
		offset: offset, payload: msg, units: units, weight: weight,
	})
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
	// Fail fast on an already-cancelled ctx so a stopping sender doesn't dequeue
	// one more record from a non-empty queue.
	if err := ctx.Err(); err != nil {
		return item[Req]{}, err
	}
	b.mu.Lock()
	for len(b.queue) == 0 && !b.closed {
		// Take b.mu in the callback so the Broadcast can't run before Wait
		// registers the waiter; otherwise the wake-up is lost.
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
	// Re-check after waking: teardown may have started while an item was queued.
	if err := ctx.Err(); err != nil {
		b.mu.Unlock()
		return item[Req]{}, err
	}
	it := b.queue[0]
	b.queue[0] = item[Req]{} // release the departed slot's payload for GC
	b.queue = b.queue[1:]
	it.pendingAt = time.Now()
	b.flight = append(b.flight, it)
	b.flightRevision++
	b.mu.Unlock()
	return it, nil
}

// discardThrough removes every in-flight item whose offset is <= offset (all
// now acknowledged by the server), releases its count and byte capacity, and
// grants queued admission waiters in FIFO order. It is the receiver's only hook
// for ack-driven eviction. Returns the contiguous discarded offset range
// without allocating per-item callback metadata.
func (b *buffer[Req]) discardThrough(offset int64) discardResult {
	result, _, _ := b.acknowledge(AckResolution{
		FullyAcknowledgedOffset: offset,
		PartialOffset:           -1,
	})
	return result
}

// acknowledge applies logical durability progress to the in-flight prefix. A
// partial acknowledgment is retained on the first unacknowledged item so
// recovery or GetUnacked can slice its payload through the protocol hook.
func (b *buffer[Req]) acknowledge(
	resolution AckResolution,
) (discardResult, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	discardCount := 0
	if resolution.FullyAcknowledgedOffset >= 0 {
		for discardCount < len(b.flight) &&
			b.flight[discardCount].offset <= resolution.FullyAcknowledgedOffset {
			discardCount++
		}
		if discardCount == 0 ||
			b.flight[discardCount-1].offset != resolution.FullyAcknowledgedOffset {
			return discardResult{}, false, fmt.Errorf(
				"stream: fully acknowledged logical offset %d is not in flight",
				resolution.FullyAcknowledgedOffset,
			)
		}
	}
	if resolution.PartialOffset >= 0 {
		if discardCount >= len(b.flight) ||
			b.flight[discardCount].offset != resolution.PartialOffset {
			return discardResult{}, false, fmt.Errorf(
				"stream: partially acknowledged logical offset %d is not first in flight",
				resolution.PartialOffset,
			)
		}
		partial := b.flight[discardCount]
		if resolution.PartialUnits == 0 ||
			resolution.PartialUnits >= partial.units {
			return discardResult{}, false, fmt.Errorf(
				"stream: partial prefix %d is invalid for logical offset %d with %d units",
				resolution.PartialUnits, resolution.PartialOffset, partial.units,
			)
		}
	}

	var result discardResult
	var releasedBytes int64
	for range discardCount {
		if result.count == 0 {
			result.first = b.flight[0].offset
		}
		result.last = b.flight[0].offset
		result.count++
		releasedBytes += b.flight[0].weight
		b.flight[0] = item[Req]{} // release the acked payload for GC
		b.flight = b.flight[1:]
	}

	progressed := result.count > 0
	if resolution.PartialOffset >= 0 {
		if resolution.PartialUnits > b.flight[0].ackedUnits {
			b.flight[0].ackedUnits = resolution.PartialUnits
			progressed = true
		}
	}
	if progressed {
		b.flightRevision++
	}
	b.usedItems -= result.count
	b.usedBytes -= releasedBytes
	b.grantWaitersLocked()
	return result, progressed, nil
}

// requeue moves all in-flight items back to the front of the pending queue so
// they are re-sent after a reconnect. Called by the supervisor on stream failure.
func (b *buffer[Req]) requeue() {
	_ = b.requeueWithSlicer(nil)
}

// requeueWithSlicer transforms partially acknowledged items outside b.mu, then
// validates and installs the snapshot before moving all in-flight items to the
// queue. Concurrent queue admission remains available while Arrow decoding,
// compression, and chunk replanning run. The retained byte charge stays
// conservative until the item is fully discarded.
func (b *buffer[Req]) requeueWithSlicer(
	slice func(payload Req, acknowledgedPrefix uint64) (Req, error),
) error {
	for {
		b.mu.Lock()
		if len(b.flight) == 0 {
			b.mu.Unlock()
			return nil
		}
		revision := b.flightRevision
		snapshot := append([]item[Req](nil), b.flight...)
		b.mu.Unlock()

		for i := range snapshot {
			if snapshot[i].ackedUnits == 0 {
				snapshot[i].pendingAt = time.Time{}
				continue
			}
			if slice == nil {
				return fmt.Errorf(
					"stream: no payload slicer for partially acknowledged logical offset %d",
					snapshot[i].offset,
				)
			}
			payload, err := slice(snapshot[i].payload, snapshot[i].ackedUnits)
			if err != nil {
				return fmt.Errorf(
					"stream: slice logical offset %d after %d acknowledged units: %w",
					snapshot[i].offset, snapshot[i].ackedUnits, err,
				)
			}
			snapshot[i].payload = payload
			snapshot[i].units -= snapshot[i].ackedUnits
			snapshot[i].ackedUnits = 0
			snapshot[i].pendingAt = time.Time{}
		}

		b.mu.Lock()
		if b.flightRevision != revision {
			b.mu.Unlock()
			continue
		}
		// Prepend the validated in-flight snapshot before every item admitted
		// while the transform ran.
		requeued := make([]item[Req], 0, len(snapshot)+len(b.queue))
		requeued = append(requeued, snapshot...)
		requeued = append(requeued, b.queue...)
		b.queue = requeued
		for i := range b.flight {
			b.flight[i] = item[Req]{}
		}
		b.flight = b.flight[:0]
		b.flightRevision++
		b.mu.Unlock()
		b.cond.Broadcast()
		return nil
	}
}

// highestInFlight returns the greatest offset the sender has observed on the
// current connection. Pending records are deliberately excluded: the server
// cannot legitimately acknowledge work that has not entered the send path.
func (b *buffer[Req]) highestInFlight() (int64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.flight) == 0 {
		return 0, false
	}
	return b.flight[len(b.flight)-1].offset, true
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
	b.flightRevision++
	b.closed = true
	b.accountingReset = true
	b.usedItems = 0
	b.usedBytes = 0
	b.failWaitersLocked(errClosed)
	b.mu.Unlock()
	b.cond.Broadcast()
	return all
}

// close marks the buffer closed and wakes blocked operations.
// Pending items are not discarded — drain must be called to retrieve them.
func (b *buffer[Req]) close() {
	b.mu.Lock()
	b.closed = true
	b.failWaitersLocked(errClosed)
	b.mu.Unlock()
	b.cond.Broadcast()
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

// oldestInFlightDeadline returns the absolute deadline of the oldest pending
// item. Partial acknowledgments leave pendingAt unchanged, so they cannot extend
// that item's lack-of-ack budget. Replay assigns a fresh connection-local
// pendingAt when next observes the item again.
func (b *buffer[Req]) oldestInFlightDeadline(timeout time.Duration) (time.Time, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.flight) == 0 || b.flight[0].pendingAt.IsZero() {
		return time.Time{}, false
	}
	return b.flight[0].pendingAt.Add(timeout), true
}
