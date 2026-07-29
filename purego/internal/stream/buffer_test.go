package stream

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// dummyMsg returns a non-nil encodedMsg for use in buffer tests.
func dummyMsg(offset int64) encodedMsg {
	msg, _ := protoEncoder{}.encode([]byte("x"))
	protoEncoder{}.stampOffset(msg, offset)
	return msg
}

func TestBufferEnqueueNext(t *testing.T) {
	b := newBuffer[encodedMsg](4)

	if err := b.enqueue(context.Background(), 1, dummyMsg(1)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	it, err := b.next(context.Background())
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if it.offset != 1 {
		t.Fatalf("want offset 1, got %d", it.offset)
	}
}

func TestBufferFIFOOrder(t *testing.T) {
	b := newBuffer[encodedMsg](8)
	for i := int64(1); i <= 5; i++ {
		if err := b.enqueue(context.Background(), i, dummyMsg(i)); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	for i := int64(1); i <= 5; i++ {
		it, err := b.next(context.Background())
		if err != nil {
			t.Fatalf("next %d: %v", i, err)
		}
		if it.offset != i {
			t.Fatalf("want offset %d, got %d", i, it.offset)
		}
	}
}

func TestBufferBackpressure(t *testing.T) {
	const cap = 2
	b := newBuffer[encodedMsg](cap)

	// Fill the buffer to capacity.
	for i := int64(1); i <= cap; i++ {
		if err := b.enqueue(context.Background(), i, dummyMsg(i)); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	// A third enqueue should block until a slot is freed.
	var enqueued atomic.Bool
	go func() {
		_ = b.enqueue(context.Background(), 3, dummyMsg(3))
		enqueued.Store(true)
	}()

	time.Sleep(20 * time.Millisecond)
	if enqueued.Load() {
		t.Fatal("enqueue should be blocked at capacity")
	}

	// Observe and discard one item to free a slot.
	it, err := b.next(context.Background())
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	b.discardThrough(it.offset)

	// Now the blocked enqueue should complete.
	deadline := time.Now().Add(200 * time.Millisecond)
	for !enqueued.Load() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !enqueued.Load() {
		t.Fatal("enqueue did not unblock after discard")
	}
}

func TestBufferByteBackpressureAndRelease(t *testing.T) {
	b := newBuffer[encodedMsg](4, 3)
	if err := b.reserve(context.Background(), 3); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if err := b.append(1, dummyMsg(1), 3); err != nil {
		t.Fatalf("append: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		if err := b.reserve(context.Background(), 1); err != nil {
			done <- err
			return
		}
		done <- b.append(2, dummyMsg(2), 1)
	}()
	select {
	case err := <-done:
		t.Fatalf("byte-limited append completed early: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	it, err := b.next(context.Background())
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	b.discardThrough(it.offset)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("blocked append: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("byte-limited append did not unblock")
	}
	if items, bytes := b.usage(); items != 1 || bytes != 1 {
		t.Fatalf("usage = (%d, %d), want (1, 1)", items, bytes)
	}
}

func TestBufferCapacityWaitersAreFIFO(t *testing.T) {
	b := newBuffer[encodedMsg](3, 3)
	if err := b.reserve(context.Background(), 3); err != nil {
		t.Fatalf("initial reserve: %v", err)
	}
	if err := b.append(0, dummyMsg(0), 3); err != nil {
		t.Fatalf("initial append: %v", err)
	}

	firstDone := make(chan error, 1)
	go func() {
		if err := b.reserve(context.Background(), 3); err != nil {
			firstDone <- err
			return
		}
		firstDone <- b.append(1, dummyMsg(1), 3)
	}()
	waitCondition(t, func() bool { return b.waiterCount() == 1 }, time.Second)

	secondDone := make(chan error, 1)
	go func() {
		if err := b.reserve(context.Background(), 1); err != nil {
			secondDone <- err
			return
		}
		secondDone <- b.append(2, dummyMsg(2), 1)
	}()
	waitCondition(t, func() bool { return b.waiterCount() == 2 }, time.Second)

	it, err := b.next(context.Background())
	if err != nil {
		t.Fatalf("next initial: %v", err)
	}
	b.discardThrough(it.offset)
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first waiter: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first waiter did not unblock")
	}
	select {
	case err := <-secondDone:
		t.Fatalf("second waiter bypassed FIFO order: %v", err)
	default:
	}

	it, err = b.next(context.Background())
	if err != nil {
		t.Fatalf("next first waiter: %v", err)
	}
	b.discardThrough(it.offset)
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("second waiter: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second waiter did not unblock")
	}
}

func TestBufferRecoveryDoesNotDoubleChargeBytes(t *testing.T) {
	b := newBuffer[encodedMsg](4, 10)
	if err := b.reserve(context.Background(), 7); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if err := b.append(1, dummyMsg(1), 7); err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := b.next(context.Background()); err != nil {
		t.Fatalf("next: %v", err)
	}
	b.requeue()
	if items, bytes := b.usage(); items != 1 || bytes != 7 {
		t.Fatalf("usage after requeue = (%d, %d), want (1, 7)", items, bytes)
	}
}

func TestBufferByteWaiterUnblocksOnClose(t *testing.T) {
	b := newBuffer[encodedMsg](4, 1)
	if err := b.reserve(context.Background(), 1); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if err := b.append(1, dummyMsg(1), 1); err != nil {
		t.Fatalf("append: %v", err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- b.reserve(context.Background(), 1) }()
	b.close()
	select {
	case err := <-errCh:
		if err != errClosed {
			t.Fatalf("reserve after close = %v, want errClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("byte waiter did not unblock on close")
	}
}

func TestBufferContextCancelUnblocksEnqueue(t *testing.T) {
	b := newBuffer[encodedMsg](1)
	if err := b.enqueue(context.Background(), 1, dummyMsg(1)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- b.enqueue(ctx, 2, dummyMsg(2))
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err != context.Canceled {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("enqueue did not unblock on context cancel")
	}
}

func TestBufferRequeueResendsInOrder(t *testing.T) {
	b := newBuffer[encodedMsg](4)
	for i := int64(1); i <= 3; i++ {
		if err := b.enqueue(context.Background(), i, dummyMsg(i)); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	// Observe all three (move to in-flight).
	for range 3 {
		if _, err := b.next(context.Background()); err != nil {
			t.Fatalf("next: %v", err)
		}
	}

	// Requeue moves them back to the front of the queue.
	b.requeue()

	for i := int64(1); i <= 3; i++ {
		it, err := b.next(context.Background())
		if err != nil {
			t.Fatalf("next after requeue %d: %v", i, err)
		}
		if it.offset != i {
			t.Fatalf("want offset %d after requeue, got %d", i, it.offset)
		}
	}
}

func TestBufferDrainReturnsAll(t *testing.T) {
	b := newBuffer[encodedMsg](8)
	for i := int64(1); i <= 4; i++ {
		if err := b.enqueue(context.Background(), i, dummyMsg(i)); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}
	// Observe two to put them in-flight.
	for range 2 {
		if _, err := b.next(context.Background()); err != nil {
			t.Fatalf("next: %v", err)
		}
	}

	items := b.drain()
	if len(items) != 4 {
		t.Fatalf("want 4 items from drain, got %d", len(items))
	}
	// Verify in-flight items come first (offsets 1,2), then pending (3,4).
	for i, want := range []int64{1, 2, 3, 4} {
		if items[i].offset != want {
			t.Fatalf("drain[%d]: want offset %d, got %d", i, want, items[i].offset)
		}
	}
}

func TestBufferEnqueueAfterCloseErrors(t *testing.T) {
	b := newBuffer[encodedMsg](4)
	b.close()
	err := b.enqueue(context.Background(), 1, dummyMsg(1))
	if err != errClosed {
		t.Fatalf("want errClosed, got %v", err)
	}
}

func TestBufferNextAfterCloseAndDrainErrors(t *testing.T) {
	b := newBuffer[encodedMsg](4)
	b.drain()
	_, err := b.next(context.Background())
	if err != errClosed {
		t.Fatalf("want errClosed, got %v", err)
	}
}

func TestBufferConcurrentEnqueueDiscard(t *testing.T) {
	const n = 100
	b := newBuffer[encodedMsg](16)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			it, err := b.next(context.Background())
			if err != nil {
				return
			}
			b.discardThrough(it.offset)
		}
	}()

	for i := int64(1); i <= n; i++ {
		if err := b.enqueue(context.Background(), i, dummyMsg(i)); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	wg.Wait()

	if got := b.len(); got != 0 {
		t.Fatalf("want buffer empty after all discards, got len=%d", got)
	}
}

// next must unblock when its ctx is cancelled while parked on an empty buffer.
// The AfterFunc broadcast has to be ordered under b.mu against cond.Wait, or
// the wake-up can be lost and next sleeps forever on a cancelled ctx.
func TestBufferNextContextCancelUnblocks(t *testing.T) {
	b := newBuffer[encodedMsg](4)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := b.next(ctx)
		errCh <- err
	}()

	time.Sleep(20 * time.Millisecond) // let it park in cond.Wait
	cancel()

	select {
	case err := <-errCh:
		if err != context.Canceled {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("next did not unblock on context cancel")
	}
}

// next must return immediately on an already-cancelled ctx and leave a queued
// item untouched, so a stopping sender never drains one more record.
func TestBufferNextAlreadyCancelledCtx(t *testing.T) {
	b := newBuffer[encodedMsg](4)
	if err := b.enqueue(context.Background(), 1, dummyMsg(1)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := b.next(ctx); err != context.Canceled {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if got := b.len(); got != 1 {
		t.Fatalf("want item left in buffer, got len=%d", got)
	}
}

// A non-positive cap is normalized to the default rather than deadlocking (0)
// or panicking (<0), so the primitive stays live on a bad value.
func TestNewBufferNormalizesNonPositiveCap(t *testing.T) {
	for _, cap := range []int{0, -1} {
		b := newBuffer[encodedMsg](cap)
		if err := b.enqueue(context.Background(), 1, dummyMsg(1)); err != nil {
			t.Fatalf("cap %d: enqueue should not block/fail, got %v", cap, err)
		}
	}
}
