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
	msg, _ := protoEncoder{}.encode(offset, []byte("x"))
	return msg
}

func TestBufferEnqueueNext(t *testing.T) {
	b := newBuffer(4)

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
	b := newBuffer(8)
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
	b := newBuffer(cap)

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

func TestBufferContextCancelUnblocksEnqueue(t *testing.T) {
	b := newBuffer(1)
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
	b := newBuffer(4)
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
	b := newBuffer(8)
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
	b := newBuffer(4)
	b.close()
	err := b.enqueue(context.Background(), 1, dummyMsg(1))
	if err != errClosed {
		t.Fatalf("want errClosed, got %v", err)
	}
}

func TestBufferNextAfterCloseAndDrainErrors(t *testing.T) {
	b := newBuffer(4)
	b.drain()
	_, err := b.next(context.Background())
	if err != errClosed {
		t.Fatalf("want errClosed, got %v", err)
	}
}

func TestBufferConcurrentEnqueueDiscard(t *testing.T) {
	const n = 100
	b := newBuffer(16)

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
