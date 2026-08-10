package stream

import "context"

// len returns the buffered item count.
func (b *buffer[Req]) len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.queue) + len(b.flight)
}

// enqueue combines reserve and append for tests.
func (b *buffer[Req]) enqueue(ctx context.Context, offset int64, msg Req) error {
	if err := b.reserve(ctx, 1); err != nil {
		return err
	}
	return b.append(offset, msg, 1)
}

// append adds a single-unit item, the shape every atomic-protocol test uses.
func (b *buffer[Req]) append(offset int64, msg Req, weight int64) error {
	return b.appendUnits(offset, msg, 1, weight)
}

// requeue replays the in-flight set without a payload slicer.
func (b *buffer[Req]) requeue() {
	_ = b.requeueWithSlicer(nil)
}

// discardThrough acknowledges every in-flight item up to and including offset.
func (b *buffer[Req]) discardThrough(offset int64) discardResult {
	result, _, _ := b.acknowledge(AckResolution{
		FullyAcknowledgedOffset: offset,
		PartialOffset:           -1,
	})
	return result
}

func (b *buffer[Req]) usage() (int, int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.usedItems, b.usedBytes
}

func (b *buffer[Req]) waiterCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.waiters.Len()
}
