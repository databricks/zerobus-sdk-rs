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
