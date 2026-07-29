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
	if err := b.reserve(ctx); err != nil {
		return err
	}
	return b.append(offset, msg)
}
