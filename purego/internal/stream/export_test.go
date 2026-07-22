package stream

import "context"

// len returns the total number of items in the buffer (pending + in-flight).
// Test-only helper: not used in production code paths, so it lives here.
func (b *buffer[Req]) len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.queue) + len(b.flight)
}

// enqueue combines reserve+append for tests that predate the split. Production
// code uses reserve and append directly so the semaphore wait happens outside
// the core's offset-assignment critical section.
func (b *buffer[Req]) enqueue(ctx context.Context, offset int64, msg Req) error {
	if err := b.reserve(ctx); err != nil {
		return err
	}
	return b.append(offset, msg)
}
