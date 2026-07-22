package stream

// len returns the total number of items in the buffer (pending + in-flight).
// Test-only helper: not used in production code paths, so it lives here.
func (b *buffer[Req]) len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.queue) + len(b.flight)
}
