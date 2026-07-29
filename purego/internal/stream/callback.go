package stream

import (
	"sync"
	"time"
)

type callbackEvent struct {
	offset int64
	err    error
}

type callbackRange struct {
	first int64
	last  int64
	err   error
}

// callbackDispatcher runs callbacks outside stream workers.
// Adjacent offsets share one queue entry.
type callbackDispatcher struct {
	callback AckCallback
	mu       sync.Mutex
	cond     *sync.Cond
	queue    []callbackRange
	closed   bool
	done     chan struct{}

	closeOnce sync.Once
}

func newCallbackDispatcher(callback AckCallback) *callbackDispatcher {
	if callback == nil {
		return nil
	}
	d := &callbackDispatcher{
		callback: callback,
		done:     make(chan struct{}),
	}
	d.cond = sync.NewCond(&d.mu)
	go d.run()
	return d
}

func (d *callbackDispatcher) run() {
	defer close(d.done)
	for {
		d.mu.Lock()
		for len(d.queue) == 0 && !d.closed {
			d.cond.Wait()
		}
		if len(d.queue) == 0 {
			d.mu.Unlock()
			return
		}
		next := d.queue[0]
		d.queue[0] = callbackRange{}
		d.queue = d.queue[1:]
		d.mu.Unlock()

		for offset := next.first; ; offset++ {
			d.invoke(callbackEvent{offset: offset, err: next.err})
			if offset == next.last {
				break
			}
		}
	}
}

// invoke prevents callback panics from stopping the stream.
func (d *callbackDispatcher) invoke(event callbackEvent) {
	defer func() { _ = recover() }()
	if event.err == nil {
		d.callback.OnAck(event.offset)
		return
	}
	d.callback.OnError(event.offset, event.err)
}

// enqueueAcks queues and coalesces acknowledged offsets. Coalescing a range into
// its predecessor assumes dense offsets: run dispatches every offset in
// [first, last], so a gap would fabricate callbacks for offsets never ingested.
func (d *callbackDispatcher) enqueueAcks(first, last int64) {
	if d == nil || first > last {
		return
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	if n := len(d.queue); n > 0 && d.queue[n-1].err == nil && d.queue[n-1].last+1 == first {
		d.queue[n-1].last = last
	} else {
		d.queue = append(d.queue, callbackRange{first: first, last: last})
	}
	d.mu.Unlock()
	d.cond.Signal()
}

// enqueueErrors queues one unacknowledged offset range.
func (d *callbackDispatcher) enqueueErrors(first, last int64, err error) {
	if d == nil || first > last {
		return
	}
	d.mu.Lock()
	if !d.closed {
		d.queue = append(d.queue, callbackRange{first: first, last: last, err: err})
	}
	d.mu.Unlock()
	d.cond.Signal()
}

// shutdown closes the queue and briefly waits for the worker.
// The stream must publish done before calling shutdown.
func (d *callbackDispatcher) shutdown(timeout time.Duration) {
	if d == nil {
		return
	}
	d.closeOnce.Do(func() {
		d.mu.Lock()
		d.closed = true
		d.mu.Unlock()
		d.cond.Broadcast()
	})
	if timeout <= 0 {
		return
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-d.done:
	case <-timer.C:
	}
}
