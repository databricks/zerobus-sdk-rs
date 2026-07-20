package stream

import (
	"errors"
	"time"
)

// errClosed is returned by buffer operations when the buffer has been closed.
var errClosed = errors.New("stream: buffer closed")

// pauseSignal is the cause runOnce returns when the server sent a
// CloseStreamSignal: the server is about to close this stream and the client
// should pause (stop sending, keep buffering and draining acks) then reconnect,
// rather than treat it as a failure that counts against the recovery budget. It
// carries the server-requested pause duration.
type pauseSignal struct {
	// duration is the server-requested pause window; zero if unspecified.
	duration time.Duration
}

func (pauseSignal) Error() string { return "stream: server requested pause (close-stream signal)" }
