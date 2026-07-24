package stream

import (
	"errors"
	"fmt"
	"time"

	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

// errClosed is returned by buffer operations when the buffer has been closed.
var errClosed = errors.New("stream: buffer closed")

// errUnsupportedRecordType is returned when no encoder/ackModel exists for a
// record type (e.g. RECORD_TYPE_UNSPECIFIED).
func errUnsupportedRecordType(rt zerobuspb.RecordType) error {
	return fmt.Errorf("stream: unsupported record type %v", rt)
}

// pauseSignal is what the ackModel returns when the server sent a
// CloseStreamSignal: the server is about to close this stream and the client
// should pause (stop sending, keep buffering and draining acks) then
// reconnect, rather than treat it as a failure counted against the
// recovery budget. It carries the server-requested pause duration.
//
// Consumers of pauseSignal (runOnce, supervisor) land in later PRs; the
// type itself lives here because the ackModel decodes it out of the wire
// CloseStreamSignal.
type pauseSignal struct {
	// duration is the server-requested pause window; zero if unspecified.
	duration time.Duration
}

func (pauseSignal) Error() string { return "stream: server requested pause (close-stream signal)" }
