package transport

import (
	"time"

	"google.golang.org/grpc/credentials/insecure"
)

// WithInsecure disables transport security. Auth tokens ride gRPC metadata, so
// an insecure connection would send bearer tokens in plaintext; it lives in a
// _test.go file so production code can't reach it. Tests use it for in-memory
// and local plaintext endpoints.
func WithInsecure() DialOption {
	return func(cfg *dialConfig) { cfg.creds = insecure.NewCredentials() }
}

// SetDefaultDrainTimeout overrides the no-deadline gracefulClose timeout and
// returns a restore func, so tests exercise the default-applied path quickly.
func SetDefaultDrainTimeout(d time.Duration) (restore func()) {
	prev := defaultDrainTimeout
	defaultDrainTimeout = d
	return func() { defaultDrainTimeout = prev }
}

// SetDefaultHeadersTimeout overrides the no-deadline header-resolution timeout
// and returns a restore func for tests.
func SetDefaultHeadersTimeout(d time.Duration) (restore func()) {
	prev := defaultHeadersTimeout
	defaultHeadersTimeout = d
	return func() { defaultHeadersTimeout = prev }
}

// SetDefaultHandshakeTimeout overrides the no-deadline handshake timeout and
// returns a restore func for tests.
func SetDefaultHandshakeTimeout(d time.Duration) (restore func()) {
	prev := defaultHandshakeTimeout
	defaultHandshakeTimeout = d
	return func() { defaultHandshakeTimeout = prev }
}
