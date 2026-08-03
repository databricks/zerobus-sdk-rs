package zerobus

import (
	"time"

	"github.com/databricks/zerobus-sdk/purego/internal/transport"
)

// Test-only accessors for unexported helpers and injection points.

// GRPCTarget exposes grpcTarget for endpoint-normalization tests.
func GRPCTarget(endpoint string) (string, error) { return grpcTarget(endpoint) }

// NewWithConn builds an SDK around an already-dialed transport connection,
// bypassing New's dialing so tests can point the SDK at an in-memory server.
func NewWithConn(conn *transport.Conn, zerobusEndpoint, ucEndpoint string, opts ...Option) *SDK {
	var cfg sdkConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return newSDK(conn, zerobusEndpoint, ucEndpoint, cfg)
}

// OpenStreamCount reports how many streams the SDK still tracks for Close, so
// tests can assert that Stream.Close deregisters itself.
func (s *SDK) OpenStreamCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.streams)
}

// ResolveStreamConfig applies the given options and returns the resolved record
// type, descriptor, and a subset of the core config for assertion.
func ResolveStreamConfig(opts ...StreamOption) (recordType int32, descriptor []byte, maxInflight int, recovery RecoverySetting, waitReady bool) {
	sc := defaultStreamConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&sc)
		}
	}
	return int32(sc.recordType), sc.descriptor, sc.cfg.MaxInflight, sc.cfg.Recovery, sc.waitReady
}

// ResolveStreamTuning applies options and returns the public tuning values that
// need wiring assertions.
func ResolveStreamTuning(opts ...StreamOption) (
	recoveryTimeout, recoveryBackoff, lackOfAckTimeout time.Duration,
	maxBatchRecords int,
	streamPausedMaxWait *time.Duration,
) {
	sc := defaultStreamConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&sc)
		}
	}
	return sc.cfg.RecoveryTimeout, sc.cfg.RecoveryBackoff, sc.cfg.LackOfAckTimeout,
		sc.cfg.MaxBatchRecords, sc.cfg.StreamPausedMaxWait
}
