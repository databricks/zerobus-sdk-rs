package zerobus

import "github.com/databricks/zerobus-sdk/purego/internal/transport"

// Test-only accessors for unexported helpers and injection points.

// GRPCTarget exposes grpcTarget for endpoint-normalization tests.
func GRPCTarget(endpoint string) (string, error) { return grpcTarget(endpoint) }

// NewWithConn builds an SDK around an already-dialed transport connection,
// bypassing New's dialing so tests can point the SDK at an in-memory server.
func NewWithConn(conn *transport.Conn, zerobusEndpoint, ucEndpoint string) *SDK {
	return &SDK{
		zerobusEndpoint: zerobusEndpoint,
		ucEndpoint:      ucEndpoint,
		conn:            conn,
	}
}

// ResolveStreamConfig applies the given options and returns the resolved record
// type, descriptor, and a subset of the core config for assertion.
func ResolveStreamConfig(opts ...StreamOption) (recordType int32, descriptor []byte, maxInflight int, recovery RecoverySetting) {
	sc := defaultStreamConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&sc)
		}
	}
	return int32(sc.recordType), sc.descriptor, sc.cfg.MaxInflight, sc.cfg.Recovery
}
