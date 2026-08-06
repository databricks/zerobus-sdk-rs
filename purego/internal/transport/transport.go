// Package transport opens and manages the gRPC connection to the Zerobus
// ingestion service.
//
// It is the lowest layer of the pure-Go SDK: it dials the service, applies TLS
// and authentication metadata, and exposes bidirectional EphemeralStream and
// Arrow Flight DoPut RPCs. It does not implement batching, offsets, or
// acknowledgment handling beyond validating each protocol's setup response.
package transport

import (
	"crypto/tls"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

// Conn is a connection to the Zerobus service. A single Conn is safe for
// concurrent use and may back many streams; callers must Close it when done.
type Conn struct {
	cc           *grpc.ClientConn
	client       zerobuspb.ZerobusClient
	flightClient flight.FlightServiceClient
}

// Dial connects to the Zerobus gRPC service at endpoint.
//
// endpoint uses the same target syntax accepted by grpc.NewClient (for example
// "host:port", "dns:///host:port", or resolver-specific targets used in tests).
//
// The connection is secured with TLS using the host's root CAs unless
// WithTLSConfig overrides it. Like grpc.NewClient, dialing is lazy: the TCP/TLS
// handshake happens on the first stream, not here.
func Dial(endpoint string, opts ...DialOption) (*Conn, error) {
	cfg := dialConfig{
		creds: credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12}),
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	dialOpts := append([]grpc.DialOption{grpc.WithTransportCredentials(cfg.creds)}, cfg.grpcOpts...)
	cc, err := grpc.NewClient(endpoint, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("transport: dial %q: %w", endpoint, err)
	}
	return &Conn{
		cc:           cc,
		client:       zerobuspb.NewZerobusClient(cc),
		flightClient: flight.NewFlightServiceClient(cc),
	}, nil
}

// Close releases the underlying connection. In-flight streams are terminated.
func (c *Conn) Close() error {
	if err := c.cc.Close(); err != nil {
		return fmt.Errorf("transport: close: %w", err)
	}
	return nil
}

// dialConfig is the resolved set of dial options.
type dialConfig struct {
	creds    credentials.TransportCredentials
	grpcOpts []grpc.DialOption
}

// A DialOption configures how Dial connects to the service.
type DialOption func(*dialConfig)

// WithTLSConfig secures the connection with a custom TLS configuration,
// replacing the default of system root CAs.
func WithTLSConfig(tc *tls.Config) DialOption {
	return func(cfg *dialConfig) { cfg.creds = credentials.NewTLS(tc) }
}

// WithGRPCDialOptions passes additional options straight through to gRPC. It is
// intended for advanced tuning (keepalives, interceptors) and for injecting an
// in-memory listener in tests.
func WithGRPCDialOptions(opts ...grpc.DialOption) DialOption {
	return func(cfg *dialConfig) { cfg.grpcOpts = append(cfg.grpcOpts, opts...) }
}
