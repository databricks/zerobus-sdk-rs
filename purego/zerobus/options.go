package zerobus

import (
	"crypto/tls"
	"time"

	"github.com/databricks/zerobus-sdk/purego/internal/stream"
	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

// Option configures a SDK created with New.
type Option func(*sdkConfig)

type sdkConfig struct {
	applicationName string
	tlsConfig       *tls.Config
}

// WithApplicationName appends a caller-supplied identifier such as "my-app/1.0"
// to the gRPC user-agent header. Leading and trailing whitespace is trimmed and
// blank names are ignored. The final value is "zerobus-sdk-go-purego/<version>
// <name>".
func WithApplicationName(name string) Option {
	return func(c *sdkConfig) { c.applicationName = name }
}

// WithTLSConfig secures the connection with a custom TLS configuration,
// replacing the default of system root CAs. It is intended for pinning a custom
// CA or for tests; production callers rarely need it.
func WithTLSConfig(tc *tls.Config) Option {
	return func(c *sdkConfig) { c.tlsConfig = tc }
}

// RecoverySetting controls whether a stream reconnects after a recoverable
// failure. Its zero value enables recovery.
type RecoverySetting = stream.RecoverySetting

const (
	// RecoveryEnabled reconnects on recoverable failures. It is the zero value.
	RecoveryEnabled = stream.RecoveryEnabled
	// RecoveryDisabled fails the stream on the first error without reconnecting.
	RecoveryDisabled = stream.RecoveryDisabled
)

// AckCallback receives asynchronous per-offset acknowledgement and error
// notifications. Register one with WithAckCallback as an alternative to blocking
// on Flush or WaitForOffset. Callbacks run on a dedicated worker, so
// implementations must return promptly and must not call back into the stream.
type AckCallback = stream.AckCallback

// StreamOption configures a stream created with CreateStream or
// CreateStreamWithProvider. Record type, recovery behavior, buffering limits,
// and the ack callback are all set through these options; unset options take
// their defaults.
type StreamOption func(*streamConfig)

type streamConfig struct {
	recordType zerobuspb.RecordType
	descriptor []byte
	callback   AckCallback
	cfg        stream.Config
}

func defaultStreamConfig() streamConfig {
	return streamConfig{
		// JSON is the default so a record type is always valid without a
		// descriptor; WithProto switches to proto and supplies the descriptor.
		recordType: zerobuspb.RecordType_JSON,
		cfg:        stream.DefaultConfig(),
	}
}

// WithJSON selects JSON record encoding. It is the default when no encoding
// option is given.
func WithJSON() StreamOption {
	return func(c *streamConfig) {
		c.recordType = zerobuspb.RecordType_JSON
		c.descriptor = nil
	}
}

// WithProto selects Protocol Buffer record encoding. descriptorProto is the
// serialized message descriptor (a FileDescriptorProto / DescriptorProto) the
// service uses to interpret the raw protobuf record bytes; it is required for
// proto streams.
func WithProto(descriptorProto []byte) StreamOption {
	return func(c *streamConfig) {
		c.recordType = zerobuspb.RecordType_PROTO
		c.descriptor = descriptorProto
	}
}

// WithAckCallback registers a callback for asynchronous ack/error notification.
// It is an alternative to blocking on Flush or WaitForOffset; both may be used
// together.
func WithAckCallback(cb AckCallback) StreamOption {
	return func(c *streamConfig) { c.callback = cb }
}

// WithRecovery sets whether the stream reconnects after a recoverable failure.
// Recovery is enabled by default.
func WithRecovery(r RecoverySetting) StreamOption {
	return func(c *streamConfig) { c.cfg.Recovery = r }
}

// WithMaxInflight caps the number of unacknowledged ingest calls buffered before
// IngestRecordOffset / IngestRecordsOffset block for backpressure. One call
// occupies one slot regardless of how many records it carries. A non-positive
// value keeps the default.
func WithMaxInflight(n int) StreamOption {
	return func(c *streamConfig) { c.cfg.MaxInflight = n }
}

// WithMaxBufferedPayloadBytes caps the estimated encoded memory retained by
// queued and in-flight payloads. Whichever of this and WithMaxInflight binds
// first applies backpressure. A non-positive value keeps the default.
func WithMaxBufferedPayloadBytes(n int64) StreamOption {
	return func(c *streamConfig) { c.cfg.MaxBufferedPayloadBytes = n }
}

// WithRecoveryRetries limits consecutive reconnect failures before the stream
// gives up. A non-positive value keeps the default (4).
func WithRecoveryRetries(n int) StreamOption {
	return func(c *streamConfig) { c.cfg.RecoveryRetries = n }
}

// WithFlushTimeout sets the upper bound on how long Flush and WaitForOffset wait
// for acknowledgement. A caller context may shorten this budget but cannot
// extend it. A non-positive value keeps the default (5 minutes).
func WithFlushTimeout(d time.Duration) StreamOption {
	return func(c *streamConfig) { c.cfg.FlushTimeout = d }
}

// WithMaxPayloadBytes caps the encoded size of a single ingest request. A
// non-positive value keeps the default (just under the 10 MiB service limit).
func WithMaxPayloadBytes(n int) StreamOption {
	return func(c *streamConfig) { c.cfg.MaxPayloadBytes = n }
}
