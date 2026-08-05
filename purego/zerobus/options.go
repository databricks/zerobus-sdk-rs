package zerobus

import (
	"crypto/tls"
	"net/http"
	"time"

	"github.com/databricks/zerobus-sdk/purego/internal/stream"
	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

// Option configures a SDK created with New.
type Option func(*sdkConfig)

type sdkConfig struct {
	applicationName           string
	tlsConfig                 *tls.Config
	httpClient                *http.Client
	dynamicSchemaFetchTimeout time.Duration
}

// WithApplicationName appends a caller-supplied identifier such as "my-app/1.0"
// to the gRPC user-agent header. Leading and trailing whitespace is trimmed and
// blank names are ignored. Names must be valid ASCII user-agent values; invalid
// UTF-8, control characters, and DEL cause New to fail. The final value is
// "zerobus-sdk-go-purego/<version> <name>".
func WithApplicationName(name string) Option {
	return func(c *sdkConfig) { c.applicationName = name }
}

// WithTLSConfig replaces the default system-root TLS configuration.
func WithTLSConfig(tc *tls.Config) Option {
	return func(c *sdkConfig) { c.tlsConfig = tc }
}

// WithProtoDescriptorFetchTimeout sets the timeout used by
// FetchProtoDescriptor for Unity Catalog schema requests.
// A non-positive value keeps the default.
func WithProtoDescriptorFetchTimeout(d time.Duration) Option {
	return func(c *sdkConfig) { c.dynamicSchemaFetchTimeout = d }
}

// WithHTTPClient overrides the HTTP client used for OAuth and Unity Catalog
// schema requests.
// A nil client is ignored.
func WithHTTPClient(client *http.Client) Option {
	return func(c *sdkConfig) {
		if client != nil {
			c.httpClient = client
		}
	}
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
// implementations should return promptly to avoid delaying later notifications.
// Stream methods, including Close, may be called from a callback.
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
	waitReady  bool
	cfg        stream.Config
}

func defaultStreamConfig() streamConfig {
	return streamConfig{
		recordType: zerobuspb.RecordType_PROTO,
		cfg:        stream.DefaultConfig(),
	}
}

// WithJSON selects JSON record encoding.
func WithJSON() StreamOption {
	return func(c *streamConfig) {
		c.recordType = zerobuspb.RecordType_JSON
		c.descriptor = nil
	}
}

// WithProto selects Protocol Buffer record encoding. descriptorProto must be a
// serialized DescriptorProto.
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

// WithWaitForReady makes CreateStream / CreateStreamWithProvider wait for the
// first stream open to succeed (or fail terminally) before returning. The
// creation context directly bounds token resolution, handshake, retry backoff,
// and every attempt before first-open succeeds. Its cancellation is detached
// after success, so it does not own the live stream.
//
// Without this option, stream open remains asynchronous: context values
// propagate to the stream, but cancellation and deadlines are detached because
// first-open outlives the CreateStream call. Failures surface on the first
// Flush, WaitForOffset, or ack callback.
func WithWaitForReady() StreamOption {
	return func(c *streamConfig) { c.waitReady = true }
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

// WithRecoveryTimeout bounds each stream-open attempt during recovery. A
// non-positive value keeps the default (15 seconds).
func WithRecoveryTimeout(d time.Duration) StreamOption {
	return func(c *streamConfig) { c.cfg.RecoveryTimeout = d }
}

// WithRecoveryBackoff sets the delay between consecutive recovery attempts. A
// non-positive value keeps the default (2 seconds).
func WithRecoveryBackoff(d time.Duration) StreamOption {
	return func(c *streamConfig) { c.cfg.RecoveryBackoff = d }
}

// WithLackOfAckTimeout bounds how long records may remain in flight without an
// acknowledgement before recovery starts. A non-positive value keeps the
// default (60 seconds).
func WithLackOfAckTimeout(d time.Duration) StreamOption {
	return func(c *streamConfig) { c.cfg.LackOfAckTimeout = d }
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

// WithMaxBatchRecords caps the number of records accepted by one
// IngestRecordsOffset call. A non-positive value keeps the default (100,000).
func WithMaxBatchRecords(n int) StreamOption {
	return func(c *streamConfig) { c.cfg.MaxBatchRecords = n }
}

// WithStreamPausedMaxWait caps how long the client honors a server-requested
// pause before reconnecting. If omitted, the full server-requested duration is
// honored. Passing zero reconnects immediately; a positive duration waits for
// the shorter of that duration and the server request.
func WithStreamPausedMaxWait(d time.Duration) StreamOption {
	return func(c *streamConfig) {
		c.cfg.StreamPausedMaxWait = &d
	}
}
