package zerobus

// RecordType represents the type of records to ingest
type RecordType int32

const (
	// RecordTypeUnspecified indicates no specific record type
	RecordTypeUnspecified RecordType = 0
	// RecordTypeProto indicates Protocol Buffer encoded records
	RecordTypeProto RecordType = 1
	// RecordTypeJson indicates JSON encoded records
	RecordTypeJson RecordType = 2
	// RecordTypeAvro indicates Avro encoded records (Beta, requires avro build tag)
	RecordTypeAvro RecordType = 4
)

// StreamConfigurationOptions contains configuration options for creating a stream
type StreamConfigurationOptions struct {
	// Maximum number of requests that can be in-flight (pending acknowledgment) at once.
	// Default: 1,000,000
	MaxInflightRequests uint64

	// Enable automatic stream recovery on retryable failures.
	// Default: true
	//
	// NOTE: Because false is Go's zero value for bool, setting Recovery = false
	// on a zero-value struct is indistinguishable from "not set" and will be
	// overridden with the default (true). Always start from
	// DefaultStreamConfigurationOptions() before setting Recovery = false:
	//
	//   opts := zerobus.DefaultStreamConfigurationOptions()
	//   opts.Recovery = false
	Recovery bool

	// Timeout for each recovery attempt in milliseconds
	// Default: 15000 (15 seconds)
	RecoveryTimeoutMs uint64

	// Backoff delay between recovery attempts in milliseconds
	// Default: 2000 (2 seconds)
	RecoveryBackoffMs uint64

	// Maximum number of recovery retry attempts
	// Default: 4
	RecoveryRetries uint32

	// Server acknowledgment timeout in milliseconds
	// Default: 60000 (60 seconds)
	ServerLackOfAckTimeoutMs uint64

	// Flush operation timeout in milliseconds
	// Default: 300000 (5 minutes)
	FlushTimeoutMs uint64

	// Type of record to ingest (Proto, Json, or Unspecified)
	// Default: RecordTypeProto
	RecordType RecordType

	// Maximum time in milliseconds to wait during graceful stream close
	// when server sends a CloseStreamSignal.
	// - nil: Wait for the full server-specified duration (most graceful, default)
	// - 0: Immediate recovery, close stream right away
	// - x: Wait up to min(x, server_duration) milliseconds
	// Default: nil (wait for full server duration)
	StreamPausedMaxWaitTimeMs *uint64
}

// DefaultStreamConfigurationOptions returns the default configuration options
func DefaultStreamConfigurationOptions() *StreamConfigurationOptions {
	return &StreamConfigurationOptions{
		MaxInflightRequests:      1_000_000,
		Recovery:                 true,
		RecoveryTimeoutMs:        15000,
		RecoveryBackoffMs:        2000,
		RecoveryRetries:          4,
		ServerLackOfAckTimeoutMs: 60000,
		FlushTimeoutMs:           300000,
		RecordType:               RecordTypeProto,
	}
}

// TableProperties contains information about the target table
type TableProperties struct {
	// Fully qualified table name (catalog.schema.table)
	TableName string

	// Protocol buffer descriptor (required for Proto record type, nil for JSON)
	// This should be a serialized prost_types::DescriptorProto
	DescriptorProto []byte
}
