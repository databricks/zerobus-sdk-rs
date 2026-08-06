package zerobus

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"

	"github.com/databricks/zerobus-sdk/purego/internal/arrowproto"
	"github.com/databricks/zerobus-sdk/purego/internal/auth"
	"github.com/databricks/zerobus-sdk/purego/internal/stream"
)

const defaultArrowMaxInflight = 1_000

// ArrowStream is a Beta Arrow Flight ingestion stream.
//
// Ingestion is asynchronous: queue batches in a loop and call Flush once.
// Each successful ingest returns a logical batch offset without waiting for
// server acknowledgement. Ack callbacks registered with WithAckCallback fire
// only when the complete logical batch offset is durable; a partial row ack
// does not fire a callback.
//
// The default limit is 1,000 unacknowledged logical batches. Large batches are
// split by rows into FlightData messages no larger than 2 MiB. These frames do
// not change the logical offset or caller-visible batch boundary.
type ArrowStream struct {
	core     *stream.CoreStream[*arrowproto.Payload, *flight.PutResult]
	protocol *arrowproto.Protocol
	sdk      *SDK
}

// EncodeArrowSchemaIPC serializes schema as a schema-only Arrow IPC stream.
// The result can be passed to CreateArrowStreamFromIPC.
func EncodeArrowSchemaIPC(schema *arrow.Schema) ([]byte, error) {
	data, err := arrowproto.EncodeSchemaIPC(schema)
	if err != nil {
		return nil, &Error{Op: "EncodeArrowSchemaIPC", cause: err, retryable: false}
	}
	return data, nil
}

// DecodeArrowSchemaIPC parses a schema-only Arrow IPC stream. The returned
// schema is independent of data and may be reused across stream constructors.
func DecodeArrowSchemaIPC(data []byte) (*arrow.Schema, error) {
	schema, err := arrowproto.DecodeSchemaIPC(data)
	if err != nil {
		return nil, &Error{Op: "DecodeArrowSchemaIPC", cause: err, retryable: false}
	}
	return schema, nil
}

// CreateArrowStream creates an OAuth-authenticated Beta Arrow Flight stream
// from a typed schema. The schema is copied and is not retained.
func (s *SDK) CreateArrowStream(
	ctx context.Context,
	tableName string,
	schema *arrow.Schema,
	clientID, clientSecret string,
	opts ...StreamOption,
) (*ArrowStream, error) {
	authOpts := []auth.OAuthOption{auth.WithSharedTokenCache(s.tokenCache)}
	if s.httpClient != nil {
		authOpts = append(authOpts, auth.WithHTTPClient(s.httpClient))
	}
	provider, err := auth.NewOAuthHeadersProvider(
		clientID, clientSecret, s.zerobusEndpoint, s.ucEndpoint,
		authOpts...,
	)
	if err != nil {
		return nil, &Error{Op: "CreateArrowStream", cause: err, retryable: false}
	}
	return s.createArrowStream(
		ctx, "CreateArrowStream", tableName, schema, provider, opts...,
	)
}

// CreateArrowStreamWithProvider creates a Beta Arrow Flight stream with a
// custom HeadersProvider. The schema is copied and is not retained.
func (s *SDK) CreateArrowStreamWithProvider(
	ctx context.Context,
	tableName string,
	schema *arrow.Schema,
	provider HeadersProvider,
	opts ...StreamOption,
) (*ArrowStream, error) {
	if provider == nil {
		return nil, &Error{
			Op:        "CreateArrowStreamWithProvider",
			cause:     fmt.Errorf("headers provider is required"),
			retryable: false,
		}
	}
	return s.createArrowStream(
		ctx, "CreateArrowStreamWithProvider", tableName, schema, provider, opts...,
	)
}

// CreateArrowStreamFromIPC creates an OAuth-authenticated Beta Arrow Flight
// stream from a schema-only Arrow IPC stream. The input bytes are not retained.
func (s *SDK) CreateArrowStreamFromIPC(
	ctx context.Context,
	tableName string,
	schemaIPC []byte,
	clientID, clientSecret string,
	opts ...StreamOption,
) (*ArrowStream, error) {
	schema, err := arrowproto.DecodeSchemaIPC(schemaIPC)
	if err != nil {
		return nil, &Error{
			Op: "CreateArrowStreamFromIPC", cause: err, retryable: false,
		}
	}
	authOpts := []auth.OAuthOption{auth.WithSharedTokenCache(s.tokenCache)}
	if s.httpClient != nil {
		authOpts = append(authOpts, auth.WithHTTPClient(s.httpClient))
	}
	provider, err := auth.NewOAuthHeadersProvider(
		clientID, clientSecret, s.zerobusEndpoint, s.ucEndpoint,
		authOpts...,
	)
	if err != nil {
		return nil, &Error{
			Op: "CreateArrowStreamFromIPC", cause: err, retryable: false,
		}
	}
	return s.createArrowStream(
		ctx, "CreateArrowStreamFromIPC", tableName, schema, provider, opts...,
	)
}

// CreateArrowStreamFromIPCWithProvider creates a Beta Arrow Flight stream with
// a custom HeadersProvider from schema-only Arrow IPC. Input is not retained.
func (s *SDK) CreateArrowStreamFromIPCWithProvider(
	ctx context.Context,
	tableName string,
	schemaIPC []byte,
	provider HeadersProvider,
	opts ...StreamOption,
) (*ArrowStream, error) {
	schema, err := arrowproto.DecodeSchemaIPC(schemaIPC)
	if err != nil {
		return nil, &Error{
			Op: "CreateArrowStreamFromIPCWithProvider", cause: err, retryable: false,
		}
	}
	if provider == nil {
		return nil, &Error{
			Op:        "CreateArrowStreamFromIPCWithProvider",
			cause:     fmt.Errorf("headers provider is required"),
			retryable: false,
		}
	}
	return s.createArrowStream(
		ctx,
		"CreateArrowStreamFromIPCWithProvider",
		tableName,
		schema,
		provider,
		opts...,
	)
}

func (s *SDK) createArrowStream(
	ctx context.Context,
	op, tableName string,
	schema *arrow.Schema,
	provider HeadersProvider,
	opts ...StreamOption,
) (*ArrowStream, error) {
	sc := arrowStreamConfigFromOptions(opts)
	if strings.TrimSpace(tableName) == "" {
		return nil, &Error{
			Op: op, cause: fmt.Errorf("table name is required"), retryable: false,
		}
	}
	if schema == nil {
		return nil, &Error{
			Op: op, cause: fmt.Errorf("Arrow schema is required"), retryable: false,
		}
	}

	protocol, err := arrowproto.New(schema, arrowproto.Options{
		Compression: arrowProtocolCompression(sc.arrowCompression),
	})
	if err != nil {
		return nil, &Error{Op: op, cause: err, retryable: false}
	}
	params := stream.StreamParams{
		TableName:       tableName,
		HeadersProvider: provider,
	}
	openingCtx := context.WithoutCancel(ctx)
	if sc.waitReady {
		openingCtx = ctx
	}
	core, err := protocol.NewCoreStream(
		openingCtx, s.conn, params, sc.cfg, sc.callback,
	)
	if err != nil {
		return nil, &Error{Op: op, cause: err, retryable: false}
	}
	st := &ArrowStream{core: core, protocol: protocol, sdk: s}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = core.Terminate()
		return nil, &Error{Op: op, cause: errSDKClosed, retryable: false}
	}
	s.streams[st] = struct{}{}
	s.mu.Unlock()

	if sc.waitReady {
		if err := core.WaitReady(ctx); err != nil {
			_ = st.terminate()
			s.forget(st)
			return nil, wrapErr(op, err)
		}
	}
	return st, nil
}

func arrowStreamConfigFromOptions(opts []StreamOption) streamConfig {
	sc := streamConfigFromOptions(opts)
	if !sc.maxInflightSet {
		sc.cfg.MaxInflight = defaultArrowMaxInflight
	}
	if sc.arrowConnectionTimeout > 0 {
		sc.cfg.RecoveryTimeout = sc.arrowConnectionTimeout
	}
	// Arrow frames are independently row-chunked to a 2 MiB wire target.
	// MaxPayloadBytes is a proto/JSON request limit and must not reject the
	// canonical IPC payload retained behind those frames.
	sc.cfg.MaxPayloadBytes = math.MaxInt
	return sc
}

// IngestBatch queues one non-empty Arrow RecordBatch with the exact stream
// schema and returns its logical offset. It serializes before returning and
// retains no caller-owned Arrow objects, so the caller may Release the batch
// immediately, including after an error.
func (s *ArrowStream) IngestBatch(batch arrow.RecordBatch) (int64, error) {
	return s.IngestBatchContext(context.Background(), batch)
}

// IngestBatchContext is IngestBatch with caller context for backpressure.
func (s *ArrowStream) IngestBatchContext(
	ctx context.Context,
	batch arrow.RecordBatch,
) (int64, error) {
	if err := ctx.Err(); err != nil {
		return -1, wrapErr("IngestBatch", err)
	}
	estimate, err := s.protocol.EstimateRecordBatchRetainedSize(batch)
	if err != nil {
		return -1, &Error{Op: "IngestBatch", cause: err, retryable: false}
	}
	offset, err := s.core.EnqueuePayloadBuilder(
		ctx,
		estimate,
		func() (*arrowproto.Payload, uint64, int64, error) {
			payload, err := s.protocol.EncodeRecordBatch(batch)
			if err != nil {
				return nil, 0, 0, err
			}
			return payload, payload.UnitCount(), payload.RetainedSize(), nil
		},
	)
	return offset, wrapErr("IngestBatch", err)
}

// IngestIPCBatch queues one self-contained Arrow IPC stream containing exactly
// one non-empty RecordBatch with the exact stream schema and complete
// dictionaries. Input bytes are copied before return and may be reused.
func (s *ArrowStream) IngestIPCBatch(data []byte) (int64, error) {
	return s.IngestIPCBatchContext(context.Background(), data)
}

// IngestIPCBatchContext is IngestIPCBatch with caller context for backpressure.
func (s *ArrowStream) IngestIPCBatchContext(
	ctx context.Context,
	data []byte,
) (int64, error) {
	if err := ctx.Err(); err != nil {
		return -1, wrapErr("IngestIPCBatch", err)
	}
	estimate, err := s.protocol.EstimateIPCRetainedSize(data)
	if err != nil {
		return -1, &Error{Op: "IngestIPCBatch", cause: err, retryable: false}
	}
	offset, err := s.core.EnqueuePayloadBuilder(
		ctx,
		estimate,
		func() (*arrowproto.Payload, uint64, int64, error) {
			payload, err := s.protocol.EncodeIPC(data)
			if err != nil {
				return nil, 0, 0, err
			}
			return payload, payload.UnitCount(), payload.RetainedSize(), nil
		},
	)
	return offset, wrapErr("IngestIPCBatch", err)
}

// Flush waits until all batches queued before the call are fully acknowledged.
func (s *ArrowStream) Flush() error {
	return wrapErr("Flush", s.core.Flush(context.Background()))
}

// FlushContext is Flush with caller context.
func (s *ArrowStream) FlushContext(ctx context.Context) error {
	return wrapErr("Flush", s.core.Flush(ctx))
}

// WaitForOffset waits until the complete logical batch offset is acknowledged.
// A partial row acknowledgement does not complete the wait.
func (s *ArrowStream) WaitForOffset(offset int64) error {
	return wrapErr(
		"WaitForOffset",
		s.core.WaitForOffset(context.Background(), offset),
	)
}

// WaitForOffsetContext is WaitForOffset with caller context.
func (s *ArrowStream) WaitForOffsetContext(
	ctx context.Context,
	offset int64,
) error {
	return wrapErr("WaitForOffset", s.core.WaitForOffset(ctx, offset))
}

// GetUnackedBatches returns unacknowledged Arrow batches after stream close or
// terminal failure. A partially acknowledged batch is returned as its
// unacknowledged row suffix and can be replayed with IngestBatch on a fresh
// stream.
//
// The caller owns every returned RecordBatch reference and must call Release on
// each one, including when only inspecting the result.
func (s *ArrowStream) GetUnackedBatches() ([]arrow.RecordBatch, error) {
	ipcBatches, err := s.GetUnackedIPCBatches()
	if err != nil {
		return nil, err
	}
	batches := make([]arrow.RecordBatch, 0, len(ipcBatches))
	for _, data := range ipcBatches {
		batch, decodeErr := s.protocol.DecodeIPCRecordBatch(data)
		if decodeErr != nil {
			for _, owned := range batches {
				owned.Release()
			}
			return nil, &Error{
				Op: "GetUnackedBatches", cause: decodeErr, retryable: false,
			}
		}
		batches = append(batches, batch)
	}
	return batches, nil
}

// GetUnackedIPCBatches returns unacknowledged batches as independent,
// self-contained Arrow IPC byte slices after stream close or terminal failure.
// Each call returns fresh copies that remain valid after Close and may be
// mutated without affecting later calls. Replay them with IngestIPCBatch on a
// fresh stream.
func (s *ArrowStream) GetUnackedIPCBatches() ([][]byte, error) {
	groups, err := s.core.GetUnackedBatches()
	if err != nil {
		return nil, wrapErr("GetUnackedIPCBatches", err)
	}
	out := make([][]byte, 0, len(groups))
	for _, group := range groups {
		if len(group) != 1 {
			return nil, &Error{
				Op: "GetUnackedIPCBatches",
				cause: fmt.Errorf(
					"Arrow payload decoded to %d IPC batches, want 1",
					len(group),
				),
				retryable: false,
			}
		}
		// CoreStream.Decode already returns caller-owned bytes. Forward that
		// ownership directly instead of cloning the full IPC payload twice.
		out = append(out, group[0])
	}
	return out, nil
}

// Close flushes queued batches, tears down the stream, and deregisters it from
// the creating SDK. It is idempotent.
func (s *ArrowStream) Close() error {
	err := s.core.Close()
	if s.sdk != nil {
		s.sdk.forget(s)
	}
	return wrapErr("Close", err)
}

// terminate tears down the stream without a final flush (used by SDK.Close).
func (s *ArrowStream) terminate() error {
	return wrapErr("Close", s.core.Terminate())
}

// IsClosed reports whether the stream has closed or failed terminally.
func (s *ArrowStream) IsClosed() bool { return s.core.IsClosed() }

// ID returns the stable client-generated logical stream identifier.
func (s *ArrowStream) ID() string { return s.core.ID() }

// ServerID returns the identifier for the latest Flight DoPut connection.
func (s *ArrowStream) ServerID() string { return s.core.ServerID() }
