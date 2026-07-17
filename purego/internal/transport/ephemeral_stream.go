package transport

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

// StreamParams describes the stream to open: which table to ingest into, how
// records are encoded, and which headers provider supplies its credentials.
type StreamParams struct {
	// TableName is the fully-qualified target table (catalog.schema.table).
	TableName string
	// RecordType selects the wire encoding of ingested records.
	RecordType zerobuspb.RecordType
	// DescriptorProto is the serialized message descriptor. Required when
	// RecordType is PROTO and ignored otherwise.
	DescriptorProto []byte
	// HeadersProvider supplies the auth (and any other) metadata headers for the
	// stream. See HeadersProvider for timeout behavior when GetHeaders may block
	// (e.g. token mint). When nil, the stream is opened without an auth header.
	HeadersProvider HeadersProvider
}

// Stream is an open ephemeral ingestion stream over the proto/JSON
// EphemeralStream RPC, already past the create-stream handshake. It is a thin
// pipe: higher layers construct requests and interpret responses.
//
// The send/receive plumbing, teardown, and handshake flow live in the embedded
// rawStream; this type adds EphemeralStream wire types and the two handshake
// hooks. As with the embedded rawStream, a Stream is not safe for concurrent
// Send and should use a single writer goroutine.
type Stream struct {
	rawStream[zerobuspb.EphemeralStreamRequest, zerobuspb.EphemeralStreamResponse]
}

// Open starts an EphemeralStream, runs the create-stream handshake, and returns
// the live stream once the server acknowledges it with a stream ID.
//
// ctx bounds opening the stream — GetHeaders (when a HeadersProvider is set),
// the RPC start, and the handshake. When ctx has no deadline, Open applies
// default bounds independently to GetHeaders and the handshake so a slow token
// mint can't consume the entire open budget.
// Cancelling ctx aborts an in-progress Open promptly, but once Open succeeds
// the live stream is detached and cancelled only by Close, so a later ctx
// timeout won't tear it down mid-ingest. ctx's values (including caller
// metadata) carry onto the live stream. The caller must Close the Stream.
func (c *Conn) Open(ctx context.Context, p StreamParams) (*Stream, error) {
	// Normalize once so the metadata header and the create request agree, and so
	// the validation below also governs the value actually sent.
	p.TableName = strings.TrimSpace(p.TableName)
	if p.TableName == "" {
		return nil, fmt.Errorf("transport: open: table name is required")
	}
	switch p.RecordType {
	case zerobuspb.RecordType_PROTO, zerobuspb.RecordType_JSON:
		// Supported.
	default:
		return nil, fmt.Errorf("transport: open %q: unsupported record type %v", p.TableName, p.RecordType)
	}
	if p.RecordType == zerobuspb.RecordType_PROTO && len(p.DescriptorProto) == 0 {
		return nil, fmt.Errorf("transport: open %q: descriptor proto required for PROTO records", p.TableName)
	}
	headersCtx := ctx
	useDefaultBudgets := false
	if _, ok := ctx.Deadline(); !ok {
		useDefaultBudgets = true
		var cancelHeaders context.CancelFunc
		headersCtx, cancelHeaders = context.WithTimeout(ctx, defaultHeadersTimeout)
		defer cancelHeaders()
	}

	headers, err := p.resolveHeaders(headersCtx)
	if err != nil {
		return nil, err
	}

	handshakeCtx := ctx
	if useDefaultBudgets {
		var cancelHandshake context.CancelFunc
		handshakeCtx, cancelHandshake = context.WithTimeout(ctx, defaultHandshakeTimeout)
		defer cancelHandshake()
	}

	// Detach the live stream from ctx's cancel/deadline (WithoutCancel keeps its
	// values, so caller metadata survives); Close is its only canceller.
	streamCtx := withStreamMetadataHeaders(context.WithoutCancel(ctx), p.TableName, headers)
	streamCtx, cancelStream := context.WithCancel(streamCtx)

	// Until the handshake succeeds, bridge handshakeCtx to cancelStream so a caller
	// cancel/timeout unblocks the in-progress RPC start, first Send, and readiness
	// wait. stopBridge removes it on success so the live stream is fully detached.
	stopBridge := context.AfterFunc(handshakeCtx, cancelStream)

	stream, err := c.open(handshakeCtx, streamCtx, cancelStream, p)
	if err != nil {
		if p.HeadersProvider != nil && isAuthRejection(err) {
			p.HeadersProvider.Invalidate(ctx, p.TableName)
		}
		// Deregister the bridge before returning so its AfterFunc doesn't linger
		// until handshakeCtx ends (which, on the default-budget path, is bounded,
		// but on a caller-supplied long-lived ctx would otherwise stay pinned).
		stopBridge()
		cancelStream()
		return nil, err
	}
	// If the bridge already fired, the stream is being torn down: fail rather than
	// return one racing cancellation.
	if !stopBridge() {
		cancelStream()
		return nil, fmt.Errorf("transport: open %q: %w", p.TableName, handshakeCtx.Err())
	}
	stream.cancel = cancelStream
	return stream, nil
}

// open starts the RPC on streamCtx and runs the handshake bounded by hctx.
// teardown cancels streamCtx so the handshake can reap its recv goroutine on
// cancel/timeout. It supplies the proto-specific hooks and annotates their errors.
func (c *Conn) open(hctx, streamCtx context.Context, teardown context.CancelFunc, p StreamParams) (*Stream, error) {
	rpc, err := c.client.EphemeralStream(streamCtx)
	if err != nil {
		return nil, fmt.Errorf("transport: open %q: %w", p.TableName, err)
	}

	s := &Stream{}
	s.rpc = rpc
	s.setID("ephemeral-stream") // placeholder label until the server assigns an ID
	if err := s.handshake(
		hctx,
		teardown,
		func(rpc bidiRPC[zerobuspb.EphemeralStreamRequest, zerobuspb.EphemeralStreamResponse]) error {
			return sendCreateStream(rpc, p)
		},
		confirmCreateStream,
	); err != nil {
		return nil, fmt.Errorf("transport: open %q: %w", p.TableName, err)
	}
	return s, nil
}

// sendCreateStream writes the create-stream request that opens a proto/JSON
// ingestion stream. It is the proto path's handshake setup hook.
func sendCreateStream(rpc bidiRPC[zerobuspb.EphemeralStreamRequest, zerobuspb.EphemeralStreamResponse], p StreamParams) error {
	create := &zerobuspb.CreateIngestStreamRequest{
		TableName:  proto.String(p.TableName),
		RecordType: p.RecordType.Enum(),
	}
	if p.RecordType == zerobuspb.RecordType_PROTO {
		create.DescriptorProto = p.DescriptorProto
	}
	req := &zerobuspb.EphemeralStreamRequest{
		Payload: &zerobuspb.EphemeralStreamRequest_CreateStream{CreateStream: create},
	}
	if err := rpc.Send(req); err != nil {
		return fmt.Errorf("send create-stream: %w", err)
	}
	return nil
}

// confirmCreateStream validates the create-stream response and returns the
// server-assigned stream ID. It is the proto path's handshake readiness hook.
func confirmCreateStream(resp *zerobuspb.EphemeralStreamResponse) (string, error) {
	created := resp.GetCreateStreamResponse()
	if created == nil {
		return "", fmt.Errorf("unexpected first response %T", resp.GetPayload())
	}
	id := created.GetStreamId()
	if id == "" {
		return "", fmt.Errorf("server returned an empty stream ID")
	}
	return id, nil
}

// ID returns the server-assigned stream identifier from the handshake.
func (s *Stream) ID() string { return s.name() }

// Send writes one request to the server. It is not safe for concurrent use.
func (s *Stream) Send(req *zerobuspb.EphemeralStreamRequest) error { return s.send(req) }

// Recv blocks for the next response. It returns io.EOF unwrapped once the
// server closes the stream cleanly, so callers can compare against it directly.
func (s *Stream) Recv() (*zerobuspb.EphemeralStreamResponse, error) { return s.recv() }

// CloseSend signals that no more requests will be sent, half-closing the stream
// while leaving Recv open to drain remaining responses. For a graceful shutdown,
// prefer GracefulClose, which performs the CloseSend/drain/Close sequence for you.
func (s *Stream) CloseSend() error { return s.closeSend() }

// GracefulClose half-closes the send side, drains remaining responses until the
// server ends the stream, then releases resources. Prefer it over Close when done
// sending: draining to end-of-stream lets the server see an orderly close rather
// than the abrupt reset Close produces.
//
// ctx bounds the drain; on expiry or a stream error it hard-aborts like Close and
// returns the cause, else nil. Must not be called concurrently with Recv, and no
// Send may follow (the send side is half-closed). A later Close is a no-op, since
// GracefulClose always releases resources before it returns.
func (s *Stream) GracefulClose(ctx context.Context) error { return s.gracefulClose(ctx) }

// Close aborts the stream and releases its resources. It is idempotent and safe
// to call after a graceful CloseSend/drain (or GracefulClose). Unlike GracefulClose
// it does not wait for the server: any in-flight Send or Recv is unblocked with a
// cancellation error.
func (s *Stream) Close() { s.close() }
