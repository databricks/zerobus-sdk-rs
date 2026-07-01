package transport

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

// StreamParams describes the stream to open: which table to ingest into, how
// records are encoded, and which auth credential to send.
type StreamParams struct {
	// TableName is the fully-qualified target table (catalog.schema.table).
	TableName string
	// RecordType selects the wire encoding of ingested records.
	RecordType zerobuspb.RecordType
	// DescriptorProto is the serialized message descriptor. Required when
	// RecordType is PROTO and ignored otherwise.
	DescriptorProto []byte
	// Token is the credential sent in the authorization header. A bare token is
	// prefixed with "Bearer "; a value that already carries a scheme (e.g.
	// "Bearer ..." or "Basic ...") is sent verbatim. Empty means no header.
	Token string
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

// Open starts an EphemeralStream, performs the create-stream handshake, and
// returns the live stream once the server has acknowledged it with a stream ID.
//
// The returned Stream owns a child of ctx, so cancelling ctx tears the stream
// down; if the handshake itself fails, Open releases that child before
// returning. The caller must Close the returned Stream.
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

	ctx, cancel := streamContext(ctx, p.TableName, p.Token)
	stream, err := c.open(ctx, p)
	if err != nil {
		cancel()
		return nil, err
	}
	stream.cancel = cancel
	return stream, nil
}

// open opens the RPC and runs the handshake on an already-prepared context. On
// any error the caller (Open) cancels the context. The generic send/await/
// validate flow lives in rawStream.handshake; open supplies only the two
// proto-specific hooks and annotates their concise errors with the operation.
func (c *Conn) open(ctx context.Context, p StreamParams) (*Stream, error) {
	rpc, err := c.client.EphemeralStream(ctx)
	if err != nil {
		return nil, fmt.Errorf("transport: open %q: %w", p.TableName, err)
	}

	s := &Stream{}
	s.rpc = rpc
	s.name = "ephemeral-stream"
	if err := s.handshake(
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
func (s *Stream) ID() string { return s.name }

// Send writes one request to the server. It is not safe for concurrent use.
func (s *Stream) Send(req *zerobuspb.EphemeralStreamRequest) error { return s.send(req) }

// Recv blocks for the next response. It returns io.EOF unwrapped once the
// server closes the stream cleanly, so callers can compare against it directly.
func (s *Stream) Recv() (*zerobuspb.EphemeralStreamResponse, error) { return s.recv() }

// CloseSend signals that no more requests will be sent, half-closing the stream
// while leaving Recv open to drain remaining responses. For a graceful shutdown,
// call CloseSend, read until io.EOF, then Close.
func (s *Stream) CloseSend() error { return s.closeSend() }

// Close aborts the stream and releases its resources. It is idempotent and safe
// to call after a graceful CloseSend/drain. Unlike CloseSend it does not wait
// for the server: any in-flight Send or Recv is unblocked with a cancellation
// error.
func (s *Stream) Close() { s.close() }
