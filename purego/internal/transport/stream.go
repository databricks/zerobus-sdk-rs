package transport

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

// gRPC metadata keys the Zerobus service expects on the EphemeralStream RPC.
const (
	mdTableName     = "x-databricks-zerobus-table-name"
	mdAuthorization = "authorization"
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

// Stream is an open ephemeral ingestion stream, already past the create-stream
// handshake. It exposes send/receive operations over the bidirectional RPC.
// Higher layers construct requests and interpret responses. A Stream is not
// safe for concurrent Send and should use a single writer goroutine.
type Stream struct {
	rpc    grpc.BidiStreamingClient[zerobuspb.EphemeralStreamRequest, zerobuspb.EphemeralStreamResponse]
	id     string
	cancel context.CancelFunc
	once   sync.Once
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

	md := []string{mdTableName, p.TableName}
	if auth := authHeaderValue(p.Token); auth != "" {
		md = append(md, mdAuthorization, auth)
	}

	ctx, cancel := context.WithCancel(metadata.AppendToOutgoingContext(ctx, md...))

	stream, err := c.open(ctx, p)
	if err != nil {
		cancel()
		return nil, err
	}
	stream.cancel = cancel
	return stream, nil
}

// open runs the handshake on an already-prepared context. On any error the
// caller (Open) cancels the context.
func (c *Conn) open(ctx context.Context, p StreamParams) (*Stream, error) {
	rpc, err := c.client.EphemeralStream(ctx)
	if err != nil {
		return nil, fmt.Errorf("transport: open %q: %w", p.TableName, err)
	}

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
		return nil, fmt.Errorf("transport: open %q: send create-stream: %w", p.TableName, err)
	}

	resp, err := rpc.Recv()
	if err != nil {
		return nil, fmt.Errorf("transport: open %q: await create-stream response: %w", p.TableName, err)
	}
	created := resp.GetCreateStreamResponse()
	if created == nil {
		return nil, fmt.Errorf("transport: open %q: unexpected first response %T", p.TableName, resp.GetPayload())
	}
	id := created.GetStreamId()
	if id == "" {
		return nil, fmt.Errorf("transport: open %q: server returned an empty stream ID", p.TableName)
	}

	return &Stream{rpc: rpc, id: id}, nil
}

// ID returns the server-assigned stream identifier from the handshake.
func (s *Stream) ID() string { return s.id }

// Send writes one request to the server. It is not safe for concurrent use.
func (s *Stream) Send(req *zerobuspb.EphemeralStreamRequest) error {
	if err := s.rpc.Send(req); err != nil {
		return fmt.Errorf("transport: send on stream %s: %w", s.id, err)
	}
	return nil
}

// Recv blocks for the next response. It returns io.EOF unwrapped once the
// server closes the stream cleanly, so callers can compare against it directly.
func (s *Stream) Recv() (*zerobuspb.EphemeralStreamResponse, error) {
	resp, err := s.rpc.Recv()
	switch {
	case err == io.EOF:
		return nil, io.EOF
	case err != nil:
		return nil, fmt.Errorf("transport: recv on stream %s: %w", s.id, err)
	}
	return resp, nil
}

// CloseSend signals that no more requests will be sent, half-closing the stream
// while leaving Recv open to drain remaining responses. For a graceful shutdown,
// call CloseSend, read until io.EOF, then Close.
func (s *Stream) CloseSend() error {
	if err := s.rpc.CloseSend(); err != nil {
		return fmt.Errorf("transport: close-send on stream %s: %w", s.id, err)
	}
	return nil
}

// Close aborts the stream and releases its resources. It is idempotent and safe
// to call after a graceful CloseSend/drain. Unlike CloseSend it does not wait
// for the server: any in-flight Send or Recv is unblocked with a cancellation
// error.
func (s *Stream) Close() {
	s.once.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
	})
}

func authHeaderValue(token string) string {
	token = strings.TrimSpace(token)
	if token == "" {
		return ""
	}
	// If the caller already supplied a scheme (for example "Bearer ..."), keep it.
	if strings.Contains(token, " ") {
		return token
	}
	return "Bearer " + token
}
