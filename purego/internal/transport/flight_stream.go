package transport

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/flight"
)

// FlightStreamParams describes an Arrow Flight DoPut stream setup.
type FlightStreamParams struct {
	// TableName is the fully-qualified target table (catalog.schema.table).
	TableName string
	// Schema is the schema FlightData sent as the first DoPut request. It must
	// contain an IPC schema message in DataHeader and no body or app metadata.
	Schema *flight.FlightData
	// HeadersProvider supplies authentication and other gRPC metadata.
	HeadersProvider HeadersProvider
}

// FlightStream is an open Arrow Flight DoPut stream, already past the
// schema/ready handshake. It is not safe for concurrent Send calls; one sender
// may run concurrently with one receiver.
type FlightStream struct {
	rawStream[flight.FlightData, flight.PutResult]
}

// OpenFlight starts a Flight DoPut RPC, sends the schema as its first request,
// and waits for the server's ready sentinel before returning.
//
// Header resolution, open-time budgets, context detachment, and table metadata
// follow Open exactly. The live stream remains valid after ctx expires and is
// released by Close or GracefulClose.
func (c *Conn) OpenFlight(ctx context.Context, p FlightStreamParams) (*FlightStream, error) {
	p.TableName = strings.TrimSpace(p.TableName)
	if p.TableName == "" {
		return nil, fmt.Errorf("transport: open Flight stream: table name is required: %w", ErrInvalidParams)
	}
	if p.Schema == nil {
		return nil, fmt.Errorf("transport: open Flight stream %q: schema FlightData is required: %w", p.TableName, ErrInvalidParams)
	}
	if len(p.Schema.GetDataHeader()) == 0 {
		return nil, fmt.Errorf("transport: open Flight stream %q: schema DataHeader is required: %w", p.TableName, ErrInvalidParams)
	}
	if len(p.Schema.GetDataBody()) != 0 {
		return nil, fmt.Errorf("transport: open Flight stream %q: schema DataBody must be empty: %w", p.TableName, ErrInvalidParams)
	}
	if len(p.Schema.GetAppMetadata()) != 0 {
		return nil, fmt.Errorf("transport: open Flight stream %q: schema AppMetadata must be empty: %w", p.TableName, ErrInvalidParams)
	}

	headersCtx := ctx
	useDefaultBudgets := false
	if _, ok := ctx.Deadline(); !ok {
		useDefaultBudgets = true
		var cancelHeaders context.CancelFunc
		headersCtx, cancelHeaders = context.WithTimeoutCause(
			ctx,
			defaultHeadersTimeout,
			errHeadersBudgetExceeded,
		)
		defer cancelHeaders()
	}
	headers, err := resolveHeaders(headersCtx, p.TableName, p.HeadersProvider)
	if err != nil {
		return nil, err
	}

	handshakeCtx := ctx
	if useDefaultBudgets {
		var cancelHandshake context.CancelFunc
		handshakeCtx, cancelHandshake = context.WithTimeout(ctx, defaultHandshakeTimeout)
		defer cancelHandshake()
	}

	streamCtx := withStreamMetadataHeaders(context.WithoutCancel(ctx), p.TableName, headers)
	streamCtx, cancelStream := context.WithCancel(streamCtx)
	stopBridge := context.AfterFunc(handshakeCtx, cancelStream)

	stream, err := c.openFlight(handshakeCtx, streamCtx, cancelStream, p)
	if err != nil {
		stopBridge()
		cancelStream()
		return nil, err
	}
	if !stopBridge() {
		cancelStream()
		return nil, fmt.Errorf("transport: open Flight stream %q: %w", p.TableName, handshakeCtx.Err())
	}
	stream.cancel = cancelStream
	return stream, nil
}

func (c *Conn) openFlight(
	handshakeCtx context.Context,
	streamCtx context.Context,
	teardown context.CancelFunc,
	p FlightStreamParams,
) (*FlightStream, error) {
	rpc, err := c.flightClient.DoPut(streamCtx)
	if err != nil {
		return nil, fmt.Errorf("transport: open Flight stream %q: %w", p.TableName, err)
	}

	stream := &FlightStream{}
	stream.rpc = rpc
	stream.setID("flight-do-put")
	if err := stream.handshake(
		handshakeCtx,
		teardown,
		func(rpc bidiRPC[flight.FlightData, flight.PutResult]) error {
			if err := rpc.Send(p.Schema); err != nil {
				return fmt.Errorf("send Flight schema: %w", err)
			}
			return nil
		},
		confirmFlightReady,
	); err != nil {
		// Match Open: the lifecycle layer normally owns invalidation, except when
		// a deadline raced a preserved auth rejection during the handshake.
		if p.HeadersProvider != nil && IsAuthRejection(err) && handshakeCtx.Err() != nil {
			p.HeadersProvider.Invalidate(handshakeCtx, p.TableName)
		}
		return nil, fmt.Errorf("transport: open Flight stream %q: %w", p.TableName, err)
	}
	return stream, nil
}

func confirmFlightReady(result *flight.PutResult) (string, error) {
	if result == nil {
		return "", fmt.Errorf("malformed Flight ready response: nil PutResult")
	}
	metadata, err := ParseFlightAckMetadata(result.GetAppMetadata())
	if err != nil {
		return "", fmt.Errorf("malformed Flight ready response metadata: %w", err)
	}
	if !metadata.IsStreamReady() {
		return "", fmt.Errorf(
			"unexpected Flight ready response offset %d, want %d",
			metadata.AckUpToOffset,
			FlightStreamReadyOffset,
		)
	}
	if metadata.AckUpToRecords != 0 {
		return "", fmt.Errorf(
			"malformed Flight ready response: acknowledged records %d, want 0",
			metadata.AckUpToRecords,
		)
	}
	if metadata.CloseStreamDurationMS != nil {
		return "", fmt.Errorf(
			"malformed Flight ready response: close signal is not valid during setup",
		)
	}
	return "", nil
}

// ServerID returns the stable protocol label used for this DoPut connection.
// Arrow Flight's ready response does not assign a server stream ID.
func (s *FlightStream) ServerID() string { return s.name() }

// Send writes one FlightData request. It is not safe for concurrent use.
func (s *FlightStream) Send(req *flight.FlightData) error { return s.send(req) }

// Recv blocks for the next PutResult and returns io.EOF unwrapped when the
// server closes the response stream cleanly.
func (s *FlightStream) Recv() (*flight.PutResult, error) { return s.recv() }

// CloseSend half-closes requests while leaving responses open to drain.
func (s *FlightStream) CloseSend() error { return s.closeSend() }

// GracefulClose half-closes, drains responses to io.EOF, and releases the
// stream. It must not run concurrently with Recv.
func (s *FlightStream) GracefulClose(ctx context.Context) error {
	return s.gracefulClose(ctx)
}

// Close aborts the stream and releases its resources. It is idempotent.
func (s *FlightStream) Close() { s.close() }
