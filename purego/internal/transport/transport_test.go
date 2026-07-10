package transport_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/databricks/zerobus-sdk/purego/internal/transport"
	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
)

// observed captures what the fake server saw during the handshake, handed back
// over a channel so the test goroutine reads it without racing the handler.
type observed struct {
	tableName  string
	auth       string
	recordType zerobuspb.RecordType
	descriptor []byte
	// tableNameCount and authCount are how many values the server received for
	// the Zerobus-owned metadata keys, used to assert exactly one reaches it.
	tableNameCount int
	authCount      int
	// userMD is an unrelated caller-set metadata value, used to assert stream
	// setup preserves metadata it does not own.
	userMD string
}

const mdUserKey = "x-user-key"

// fakeServer is an in-memory ZerobusServer that completes the create-stream
// handshake with a fixed stream ID, then echoes each ingest record back as a
// durability ack.
type fakeServer struct {
	zerobuspb.UnimplementedZerobusServer
	streamID string
	seen     chan observed
	// badHandshake makes the server reply to the create request with an
	// unexpected message instead of a CreateStreamResponse.
	badHandshake bool
	// hangHandshake makes the server accept the create request but never reply,
	// blocking until the stream context is cancelled. Used to exercise the
	// handshake deadline.
	hangHandshake bool
	// hangDrain makes the server ignore the client's half-close and never end the
	// stream, so GracefulClose can't drain to io.EOF and must hit its ctx deadline.
	hangDrain bool
}

func (f *fakeServer) EphemeralStream(stream zerobuspb.Zerobus_EphemeralStreamServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	create := req.GetCreateStream()
	if create == nil {
		return io.ErrUnexpectedEOF
	}

	md, _ := metadata.FromIncomingContext(stream.Context())
	f.seen <- observed{
		tableName:      firstMD(md, "x-databricks-zerobus-table-name"),
		auth:           firstMD(md, "authorization"),
		recordType:     create.GetRecordType(),
		descriptor:     create.GetDescriptorProto(),
		tableNameCount: len(md.Get("x-databricks-zerobus-table-name")),
		authCount:      len(md.Get("authorization")),
		userMD:         firstMD(md, mdUserKey),
	}

	if f.hangHandshake {
		<-stream.Context().Done()
		return stream.Context().Err()
	}

	var resp *zerobuspb.EphemeralStreamResponse
	if f.badHandshake {
		resp = &zerobuspb.EphemeralStreamResponse{
			Payload: &zerobuspb.EphemeralStreamResponse_IngestRecordResponse{
				IngestRecordResponse: &zerobuspb.IngestRecordResponse{},
			},
		}
	} else {
		resp = &zerobuspb.EphemeralStreamResponse{
			Payload: &zerobuspb.EphemeralStreamResponse_CreateStreamResponse{
				CreateStreamResponse: &zerobuspb.CreateIngestStreamResponse{
					StreamId: proto.String(f.streamID),
				},
			},
		}
	}
	if err := stream.Send(resp); err != nil {
		return err
	}

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			if f.hangDrain {
				<-stream.Context().Done() // withhold the clean end
				return stream.Context().Err()
			}
			return nil
		}
		if err != nil {
			return err
		}
		ingest := req.GetIngestRecord()
		if ingest == nil {
			continue
		}
		if err := stream.Send(&zerobuspb.EphemeralStreamResponse{
			Payload: &zerobuspb.EphemeralStreamResponse_IngestRecordResponse{
				IngestRecordResponse: &zerobuspb.IngestRecordResponse{
					DurabilityAckUpToOffset: ingest.OffsetId,
				},
			},
		}); err != nil {
			return err
		}
	}
}

// firstMD returns the first metadata value for key, or "".
func firstMD(md metadata.MD, key string) string {
	if vs := md.Get(key); len(vs) > 0 {
		return vs[0]
	}
	return ""
}

// dialFake starts srv on an in-memory listener and returns a Conn wired to it.
// The server and connection are torn down via t.Cleanup.
func dialFake(t *testing.T, srv *fakeServer) *transport.Conn {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	gsrv := grpc.NewServer()
	zerobuspb.RegisterZerobusServer(gsrv, srv)
	go func() {
		if err := gsrv.Serve(lis); err != nil {
			t.Errorf("fake server stopped: %v", err)
		}
	}()
	t.Cleanup(gsrv.Stop)

	dialer := grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	})
	conn, err := transport.Dial("passthrough:///bufnet",
		transport.WithInsecure(),
		transport.WithGRPCDialOptions(dialer),
	)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestOpenHandshake(t *testing.T) {
	srv := &fakeServer{streamID: "stream-123", seen: make(chan observed, 1)}
	conn := dialFake(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := conn.Open(ctx, transport.StreamParams{
		TableName:       "main.sales.orders",
		RecordType:      zerobuspb.RecordType_PROTO,
		DescriptorProto: []byte("descriptor-bytes"),
		Token:           "tok-abc",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got, want := stream.ID(), "stream-123"; got != want {
		t.Errorf("stream ID = %q, want %q", got, want)
	}

	got := <-srv.seen
	if got.tableName != "main.sales.orders" {
		t.Errorf("server saw table name %q, want %q", got.tableName, "main.sales.orders")
	}
	if got.auth != "Bearer tok-abc" {
		t.Errorf("server saw authorization %q, want %q", got.auth, "Bearer tok-abc")
	}
	if got.recordType != zerobuspb.RecordType_PROTO {
		t.Errorf("server saw record type %v, want PROTO", got.recordType)
	}
	if string(got.descriptor) != "descriptor-bytes" {
		t.Errorf("server saw descriptor %q, want %q", got.descriptor, "descriptor-bytes")
	}
}

func TestOpenPreservesKnownAuthScheme(t *testing.T) {
	for _, tok := range []string{"Bearer tok", "basic dXNlcg==", "DPoP proof"} {
		srv := &fakeServer{streamID: "s", seen: make(chan observed, 1)}
		conn := dialFake(t, srv)

		_, err := conn.Open(context.Background(), transport.StreamParams{
			TableName:  "c.s.t",
			RecordType: zerobuspb.RecordType_JSON,
			Token:      tok,
		})
		if err != nil {
			t.Fatalf("Open %q: %v", tok, err)
		}
		if got := <-srv.seen; got.auth != tok {
			t.Errorf("token %q: server saw authorization %q, want it verbatim", tok, got.auth)
		}
	}
}

func TestOpenPrefixesUnknownScheme(t *testing.T) {
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1)}
	conn := dialFake(t, srv)

	// A value whose first word is not a known scheme is a bare token and gets
	// prefixed, rather than being sent unprefixed.
	_, err := conn.Open(context.Background(), transport.StreamParams{
		TableName:  "c.s.t",
		RecordType: zerobuspb.RecordType_JSON,
		Token:      "my token",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got := <-srv.seen; got.auth != "Bearer my token" {
		t.Errorf("server saw authorization %q, want %q", got.auth, "Bearer my token")
	}
}

func TestStreamSendRecv(t *testing.T) {
	srv := &fakeServer{streamID: "s1", seen: make(chan observed, 1)}
	conn := dialFake(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := conn.Open(ctx, transport.StreamParams{
		TableName:  "c.s.t",
		RecordType: zerobuspb.RecordType_JSON,
		Token:      "tok",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	<-srv.seen

	if err := stream.Send(&zerobuspb.EphemeralStreamRequest{
		Payload: &zerobuspb.EphemeralStreamRequest_IngestRecord{
			IngestRecord: &zerobuspb.IngestRecordRequest{
				OffsetId: proto.Int64(42),
				Record:   &zerobuspb.IngestRecordRequest_JsonRecord{JsonRecord: `{"id":1}`},
			},
		},
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if got := resp.GetIngestRecordResponse().GetDurabilityAckUpToOffset(); got != 42 {
		t.Errorf("durability ack offset = %d, want 42", got)
	}
}

func TestOpenProtoRequiresDescriptor(t *testing.T) {
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1)}
	conn := dialFake(t, srv)

	_, err := conn.Open(context.Background(), transport.StreamParams{
		TableName:  "c.s.t",
		RecordType: zerobuspb.RecordType_PROTO, // no descriptor
		Token:      "tok",
	})
	if err == nil {
		t.Fatal("Open with PROTO and no descriptor: got nil error, want failure")
	}
}

func TestOpenRequiresTableName(t *testing.T) {
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1)}
	conn := dialFake(t, srv)

	_, err := conn.Open(context.Background(), transport.StreamParams{
		TableName:  "  ",
		RecordType: zerobuspb.RecordType_JSON,
		Token:      "tok",
	})
	if err == nil {
		t.Fatal("Open with empty table name: got nil error, want failure")
	}
}

func TestOpenTrimsTableName(t *testing.T) {
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1)}
	conn := dialFake(t, srv)

	_, err := conn.Open(context.Background(), transport.StreamParams{
		TableName:  "  c.s.t  ",
		RecordType: zerobuspb.RecordType_JSON,
		Token:      "tok",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got := <-srv.seen; got.tableName != "c.s.t" {
		t.Errorf("server saw table name %q, want %q", got.tableName, "c.s.t")
	}
}

func TestOpenRejectsUnsupportedRecordType(t *testing.T) {
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1)}
	conn := dialFake(t, srv)

	_, err := conn.Open(context.Background(), transport.StreamParams{
		TableName:  "c.s.t",
		RecordType: zerobuspb.RecordType(999),
		Token:      "tok",
	})
	if err == nil {
		t.Fatal("Open with unsupported record type: got nil error, want failure")
	}
}

func TestOpenRejectsUnexpectedResponse(t *testing.T) {
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1), badHandshake: true}
	conn := dialFake(t, srv)

	_, err := conn.Open(context.Background(), transport.StreamParams{
		TableName:  "c.s.t",
		RecordType: zerobuspb.RecordType_JSON,
		Token:      "tok",
	})
	if err == nil {
		t.Fatal("Open with non-create first response: got nil error, want failure")
	}
}

func TestOpenOmitsEmptyToken(t *testing.T) {
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1)}
	conn := dialFake(t, srv)

	_, err := conn.Open(context.Background(), transport.StreamParams{
		TableName:  "c.s.t",
		RecordType: zerobuspb.RecordType_JSON, // no token set
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got := <-srv.seen; got.auth != "" {
		t.Errorf("server saw authorization %q, want it absent", got.auth)
	}
}

func TestStreamCloseAbortsRecv(t *testing.T) {
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1)}
	conn := dialFake(t, srv)

	stream, err := conn.Open(context.Background(), transport.StreamParams{
		TableName:  "c.s.t",
		RecordType: zerobuspb.RecordType_JSON,
		Token:      "tok",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	<-srv.seen

	stream.Close()
	stream.Close() // idempotent

	if _, err := stream.Recv(); err == nil {
		t.Fatal("Recv after Close: got nil error, want cancellation")
	}
}

// TestOpenReplacesInheritedMetadata verifies that when the caller's context
// already carries the Zerobus-owned keys, the server receives exactly one
// intended value for each rather than a duplicate that first-value-wins could
// mis-route — while unrelated caller metadata is preserved.
func TestOpenReplacesInheritedMetadata(t *testing.T) {
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1)}
	conn := dialFake(t, srv)

	ctx := metadata.AppendToOutgoingContext(context.Background(),
		"x-databricks-zerobus-table-name", "stale.table",
		"authorization", "Bearer stale-token",
		mdUserKey, "keep-me",
	)

	_, err := conn.Open(ctx, transport.StreamParams{
		TableName:  "c.s.t",
		RecordType: zerobuspb.RecordType_JSON,
		Token:      "fresh-token",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	got := <-srv.seen
	if got.tableNameCount != 1 || got.tableName != "c.s.t" {
		t.Errorf("table name: count=%d value=%q, want count=1 value=%q", got.tableNameCount, got.tableName, "c.s.t")
	}
	if got.authCount != 1 || got.auth != "Bearer fresh-token" {
		t.Errorf("authorization: count=%d value=%q, want count=1 value=%q", got.authCount, got.auth, "Bearer fresh-token")
	}
	if got.userMD != "keep-me" {
		t.Errorf("unrelated caller metadata = %q, want it preserved as %q", got.userMD, "keep-me")
	}
}

// TestOpenReplacesInheritedAuthWhenTokenEmpty verifies that a stale inherited
// authorization value is dropped, not forwarded, when no token is supplied.
func TestOpenReplacesInheritedAuthWhenTokenEmpty(t *testing.T) {
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1)}
	conn := dialFake(t, srv)

	ctx := metadata.AppendToOutgoingContext(context.Background(),
		"authorization", "Bearer stale-token")

	_, err := conn.Open(ctx, transport.StreamParams{
		TableName:  "c.s.t",
		RecordType: zerobuspb.RecordType_JSON, // no token
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got := <-srv.seen; got.authCount != 0 {
		t.Errorf("authorization count = %d (value %q), want the stale value dropped", got.authCount, got.auth)
	}
}

// TestOpenHonorsCallerDeadline verifies the handshake respects a caller-imposed
// deadline: a server that accepts the stream but never replies makes Open fail
// rather than hang.
func TestOpenHonorsCallerDeadline(t *testing.T) {
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1), hangHandshake: true}
	conn := dialFake(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := conn.Open(ctx, transport.StreamParams{
		TableName:  "c.s.t",
		RecordType: zerobuspb.RecordType_JSON,
		Token:      "tok",
	})
	if err == nil {
		t.Fatal("Open against a hanging handshake: got nil error, want deadline failure")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Open took %v, expected it to fail near the 200ms deadline", elapsed)
	}
}

// TestOpenAbortsOnCallerCancel verifies that cancelling ctx mid-open (server
// accepted the stream but withholds the readiness response) aborts Open promptly
// rather than waiting out the default handshake timeout.
func TestOpenAbortsOnCallerCancel(t *testing.T) {
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1), hangHandshake: true}
	conn := dialFake(t, srv)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { // cancel once Open is blocked on the readiness response
		<-srv.seen
		cancel()
	}()

	start := time.Now()
	_, err := conn.Open(ctx, transport.StreamParams{
		TableName:  "c.s.t",
		RecordType: zerobuspb.RecordType_JSON,
		Token:      "tok",
	})
	if err == nil {
		t.Fatal("Open with caller cancel mid-open: got nil error, want cancellation")
	}
	// Well under defaultHandshakeTimeout (30s): the cancel ended it, not the timeout.
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Open took %v, expected prompt abort on caller cancel", elapsed)
	}
}

// TestOpenDeadlineDoesNotTearDownStream verifies that a deadline used only to
// bound Open does not cancel the established stream: after a successful Open
// under a short-lived context, the stream stays usable past that deadline.
func TestOpenDeadlineDoesNotTearDownStream(t *testing.T) {
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1)}
	conn := dialFake(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	stream, err := conn.Open(ctx, transport.StreamParams{
		TableName:  "c.s.t",
		RecordType: zerobuspb.RecordType_JSON,
		Token:      "tok",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	<-srv.seen

	// Wait until the Open context has expired, then confirm the stream is still
	// live by exchanging a record.
	<-ctx.Done()

	if err := stream.Send(&zerobuspb.EphemeralStreamRequest{
		Payload: &zerobuspb.EphemeralStreamRequest_IngestRecord{
			IngestRecord: &zerobuspb.IngestRecordRequest{
				OffsetId: proto.Int64(7),
				Record:   &zerobuspb.IngestRecordRequest_JsonRecord{JsonRecord: `{"id":1}`},
			},
		},
	}); err != nil {
		t.Fatalf("Send after Open deadline: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("Recv after Open deadline: %v", err)
	}
}

// TestStreamGracefulCloseDefaultsDeadline: passing a context with no deadline
// (e.g. context.Background()) must not hang when the server stalls — the
// function applies defaultHandshakeTimeout internally, mirroring Open.
// Runs under -short but takes ~defaultHandshakeTimeout (30s) to complete.
func TestStreamGracefulCloseDefaultsDeadline(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: takes defaultHandshakeTimeout (30s) to exercise the no-deadline path")
	}
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1), hangDrain: true}
	conn := dialFake(t, srv)

	stream, err := conn.Open(context.Background(), transport.StreamParams{
		TableName:  "c.s.t",
		RecordType: zerobuspb.RecordType_JSON,
		Token:      "tok",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	<-srv.seen

	// No deadline on the context — the function must impose one itself and
	// return an error rather than hanging forever.
	err = stream.GracefulClose(context.Background())
	if err == nil {
		t.Fatal("GracefulClose against a stalled server with no deadline: got nil, want timeout error")
	}
}

// TestStreamGracefulClose: the server ends the stream on half-close, so
// GracefulClose drains to a clean io.EOF and returns nil.
func TestStreamGracefulClose(t *testing.T) {
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1)}
	conn := dialFake(t, srv)

	stream, err := conn.Open(context.Background(), transport.StreamParams{
		TableName:  "c.s.t",
		RecordType: zerobuspb.RecordType_JSON,
		Token:      "tok",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	<-srv.seen

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := stream.GracefulClose(ctx); err != nil {
		t.Fatalf("GracefulClose: %v", err)
	}
	stream.Close() // idempotent no-op after a graceful shutdown
}

// TestStreamGracefulCloseDrainsPending: an in-flight ack precedes io.EOF, so
// GracefulClose must discard it and keep draining rather than stop at the first
// response.
func TestStreamGracefulCloseDrainsPending(t *testing.T) {
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1)}
	conn := dialFake(t, srv)

	stream, err := conn.Open(context.Background(), transport.StreamParams{
		TableName:  "c.s.t",
		RecordType: zerobuspb.RecordType_JSON,
		Token:      "tok",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	<-srv.seen

	if err := stream.Send(&zerobuspb.EphemeralStreamRequest{
		Payload: &zerobuspb.EphemeralStreamRequest_IngestRecord{
			IngestRecord: &zerobuspb.IngestRecordRequest{
				OffsetId: proto.Int64(1),
				Record:   &zerobuspb.IngestRecordRequest_JsonRecord{JsonRecord: `{"id":1}`},
			},
		},
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := stream.GracefulClose(ctx); err != nil {
		t.Fatalf("GracefulClose with a pending response: %v", err)
	}
}

// TestStreamGracefulCloseHonorsDeadline: the server never ends the stream, so
// GracefulClose returns its ctx error promptly instead of blocking, and leaves
// the stream torn down.
func TestStreamGracefulCloseHonorsDeadline(t *testing.T) {
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1), hangDrain: true}
	conn := dialFake(t, srv)

	stream, err := conn.Open(context.Background(), transport.StreamParams{
		TableName:  "c.s.t",
		RecordType: zerobuspb.RecordType_JSON,
		Token:      "tok",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	<-srv.seen

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err = stream.GracefulClose(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("GracefulClose against a server that never ends the stream: got %v, want DeadlineExceeded", err)
	}
	// Bounded-out drain tears the stream down: Recv is unblocked, not hanging.
	if _, err := stream.Recv(); err == nil {
		t.Fatal("Recv after a deadline-bounded GracefulClose: got nil error, want cancellation")
	}
}
