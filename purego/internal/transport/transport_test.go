package transport_test

import (
	"context"
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
}

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
		tableName:  firstMD(md, "x-databricks-zerobus-table-name"),
		auth:       firstMD(md, "authorization"),
		recordType: create.GetRecordType(),
		descriptor: create.GetDescriptorProto(),
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

func TestOpenPreservesExplicitAuthScheme(t *testing.T) {
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1)}
	conn := dialFake(t, srv)

	_, err := conn.Open(context.Background(), transport.StreamParams{
		TableName:  "c.s.t",
		RecordType: zerobuspb.RecordType_JSON,
		Token:      "Custom tok",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got := <-srv.seen; got.auth != "Custom tok" {
		t.Errorf("server saw authorization %q, want %q", got.auth, "Custom tok")
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
