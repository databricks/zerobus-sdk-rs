package zerobus_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/databricks/zerobus-sdk/purego/internal/transport"
	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
	"github.com/databricks/zerobus-sdk/purego/zerobus"
)

func TestGRPCTarget(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		want     string
		wantErr  bool
	}{
		{name: "https URL", endpoint: "https://ws.zerobus.databricks.com", want: "ws.zerobus.databricks.com:443"},
		{name: "https URL with port", endpoint: "https://ws.zerobus.databricks.com:8443", want: "ws.zerobus.databricks.com:8443"},
		{name: "http URL", endpoint: "http://localhost:8080", want: "localhost:8080"},
		{name: "bare host", endpoint: "ws.zerobus.databricks.com", want: "ws.zerobus.databricks.com:443"},
		{name: "host:port", endpoint: "ws.zerobus.databricks.com:9000", want: "ws.zerobus.databricks.com:9000"},
		{name: "resolver target passthrough", endpoint: "passthrough:///bufnet", want: "passthrough:///bufnet"},
		{name: "dns target", endpoint: "dns:///ws.zerobus.databricks.com:443", want: "dns:///ws.zerobus.databricks.com:443"},
		{name: "trims whitespace", endpoint: "  https://ws.databricks.com  ", want: "ws.databricks.com:443"},
		{name: "empty", endpoint: "", wantErr: true},
		{name: "empty host", endpoint: "https://", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := zerobus.GRPCTarget(tc.endpoint)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("GRPCTarget(%q) = %q, want error", tc.endpoint, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("GRPCTarget(%q): %v", tc.endpoint, err)
			}
			if got != tc.want {
				t.Errorf("GRPCTarget(%q) = %q, want %q", tc.endpoint, got, tc.want)
			}
		})
	}
}

func TestResolveStreamConfigDefaults(t *testing.T) {
	rt, desc, maxInflight, recovery := zerobus.ResolveStreamConfig()
	if rt != int32(zerobuspb.RecordType_JSON) {
		t.Errorf("default record type = %d, want JSON(%d)", rt, zerobuspb.RecordType_JSON)
	}
	if desc != nil {
		t.Errorf("default descriptor = %v, want nil", desc)
	}
	if maxInflight != stream_DefaultMaxInflight {
		t.Errorf("default MaxInflight = %d, want %d", maxInflight, stream_DefaultMaxInflight)
	}
	if recovery != zerobus.RecoveryEnabled {
		t.Errorf("default recovery = %v, want RecoveryEnabled", recovery)
	}
}

// stream_DefaultMaxInflight mirrors internal/stream.DefaultMaxInflight so the
// test asserts the wired-through default without importing the internal const.
const stream_DefaultMaxInflight = 1_000_000

func TestResolveStreamConfigOptions(t *testing.T) {
	desc := []byte("descriptor-bytes")
	rt, gotDesc, maxInflight, recovery := zerobus.ResolveStreamConfig(
		zerobus.WithProto(desc),
		zerobus.WithMaxInflight(42),
		zerobus.WithRecovery(zerobus.RecoveryDisabled),
	)
	if rt != int32(zerobuspb.RecordType_PROTO) {
		t.Errorf("record type = %d, want PROTO(%d)", rt, zerobuspb.RecordType_PROTO)
	}
	if string(gotDesc) != string(desc) {
		t.Errorf("descriptor = %q, want %q", gotDesc, desc)
	}
	if maxInflight != 42 {
		t.Errorf("MaxInflight = %d, want 42", maxInflight)
	}
	if recovery != zerobus.RecoveryDisabled {
		t.Errorf("recovery = %v, want RecoveryDisabled", recovery)
	}
}

func TestWithJSONClearsDescriptor(t *testing.T) {
	// WithProto then WithJSON must drop the descriptor: a JSON stream carries none.
	rt, desc, _, _ := zerobus.ResolveStreamConfig(zerobus.WithProto([]byte("d")), zerobus.WithJSON())
	if rt != int32(zerobuspb.RecordType_JSON) {
		t.Errorf("record type = %d, want JSON", rt)
	}
	if desc != nil {
		t.Errorf("descriptor = %q, want nil after WithJSON", desc)
	}
}

func TestErrorRetryable(t *testing.T) {
	// A non-retryable underlying error stays non-retryable through the helper.
	if zerobus.Retryable(context.Canceled) {
		t.Error("context.Canceled classified retryable")
	}
	if zerobus.Retryable(nil) {
		t.Error("nil classified retryable")
	}
}

// --- integration test against an in-memory server ---

// echoServer completes the create-stream handshake with a fixed ID, then echoes
// each ingested record's offset back as a durability ack.
type echoServer struct {
	zerobuspb.UnimplementedZerobusServer
	streamID string
}

func (s *echoServer) EphemeralStream(stream zerobuspb.Zerobus_EphemeralStreamServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	if req.GetCreateStream() == nil {
		return io.ErrUnexpectedEOF
	}
	if err := stream.Send(&zerobuspb.EphemeralStreamResponse{
		Payload: &zerobuspb.EphemeralStreamResponse_CreateStreamResponse{
			CreateStreamResponse: &zerobuspb.CreateIngestStreamResponse{
				StreamId: proto.String(s.streamID),
			},
		},
	}); err != nil {
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
		// Ack single records and batches alike by echoing the wire offset the
		// sender stamped on the request.
		var offset *int64
		if ingest := req.GetIngestRecord(); ingest != nil {
			offset = ingest.OffsetId
		} else if batch := req.GetIngestRecordBatch(); batch != nil {
			offset = batch.OffsetId
		} else {
			continue
		}
		if err := stream.Send(&zerobuspb.EphemeralStreamResponse{
			Payload: &zerobuspb.EphemeralStreamResponse_IngestRecordResponse{
				IngestRecordResponse: &zerobuspb.IngestRecordResponse{
					DurabilityAckUpToOffset: offset,
				},
			},
		}); err != nil {
			return err
		}
	}
}

func dialEcho(t *testing.T, srv *echoServer) *transport.Conn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gsrv := grpc.NewServer()
	zerobuspb.RegisterZerobusServer(gsrv, srv)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := gsrv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("fake server stopped: %v", err)
		}
	}()
	t.Cleanup(func() {
		gsrv.Stop()
		<-done
	})
	dialer := grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	})
	// bufconn carries no TLS; auth rides plaintext metadata, which is fine for an
	// in-memory test server. Insecure creds are injected via the pass-through gRPC
	// dial options rather than the transport's test-only WithInsecure hook.
	conn, err := transport.Dial("passthrough:///bufnet",
		transport.WithGRPCDialOptions(dialer, grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestStreamIngestFlushClose(t *testing.T) {
	conn := dialEcho(t, &echoServer{streamID: "stream-xyz"})
	sdk := zerobus.NewWithConn(conn, "https://ws.zerobus.databricks.com", "https://ws.databricks.com")

	provider := zerobus.NewStaticHeadersProvider(map[string]string{
		"authorization": "Bearer test-token",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := sdk.CreateStreamWithProvider(ctx, "main.sales.orders", provider, zerobus.WithJSON())
	if err != nil {
		t.Fatalf("CreateStreamWithProvider: %v", err)
	}
	defer stream.Close()

	if stream.ID() == "" {
		t.Error("stream ID is empty")
	}

	var last int64
	for i := 0; i < 100; i++ {
		off, err := stream.IngestRecordOffset([]byte(`{"id":1}`))
		if err != nil {
			t.Fatalf("IngestRecordOffset[%d]: %v", i, err)
		}
		last = off
	}
	if last != 99 {
		t.Errorf("last offset = %d, want 99", last)
	}

	if err := stream.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := stream.WaitForOffset(last); err != nil {
		t.Fatalf("WaitForOffset(%d): %v", last, err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !stream.IsClosed() {
		t.Error("IsClosed = false after Close")
	}
}

func TestStreamBatchIngest(t *testing.T) {
	conn := dialEcho(t, &echoServer{streamID: "stream-batch"})
	sdk := zerobus.NewWithConn(conn, "https://ws.zerobus.databricks.com", "https://ws.databricks.com")
	provider := zerobus.NewStaticHeadersProvider(map[string]string{"authorization": "Bearer t"})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := sdk.CreateStreamWithProvider(ctx, "main.sales.orders", provider)
	if err != nil {
		t.Fatalf("CreateStreamWithProvider: %v", err)
	}
	defer stream.Close()

	// Empty batch is a no-op returning -1.
	if off, err := stream.IngestRecordsOffset(nil); err != nil || off != -1 {
		t.Fatalf("empty IngestRecordsOffset = (%d, %v), want (-1, nil)", off, err)
	}

	off, err := stream.IngestRecordsOffset([][]byte{[]byte(`{"a":1}`), []byte(`{"b":2}`)})
	if err != nil {
		t.Fatalf("IngestRecordsOffset: %v", err)
	}
	if off != 0 {
		t.Errorf("batch offset = %d, want 0", off)
	}
	if err := stream.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}
