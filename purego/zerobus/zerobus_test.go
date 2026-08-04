package zerobus_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

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
		{name: "http URL rejected", endpoint: "http://localhost:8080", wantErr: true},
		{name: "bare host", endpoint: "ws.zerobus.databricks.com", want: "ws.zerobus.databricks.com:443"},
		{name: "host:port", endpoint: "ws.zerobus.databricks.com:9000", want: "ws.zerobus.databricks.com:9000"},
		{name: "IPv6 URL", endpoint: "https://[::1]", want: "[::1]:443"},
		{name: "IPv6 URL with port", endpoint: "https://[::1]:8443", want: "[::1]:8443"},
		{name: "bare IPv6", endpoint: "::1", want: "[::1]:443"},
		{name: "bracketed IPv6 with port", endpoint: "[::1]:8443", want: "[::1]:8443"},
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
	rt, desc, maxInflight, recovery, waitReady := zerobus.ResolveStreamConfig()
	if rt != int32(zerobuspb.RecordType_PROTO) {
		t.Errorf("default record type = %d, want PROTO(%d)", rt, zerobuspb.RecordType_PROTO)
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
	if waitReady {
		t.Error("default waitReady = true, want false")
	}
}

// stream_DefaultMaxInflight mirrors internal/stream.DefaultMaxInflight so the
// test asserts the wired-through default without importing the internal const.
const stream_DefaultMaxInflight = 1_000_000

func TestResolveStreamConfigOptions(t *testing.T) {
	desc := []byte("descriptor-bytes")
	rt, gotDesc, maxInflight, recovery, waitReady := zerobus.ResolveStreamConfig(
		zerobus.WithProto(desc),
		zerobus.WithMaxInflight(42),
		zerobus.WithRecovery(zerobus.RecoveryDisabled),
		zerobus.WithWaitForReady(),
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
	if !waitReady {
		t.Error("waitReady = false, want true")
	}
}

func TestWithJSONClearsDescriptor(t *testing.T) {
	// WithProto then WithJSON must drop the descriptor: a JSON stream carries none.
	rt, desc, _, _, _ := zerobus.ResolveStreamConfig(zerobus.WithProto([]byte("d")), zerobus.WithJSON())
	if rt != int32(zerobuspb.RecordType_JSON) {
		t.Errorf("record type = %d, want JSON", rt)
	}
	if desc != nil {
		t.Errorf("descriptor = %q, want nil after WithJSON", desc)
	}
}

func TestResolveStreamTuningOptions(t *testing.T) {
	pauseWait := 3 * time.Second
	recoveryTimeout, recoveryBackoff, lackOfAckTimeout, maxBatchRecords, gotPauseWait :=
		zerobus.ResolveStreamTuning(
			zerobus.WithRecoveryTimeout(11*time.Second),
			zerobus.WithRecoveryBackoff(12*time.Second),
			zerobus.WithLackOfAckTimeout(13*time.Second),
			zerobus.WithMaxBatchRecords(14),
			zerobus.WithStreamPausedMaxWait(pauseWait),
		)
	if recoveryTimeout != 11*time.Second {
		t.Errorf("RecoveryTimeout = %v, want 11s", recoveryTimeout)
	}
	if recoveryBackoff != 12*time.Second {
		t.Errorf("RecoveryBackoff = %v, want 12s", recoveryBackoff)
	}
	if lackOfAckTimeout != 13*time.Second {
		t.Errorf("LackOfAckTimeout = %v, want 13s", lackOfAckTimeout)
	}
	if maxBatchRecords != 14 {
		t.Errorf("MaxBatchRecords = %d, want 14", maxBatchRecords)
	}
	if gotPauseWait == nil || *gotPauseWait != pauseWait {
		t.Errorf("StreamPausedMaxWait = %v, want %v", gotPauseWait, pauseWait)
	}

	_, _, _, _, zeroPauseWait := zerobus.ResolveStreamTuning(zerobus.WithStreamPausedMaxWait(0))
	if zeroPauseWait == nil || *zeroPauseWait != 0 {
		t.Errorf("explicit zero StreamPausedMaxWait = %v, want pointer to zero", zeroPauseWait)
	}
}

func TestNewRejectsInvalidApplicationName(t *testing.T) {
	tests := []struct {
		name string
		app  string
	}{
		{name: "invalid UTF-8", app: string([]byte{0xff})},
		{name: "NUL", app: "my-app\x00suffix"},
		{name: "newline", app: "my-app\nsuffix"},
		{name: "DEL", app: "my-app\x7f"},
		{name: "non-ASCII", app: "my-app-é"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sdk, err := zerobus.New(
				"https://ws.zerobus.databricks.com",
				"https://ws.databricks.com",
				zerobus.WithApplicationName(tc.app),
			)
			if err == nil {
				_ = sdk.Close()
				t.Fatal("New succeeded, want invalid application-name error")
			}
			if zerobus.Retryable(err) {
				t.Errorf("error %v is retryable, want permanent", err)
			}
		})
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
	noAcks   bool
	noReady  bool
	creates  chan *zerobuspb.CreateIngestStreamRequest
	ingests  chan *zerobuspb.EphemeralStreamRequest
}

func (s *echoServer) EphemeralStream(stream zerobuspb.Zerobus_EphemeralStreamServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	if req.GetCreateStream() == nil {
		return io.ErrUnexpectedEOF
	}
	if s.creates != nil {
		s.creates <- proto.Clone(req.GetCreateStream()).(*zerobuspb.CreateIngestStreamRequest)
	}
	if s.noReady {
		<-stream.Context().Done()
		return stream.Context().Err()
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
		if s.noAcks {
			continue
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
		if s.ingests != nil {
			s.ingests <- proto.Clone(req).(*zerobuspb.EphemeralStreamRequest)
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
	stream, err := sdk.CreateStreamWithProvider(ctx, "main.sales.orders", provider, zerobus.WithJSON())
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

func TestCreateProtoStreamAcceptsFileDescriptorProto(t *testing.T) {
	fileDescriptor, err := proto.Marshal(&descriptorpb.FileDescriptorProto{
		Name:   proto.String("orders.proto"),
		Syntax: proto.String("proto2"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("Order"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:   proto.String("id"),
						Number: proto.Int32(1),
						Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal FileDescriptorProto: %v", err)
	}

	conn := dialEcho(t, &echoServer{streamID: "stream-file-descriptor"})
	sdk := zerobus.NewWithConn(conn, "https://ws.zerobus.databricks.com", "https://ws.databricks.com")
	defer sdk.Close()
	stream, err := sdk.CreateStreamWithProvider(
		context.Background(),
		"main.sales.orders",
		zerobus.NewStaticHeadersProvider(map[string]string{"authorization": "Bearer token"}),
		zerobus.WithProto(fileDescriptor),
		zerobus.WithWaitForReady(),
	)
	if err != nil {
		t.Fatalf("CreateStreamWithProvider: %v", err)
	}
	defer stream.Close()

	if _, err := stream.IngestRecordOffset([]byte{0x08, 0x01}); err != nil {
		t.Fatalf("IngestRecordOffset: %v", err)
	}
	if err := stream.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

// ucTokenServer is a stand-in for the Unity Catalog OIDC token endpoint that
// counts how many client-credentials mints it served.
type ucTokenServer struct {
	mints atomic.Int64
}

func (s *ucTokenServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/oidc/v1/token" {
		http.NotFound(w, r)
		return
	}
	s.mints.Add(1)
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, `{"access_token":"minted-token","expires_in":3600}`)
}

func TestCreateStreamSharesTokenCacheAcrossStreams(t *testing.T) {
	uc := &ucTokenServer{}
	ucSrv := httptest.NewServer(uc)
	defer ucSrv.Close()

	conn := dialEcho(t, &echoServer{streamID: "stream-oauth"})
	sdk := zerobus.NewWithConn(conn, "https://ws.zerobus.databricks.com", ucSrv.URL)
	defer sdk.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Two streams on the same table with the same credentials must reuse one
	// minted token rather than each provider caching its own.
	for i := 0; i < 2; i++ {
		st, err := sdk.CreateStream(ctx, "main.sales.orders", "client-id", "client-secret", zerobus.WithJSON())
		if err != nil {
			t.Fatalf("CreateStream[%d]: %v", i, err)
		}
		if _, err := st.IngestRecordOffset([]byte(`{"id":1}`)); err != nil {
			t.Fatalf("IngestRecordOffset[%d]: %v", i, err)
		}
		// Flushing forces the background open — and therefore the token mint —
		// to have completed before the next stream is created.
		if err := st.Flush(); err != nil {
			t.Fatalf("Flush[%d]: %v", i, err)
		}
		if err := st.Close(); err != nil {
			t.Fatalf("Close[%d]: %v", i, err)
		}
	}
	if got := uc.mints.Load(); got != 1 {
		t.Errorf("token mints = %d, want 1 (cache shared across streams)", got)
	}
}

func TestCreateStreamRejectsInvalidArguments(t *testing.T) {
	conn := dialEcho(t, &echoServer{streamID: "stream-invalid"})
	sdk := zerobus.NewWithConn(conn, "https://ws.zerobus.databricks.com", "https://ws.databricks.com")
	defer sdk.Close()
	provider := zerobus.NewStaticHeadersProvider(map[string]string{"authorization": "Bearer t"})

	tests := []struct {
		name  string
		table string
		opts  []zerobus.StreamOption
	}{
		{name: "empty table name", table: ""},
		{name: "blank table name", table: "   "},
		{name: "default proto without descriptor", table: "main.sales.orders"},
		{
			name:  "proto without descriptor",
			table: "main.sales.orders",
			opts:  []zerobus.StreamOption{zerobus.WithProto(nil)},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st, err := sdk.CreateStreamWithProvider(context.Background(), tc.table, provider, tc.opts...)
			if err == nil {
				_ = st.Close()
				t.Fatal("CreateStreamWithProvider succeeded, want a validation error")
			}
			if zerobus.Retryable(err) {
				t.Errorf("error %v is retryable, want permanent", err)
			}
		})
	}
	if n := sdk.OpenStreamCount(); n != 0 {
		t.Errorf("SDK tracks %d streams, want 0 after rejected creates", n)
	}
}

func TestFlushReportsTerminalFailureWithNoRecords(t *testing.T) {
	conn := dialEcho(t, &echoServer{streamID: "stream-terminal"})
	sdk := zerobus.NewWithConn(conn, "https://ws.zerobus.databricks.com", "https://ws.databricks.com")
	defer sdk.Close()

	// An empty static provider fails every open, and disabling recovery makes the
	// stream terminal after the first attempt instead of retrying.
	st, err := sdk.CreateStreamWithProvider(context.Background(), "main.sales.orders",
		zerobus.NewStaticHeadersProvider(nil), zerobus.WithJSON(),
		zerobus.WithRecovery(zerobus.RecoveryDisabled))
	if err != nil {
		t.Fatalf("CreateStreamWithProvider: %v", err)
	}
	defer st.Close()

	waitFor(t, st.IsClosed, "stream to fail terminally")

	// Nothing was ingested, so there is no watermark to wait for — but Flush is
	// where a failed background open is documented to surface, so it must not
	// report success.
	if err := st.Flush(); err == nil {
		t.Error("Flush on a terminally failed stream = nil, want the open failure")
	}
}

type streamContextKey struct{}

type contextProbeProvider struct {
	key       any
	started   chan struct{}
	release   chan struct{}
	values    chan any
	startOnce sync.Once
}

func newContextProbeProvider(key any) *contextProbeProvider {
	return &contextProbeProvider{
		key:     key,
		started: make(chan struct{}),
		release: make(chan struct{}),
		values:  make(chan any, 1),
	}
}

func (p *contextProbeProvider) GetHeaders(ctx context.Context, _ string) (map[string]string, error) {
	p.startOnce.Do(func() { close(p.started) })
	select {
	case p.values <- ctx.Value(p.key):
	default:
	}
	select {
	case <-p.release:
		return map[string]string{"authorization": "Bearer t"}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (*contextProbeProvider) Invalidate(context.Context, string) {}

func TestCreateStreamAsyncPropagatesValuesButDetachesCancellation(t *testing.T) {
	conn := dialEcho(t, &echoServer{streamID: "stream-async-context"})
	sdk := zerobus.NewWithConn(conn, "https://ws.zerobus.databricks.com", "https://ws.databricks.com")
	defer sdk.Close()

	key := streamContextKey{}
	provider := newContextProbeProvider(key)
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), key, "trace-value"))
	st, err := sdk.CreateStreamWithProvider(
		ctx, "main.sales.orders", provider, zerobus.WithJSON(),
	)
	if err != nil {
		cancel()
		t.Fatalf("CreateStreamWithProvider: %v", err)
	}
	defer st.Close()

	select {
	case <-provider.started:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("headers provider was not called")
	}
	cancel()
	close(provider.release)
	select {
	case got := <-provider.values:
		if got != "trace-value" {
			t.Fatalf("context value = %v, want trace-value", got)
		}
	case <-time.After(time.Second):
		t.Fatal("headers provider did not observe context value")
	}

	if _, err := st.IngestRecordOffset([]byte(`{"id":1}`)); err != nil {
		t.Fatalf("IngestRecordOffset after caller cancellation: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush after caller cancellation: %v", err)
	}
}

func TestCreateStreamWaitForReadyContextCancelsHeadersProvider(t *testing.T) {
	conn := dialEcho(t, &echoServer{streamID: "stream-ready-context"})
	sdk := zerobus.NewWithConn(conn, "https://ws.zerobus.databricks.com", "https://ws.databricks.com")
	defer sdk.Close()

	key := streamContextKey{}
	provider := newContextProbeProvider(key)
	valueCtx := context.WithValue(context.Background(), key, "trace-value")
	ctx, cancel := context.WithTimeout(valueCtx, 25*time.Millisecond)
	defer cancel()
	st, err := sdk.CreateStreamWithProvider(
		ctx, "main.sales.orders", provider,
		zerobus.WithJSON(), zerobus.WithWaitForReady(),
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CreateStreamWithProvider error = %v, want DeadlineExceeded", err)
	}
	if st != nil {
		t.Fatalf("CreateStreamWithProvider returned stream on cancellation: %#v", st)
	}
	select {
	case got := <-provider.values:
		if got != "trace-value" {
			t.Fatalf("context value = %v, want trace-value", got)
		}
	case <-time.After(time.Second):
		t.Fatal("headers provider did not observe context value")
	}
	if n := sdk.OpenStreamCount(); n != 0 {
		t.Fatalf("SDK tracks %d streams after cancelled wait-ready create, want 0", n)
	}
}

func TestCreateStreamWaitForReadyContextCancelsHandshake(t *testing.T) {
	conn := dialEcho(t, &echoServer{streamID: "stream-ready-handshake", noReady: true})
	sdk := zerobus.NewWithConn(conn, "https://ws.zerobus.databricks.com", "https://ws.databricks.com")
	defer sdk.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	st, err := sdk.CreateStreamWithProvider(
		ctx,
		"main.sales.orders",
		zerobus.NewStaticHeadersProvider(map[string]string{"authorization": "Bearer t"}),
		zerobus.WithJSON(),
		zerobus.WithWaitForReady(),
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CreateStreamWithProvider error = %v, want DeadlineExceeded", err)
	}
	if st != nil {
		t.Fatalf("CreateStreamWithProvider returned stream on cancellation: %#v", st)
	}
	if n := sdk.OpenStreamCount(); n != 0 {
		t.Fatalf("SDK tracks %d streams after cancelled handshake, want 0", n)
	}
}

func TestCreateStreamWithProviderWaitForReadySucceeds(t *testing.T) {
	conn := dialEcho(t, &echoServer{streamID: "stream-ready"})
	sdk := zerobus.NewWithConn(conn, "https://ws.zerobus.databricks.com", "https://ws.databricks.com")
	defer sdk.Close()
	provider := zerobus.NewStaticHeadersProvider(map[string]string{"authorization": "Bearer t"})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	st, err := sdk.CreateStreamWithProvider(
		ctx, "main.sales.orders", provider, zerobus.WithJSON(), zerobus.WithWaitForReady(),
	)
	if err != nil {
		cancel()
		t.Fatalf("CreateStreamWithProvider(WithWaitForReady): %v", err)
	}
	defer st.Close()
	if got := st.ServerID(); got == "" {
		cancel()
		t.Fatal("ServerID is empty after wait-ready create")
	}
	cancel()
	if _, err := st.IngestRecordOffset([]byte(`{"id":1}`)); err != nil {
		t.Fatalf("IngestRecordOffset after creation-context cancellation: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush after creation-context cancellation: %v", err)
	}
}

func TestCreateStreamWithProviderWaitForReadyFailsFastOnOpenError(t *testing.T) {
	conn := dialEcho(t, &echoServer{streamID: "stream-ready-fail"})
	sdk := zerobus.NewWithConn(conn, "https://ws.zerobus.databricks.com", "https://ws.databricks.com")
	defer sdk.Close()

	// Missing auth headers makes open fail terminally; wait-ready mode surfaces
	// it from CreateStreamWithProvider rather than deferring to Flush.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	st, err := sdk.CreateStreamWithProvider(
		ctx,
		"main.sales.orders",
		zerobus.NewStaticHeadersProvider(nil),
		zerobus.WithJSON(),
		zerobus.WithRecovery(zerobus.RecoveryDisabled),
		zerobus.WithWaitForReady(),
	)
	if err == nil {
		if st != nil {
			_ = st.Close()
		}
		t.Fatal("CreateStreamWithProvider succeeded, want wait-ready open failure")
	}
	if st != nil {
		t.Fatalf("CreateStreamWithProvider returned stream on failure: %#v", st)
	}
	if n := sdk.OpenStreamCount(); n != 0 {
		t.Fatalf("SDK tracks %d streams after failed wait-ready create, want 0", n)
	}
}

func TestSDKCloseTerminatesOpenStreams(t *testing.T) {
	conn := dialEcho(t, &echoServer{streamID: "stream-sdk-close"})
	sdk := zerobus.NewWithConn(conn, "https://ws.zerobus.databricks.com", "https://ws.databricks.com")
	provider := zerobus.NewStaticHeadersProvider(map[string]string{"authorization": "Bearer t"})

	st, err := sdk.CreateStreamWithProvider(
		context.Background(), "main.sales.orders", provider, zerobus.WithJSON())
	if err != nil {
		t.Fatalf("CreateStreamWithProvider: %v", err)
	}
	if _, err := st.IngestRecordOffset([]byte(`{"id":1}`)); err != nil {
		t.Fatalf("IngestRecordOffset: %v", err)
	}

	if err := sdk.Close(); err != nil {
		t.Fatalf("SDK.Close: %v", err)
	}
	if !st.IsClosed() {
		t.Error("stream still open after SDK.Close")
	}
	if err := sdk.Close(); err != nil {
		t.Errorf("second SDK.Close: %v, want nil (idempotent)", err)
	}
	if _, err := sdk.CreateStreamWithProvider(
		context.Background(), "main.sales.orders", provider, zerobus.WithJSON()); err == nil {
		t.Error("CreateStreamWithProvider succeeded on a closed SDK")
	}
}

func TestStreamCloseDeregistersFromSDK(t *testing.T) {
	conn := dialEcho(t, &echoServer{streamID: "stream-deregister"})
	sdk := zerobus.NewWithConn(conn, "https://ws.zerobus.databricks.com", "https://ws.databricks.com")
	defer sdk.Close()
	provider := zerobus.NewStaticHeadersProvider(map[string]string{"authorization": "Bearer t"})

	st, err := sdk.CreateStreamWithProvider(
		context.Background(), "main.sales.orders", provider, zerobus.WithJSON())
	if err != nil {
		t.Fatalf("CreateStreamWithProvider: %v", err)
	}
	if n := sdk.OpenStreamCount(); n != 1 {
		t.Fatalf("SDK tracks %d streams, want 1 after CreateStream", n)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n := sdk.OpenStreamCount(); n != 0 {
		t.Errorf("SDK tracks %d streams, want 0 after Stream.Close", n)
	}
}

func TestIngestContextCancelsBackpressure(t *testing.T) {
	tests := []struct {
		name   string
		ingest func(*zerobus.Stream, context.Context) error
	}{
		{
			name: "record",
			ingest: func(st *zerobus.Stream, ctx context.Context) error {
				_, err := st.IngestJSONOffsetContext(ctx, []byte(`{"id":2}`))
				return err
			},
		},
		{
			name: "batch",
			ingest: func(st *zerobus.Stream, ctx context.Context) error {
				_, err := st.IngestJSONRecordsOffsetContext(ctx, [][]byte{[]byte(`{"id":2}`)})
				return err
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conn := dialEcho(t, &echoServer{streamID: "stream-backpressure", noAcks: true})
			sdk := zerobus.NewWithConn(
				conn, "https://ws.zerobus.databricks.com", "https://ws.databricks.com")
			t.Cleanup(func() { _ = sdk.Close() })
			provider := zerobus.NewStaticHeadersProvider(
				map[string]string{"authorization": "Bearer t"})

			st, err := sdk.CreateStreamWithProvider(
				context.Background(), "main.sales.orders", provider,
				zerobus.WithJSON(), zerobus.WithMaxInflight(1),
			)
			if err != nil {
				t.Fatalf("CreateStreamWithProvider: %v", err)
			}
			if _, err := st.IngestJSONOffset([]byte(`{"id":1}`)); err != nil {
				t.Fatalf("first IngestJSONOffset: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
			defer cancel()
			err = tc.ingest(st, ctx)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("context ingest error = %v, want DeadlineExceeded", err)
			}
		})
	}
}

// waitFor polls cond until it holds, failing the test if it never does.
func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestFetchProtoDescriptorPublicFlow(t *testing.T) {
	uc := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oidc/v1/token":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": "abc",
				"expires_in":   300,
			})
		case strings.HasPrefix(r.URL.Path, "/api/2.1/unity-catalog/tables/"):
			if got := r.Header.Get("Authorization"); got != "Bearer abc" {
				http.Error(w, "missing schema authorization", http.StatusUnauthorized)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name":         "orders",
				"catalog_name": "main",
				"schema_name":  "sales",
				"columns": []map[string]any{
					{
						"name":      "id",
						"type_name": "LONG",
						"nullable":  false,
						"position":  0,
					},
					{
						"name":      "note",
						"type_name": "STRING",
						"nullable":  true,
						"position":  1,
					},
					{
						"name":      "payload",
						"type_name": "STRUCT",
						"type_json": `{"type":"struct","fields":[` +
							`{"name":"name","type":"string","nullable":false},` +
							`{"name":"tags","type":{"type":"array","elementType":"long"},"nullable":true},` +
							`{"name":"attributes","type":{"type":"map","keyType":"string","valueType":"integer"},"nullable":true}` +
							`]}`,
						"nullable": true,
						"position": 2,
					},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer uc.Close()

	creates := make(chan *zerobuspb.CreateIngestStreamRequest, 1)
	ingests := make(chan *zerobuspb.EphemeralStreamRequest, 2)
	conn := dialEcho(t, &echoServer{
		streamID: "dynamic-stream",
		creates:  creates,
		ingests:  ingests,
	})
	sdk := zerobus.NewWithConn(
		conn,
		"https://ws.zerobus.databricks.com",
		uc.URL,
		zerobus.WithHTTPClient(uc.Client()),
	)
	t.Cleanup(func() { _ = sdk.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	descriptorBytes, err := sdk.FetchProtoDescriptor(
		ctx,
		"main.sales.orders",
		"id",
		"secret",
	)
	if err != nil {
		t.Fatalf("FetchProtoDescriptor() error = %v", err)
	}
	dynamic, err := sdk.CreateStream(
		ctx,
		"main.sales.orders",
		"id",
		"secret",
		zerobus.WithProto(descriptorBytes),
		zerobus.WithWaitForReady(),
		zerobus.WithMaxPayloadBytes(256),
	)
	if err != nil {
		t.Fatalf("CreateStream() error = %v", err)
	}
	defer dynamic.Close()

	if _, err := dynamic.IngestJSONOffset(
		[]byte(`{"payload":{"name":"missing id"}}`),
	); err == nil {
		t.Fatal("missing required id was accepted")
	}
	if got := len(ingests); got != 0 {
		t.Fatalf("failed dynamic record queued %d requests, want 0", got)
	}
	if _, err := dynamic.IngestJSONOffset(
		[]byte(`{"id":"1","payload":{}}`),
	); err == nil {
		t.Fatal("missing nested required payload.name was accepted")
	}
	if got := len(ingests); got != 0 {
		t.Fatalf("failed nested dynamic record queued %d requests, want 0", got)
	}
	singleRecord := `{"id":"1","payload":{"name":"first","tags":["10","20"],"attributes":{"a":7}},"extra":"` +
		strings.Repeat("x", 512) + `"}`
	if len(singleRecord) <= 256 {
		t.Fatalf("test JSON size = %d, want above MaxPayloadBytes", len(singleRecord))
	}
	if _, err := dynamic.IngestJSONOffset([]byte(singleRecord)); err != nil {
		t.Fatalf("IngestJSONOffset() error = %v", err)
	}
	if _, err := dynamic.IngestJSONRecordsOffset([][]byte{
		[]byte(`{"id":"2","payload":{"name":"second","tags":[],"attributes":{}}}`),
		[]byte(`{"id":"3","payload":{"name":"third","tags":["30"],"attributes":{"b":9}}}`),
	}); err != nil {
		t.Fatalf("IngestJSONRecordsOffset() error = %v", err)
	}
	if err := dynamic.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	create := <-creates
	if create.GetRecordType() != zerobuspb.RecordType_PROTO {
		t.Fatalf("record type = %v, want PROTO", create.GetRecordType())
	}
	if len(create.GetDescriptorProto()) == 0 {
		t.Fatal("dynamic create request has empty descriptor")
	}
	var descriptor descriptorpb.DescriptorProto
	if err := proto.Unmarshal(create.GetDescriptorProto(), &descriptor); err != nil {
		t.Fatalf("unmarshal dynamic descriptor: %v", err)
	}
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:        proto.String("dynamic_test.proto"),
		Syntax:      proto.String("proto2"),
		MessageType: []*descriptorpb.DescriptorProto{&descriptor},
	}, nil)
	if err != nil {
		t.Fatalf("resolve dynamic descriptor: %v", err)
	}
	messageDescriptor := file.Messages().Get(0)
	single := <-ingests
	singleBytes := single.GetIngestRecord().GetProtoEncodedRecord()
	if len(singleBytes) == 0 {
		t.Fatal("single dynamic record was not encoded as protobuf")
	}
	decoded := dynamicpb.NewMessage(messageDescriptor)
	if err := proto.Unmarshal(singleBytes, decoded); err != nil {
		t.Fatalf("decode dynamic record: %v", err)
	}
	if got := decoded.Get(messageDescriptor.Fields().ByName("id")).Int(); got != 1 {
		t.Fatalf("decoded id = %d, want 1", got)
	}
	if decoded.Has(messageDescriptor.Fields().ByName("note")) {
		t.Fatal("omitted nullable note is present")
	}
	payloadField := messageDescriptor.Fields().ByName("payload")
	payload := decoded.Get(payloadField).Message()
	payloadDescriptor := payloadField.Message()
	if got := payload.Get(payloadDescriptor.Fields().ByName("name")).String(); got != "first" {
		t.Fatalf("decoded payload.name = %q, want first", got)
	}
	tags := payload.Get(payloadDescriptor.Fields().ByName("tags")).List()
	if tags.Len() != 2 || tags.Get(0).Int() != 10 || tags.Get(1).Int() != 20 {
		t.Fatalf("decoded tags = %v, want [10 20]", tags)
	}
	attributes := payload.Get(payloadDescriptor.Fields().ByName("attributes")).Map()
	if got := attributes.Get(protoreflect.ValueOfString("a").MapKey()).Int(); got != 7 {
		t.Fatalf("decoded attributes[a] = %d, want 7", got)
	}
	batch := <-ingests
	if got := len(batch.GetIngestRecordBatch().GetProtoEncodedBatch().GetRecords()); got != 2 {
		t.Fatalf("dynamic batch records = %d, want 2", got)
	}
}
