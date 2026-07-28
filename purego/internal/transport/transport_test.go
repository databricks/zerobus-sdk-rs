package transport_test

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
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
	// authRejectCode, when not codes.OK, makes open fail with that gRPC status
	// after receiving create.
	authRejectCode codes.Code
	// drainGate, when non-nil, holds io.EOF back until closed, so a test can
	// assert GracefulClose keeps draining rather than returning at the first ack.
	drainGate chan struct{}
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
	if f.authRejectCode != codes.OK {
		return status.Error(f.authRejectCode, "bad credentials")
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
				<-stream.Context().Done()
				return stream.Context().Err()
			}
			// Hold EOF until the test releases the gate.
			if f.drainGate != nil {
				<-f.drainGate
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

type stubHeadersProvider struct {
	headers         map[string]string
	calls           atomic.Int32
	invalidateCalls atomic.Int32
	lastTable       atomic.Value // string
}

func (p *stubHeadersProvider) GetHeaders(_ context.Context, tableName string) (map[string]string, error) {
	p.calls.Add(1)
	p.lastTable.Store(tableName)
	out := make(map[string]string, len(p.headers))
	for k, v := range p.headers {
		out[k] = v
	}
	return out, nil
}

func (p *stubHeadersProvider) Invalidate(_ context.Context, tableName string) {
	p.invalidateCalls.Add(1)
	p.lastTable.Store(tableName)
}

// authProvider returns a HeadersProvider that supplies a single authorization
// header, for tests that just need the stream authenticated.
func authProvider(token string) transport.HeadersProvider {
	return &stubHeadersProvider{headers: map[string]string{"authorization": token}}
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
		HeadersProvider: authProvider("Bearer tok-abc"),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got, want := stream.ServerID(), "stream-123"; got != want {
		t.Errorf("server stream ID = %q, want %q", got, want)
	}
	if got, want := stream.ID(), stream.ServerID(); got != want {
		t.Errorf("deprecated ID = %q, want %q", got, want)
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

// TestOpenSendsAuthValueVerbatim verifies the transport sends the provider's
// authorization value exactly as given (only trimmed), leaving scheme formatting
// to the provider rather than rewriting it.
func TestOpenSendsAuthValueVerbatim(t *testing.T) {
	for _, tok := range []string{"Bearer tok", "basic dXNlcg==", "DPoP proof", "raw-token", "Custom abc"} {
		srv := &fakeServer{streamID: "s", seen: make(chan observed, 1)}
		conn := dialFake(t, srv)

		_, err := conn.Open(context.Background(), transport.StreamParams{
			TableName:       "c.s.t",
			RecordType:      zerobuspb.RecordType_JSON,
			HeadersProvider: authProvider(tok),
		})
		if err != nil {
			t.Fatalf("Open %q: %v", tok, err)
		}
		if got := <-srv.seen; got.auth != tok {
			t.Errorf("token %q: server saw authorization %q, want it verbatim", tok, got.auth)
		}
	}
}

func TestOpenRejectsInvalidHeaders(t *testing.T) {
	cases := []struct {
		name    string
		headers map[string]string
	}{
		{
			name:    "authorization with newline",
			headers: map[string]string{"authorization": "tok\nen"},
		},
		{
			name:    "authorization with null",
			headers: map[string]string{"authorization": "tok\x00en"},
		},
		{
			name:    "authorization with carriage return after scheme",
			headers: map[string]string{"authorization": "Bearer tok\ren"},
		},
		{
			name:    "custom header with carriage return",
			headers: map[string]string{mdUserKey: "val\ruer"},
		},
		{
			name:    "authorization with non-ASCII value",
			headers: map[string]string{"authorization": "Bearer tøken"},
		},
		{
			name:    "custom header key with space",
			headers: map[string]string{"x bad": "v"},
		},
		{
			name:    "custom header key with non-ASCII",
			headers: map[string]string{"x-bäd": "v"},
		},
		{
			name: "duplicate normalized key",
			headers: map[string]string{
				"authorization":   "tok-1",
				" Authorization ": "tok-2",
			},
		},
		{
			name:    "empty key",
			headers: map[string]string{"": "v"},
		},
		{
			name:    "whitespace-only key",
			headers: map[string]string{"   ": "v"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := &fakeServer{streamID: "s", seen: make(chan observed, 1)}
			conn := dialFake(t, srv)

			_, err := conn.Open(context.Background(), transport.StreamParams{
				TableName:       "c.s.t",
				RecordType:      zerobuspb.RecordType_JSON,
				HeadersProvider: &stubHeadersProvider{headers: tc.headers},
			})
			if err == nil {
				t.Fatal("Open with invalid provider header: got nil error, want rejection")
			}
		})
	}
}

func TestStreamSendRecv(t *testing.T) {
	srv := &fakeServer{streamID: "s1", seen: make(chan observed, 1)}
	conn := dialFake(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := conn.Open(ctx, transport.StreamParams{
		TableName:       "c.s.t",
		RecordType:      zerobuspb.RecordType_JSON,
		HeadersProvider: authProvider("tok"),
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
		TableName:       "c.s.t",
		RecordType:      zerobuspb.RecordType_PROTO, // no descriptor
		HeadersProvider: authProvider("tok"),
	})
	if err == nil {
		t.Fatal("Open with PROTO and no descriptor: got nil error, want failure")
	}
}

func TestOpenRequiresTableName(t *testing.T) {
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1)}
	conn := dialFake(t, srv)

	_, err := conn.Open(context.Background(), transport.StreamParams{
		TableName:       "  ",
		RecordType:      zerobuspb.RecordType_JSON,
		HeadersProvider: authProvider("tok"),
	})
	if err == nil {
		t.Fatal("Open with empty table name: got nil error, want failure")
	}
}

func TestOpenTrimsTableName(t *testing.T) {
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1)}
	conn := dialFake(t, srv)

	_, err := conn.Open(context.Background(), transport.StreamParams{
		TableName:       "  c.s.t  ",
		RecordType:      zerobuspb.RecordType_JSON,
		HeadersProvider: authProvider("tok"),
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
		TableName:       "c.s.t",
		RecordType:      zerobuspb.RecordType(999),
		HeadersProvider: authProvider("tok"),
	})
	if err == nil {
		t.Fatal("Open with unsupported record type: got nil error, want failure")
	}
}

func TestOpenRejectsUnexpectedResponse(t *testing.T) {
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1), badHandshake: true}
	conn := dialFake(t, srv)

	_, err := conn.Open(context.Background(), transport.StreamParams{
		TableName:       "c.s.t",
		RecordType:      zerobuspb.RecordType_JSON,
		HeadersProvider: authProvider("tok"),
	})
	if err == nil {
		t.Fatal("Open with non-create first response: got nil error, want failure")
	}
}

func TestOpenOmitsAuthWithoutProvider(t *testing.T) {
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1)}
	conn := dialFake(t, srv)

	_, err := conn.Open(context.Background(), transport.StreamParams{
		TableName:  "c.s.t",
		RecordType: zerobuspb.RecordType_JSON, // no provider set
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got := <-srv.seen; got.auth != "" {
		t.Errorf("server saw authorization %q, want it absent", got.auth)
	}
}

func TestOpenUsesHeadersProviderAndTableIsAuthoritative(t *testing.T) {
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1)}
	conn := dialFake(t, srv)

	p := &stubHeadersProvider{
		headers: map[string]string{
			"authorization":                   "Bearer provider-token",
			"x-databricks-zerobus-table-name": "wrong.table.name",
			mdUserKey:                         "provider-md",
		},
	}
	_, err := conn.Open(context.Background(), transport.StreamParams{
		TableName:       "c.s.t",
		RecordType:      zerobuspb.RecordType_JSON,
		HeadersProvider: p,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	got := <-srv.seen
	if got.tableName != "c.s.t" {
		t.Fatalf("server saw table %q, want %q", got.tableName, "c.s.t")
	}
	if got.auth != "Bearer provider-token" {
		t.Fatalf("server saw auth %q, want %q", got.auth, "Bearer provider-token")
	}
	if got.userMD != "provider-md" {
		t.Fatalf("server saw custom metadata %q, want %q", got.userMD, "provider-md")
	}
	if p.calls.Load() != 1 {
		t.Fatalf("GetHeaders calls = %d, want 1", p.calls.Load())
	}
	last, _ := p.lastTable.Load().(string)
	if last != "c.s.t" {
		t.Fatalf("provider saw table %q, want %q", last, "c.s.t")
	}
}

// TestOpenHeadersProviderNoAuthSendsNoAuthHeader verifies that a provider which
// returns only non-auth headers opens the stream without an authorization header
// rather than synthesizing one.
func TestOpenHeadersProviderNoAuthSendsNoAuthHeader(t *testing.T) {
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1)}
	conn := dialFake(t, srv)

	p := &stubHeadersProvider{
		headers: map[string]string{mdUserKey: "provider-md"},
	}
	_, err := conn.Open(context.Background(), transport.StreamParams{
		TableName:       "c.s.t",
		RecordType:      zerobuspb.RecordType_JSON,
		HeadersProvider: p,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	got := <-srv.seen
	if got.authCount != 0 {
		t.Fatalf("server saw authorization %q (count %d), want none", got.auth, got.authCount)
	}
	if got.userMD != "provider-md" {
		t.Fatalf("server saw custom metadata %q, want %q", got.userMD, "provider-md")
	}
}

func TestOpenAuthRejectionInvalidatesHeadersProvider(t *testing.T) {
	for _, tc := range []struct {
		name string
		code codes.Code
	}{
		{name: "Unauthenticated", code: codes.Unauthenticated},
		{name: "PermissionDenied", code: codes.PermissionDenied},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := &fakeServer{streamID: "s", seen: make(chan observed, 1), authRejectCode: tc.code}
			conn := dialFake(t, srv)

			p := &stubHeadersProvider{
				headers: map[string]string{"authorization": "tok"},
			}
			_, err := conn.Open(context.Background(), transport.StreamParams{
				TableName:       "c.s.t",
				RecordType:      zerobuspb.RecordType_JSON,
				HeadersProvider: p,
			})
			if err == nil {
				t.Fatal("Open with server auth rejection: got nil error")
			}
			if p.invalidateCalls.Load() != 1 {
				t.Fatalf("Invalidate calls = %d, want 1", p.invalidateCalls.Load())
			}
			if last, _ := p.lastTable.Load().(string); last != "c.s.t" {
				t.Fatalf("Invalidate saw table %q, want %q", last, "c.s.t")
			}
		})
	}
}

// TestOpenNonAuthRejectionDoesNotInvalidate verifies that a non-auth failure
// (e.g. a bad handshake, or a non-auth gRPC code) leaves the provider's cached
// credentials intact: only Unauthenticated/PermissionDenied invalidate.
func TestOpenNonAuthRejectionDoesNotInvalidate(t *testing.T) {
	for _, tc := range []struct {
		name string
		srv  *fakeServer
	}{
		{
			name: "Internal code",
			srv:  &fakeServer{streamID: "s", seen: make(chan observed, 1), authRejectCode: codes.Internal},
		},
		{
			name: "unexpected first response",
			srv:  &fakeServer{streamID: "s", seen: make(chan observed, 1), badHandshake: true},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := dialFake(t, tc.srv)

			p := &stubHeadersProvider{headers: map[string]string{"authorization": "tok"}}
			_, err := conn.Open(context.Background(), transport.StreamParams{
				TableName:       "c.s.t",
				RecordType:      zerobuspb.RecordType_JSON,
				HeadersProvider: p,
			})
			if err == nil {
				t.Fatal("Open against a non-auth failure: got nil error")
			}
			if p.invalidateCalls.Load() != 0 {
				t.Fatalf("Invalidate calls = %d, want 0 for a non-auth failure", p.invalidateCalls.Load())
			}
		})
	}
}

// errHeadersProvider returns a fixed error from GetHeaders, to assert Open
// surfaces a provider failure wrapped rather than opening the stream.
type errHeadersProvider struct{ err error }

func (p *errHeadersProvider) GetHeaders(context.Context, string) (map[string]string, error) {
	return nil, p.err
}

func (p *errHeadersProvider) Invalidate(context.Context, string) {}

// TestOpenSurfacesHeadersProviderError verifies that when GetHeaders returns a
// non-context error, Open fails with it wrapped rather than opening the stream.
func TestOpenSurfacesHeadersProviderError(t *testing.T) {
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1)}
	conn := dialFake(t, srv)

	sentinel := errors.New("mint failed")
	_, err := conn.Open(context.Background(), transport.StreamParams{
		TableName:       "c.s.t",
		RecordType:      zerobuspb.RecordType_JSON,
		HeadersProvider: &errHeadersProvider{err: sentinel},
	})
	if err == nil {
		t.Fatal("Open with a failing headers provider: got nil error, want failure")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("Open error = %v, want it to wrap %v", err, sentinel)
	}
}

// blockingHeadersProvider blocks in GetHeaders until the passed context is done,
// then returns its error. It lets a test assert that Open bounds GetHeaders with
// the open deadline rather than hanging on a stalled credential mint.
type blockingHeadersProvider struct {
	ctxErr chan error
}

func (p *blockingHeadersProvider) GetHeaders(ctx context.Context, _ string) (map[string]string, error) {
	<-ctx.Done()
	p.ctxErr <- ctx.Err()
	return nil, ctx.Err()
}

func (p *blockingHeadersProvider) Invalidate(context.Context, string) {}

// TestOpenBoundsGetHeadersWithDeadline verifies GetHeaders runs under the open
// deadline: a provider that blocks is cancelled when the caller's ctx expires,
// so Open fails promptly instead of hanging on a stalled credential mint.
func TestOpenBoundsGetHeadersWithDeadline(t *testing.T) {
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1)}
	conn := dialFake(t, srv)

	p := &blockingHeadersProvider{ctxErr: make(chan error, 1)}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err := conn.Open(ctx, transport.StreamParams{
		TableName:       "c.s.t",
		RecordType:      zerobuspb.RecordType_JSON,
		HeadersProvider: p,
	})
	if err == nil {
		t.Fatal("Open with a blocking headers provider: got nil error, want deadline failure")
	}
	if gotErr := <-p.ctxErr; !errors.Is(gotErr, context.DeadlineExceeded) {
		t.Fatalf("GetHeaders context error = %v, want DeadlineExceeded", gotErr)
	}
}

// delayedHeadersProvider sleeps before returning headers unless ctx is done.
type delayedHeadersProvider struct {
	delay time.Duration
}

func (p *delayedHeadersProvider) GetHeaders(ctx context.Context, _ string) (map[string]string, error) {
	timer := time.NewTimer(p.delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return map[string]string{"authorization": "tok"}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *delayedHeadersProvider) Invalidate(context.Context, string) {}

// TestOpenNoDeadlineUsesIndependentHeaderAndHandshakeBudgets verifies that with
// no caller deadline, Open applies separate default budgets to GetHeaders and to
// the handshake: a slow (but successful) GetHeaders call does not consume the
// handshake budget.
func TestOpenNoDeadlineUsesIndependentHeaderAndHandshakeBudgets(t *testing.T) {
	defer transport.SetDefaultHeadersTimeout(60 * time.Millisecond)()
	defer transport.SetDefaultHandshakeTimeout(60 * time.Millisecond)()

	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1), hangHandshake: true}
	conn := dialFake(t, srv)

	start := time.Now()
	_, err := conn.Open(context.Background(), transport.StreamParams{
		TableName:       "c.s.t",
		RecordType:      zerobuspb.RecordType_JSON,
		HeadersProvider: &delayedHeadersProvider{delay: 40 * time.Millisecond},
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Open with hanging handshake and no caller deadline: err = %v, want DeadlineExceeded", err)
	}
	elapsed := time.Since(start)
	// With split budgets this should be roughly headers delay + handshake timeout.
	// A single shared budget would fail much earlier.
	if elapsed < 80*time.Millisecond {
		t.Fatalf("Open returned too quickly (%v); expected separate header and handshake budgets", elapsed)
	}
}

func TestStreamCloseAbortsRecv(t *testing.T) {
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1)}
	conn := dialFake(t, srv)

	stream, err := conn.Open(context.Background(), transport.StreamParams{
		TableName:       "c.s.t",
		RecordType:      zerobuspb.RecordType_JSON,
		HeadersProvider: authProvider("tok"),
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
		TableName:       "c.s.t",
		RecordType:      zerobuspb.RecordType_JSON,
		HeadersProvider: authProvider("Bearer fresh-token"),
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

// TestOpenReplacesInheritedAuthWhenNoProvider verifies that a stale inherited
// authorization value is dropped, not forwarded, when no HeadersProvider is set.
func TestOpenReplacesInheritedAuthWhenNoProvider(t *testing.T) {
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1)}
	conn := dialFake(t, srv)

	ctx := metadata.AppendToOutgoingContext(context.Background(),
		"authorization", "Bearer stale-token")

	_, err := conn.Open(ctx, transport.StreamParams{
		TableName:  "c.s.t",
		RecordType: zerobuspb.RecordType_JSON, // no provider
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

	_, err := conn.Open(ctx, transport.StreamParams{
		TableName:       "c.s.t",
		RecordType:      zerobuspb.RecordType_JSON,
		HeadersProvider: authProvider("tok"),
	})
	// Assert on the returned deadline error, not wall-clock, so the test can't flake.
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Open against a hanging handshake: err = %v, want DeadlineExceeded", err)
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

	_, err := conn.Open(ctx, transport.StreamParams{
		TableName:       "c.s.t",
		RecordType:      zerobuspb.RecordType_JSON,
		HeadersProvider: authProvider("tok"),
	})
	// Assert the cancel ended Open (not the 15s handshake timeout), from the
	// returned error rather than wall-clock, so the test can't flake.
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Open with caller cancel mid-open: err = %v, want Canceled", err)
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
		TableName:       "c.s.t",
		RecordType:      zerobuspb.RecordType_JSON,
		HeadersProvider: authProvider("tok"),
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

// TestStreamGracefulCloseDefaultsDeadline: with no deadline on the context,
// GracefulClose must apply defaultDrainTimeout and not hang on a stalled server.
// The default is shrunk to a few ms so the path runs quickly.
func TestStreamGracefulCloseDefaultsDeadline(t *testing.T) {
	defer transport.SetDefaultDrainTimeout(50 * time.Millisecond)()

	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1), hangDrain: true}
	conn := dialFake(t, srv)

	stream, err := conn.Open(context.Background(), transport.StreamParams{
		TableName:       "c.s.t",
		RecordType:      zerobuspb.RecordType_JSON,
		HeadersProvider: authProvider("tok"),
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
		TableName:       "c.s.t",
		RecordType:      zerobuspb.RecordType_JSON,
		HeadersProvider: authProvider("tok"),
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
	stream.Close()
}

// TestStreamGracefulCloseDrainsPending: an in-flight ack precedes io.EOF, so
// GracefulClose must discard it and keep draining. drainGate holds EOF back to
// prove it's still draining after the ack, then returns nil once EOF arrives.
func TestStreamGracefulCloseDrainsPending(t *testing.T) {
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1), drainGate: make(chan struct{})}
	conn := dialFake(t, srv)

	stream, err := conn.Open(context.Background(), transport.StreamParams{
		TableName:       "c.s.t",
		RecordType:      zerobuspb.RecordType_JSON,
		HeadersProvider: authProvider("tok"),
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

	done := make(chan error, 1)
	go func() { done <- stream.GracefulClose(ctx) }()

	// The server sends the ack, then blocks before EOF. A correct drain discards
	// the ack and waits for EOF, so GracefulClose must not have returned yet.
	select {
	case err := <-done:
		t.Fatalf("GracefulClose returned at the pending ack (err=%v); it must keep draining to io.EOF", err)
	case <-time.After(200 * time.Millisecond):
	}

	// Release EOF; the drain completes cleanly.
	close(srv.drainGate)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("GracefulClose after draining the pending ack: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("GracefulClose did not return after io.EOF was released")
	}
}

// TestStreamGracefulCloseHonorsDeadline: the server never ends the stream, so
// GracefulClose returns its ctx error promptly instead of blocking, and leaves
// the stream torn down.
func TestStreamGracefulCloseHonorsDeadline(t *testing.T) {
	srv := &fakeServer{streamID: "s", seen: make(chan observed, 1), hangDrain: true}
	conn := dialFake(t, srv)

	stream, err := conn.Open(context.Background(), transport.StreamParams{
		TableName:       "c.s.t",
		RecordType:      zerobuspb.RecordType_JSON,
		HeadersProvider: authProvider("tok"),
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
