package transport_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/databricks/zerobus-sdk/purego/internal/transport"
)

type flightObserved struct {
	tableName      string
	auth           string
	userMD         string
	tableNameCount int
	authCount      int
	schema         *flight.FlightData
}

type mockFlightServer struct {
	flight.BaseFlightServer

	readyMetadata []byte
	readyGate     <-chan struct{}
	sendAcks      bool
	seen          chan flightObserved
	received      chan *flight.FlightData
}

func (s *mockFlightServer) DoPut(stream flight.FlightService_DoPutServer) error {
	schema, err := stream.Recv()
	if err != nil {
		return err
	}
	md, _ := metadata.FromIncomingContext(stream.Context())
	s.seen <- flightObserved{
		tableName:      firstMD(md, "x-databricks-zerobus-table-name"),
		auth:           firstMD(md, "authorization"),
		userMD:         firstMD(md, mdUserKey),
		tableNameCount: len(md.Get("x-databricks-zerobus-table-name")),
		authCount:      len(md.Get("authorization")),
		schema:         proto.Clone(schema).(*flight.FlightData),
	}

	if s.readyGate != nil {
		select {
		case <-s.readyGate:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
	if err := stream.Send(&flight.PutResult{AppMetadata: s.readyMetadata}); err != nil {
		return err
	}

	for {
		data, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		copied := proto.Clone(data).(*flight.FlightData)
		s.received <- copied
		if !s.sendAcks {
			continue
		}
		batchMetadata, err := transport.ParseFlightBatchMetadata(data.GetAppMetadata())
		if err != nil {
			return err
		}
		ack, err := json.Marshal(transport.FlightAckMetadata{
			AckUpToOffset:  batchMetadata.OffsetID,
			AckUpToRecords: uint64(batchMetadata.OffsetID + 1),
		})
		if err != nil {
			return err
		}
		if err := stream.Send(&flight.PutResult{AppMetadata: ack}); err != nil {
			return err
		}
	}
}

func dialFlightFake(t *testing.T, srv *mockFlightServer) *transport.Conn {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	flight.RegisterFlightServiceServer(grpcServer, srv)
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("mock Flight server stopped: %v", err)
		}
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		<-serveDone
	})

	dialer := grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	})
	conn, err := transport.Dial(
		"passthrough:///flight-bufnet",
		transport.WithInsecure(),
		transport.WithGRPCDialOptions(dialer),
	)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func mustFlightAckMetadata(t *testing.T, offset int64, records uint64) []byte {
	t.Helper()
	data, err := json.Marshal(transport.FlightAckMetadata{
		AckUpToOffset:  offset,
		AckUpToRecords: records,
	})
	if err != nil {
		t.Fatalf("marshal Flight ack metadata: %v", err)
	}
	return data
}

func TestOpenFlightSendsHeadersAndSchemaBeforeReady(t *testing.T) {
	readyGate := make(chan struct{})
	srv := &mockFlightServer{
		readyMetadata: mustFlightAckMetadata(t, transport.FlightStreamReadyOffset, 0),
		readyGate:     readyGate,
		seen:          make(chan flightObserved, 1),
		received:      make(chan *flight.FlightData, 1),
	}
	conn := dialFlightFake(t, srv)

	provider := &stubHeadersProvider{headers: map[string]string{
		"authorization":                   "Bearer fresh-token",
		"x-databricks-zerobus-table-name": "wrong.table",
		mdUserKey:                         "provider-value",
	}}
	ctx := metadata.AppendToOutgoingContext(
		context.Background(),
		"authorization", "Bearer stale-token",
		"x-databricks-zerobus-table-name", "stale.table",
	)
	schema := &flight.FlightData{DataHeader: []byte("ipc-schema-header")}

	type openResult struct {
		stream *transport.FlightStream
		err    error
	}
	opened := make(chan openResult, 1)
	go func() {
		stream, err := conn.OpenFlight(ctx, transport.FlightStreamParams{
			TableName:       "  catalog.schema.table  ",
			Schema:          schema,
			HeadersProvider: provider,
		})
		opened <- openResult{stream: stream, err: err}
	}()

	observed := <-srv.seen
	if observed.tableNameCount != 1 || observed.tableName != "catalog.schema.table" {
		t.Errorf(
			"table metadata count=%d value=%q, want count=1 value=%q",
			observed.tableNameCount,
			observed.tableName,
			"catalog.schema.table",
		)
	}
	if observed.authCount != 1 || observed.auth != "Bearer fresh-token" {
		t.Errorf(
			"authorization count=%d value=%q, want count=1 value=%q",
			observed.authCount,
			observed.auth,
			"Bearer fresh-token",
		)
	}
	if observed.userMD != "provider-value" {
		t.Errorf("custom metadata = %q, want provider-value", observed.userMD)
	}
	if !proto.Equal(observed.schema, schema) {
		t.Errorf("first FlightData = %v, want schema %v", observed.schema, schema)
	}

	select {
	case result := <-opened:
		t.Fatalf("OpenFlight returned before ready response: stream=%v err=%v", result.stream, result.err)
	case <-time.After(100 * time.Millisecond):
	}
	close(readyGate)

	result := <-opened
	if result.err != nil {
		t.Fatalf("OpenFlight: %v", result.err)
	}
	t.Cleanup(result.stream.Close)
	if got := result.stream.ServerID(); got != "flight-do-put" {
		t.Errorf("ServerID = %q, want flight-do-put", got)
	}
	if provider.calls.Load() != 1 {
		t.Errorf("GetHeaders calls = %d, want 1", provider.calls.Load())
	}
	if got, _ := provider.lastTable.Load().(string); got != "catalog.schema.table" {
		t.Errorf("provider table = %q, want catalog.schema.table", got)
	}
}

func TestOpenFlightReadyMetadataValidation(t *testing.T) {
	tests := []struct {
		name      string
		metadata  string
		wantError string
	}{
		{
			name:     "ready",
			metadata: `{"ack_up_to_offset":-1,"ack_up_to_records":0}`,
		},
		{
			name:      "real ack is not ready",
			metadata:  `{"ack_up_to_offset":0,"ack_up_to_records":1}`,
			wantError: "want -1",
		},
		{
			name:      "missing records",
			metadata:  `{"ack_up_to_offset":-1}`,
			wantError: "missing required field",
		},
		{
			name:      "ready cannot acknowledge records",
			metadata:  `{"ack_up_to_offset":-1,"ack_up_to_records":1}`,
			wantError: "want 0",
		},
		{
			name:      "ready cannot request close",
			metadata:  `{"ack_up_to_offset":-1,"ack_up_to_records":0,"close_stream_duration_ms":1}`,
			wantError: "close signal",
		},
		{
			name:     "unknown field is additive",
			metadata: `{"ack_up_to_offset":-1,"ack_up_to_records":0,"extra":true}`,
		},
		{
			name:      "duplicate field",
			metadata:  `{"ack_up_to_offset":-1,"ack_up_to_offset":-1,"ack_up_to_records":0}`,
			wantError: "duplicate field",
		},
		{
			name:      "empty",
			metadata:  ``,
			wantError: "metadata is empty",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			srv := &mockFlightServer{
				readyMetadata: []byte(test.metadata),
				seen:          make(chan flightObserved, 1),
				received:      make(chan *flight.FlightData, 1),
			}
			conn := dialFlightFake(t, srv)

			stream, err := conn.OpenFlight(context.Background(), transport.FlightStreamParams{
				TableName: "catalog.schema.table",
				Schema:    &flight.FlightData{DataHeader: []byte("schema")},
			})
			if test.wantError == "" {
				if err != nil {
					t.Fatalf("OpenFlight: %v", err)
				}
				t.Cleanup(stream.Close)
				return
			}
			if err == nil {
				stream.Close()
				t.Fatalf("OpenFlight: got nil error, want %q", test.wantError)
			}
			if !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("OpenFlight error = %q, want it to contain %q", err, test.wantError)
			}
		})
	}
}

func TestOpenFlightValidatesSchemaRequest(t *testing.T) {
	srv := &mockFlightServer{
		seen:     make(chan flightObserved, 1),
		received: make(chan *flight.FlightData, 1),
	}
	conn := dialFlightFake(t, srv)
	valid := &flight.FlightData{DataHeader: []byte("schema")}

	tests := []struct {
		name      string
		tableName string
		schema    *flight.FlightData
	}{
		{name: "blank table", tableName: " ", schema: valid},
		{name: "nil schema", tableName: "catalog.schema.table"},
		{
			name:      "missing schema header",
			tableName: "catalog.schema.table",
			schema:    &flight.FlightData{},
		},
		{
			name:      "schema body",
			tableName: "catalog.schema.table",
			schema: &flight.FlightData{
				DataHeader: []byte("schema"),
				DataBody:   []byte("unexpected"),
			},
		},
		{
			name:      "schema app metadata",
			tableName: "catalog.schema.table",
			schema: &flight.FlightData{
				DataHeader:  []byte("schema"),
				AppMetadata: []byte("unexpected"),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stream, err := conn.OpenFlight(
				context.Background(),
				transport.FlightStreamParams{
					TableName: test.tableName,
					Schema:    test.schema,
				},
			)
			if stream != nil {
				stream.Close()
				t.Fatal("OpenFlight returned a stream for invalid setup")
			}
			if !errors.Is(err, transport.ErrInvalidParams) {
				t.Fatalf("OpenFlight error = %v, want ErrInvalidParams", err)
			}
		})
	}
}

func TestFlightStreamSequentialSendRecvAndGracefulClose(t *testing.T) {
	srv := &mockFlightServer{
		readyMetadata: mustFlightAckMetadata(t, transport.FlightStreamReadyOffset, 0),
		sendAcks:      true,
		seen:          make(chan flightObserved, 1),
		received:      make(chan *flight.FlightData, 2),
	}
	conn := dialFlightFake(t, srv)

	stream, err := conn.OpenFlight(context.Background(), transport.FlightStreamParams{
		TableName: "catalog.schema.table",
		Schema:    &flight.FlightData{DataHeader: []byte("schema")},
	})
	if err != nil {
		t.Fatalf("OpenFlight: %v", err)
	}
	<-srv.seen

	for offset, body := range [][]byte{[]byte("first"), []byte("second")} {
		appMetadata, err := json.Marshal(transport.FlightBatchMetadata{OffsetID: int64(offset)})
		if err != nil {
			t.Fatalf("marshal offset %d: %v", offset, err)
		}
		if err := stream.Send(&flight.FlightData{
			DataHeader:  []byte("record-batch"),
			AppMetadata: appMetadata,
			DataBody:    body,
		}); err != nil {
			t.Fatalf("Send offset %d: %v", offset, err)
		}
	}

	for wantOffset, wantBody := range [][]byte{[]byte("first"), []byte("second")} {
		request := <-srv.received
		batchMetadata, err := transport.ParseFlightBatchMetadata(request.GetAppMetadata())
		if err != nil {
			t.Fatalf("parse request metadata: %v", err)
		}
		if batchMetadata.OffsetID != int64(wantOffset) {
			t.Errorf("request offset = %d, want %d", batchMetadata.OffsetID, wantOffset)
		}
		if string(request.GetDataBody()) != string(wantBody) {
			t.Errorf("request body = %q, want %q", request.GetDataBody(), wantBody)
		}

		result, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv offset %d: %v", wantOffset, err)
		}
		ack, err := transport.ParseFlightAckMetadata(result.GetAppMetadata())
		if err != nil {
			t.Fatalf("parse ack metadata: %v", err)
		}
		if ack.AckUpToOffset != int64(wantOffset) {
			t.Errorf("ack offset = %d, want %d", ack.AckUpToOffset, wantOffset)
		}
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := stream.GracefulClose(closeCtx); err != nil {
		t.Fatalf("GracefulClose: %v", err)
	}
	stream.Close()
}

func TestFlightBatchMetadataStrictValidation(t *testing.T) {
	for _, test := range []struct {
		name     string
		metadata string
		wantErr  bool
	}{
		{name: "valid", metadata: `{"offset_id":0}`},
		{name: "missing", metadata: `{}`, wantErr: true},
		{name: "negative", metadata: `{"offset_id":-1}`, wantErr: true},
		{name: "null", metadata: `{"offset_id":null}`, wantErr: true},
		{name: "fractional", metadata: `{"offset_id":1.5}`, wantErr: true},
		{name: "unknown is additive", metadata: `{"offset_id":0,"other":1}`},
		{name: "duplicate known", metadata: `{"offset_id":0,"offset_id":0}`, wantErr: true},
		{name: "duplicate unknown", metadata: `{"offset_id":0,"other":1,"other":2}`, wantErr: true},
		{name: "trailing", metadata: `{"offset_id":0} true`, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := transport.ParseFlightBatchMetadata([]byte(test.metadata))
			if (err != nil) != test.wantErr {
				t.Fatalf("ParseFlightBatchMetadata error = %v, wantErr=%v", err, test.wantErr)
			}
		})
	}
}

func TestFlightAckMetadataCompatibleStrictValidation(t *testing.T) {
	for _, test := range []struct {
		name     string
		metadata string
		wantErr  bool
	}{
		{
			name:     "valid",
			metadata: `{"ack_up_to_offset":0,"ack_up_to_records":1}`,
		},
		{
			name:     "unknown fields are additive",
			metadata: `{"ack_up_to_offset":0,"future":{"nested":true},"ack_up_to_records":1}`,
		},
		{
			name:     "close-only sentinel",
			metadata: `{"ack_up_to_offset":-1,"ack_up_to_records":0,"close_stream_duration_ms":1}`,
		},
		{name: "missing offset", metadata: `{"ack_up_to_records":0}`, wantErr: true},
		{name: "missing records", metadata: `{"ack_up_to_offset":0}`, wantErr: true},
		{name: "null offset", metadata: `{"ack_up_to_offset":null,"ack_up_to_records":0}`, wantErr: true},
		{name: "null records", metadata: `{"ack_up_to_offset":0,"ack_up_to_records":null}`, wantErr: true},
		{
			name:     "null close",
			metadata: `{"ack_up_to_offset":0,"ack_up_to_records":0,"close_stream_duration_ms":null}`,
			wantErr:  true,
		},
		{name: "bad offset type", metadata: `{"ack_up_to_offset":"0","ack_up_to_records":0}`, wantErr: true},
		{name: "fractional records", metadata: `{"ack_up_to_offset":0,"ack_up_to_records":1.5}`, wantErr: true},
		{name: "negative offset range", metadata: `{"ack_up_to_offset":-2,"ack_up_to_records":0}`, wantErr: true},
		{name: "negative records range", metadata: `{"ack_up_to_offset":0,"ack_up_to_records":-1}`, wantErr: true},
		{
			name:     "close duration overflow",
			metadata: `{"ack_up_to_offset":0,"ack_up_to_records":0,"close_stream_duration_ms":9223372036855}`,
			wantErr:  true,
		},
		{
			name:     "duplicate known",
			metadata: `{"ack_up_to_offset":0,"ack_up_to_records":0,"ack_up_to_records":0}`,
			wantErr:  true,
		},
		{
			name:     "duplicate unknown",
			metadata: `{"ack_up_to_offset":0,"future":1,"ack_up_to_records":0,"future":2}`,
			wantErr:  true,
		},
		{
			name:     "trailing JSON",
			metadata: `{"ack_up_to_offset":0,"ack_up_to_records":0} {}`,
			wantErr:  true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := transport.ParseFlightAckMetadata([]byte(test.metadata))
			if (err != nil) != test.wantErr {
				t.Fatalf(
					"ParseFlightAckMetadata error = %v, wantErr=%v",
					err,
					test.wantErr,
				)
			}
		})
	}
}
