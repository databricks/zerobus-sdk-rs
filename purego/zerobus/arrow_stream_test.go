package zerobus_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/databricks/zerobus-sdk/purego/internal/transport"
	"github.com/databricks/zerobus-sdk/purego/zerobus"
)

type publicFlightServer struct {
	flight.BaseFlightServer

	ackRecords  []uint64
	ackGate     <-chan struct{}
	partialAck  uint64
	partialSent chan struct{}
	schemas     chan *flight.FlightData
	dataFrames  chan *flight.FlightData
	readyOffset int64
}

func (s *publicFlightServer) DoPut(stream flight.FlightService_DoPutServer) error {
	schema, err := stream.Recv()
	if err != nil {
		return err
	}
	if s.schemas != nil {
		s.schemas <- proto.Clone(schema).(*flight.FlightData)
	}
	readyOffset := s.readyOffset
	if readyOffset == 0 {
		readyOffset = transport.FlightStreamReadyOffset
	}
	if err := stream.Send(&flight.PutResult{
		AppMetadata: arrowAckMetadata(readyOffset, 0),
	}); err != nil {
		return err
	}

	nextAck := 0
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if s.dataFrames != nil {
			s.dataFrames <- proto.Clone(frame).(*flight.FlightData)
		}
		metadata, err := transport.ParseFlightBatchMetadata(frame.GetAppMetadata())
		if err != nil {
			return err
		}
		if s.partialAck > 0 {
			if err := stream.Send(&flight.PutResult{
				AppMetadata: arrowAckMetadata(metadata.OffsetID, s.partialAck),
			}); err != nil {
				return err
			}
			s.partialAck = 0
			if s.partialSent != nil {
				close(s.partialSent)
			}
			if s.ackGate != nil {
				select {
				case <-s.ackGate:
				case <-stream.Context().Done():
					return stream.Context().Err()
				}
			}
		}
		if nextAck >= len(s.ackRecords) {
			continue
		}
		if err := stream.Send(&flight.PutResult{
			AppMetadata: arrowAckMetadata(metadata.OffsetID, s.ackRecords[nextAck]),
		}); err != nil {
			return err
		}
		nextAck++
	}
}

func arrowAckMetadata(offset int64, records uint64) []byte {
	data, err := json.Marshal(transport.FlightAckMetadata{
		AckUpToOffset: offset, AckUpToRecords: records,
	})
	if err != nil {
		panic(err)
	}
	return data
}

func dialPublicFlight(t *testing.T, server *publicFlightServer) *transport.Conn {
	t.Helper()
	listener := bufconn.Listen(16 * 1024 * 1024)
	grpcServer := grpc.NewServer()
	flight.RegisterFlightServiceServer(grpcServer, server)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := grpcServer.Serve(listener); err != nil &&
			!errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("Flight server stopped: %v", err)
		}
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		<-done
	})

	dialer := grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	})
	conn, err := transport.Dial(
		"passthrough:///public-flight",
		transport.WithGRPCDialOptions(
			dialer,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		),
	)
	if err != nil {
		t.Fatalf("transport.Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func newPublicArrowSDK(
	t *testing.T,
	server *publicFlightServer,
) *zerobus.SDK {
	t.Helper()
	return zerobus.NewWithConn(
		dialPublicFlight(t, server),
		"https://workspace.zerobus.databricks.com",
		"https://workspace.databricks.com",
	)
}

func publicArrowSchema(metadata *arrow.Metadata) *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{{
		Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false,
	}}, metadata)
}

func publicArrowBatch(
	allocator memory.Allocator,
	schema *arrow.Schema,
	values ...int32,
) arrow.RecordBatch {
	builder := array.NewInt32Builder(allocator)
	builder.AppendValues(values, nil)
	column := builder.NewArray()
	builder.Release()
	batch := array.NewRecordBatch(schema, []arrow.Array{column}, int64(len(values)))
	column.Release()
	return batch
}

func publicArrowBatchIPC(
	t *testing.T,
	schema *arrow.Schema,
	batches ...arrow.RecordBatch,
) []byte {
	t.Helper()
	var output bytes.Buffer
	writer := ipc.NewWriter(&output, ipc.WithSchema(schema))
	for _, batch := range batches {
		if err := writer.Write(batch); err != nil {
			t.Fatalf("IPC Write: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("IPC Close: %v", err)
	}
	return bytes.Clone(output.Bytes())
}

func publicArrowIDs(t *testing.T, batch arrow.RecordBatch) []int32 {
	t.Helper()
	values, ok := batch.Column(0).(*array.Int32)
	if !ok {
		t.Fatalf("column type = %T, want *array.Int32", batch.Column(0))
	}
	return slices.Clone(values.Int32Values())
}

func staticArrowProvider() zerobus.HeadersProvider {
	return zerobus.NewStaticHeadersProvider(
		map[string]string{"authorization": "Bearer test-token"},
	)
}

type publicCallbackError struct {
	offset int64
	err    error
}

type publicRecordingCallback struct {
	acks chan int64
	errs chan publicCallbackError
}

func (c *publicRecordingCallback) OnAck(offset int64) {
	c.acks <- offset
}

func (c *publicRecordingCallback) OnError(offset int64, err error) {
	c.errs <- publicCallbackError{offset: offset, err: err}
}

func TestArrowStreamQueueFlushAndFullOffsetCallbacks(t *testing.T) {
	server := &publicFlightServer{
		ackRecords: []uint64{2, 4},
		schemas:    make(chan *flight.FlightData, 1),
		dataFrames: make(chan *flight.FlightData, 2),
	}
	sdk := newPublicArrowSDK(t, server)
	defer sdk.Close()

	callbacks := &publicRecordingCallback{
		acks: make(chan int64, 4),
		errs: make(chan publicCallbackError, 1),
	}
	schema := publicArrowSchema(nil)
	st, err := sdk.CreateArrowStreamWithProvider(
		context.Background(),
		"main.sales.orders",
		schema,
		staticArrowProvider(),
		zerobus.WithWaitForReady(),
		zerobus.WithAckCallback(callbacks),
	)
	if err != nil {
		t.Fatalf("CreateArrowStreamWithProvider: %v", err)
	}
	if st.ID() == "" {
		t.Fatal("ID is empty")
	}
	if st.ServerID() != "flight-do-put" {
		t.Fatalf("ServerID = %q, want flight-do-put", st.ServerID())
	}

	for index, values := range [][]int32{{1, 2}, {3, 4}} {
		batch := publicArrowBatch(memory.DefaultAllocator, schema, values...)
		offset, err := st.IngestBatch(batch)
		batch.Release()
		if err != nil {
			t.Fatalf("IngestBatch[%d]: %v", index, err)
		}
		if offset != int64(index) {
			t.Fatalf("offset[%d] = %d, want %d", index, offset, index)
		}
	}
	if err := st.WaitForOffset(1); err != nil {
		t.Fatalf("WaitForOffset: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	for want := int64(0); want <= 1; want++ {
		select {
		case got := <-callbacks.acks:
			if got != want {
				t.Fatalf("callback offset = %d, want %d", got, want)
			}
		case callbackErr := <-callbacks.errs:
			t.Fatalf("callback error = %+v", callbackErr)
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for callback %d", want)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !st.IsClosed() {
		t.Fatal("IsClosed = false after Close")
	}
	if got := sdk.OpenStreamCount(); got != 0 {
		t.Fatalf("tracked streams = %d, want 0", got)
	}
}

func TestArrowCallbackWaitsForCompleteLogicalOffset(t *testing.T) {
	fullAck := make(chan struct{})
	partialSent := make(chan struct{})
	server := &publicFlightServer{
		partialAck:  1,
		ackRecords:  []uint64{2},
		ackGate:     fullAck,
		partialSent: partialSent,
	}
	sdk := newPublicArrowSDK(t, server)
	defer sdk.Close()
	callbacks := &publicRecordingCallback{
		acks: make(chan int64, 1),
		errs: make(chan publicCallbackError, 1),
	}
	schema := publicArrowSchema(nil)
	st, err := sdk.CreateArrowStreamWithProvider(
		context.Background(),
		"main.sales.orders",
		schema,
		staticArrowProvider(),
		zerobus.WithWaitForReady(),
		zerobus.WithAckCallback(callbacks),
	)
	if err != nil {
		t.Fatalf("CreateArrowStreamWithProvider: %v", err)
	}
	defer st.Close()
	batch := publicArrowBatch(memory.DefaultAllocator, schema, 1, 2)
	if _, err := st.IngestBatch(batch); err != nil {
		batch.Release()
		t.Fatalf("IngestBatch: %v", err)
	}
	batch.Release()

	select {
	case <-partialSent:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not send partial ack")
	}
	select {
	case offset := <-callbacks.acks:
		t.Fatalf("partial row ack triggered offset callback %d", offset)
	case <-time.After(100 * time.Millisecond):
	}
	close(fullAck)
	select {
	case offset := <-callbacks.acks:
		if offset != 0 {
			t.Fatalf("callback offset = %d, want 0", offset)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("complete batch ack did not trigger callback")
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestArrowTypedAndIPCBatchValidation(t *testing.T) {
	server := &publicFlightServer{ackRecords: []uint64{2}}
	sdk := newPublicArrowSDK(t, server)
	defer sdk.Close()
	schema := publicArrowSchema(nil)
	st, err := sdk.CreateArrowStreamWithProvider(
		context.Background(),
		"main.sales.orders",
		schema,
		staticArrowProvider(),
		zerobus.WithWaitForReady(),
		zerobus.WithMaxPayloadBytes(1),
	)
	if err != nil {
		t.Fatalf("CreateArrowStreamWithProvider: %v", err)
	}
	defer st.Close()

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if offset, err := st.IngestBatchContext(cancelled, nil); offset != -1 ||
		!errors.Is(err, context.Canceled) {
		t.Fatalf(
			"cancelled nil IngestBatchContext = (%d,%v), want (-1, context.Canceled)",
			offset,
			err,
		)
	}
	if offset, err := st.IngestIPCBatchContext(cancelled, []byte("invalid")); offset != -1 ||
		!errors.Is(err, context.Canceled) {
		t.Fatalf(
			"cancelled invalid IngestIPCBatchContext = (%d,%v), want (-1, context.Canceled)",
			offset,
			err,
		)
	}

	empty := publicArrowBatch(memory.DefaultAllocator, schema)
	if offset, err := st.IngestBatch(empty); err == nil || offset != -1 {
		t.Fatalf("empty IngestBatch = (%d, %v), want (-1, error)", offset, err)
	}
	empty.Release()

	wrongSchema := arrow.NewSchema([]arrow.Field{{
		Name: "other", Type: arrow.PrimitiveTypes.Int32, Nullable: false,
	}}, nil)
	wrong := publicArrowBatch(memory.DefaultAllocator, wrongSchema, 1)
	if offset, err := st.IngestBatch(wrong); err == nil || offset != -1 {
		t.Fatalf("mismatched IngestBatch = (%d, %v), want (-1, error)", offset, err)
	}
	wrong.Release()

	if offset, err := st.IngestIPCBatch(nil); err == nil || offset != -1 {
		t.Fatalf("empty IngestIPCBatch = (%d, %v), want (-1, error)", offset, err)
	}
	wrongIPCBatch := publicArrowBatch(memory.DefaultAllocator, wrongSchema, 2)
	wrongIPC := publicArrowBatchIPC(t, wrongSchema, wrongIPCBatch)
	wrongIPCBatch.Release()
	if offset, err := st.IngestIPCBatch(wrongIPC); err == nil || offset != -1 {
		t.Fatalf("mismatched IngestIPCBatch = (%d, %v), want (-1, error)", offset, err)
	}

	valid := publicArrowBatch(memory.DefaultAllocator, schema, 10, 20)
	validIPC := publicArrowBatchIPC(t, schema, valid)
	valid.Release()
	offset, err := st.IngestIPCBatchContext(context.Background(), validIPC)
	if err != nil {
		t.Fatalf("IngestIPCBatchContext: %v", err)
	}
	if offset != 0 {
		t.Fatalf("IPC offset = %d, want 0", offset)
	}
	clear(validIPC)
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestArrowSchemaIPCConstructorsAndOptions(t *testing.T) {
	metadata := arrow.NewMetadata([]string{"owner"}, []string{"analytics"})
	schema := publicArrowSchema(&metadata)
	schemaIPC, err := zerobus.EncodeArrowSchemaIPC(schema)
	if err != nil {
		t.Fatalf("EncodeArrowSchemaIPC: %v", err)
	}
	decoded, err := zerobus.DecodeArrowSchemaIPC(schemaIPC)
	if err != nil {
		t.Fatalf("DecodeArrowSchemaIPC: %v", err)
	}
	if !decoded.Equal(schema) || !decoded.Metadata().Equal(schema.Metadata()) {
		t.Fatalf("decoded schema = %v, want %v", decoded, schema)
	}

	batch := publicArrowBatch(memory.DefaultAllocator, schema, 1)
	notSchemaOnly := publicArrowBatchIPC(t, schema, batch)
	batch.Release()
	if _, err := zerobus.DecodeArrowSchemaIPC(notSchemaOnly); err == nil {
		t.Fatal("DecodeArrowSchemaIPC accepted a RecordBatch")
	}

	server := &publicFlightServer{}
	sdk := newPublicArrowSDK(t, server)
	defer sdk.Close()
	if _, err := sdk.CreateArrowStreamWithProvider(
		context.Background(), "", schema, staticArrowProvider(),
	); err == nil {
		t.Fatal("blank table accepted")
	}
	if _, err := sdk.CreateArrowStreamWithProvider(
		context.Background(), "main.sales.orders", nil, staticArrowProvider(),
	); err == nil {
		t.Fatal("nil schema accepted")
	}
	if _, err := sdk.CreateArrowStreamWithProvider(
		context.Background(), "main.sales.orders", schema, nil,
	); err == nil {
		t.Fatal("nil provider accepted")
	}
	if _, err := sdk.CreateArrowStreamWithProvider(
		context.Background(),
		"main.sales.orders",
		schema,
		staticArrowProvider(),
		zerobus.WithArrowCompression(zerobus.ArrowCompression(99)),
	); err == nil {
		t.Fatal("invalid Arrow compression accepted")
	}
	if _, err := sdk.CreateArrowStreamFromIPCWithProvider(
		context.Background(),
		"main.sales.orders",
		nil,
		staticArrowProvider(),
	); err == nil {
		t.Fatal("empty schema IPC accepted")
	}

	st, err := sdk.CreateArrowStreamFromIPCWithProvider(
		context.Background(),
		"main.sales.orders",
		schemaIPC,
		staticArrowProvider(),
		zerobus.WithWaitForReady(),
	)
	if err != nil {
		t.Fatalf("CreateArrowStreamFromIPCWithProvider: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close IPC-schema stream: %v", err)
	}

	maxInflight, maxPayload, compression, recoveryTimeout :=
		zerobus.ResolveArrowStreamConfig()
	if maxInflight != 1_000 {
		t.Fatalf("default Arrow MaxInflight = %d, want 1000", maxInflight)
	}
	if maxPayload != math.MaxInt {
		t.Fatalf("default Arrow MaxPayloadBytes = %d, want MaxInt", maxPayload)
	}
	if compression != zerobus.ArrowCompressionNone {
		t.Fatalf("default Arrow compression = %d, want none", compression)
	}
	if recoveryTimeout != 15*time.Second {
		t.Fatalf("default recovery timeout = %v, want 15s", recoveryTimeout)
	}

	maxInflight, maxPayload, compression, recoveryTimeout =
		zerobus.ResolveArrowStreamConfig(
			zerobus.WithMaxInflight(7),
			zerobus.WithMaxPayloadBytes(1),
			zerobus.WithArrowCompression(zerobus.ArrowCompressionZstd),
			zerobus.WithArrowConnectionTimeout(9*time.Second),
		)
	if maxInflight != 7 || maxPayload != math.MaxInt ||
		compression != zerobus.ArrowCompressionZstd ||
		recoveryTimeout != 9*time.Second {
		t.Fatalf(
			"resolved Arrow options = (%d,%d,%d,%v)",
			maxInflight, maxPayload, compression, recoveryTimeout,
		)
	}
	maxInflight, _, _, _ = zerobus.ResolveArrowStreamConfig(
		zerobus.WithMaxInflight(0),
	)
	if maxInflight != 1_000 {
		t.Fatalf("non-positive Arrow MaxInflight = %d, want 1000", maxInflight)
	}
}

func TestArrowUnackedOwnershipAndCopiesAfterClose(t *testing.T) {
	sdk := newPublicArrowSDK(t, &publicFlightServer{})
	defer sdk.Close()
	schema := publicArrowSchema(nil)
	st, err := sdk.CreateArrowStreamWithProvider(
		context.Background(),
		"main.sales.orders",
		schema,
		staticArrowProvider(),
		zerobus.WithWaitForReady(),
		zerobus.WithRecovery(zerobus.RecoveryDisabled),
		zerobus.WithMaxInflight(1),
		zerobus.WithFlushTimeout(20*time.Millisecond),
		zerobus.WithLackOfAckTimeout(time.Hour),
	)
	if err != nil {
		t.Fatalf("CreateArrowStreamWithProvider: %v", err)
	}

	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	batch := publicArrowBatch(allocator, schema, 5, 6, 7)
	if offset, err := st.IngestBatch(batch); err != nil || offset != 0 {
		batch.Release()
		t.Fatalf("IngestBatch = (%d, %v), want (0, nil)", offset, err)
	}
	batch.Release()
	allocator.AssertSize(t, 0)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	secondBatch := publicArrowBatch(memory.DefaultAllocator, schema, 8)
	if offset, err := st.IngestBatchContext(cancelled, secondBatch); err == nil || offset != -1 {
		secondBatch.Release()
		t.Fatalf("cancelled IngestBatchContext = (%d, %v), want (-1, error)", offset, err)
	}
	secondBatch.Release()

	if _, err := st.GetUnackedBatches(); err == nil {
		t.Fatal("GetUnackedBatches succeeded on active stream")
	}
	if err := st.Close(); err == nil {
		t.Fatal("Close unexpectedly succeeded without an ack")
	}
	if !st.IsClosed() {
		t.Fatal("stream is active after Close")
	}
	if got := sdk.OpenStreamCount(); got != 0 {
		t.Fatalf("tracked streams = %d after Close, want 0", got)
	}

	first, err := st.GetUnackedIPCBatches()
	if err != nil {
		t.Fatalf("first GetUnackedIPCBatches: %v", err)
	}
	second, err := st.GetUnackedIPCBatches()
	if err != nil {
		t.Fatalf("second GetUnackedIPCBatches: %v", err)
	}
	if len(first) != 1 || len(second) != 1 || !bytes.Equal(first[0], second[0]) {
		t.Fatalf("unacked IPC batches differ: first=%d second=%d", len(first), len(second))
	}
	first[0][0] ^= 0xff
	third, err := st.GetUnackedIPCBatches()
	if err != nil {
		t.Fatalf("third GetUnackedIPCBatches: %v", err)
	}
	if bytes.Equal(first[0], third[0]) || !bytes.Equal(second[0], third[0]) {
		t.Fatal("GetUnackedIPCBatches returned aliased storage")
	}

	typed, err := st.GetUnackedBatches()
	if err != nil {
		t.Fatalf("GetUnackedBatches: %v", err)
	}
	if len(typed) != 1 || !slices.Equal(publicArrowIDs(t, typed[0]), []int32{5, 6, 7}) {
		t.Fatalf("typed unacked batches = %d", len(typed))
	}
	for _, owned := range typed {
		owned.Release()
	}
}

func TestSDKCloseOwnsArrowStreams(t *testing.T) {
	sdk := newPublicArrowSDK(t, &publicFlightServer{})
	schema := publicArrowSchema(nil)
	st, err := sdk.CreateArrowStreamWithProvider(
		context.Background(),
		"main.sales.orders",
		schema,
		staticArrowProvider(),
		zerobus.WithWaitForReady(),
		zerobus.WithRecovery(zerobus.RecoveryDisabled),
		zerobus.WithLackOfAckTimeout(time.Hour),
	)
	if err != nil {
		t.Fatalf("CreateArrowStreamWithProvider: %v", err)
	}
	batch := publicArrowBatch(memory.DefaultAllocator, schema, 8, 9)
	if _, err := st.IngestBatch(batch); err != nil {
		batch.Release()
		t.Fatalf("IngestBatch: %v", err)
	}
	batch.Release()
	if got := sdk.OpenStreamCount(); got != 1 {
		t.Fatalf("tracked streams = %d, want 1", got)
	}

	if err := sdk.Close(); err != nil {
		t.Fatalf("SDK.Close: %v", err)
	}
	if !st.IsClosed() {
		t.Fatal("Arrow stream remains open after SDK.Close")
	}
	if got := sdk.OpenStreamCount(); got != 0 {
		t.Fatalf("tracked streams = %d after SDK.Close, want 0", got)
	}
	ipcBatches, err := st.GetUnackedIPCBatches()
	if err != nil {
		t.Fatalf("GetUnackedIPCBatches after SDK.Close: %v", err)
	}
	if len(ipcBatches) != 1 {
		t.Fatalf("unacked IPC batches = %d, want 1", len(ipcBatches))
	}
	if _, err := sdk.CreateArrowStreamWithProvider(
		context.Background(),
		"main.sales.orders",
		schema,
		staticArrowProvider(),
	); err == nil {
		t.Fatal("CreateArrowStreamWithProvider succeeded after SDK.Close")
	}
}
