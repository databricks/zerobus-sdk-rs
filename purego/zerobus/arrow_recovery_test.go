package zerobus_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/databricks/zerobus-sdk/purego/internal/transport"
	"github.com/databricks/zerobus-sdk/purego/zerobus"
)

type recoveryFlightHandler func(
	connection int,
	stream flight.FlightService_DoPutServer,
	schema *flight.FlightData,
) error

type recoveryFlightServer struct {
	flight.BaseFlightServer

	connections atomic.Int64
	handler     recoveryFlightHandler

	errMu sync.Mutex
	err   error
}

func (s *recoveryFlightServer) DoPut(stream flight.FlightService_DoPutServer) error {
	connection := int(s.connections.Add(1) - 1)
	schema, err := stream.Recv()
	if err != nil {
		return err
	}
	if s.handler == nil {
		return nil
	}
	if err := s.handler(connection, stream, schema); err != nil {
		// Scripted gRPC statuses are part of the scenario under test and must
		// reach the client unchanged. Plain errors indicate a mock assertion or
		// decoding failure and are retained for a direct test failure.
		if status.Code(err) != codes.Unknown ||
			errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		s.errMu.Lock()
		if s.err == nil {
			s.err = err
		}
		s.errMu.Unlock()
		return status.Error(codes.Internal, err.Error())
	}
	return nil
}

func (s *recoveryFlightServer) connectionCount() int {
	return int(s.connections.Load())
}

func (s *recoveryFlightServer) assertNoHandlerError(t *testing.T) {
	t.Helper()
	s.errMu.Lock()
	defer s.errMu.Unlock()
	if s.err != nil {
		t.Fatalf("mock Flight handler: %v", s.err)
	}
}

func dialRecoveryFlight(
	t *testing.T,
	server flight.FlightServer,
) *transport.Conn {
	t.Helper()
	listener := bufconn.Listen(32 * 1024 * 1024)
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

	conn, err := transport.Dial(
		"passthrough:///arrow-recovery",
		transport.WithGRPCDialOptions(
			grpc.WithContextDialer(func(
				ctx context.Context,
				_ string,
			) (net.Conn, error) {
				return listener.DialContext(ctx)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		),
	)
	if err != nil {
		t.Fatalf("transport.Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func newRecoveryArrowSDK(
	t *testing.T,
	server flight.FlightServer,
) *zerobus.SDK {
	t.Helper()
	return zerobus.NewWithConn(
		dialRecoveryFlight(t, server),
		"https://workspace.zerobus.databricks.com",
		"https://workspace.databricks.com",
	)
}

func sendFlightReady(stream flight.FlightService_DoPutServer) error {
	return stream.Send(&flight.PutResult{
		AppMetadata: arrowAckMetadata(transport.FlightStreamReadyOffset, 0),
	})
}

func sendFlightAck(
	stream flight.FlightService_DoPutServer,
	offset int64,
	records uint64,
) error {
	return stream.Send(&flight.PutResult{
		AppMetadata: arrowAckMetadata(offset, records),
	})
}

func sendFlightPause(
	stream flight.FlightService_DoPutServer,
	offset int64,
	records uint64,
	duration time.Duration,
) error {
	millis := uint64(duration / time.Millisecond)
	data, err := json.Marshal(transport.FlightAckMetadata{
		AckUpToOffset:         offset,
		AckUpToRecords:        records,
		CloseStreamDurationMS: &millis,
	})
	if err != nil {
		return err
	}
	return stream.Send(&flight.PutResult{AppMetadata: data})
}

type recoveryFlightDataReader struct {
	frames []*flight.FlightData
	next   int
}

func (r *recoveryFlightDataReader) Recv() (*flight.FlightData, error) {
	if r.next >= len(r.frames) {
		return nil, io.EOF
	}
	frame := r.frames[r.next]
	r.next++
	return frame, nil
}

func decodeFlightFrame(
	schema *flight.FlightData,
	frame *flight.FlightData,
) (rows int64, ids []int32, err error) {
	reader, err := flight.NewRecordReader(&recoveryFlightDataReader{
		frames: []*flight.FlightData{schema, frame},
	})
	if err != nil {
		return 0, nil, err
	}
	defer reader.Release()
	for reader.Next() {
		batch := reader.RecordBatch()
		rows += batch.NumRows()
		if len(batch.Columns()) > 0 {
			if column, ok := batch.Column(0).(*array.Int32); ok {
				ids = append(ids, column.Int32Values()...)
			}
		}
	}
	if err := reader.Err(); err != nil {
		return 0, nil, err
	}
	return rows, slices.Clone(ids), nil
}

func receiveFlightFrame(
	stream flight.FlightService_DoPutServer,
	schema *flight.FlightData,
) (*flight.FlightData, transport.FlightBatchMetadata, int64, []int32, error) {
	frame, err := stream.Recv()
	if err != nil {
		return nil, transport.FlightBatchMetadata{}, 0, nil, err
	}
	frameMetadata, err := transport.ParseFlightBatchMetadata(frame.GetAppMetadata())
	if err != nil {
		return nil, transport.FlightBatchMetadata{}, 0, nil, err
	}
	rows, ids, err := decodeFlightFrame(schema, frame)
	return frame, frameMetadata, rows, ids, err
}

func drainFlightInput(stream flight.FlightService_DoPutServer) error {
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func waitFlightCondition(
	t *testing.T,
	timeout time.Duration,
	condition func() bool,
	description string,
) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

func createRecoveryArrowStream(
	t *testing.T,
	sdk *zerobus.SDK,
	schema *arrow.Schema,
	provider zerobus.HeadersProvider,
	options ...zerobus.StreamOption,
) *zerobus.ArrowStream {
	t.Helper()
	options = append([]zerobus.StreamOption{
		zerobus.WithWaitForReady(),
		zerobus.WithRecoveryBackoff(time.Millisecond),
		zerobus.WithRecoveryTimeout(time.Second),
		zerobus.WithRecoveryRetries(4),
		zerobus.WithFlushTimeout(5 * time.Second),
	}, options...)
	stream, err := sdk.CreateArrowStreamWithProvider(
		context.Background(),
		"main.sales.orders",
		schema,
		provider,
		options...,
	)
	if err != nil {
		t.Fatalf("CreateArrowStreamWithProvider: %v", err)
	}
	return stream
}

func ingestIDBatch(
	t *testing.T,
	stream *zerobus.ArrowStream,
	schema *arrow.Schema,
	values ...int32,
) int64 {
	t.Helper()
	batch := publicArrowBatch(memory.DefaultAllocator, schema, values...)
	offset, err := stream.IngestBatch(batch)
	batch.Release()
	if err != nil {
		t.Fatalf("IngestBatch: %v", err)
	}
	return offset
}

func TestArrowRecoveryPartialAckDisconnectSlicesReplay(t *testing.T) {
	partialSent := make(chan struct{})
	server := &recoveryFlightServer{}
	server.handler = func(
		connection int,
		stream flight.FlightService_DoPutServer,
		schema *flight.FlightData,
	) error {
		if err := sendFlightReady(stream); err != nil {
			return err
		}
		switch connection {
		case 0:
			_, frameMetadata, _, ids, err := receiveFlightFrame(stream, schema)
			if err != nil {
				return err
			}
			if frameMetadata.OffsetID != 0 ||
				!slices.Equal(ids, []int32{1, 2, 3, 4, 5}) {
				return fmt.Errorf(
					"first connection frame offset=%d ids=%v",
					frameMetadata.OffsetID,
					ids,
				)
			}
			if err := sendFlightAck(stream, 0, 2); err != nil {
				return err
			}
			close(partialSent)
			return status.Error(codes.Unavailable, "disconnect after partial ack")
		case 1:
			expected := [][]int32{{3, 4, 5}, {6, 7}}
			var cumulative uint64
			for frameIndex, wantIDs := range expected {
				_, frameMetadata, rows, ids, err := receiveFlightFrame(stream, schema)
				if err != nil {
					return err
				}
				if frameMetadata.OffsetID != int64(frameIndex) ||
					!slices.Equal(ids, wantIDs) {
					return fmt.Errorf(
						"replay frame %d offset=%d ids=%v, want offset=%d ids=%v",
						frameIndex,
						frameMetadata.OffsetID,
						ids,
						frameIndex,
						wantIDs,
					)
				}
				cumulative += uint64(rows)
				if err := sendFlightAck(
					stream,
					frameMetadata.OffsetID,
					cumulative,
				); err != nil {
					return err
				}
			}
			return drainFlightInput(stream)
		default:
			return fmt.Errorf("unexpected connection %d", connection)
		}
	}

	sdk := newRecoveryArrowSDK(t, server)
	defer sdk.Close()
	schema := publicArrowSchema(nil)
	stream := createRecoveryArrowStream(
		t,
		sdk,
		schema,
		staticArrowProvider(),
	)

	if offset := ingestIDBatch(t, stream, schema, 1, 2, 3, 4, 5); offset != 0 {
		t.Fatalf("first logical offset = %d, want 0", offset)
	}
	select {
	case <-partialSent:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not send partial acknowledgment")
	}
	if offset := ingestIDBatch(t, stream, schema, 6, 7); offset != 1 {
		t.Fatalf("second logical offset = %d, want 1", offset)
	}
	if err := stream.Flush(); err != nil {
		t.Fatalf("Flush after sliced replay: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := server.connectionCount(); got != 2 {
		t.Fatalf("DoPut connections = %d, want 2", got)
	}
	server.assertNoHandlerError(t)
}

func TestArrowInlinePartialAckPauseSlicesReplay(t *testing.T) {
	server := &recoveryFlightServer{}
	server.handler = func(
		connection int,
		stream flight.FlightService_DoPutServer,
		schema *flight.FlightData,
	) error {
		if err := sendFlightReady(stream); err != nil {
			return err
		}
		_, metadata, rows, ids, err := receiveFlightFrame(stream, schema)
		if err != nil {
			return err
		}
		switch connection {
		case 0:
			if rows != 5 || !slices.Equal(ids, []int32{1, 2, 3, 4, 5}) {
				return fmt.Errorf("first connection rows=%d ids=%v", rows, ids)
			}
			if err := sendFlightPause(stream, metadata.OffsetID, 2, 0); err != nil {
				return err
			}
			return drainFlightInput(stream)
		case 1:
			if rows != 3 || !slices.Equal(ids, []int32{3, 4, 5}) {
				return fmt.Errorf("replayed rows=%d ids=%v, want suffix [3 4 5]", rows, ids)
			}
			if err := sendFlightAck(stream, metadata.OffsetID, 3); err != nil {
				return err
			}
			return drainFlightInput(stream)
		default:
			return fmt.Errorf("unexpected connection %d", connection)
		}
	}

	sdk := newRecoveryArrowSDK(t, server)
	defer sdk.Close()
	schema := publicArrowSchema(nil)
	stream := createRecoveryArrowStream(t, sdk, schema, staticArrowProvider())
	ingestIDBatch(t, stream, schema, 1, 2, 3, 4, 5)
	if err := stream.Flush(); err != nil {
		t.Fatalf("Flush after inline partial ACK+pause: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := server.connectionCount(); got != 2 {
		t.Fatalf("DoPut connections = %d, want 2", got)
	}
	server.assertNoHandlerError(t)
}

func TestArrowInlineFullAckPauseRotatesWithoutReplay(t *testing.T) {
	secondReady := make(chan struct{})
	server := &recoveryFlightServer{}
	server.handler = func(
		connection int,
		stream flight.FlightService_DoPutServer,
		schema *flight.FlightData,
	) error {
		if err := sendFlightReady(stream); err != nil {
			return err
		}
		switch connection {
		case 0:
			_, metadata, rows, ids, err := receiveFlightFrame(stream, schema)
			if err != nil {
				return err
			}
			if rows != 2 || !slices.Equal(ids, []int32{1, 2}) {
				return fmt.Errorf("first connection rows=%d ids=%v", rows, ids)
			}
			if err := sendFlightPause(stream, metadata.OffsetID, 2, 0); err != nil {
				return err
			}
			return drainFlightInput(stream)
		case 1:
			close(secondReady)
			_, metadata, rows, ids, err := receiveFlightFrame(stream, schema)
			if err != nil {
				return err
			}
			if rows != 1 || !slices.Equal(ids, []int32{3}) {
				return fmt.Errorf(
					"rotated connection rows=%d ids=%v, want only new batch [3]",
					rows,
					ids,
				)
			}
			if err := sendFlightAck(stream, metadata.OffsetID, 1); err != nil {
				return err
			}
			return drainFlightInput(stream)
		default:
			return fmt.Errorf("unexpected connection %d", connection)
		}
	}

	sdk := newRecoveryArrowSDK(t, server)
	defer sdk.Close()
	schema := publicArrowSchema(nil)
	stream := createRecoveryArrowStream(t, sdk, schema, staticArrowProvider())
	if offset := ingestIDBatch(t, stream, schema, 1, 2); offset != 0 {
		t.Fatalf("first logical offset = %d, want 0", offset)
	}
	if err := stream.WaitForOffset(0); err != nil {
		t.Fatalf("inline full ACK did not advance before pause: %v", err)
	}
	select {
	case <-secondReady:
	case <-time.After(5 * time.Second):
		t.Fatal("inline full ACK+pause did not rotate")
	}
	if offset := ingestIDBatch(t, stream, schema, 3); offset != 1 {
		t.Fatalf("second logical offset = %d, want 1", offset)
	}
	if err := stream.Flush(); err != nil {
		t.Fatalf("Flush after inline full ACK+pause: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	server.assertNoHandlerError(t)
}

func TestArrowCloseOnlyRotationReplaysPendingBatch(t *testing.T) {
	server := &recoveryFlightServer{}
	server.handler = func(
		connection int,
		stream flight.FlightService_DoPutServer,
		schema *flight.FlightData,
	) error {
		if err := sendFlightReady(stream); err != nil {
			return err
		}
		_, metadata, rows, ids, err := receiveFlightFrame(stream, schema)
		if err != nil {
			return err
		}
		if rows != 2 || !slices.Equal(ids, []int32{10, 20}) {
			return fmt.Errorf("connection %d rows=%d ids=%v", connection, rows, ids)
		}
		switch connection {
		case 0:
			if err := sendFlightPause(
				stream,
				transport.FlightStreamReadyOffset,
				0,
				0,
			); err != nil {
				return err
			}
			return drainFlightInput(stream)
		case 1:
			if metadata.OffsetID != 0 {
				return fmt.Errorf("replay frame offset=%d, want 0", metadata.OffsetID)
			}
			if err := sendFlightAck(stream, metadata.OffsetID, 2); err != nil {
				return err
			}
			return drainFlightInput(stream)
		default:
			return fmt.Errorf("unexpected connection %d", connection)
		}
	}

	sdk := newRecoveryArrowSDK(t, server)
	defer sdk.Close()
	schema := publicArrowSchema(nil)
	stream := createRecoveryArrowStream(t, sdk, schema, staticArrowProvider())
	ingestIDBatch(t, stream, schema, 10, 20)
	if err := stream.Flush(); err != nil {
		t.Fatalf("Flush after close-only rotation replay: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := server.connectionCount(); got != 2 {
		t.Fatalf("DoPut connections = %d, want 2", got)
	}
	server.assertNoHandlerError(t)
}

func largeStringBatch(
	schema *arrow.Schema,
	rows int,
	valueBytes int,
) arrow.RecordBatch {
	idBuilder := array.NewInt32Builder(memory.DefaultAllocator)
	valueBuilder := array.NewStringBuilder(memory.DefaultAllocator)
	value := strings.Repeat("x", valueBytes)
	for row := 0; row < rows; row++ {
		idBuilder.Append(int32(row))
		valueBuilder.Append(value)
	}
	ids := idBuilder.NewArray()
	values := valueBuilder.NewArray()
	idBuilder.Release()
	valueBuilder.Release()
	batch := array.NewRecordBatch(
		schema,
		[]arrow.Array{ids, values},
		int64(rows),
	)
	ids.Release()
	values.Release()
	return batch
}

func TestArrowMultiFrameChunkAcknowledgments(t *testing.T) {
	const rows = 6_000
	var receivedFrames atomic.Int64
	server := &recoveryFlightServer{}
	server.handler = func(
		connection int,
		stream flight.FlightService_DoPutServer,
		schemaFrame *flight.FlightData,
	) error {
		if connection != 0 {
			return fmt.Errorf("unexpected connection %d", connection)
		}
		if err := sendFlightReady(stream); err != nil {
			return err
		}
		var cumulative uint64
		var expectedOffset int64
		for cumulative < uint64(rows) {
			_, frameMetadata, frameRows, _, err := receiveFlightFrame(
				stream,
				schemaFrame,
			)
			if err != nil {
				return err
			}
			if frameMetadata.OffsetID != expectedOffset {
				return fmt.Errorf(
					"frame offset=%d, want %d",
					frameMetadata.OffsetID,
					expectedOffset,
				)
			}
			if frameRows <= 0 {
				return fmt.Errorf("frame %d decoded to %d rows", expectedOffset, frameRows)
			}
			cumulative += uint64(frameRows)
			receivedFrames.Add(1)
			if err := sendFlightAck(
				stream,
				frameMetadata.OffsetID,
				cumulative,
			); err != nil {
				return err
			}
			expectedOffset++
		}
		if cumulative != uint64(rows) {
			return fmt.Errorf("acknowledged rows=%d, want %d", cumulative, rows)
		}
		return drainFlightInput(stream)
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "value", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
	sdk := newRecoveryArrowSDK(t, server)
	defer sdk.Close()
	stream := createRecoveryArrowStream(
		t,
		sdk,
		schema,
		staticArrowProvider(),
	)
	batch := largeStringBatch(schema, rows, 512)
	offset, err := stream.IngestBatch(batch)
	batch.Release()
	if err != nil || offset != 0 {
		t.Fatalf("IngestBatch = (%d, %v), want (0, nil)", offset, err)
	}
	if err := stream.Flush(); err != nil {
		t.Fatalf("Flush multi-frame batch: %v", err)
	}
	if got := receivedFrames.Load(); got < 2 {
		t.Fatalf("received Flight frames = %d, want at least 2", got)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	server.assertNoHandlerError(t)
}

func TestArrowPauseRotationBoundsIngestAndHalfCloses(t *testing.T) {
	pauseSent := make(chan struct{})
	firstEOF := make(chan bool, 1)
	secondStarted := make(chan struct{})
	server := &recoveryFlightServer{}
	server.handler = func(
		connection int,
		stream flight.FlightService_DoPutServer,
		schema *flight.FlightData,
	) error {
		if err := sendFlightReady(stream); err != nil {
			return err
		}
		switch connection {
		case 0:
			_, frameMetadata, _, ids, err := receiveFlightFrame(stream, schema)
			if err != nil {
				return err
			}
			if frameMetadata.OffsetID != 0 || !slices.Equal(ids, []int32{1}) {
				return fmt.Errorf(
					"first connection offset=%d ids=%v",
					frameMetadata.OffsetID,
					ids,
				)
			}
			if err := sendFlightAck(stream, 0, 1); err != nil {
				return err
			}
			if err := sendFlightPause(stream, 0, 1, 150*time.Millisecond); err != nil {
				return err
			}
			close(pauseSent)
			_, err = stream.Recv()
			firstEOF <- err == io.EOF
			if err == io.EOF {
				return nil
			}
			return err
		case 1:
			close(secondStarted)
			var cumulative uint64
			for frameIndex, wantID := range []int32{2, 3, 4} {
				_, frameMetadata, rows, ids, err := receiveFlightFrame(stream, schema)
				if err != nil {
					return err
				}
				if frameMetadata.OffsetID != int64(frameIndex) ||
					!slices.Equal(ids, []int32{wantID}) {
					return fmt.Errorf(
						"rotated frame %d offset=%d ids=%v",
						frameIndex,
						frameMetadata.OffsetID,
						ids,
					)
				}
				cumulative += uint64(rows)
				if err := sendFlightAck(
					stream,
					frameMetadata.OffsetID,
					cumulative,
				); err != nil {
					return err
				}
			}
			return drainFlightInput(stream)
		default:
			return fmt.Errorf("unexpected connection %d", connection)
		}
	}

	sdk := newRecoveryArrowSDK(t, server)
	defer sdk.Close()
	schema := publicArrowSchema(nil)
	stream := createRecoveryArrowStream(
		t,
		sdk,
		schema,
		staticArrowProvider(),
		zerobus.WithMaxInflight(2),
		zerobus.WithStreamPausedMaxWait(150*time.Millisecond),
	)
	if offset := ingestIDBatch(t, stream, schema, 1); offset != 0 {
		t.Fatalf("first logical offset = %d, want 0", offset)
	}
	select {
	case <-pauseSent:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not send pause")
	}
	if err := stream.WaitForOffset(0); err != nil {
		t.Fatalf("WaitForOffset(0): %v", err)
	}
	if offset := ingestIDBatch(t, stream, schema, 2); offset != 1 {
		t.Fatalf("second logical offset = %d, want 1", offset)
	}
	if offset := ingestIDBatch(t, stream, schema, 3); offset != 2 {
		t.Fatalf("third logical offset = %d, want 2", offset)
	}

	type ingestResult struct {
		offset int64
		err    error
	}
	fourth := make(chan ingestResult, 1)
	go func() {
		batch := publicArrowBatch(memory.DefaultAllocator, schema, 4)
		offset, err := stream.IngestBatch(batch)
		batch.Release()
		fourth <- ingestResult{offset: offset, err: err}
	}()
	select {
	case result := <-fourth:
		t.Fatalf("fourth ingest bypassed MaxInflight while paused: %+v", result)
	case <-time.After(40 * time.Millisecond):
	}
	select {
	case <-secondStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("paused stream did not rotate")
	}
	select {
	case result := <-fourth:
		if result.err != nil || result.offset != 3 {
			t.Fatalf("fourth ingest = (%d, %v), want (3, nil)", result.offset, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fourth ingest remained blocked after acknowledgments")
	}
	if err := stream.Flush(); err != nil {
		t.Fatalf("Flush after pause rotation: %v", err)
	}
	select {
	case orderly := <-firstEOF:
		if !orderly {
			t.Fatal("paused DoPut was canceled instead of half-closed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first DoPut did not observe client teardown")
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	server.assertNoHandlerError(t)
}

func TestArrowRecoveryDisabledRetainsUnacked(t *testing.T) {
	server := &recoveryFlightServer{}
	server.handler = func(
		connection int,
		stream flight.FlightService_DoPutServer,
		schema *flight.FlightData,
	) error {
		if connection != 0 {
			return fmt.Errorf("unexpected recovery connection %d", connection)
		}
		if err := sendFlightReady(stream); err != nil {
			return err
		}
		_, _, _, _, err := receiveFlightFrame(stream, schema)
		if err != nil {
			return err
		}
		return status.Error(codes.Unavailable, "transport disconnected")
	}

	sdk := newRecoveryArrowSDK(t, server)
	defer sdk.Close()
	schema := publicArrowSchema(nil)
	stream := createRecoveryArrowStream(
		t,
		sdk,
		schema,
		staticArrowProvider(),
		zerobus.WithRecovery(zerobus.RecoveryDisabled),
	)
	ingestIDBatch(t, stream, schema, 10, 20)
	err := stream.Flush()
	if err == nil {
		t.Fatal("Flush succeeded after unrecovered disconnect")
	}
	var apiErr *zerobus.Error
	if !errors.As(err, &apiErr) || apiErr.Op != "Flush" {
		t.Fatalf("Flush error = %T %v, want *zerobus.Error with Flush op", err, err)
	}
	if !zerobus.Retryable(err) || status.Code(err) != codes.Unavailable {
		t.Fatalf(
			"Flush error retryable=%t status=%v, want true/Unavailable",
			zerobus.Retryable(err),
			status.Code(err),
		)
	}
	if got := server.connectionCount(); got != 1 {
		t.Fatalf("DoPut connections = %d, want 1", got)
	}
	unacked, getErr := stream.GetUnackedBatches()
	if getErr != nil {
		t.Fatalf("GetUnackedBatches: %v", getErr)
	}
	defer func() {
		for _, batch := range unacked {
			batch.Release()
		}
	}()
	if len(unacked) != 1 ||
		!slices.Equal(publicArrowIDs(t, unacked[0]), []int32{10, 20}) {
		t.Fatalf("unacked batches = %d", len(unacked))
	}
	server.assertNoHandlerError(t)
}

type invalidatingArrowProvider struct {
	getCalls      atomic.Int64
	invalidations atomic.Int64
}

func (p *invalidatingArrowProvider) GetHeaders(
	context.Context,
	string,
) (map[string]string, error) {
	p.getCalls.Add(1)
	token := "stale-token"
	if p.invalidations.Load() > 0 {
		token = "fresh-token"
	}
	return map[string]string{"authorization": "Bearer " + token}, nil
}

func (p *invalidatingArrowProvider) Invalidate(context.Context, string) {
	p.invalidations.Add(1)
}

func TestArrowSetupAuthInvalidatesAndRetriesOnce(t *testing.T) {
	var observedMu sync.Mutex
	var observed []string
	server := &recoveryFlightServer{}
	server.handler = func(
		connection int,
		stream flight.FlightService_DoPutServer,
		_ *flight.FlightData,
	) error {
		incoming, _ := metadata.FromIncomingContext(stream.Context())
		observedMu.Lock()
		observed = append(observed, incoming.Get("authorization")...)
		observedMu.Unlock()
		switch connection {
		case 0:
			return status.Error(codes.Unauthenticated, "expired token")
		case 1:
			if err := sendFlightReady(stream); err != nil {
				return err
			}
			return drainFlightInput(stream)
		default:
			return status.Error(codes.Unauthenticated, "unexpected retry")
		}
	}

	provider := &invalidatingArrowProvider{}
	sdk := newRecoveryArrowSDK(t, server)
	defer sdk.Close()
	stream := createRecoveryArrowStream(
		t,
		sdk,
		publicArrowSchema(nil),
		provider,
	)
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := provider.getCalls.Load(); got != 2 {
		t.Fatalf("GetHeaders calls = %d, want 2", got)
	}
	if got := provider.invalidations.Load(); got != 1 {
		t.Fatalf("Invalidate calls = %d, want 1", got)
	}
	observedMu.Lock()
	defer observedMu.Unlock()
	if !slices.Equal(observed, []string{"Bearer stale-token", "Bearer fresh-token"}) {
		t.Fatalf("authorization headers = %v", observed)
	}
	server.assertNoHandlerError(t)
}

func TestArrowSchemaSetupFailureIsTerminal(t *testing.T) {
	server := &recoveryFlightServer{}
	server.handler = func(
		_ int,
		_ flight.FlightService_DoPutServer,
		_ *flight.FlightData,
	) error {
		return status.Error(codes.InvalidArgument, "schema mismatch")
	}

	sdk := newRecoveryArrowSDK(t, server)
	defer sdk.Close()
	_, err := sdk.CreateArrowStreamWithProvider(
		context.Background(),
		"main.sales.orders",
		publicArrowSchema(nil),
		staticArrowProvider(),
		zerobus.WithWaitForReady(),
		zerobus.WithRecoveryBackoff(time.Millisecond),
		zerobus.WithRecoveryRetries(4),
	)
	if err == nil {
		t.Fatal("schema setup failure returned a stream")
	}
	var apiErr *zerobus.Error
	if !errors.As(err, &apiErr) || apiErr.Op != "CreateArrowStreamWithProvider" {
		t.Fatalf(
			"schema setup error = %T %v, want *zerobus.Error with constructor op",
			err,
			err,
		)
	}
	if zerobus.Retryable(err) {
		t.Fatalf("schema setup error is retryable: %v", err)
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("schema setup status = %v, want InvalidArgument", status.Code(err))
	}
	if got := server.connectionCount(); got != 1 {
		t.Fatalf("schema failure opened %d DoPut streams, want 1", got)
	}
	server.assertNoHandlerError(t)
}

func TestArrowLackOfAckTimeoutRecoversAndReplays(t *testing.T) {
	firstReceived := make(chan struct{})
	server := &recoveryFlightServer{}
	server.handler = func(
		connection int,
		stream flight.FlightService_DoPutServer,
		schema *flight.FlightData,
	) error {
		if err := sendFlightReady(stream); err != nil {
			return err
		}
		_, frameMetadata, _, ids, err := receiveFlightFrame(stream, schema)
		if err != nil {
			return err
		}
		if frameMetadata.OffsetID != 0 || !slices.Equal(ids, []int32{42}) {
			return fmt.Errorf(
				"connection %d offset=%d ids=%v",
				connection,
				frameMetadata.OffsetID,
				ids,
			)
		}
		switch connection {
		case 0:
			close(firstReceived)
			<-stream.Context().Done()
			return stream.Context().Err()
		case 1:
			if err := sendFlightAck(stream, 0, 1); err != nil {
				return err
			}
			return drainFlightInput(stream)
		default:
			return fmt.Errorf("unexpected connection %d", connection)
		}
	}

	sdk := newRecoveryArrowSDK(t, server)
	defer sdk.Close()
	schema := publicArrowSchema(nil)
	stream := createRecoveryArrowStream(
		t,
		sdk,
		schema,
		staticArrowProvider(),
		zerobus.WithLackOfAckTimeout(40*time.Millisecond),
	)
	ingestIDBatch(t, stream, schema, 42)
	select {
	case <-firstReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("first connection did not receive the batch")
	}
	if err := stream.Flush(); err != nil {
		t.Fatalf("Flush after lack-of-ack recovery: %v", err)
	}
	if got := server.connectionCount(); got != 2 {
		t.Fatalf("DoPut connections = %d, want 2", got)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	server.assertNoHandlerError(t)
}

func TestArrowProtocolAckFailuresAreTerminal(t *testing.T) {
	tests := []struct {
		name     string
		metadata []byte
	}{
		{
			name:     "forward record count",
			metadata: arrowAckMetadata(0, 2),
		},
		{
			name:     "forward frame offset",
			metadata: arrowAckMetadata(1, 1),
		},
		{
			name: "ready after setup",
			metadata: arrowAckMetadata(
				transport.FlightStreamReadyOffset,
				0,
			),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := &recoveryFlightServer{}
			server.handler = func(
				connection int,
				stream flight.FlightService_DoPutServer,
				schema *flight.FlightData,
			) error {
				if connection != 0 {
					return fmt.Errorf("unexpected recovery connection %d", connection)
				}
				if err := sendFlightReady(stream); err != nil {
					return err
				}
				if _, _, _, _, err := receiveFlightFrame(stream, schema); err != nil {
					return err
				}
				if err := stream.Send(&flight.PutResult{
					AppMetadata: test.metadata,
				}); err != nil {
					return err
				}
				<-stream.Context().Done()
				return stream.Context().Err()
			}

			sdk := newRecoveryArrowSDK(t, server)
			defer sdk.Close()
			schema := publicArrowSchema(nil)
			stream := createRecoveryArrowStream(
				t,
				sdk,
				schema,
				staticArrowProvider(),
				zerobus.WithLackOfAckTimeout(time.Second),
			)
			ingestIDBatch(t, stream, schema, 1)
			err := stream.Flush()
			if err == nil {
				t.Fatal("Flush succeeded after invalid acknowledgment")
			}
			if zerobus.Retryable(err) {
				t.Fatalf("invalid acknowledgment is retryable: %v", err)
			}
			if got := server.connectionCount(); got != 1 {
				t.Fatalf("invalid acknowledgment opened %d DoPut streams", got)
			}
			server.assertNoHandlerError(t)
		})
	}
}

func TestArrowRegressiveAndDuplicateRecordAcksDoNotAdvance(t *testing.T) {
	releaseFinal := make(chan struct{})
	intermediateSent := make(chan struct{})
	server := &recoveryFlightServer{}
	server.handler = func(
		connection int,
		stream flight.FlightService_DoPutServer,
		schema *flight.FlightData,
	) error {
		if connection != 0 {
			return fmt.Errorf("unexpected connection %d", connection)
		}
		if err := sendFlightReady(stream); err != nil {
			return err
		}
		_, firstMetadata, _, _, err := receiveFlightFrame(stream, schema)
		if err != nil {
			return err
		}
		if err := sendFlightAck(stream, firstMetadata.OffsetID, 1); err != nil {
			return err
		}
		_, secondMetadata, _, _, err := receiveFlightFrame(stream, schema)
		if err != nil {
			return err
		}
		if err := sendFlightAck(stream, secondMetadata.OffsetID, 1); err != nil {
			return err
		}
		if err := sendFlightAck(stream, secondMetadata.OffsetID, 0); err != nil {
			return err
		}
		close(intermediateSent)
		select {
		case <-releaseFinal:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
		if err := sendFlightAck(stream, secondMetadata.OffsetID, 2); err != nil {
			return err
		}
		return drainFlightInput(stream)
	}

	sdk := newRecoveryArrowSDK(t, server)
	defer sdk.Close()
	schema := publicArrowSchema(nil)
	stream := createRecoveryArrowStream(
		t,
		sdk,
		schema,
		staticArrowProvider(),
	)
	ingestIDBatch(t, stream, schema, 1)
	ingestIDBatch(t, stream, schema, 2)
	if err := stream.WaitForOffset(0); err != nil {
		t.Fatalf("WaitForOffset(0): %v", err)
	}
	select {
	case <-intermediateSent:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not send duplicate/regressive acknowledgments")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := stream.WaitForOffsetContext(ctx, 1); err == nil {
		t.Fatal("duplicate/regressive record acknowledgments advanced offset 1")
	}
	close(releaseFinal)
	if err := stream.Flush(); err != nil {
		t.Fatalf("Flush after final acknowledgment: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	server.assertNoHandlerError(t)
}

func TestArrowPartialGetUnackedRepeatableAfterClose(t *testing.T) {
	partialSent := make(chan struct{})
	server := &recoveryFlightServer{}
	server.handler = func(
		_ int,
		stream flight.FlightService_DoPutServer,
		schema *flight.FlightData,
	) error {
		if err := sendFlightReady(stream); err != nil {
			return err
		}
		_, frameMetadata, _, _, err := receiveFlightFrame(stream, schema)
		if err != nil {
			return err
		}
		if err := sendFlightAck(stream, frameMetadata.OffsetID, 2); err != nil {
			return err
		}
		close(partialSent)
		return drainFlightInput(stream)
	}

	sdk := newRecoveryArrowSDK(t, server)
	defer sdk.Close()
	schema := publicArrowSchema(nil)
	stream := createRecoveryArrowStream(
		t,
		sdk,
		schema,
		staticArrowProvider(),
		zerobus.WithRecovery(zerobus.RecoveryDisabled),
		zerobus.WithFlushTimeout(100*time.Millisecond),
		zerobus.WithLackOfAckTimeout(time.Second),
	)
	ingestIDBatch(t, stream, schema, 1, 2, 3, 4, 5)
	select {
	case <-partialSent:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not send partial acknowledgment")
	}
	if err := stream.Close(); err == nil {
		t.Fatal("Close succeeded with a partially acknowledged batch")
	}

	for call := 0; call < 3; call++ {
		batches, err := stream.GetUnackedBatches()
		if err != nil {
			t.Fatalf("GetUnackedBatches call %d: %v", call, err)
		}
		if len(batches) != 1 ||
			!slices.Equal(publicArrowIDs(t, batches[0]), []int32{3, 4, 5}) {
			for _, batch := range batches {
				batch.Release()
			}
			t.Fatalf("GetUnackedBatches call %d returned %d batches", call, len(batches))
		}
		for _, batch := range batches {
			batch.Release()
		}
	}
	server.assertNoHandlerError(t)
}
