package arrowproto

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	flatbuffers "github.com/google/flatbuffers/go"
	"google.golang.org/protobuf/proto"

	"github.com/databricks/zerobus-sdk/purego/internal/stream"
	"github.com/databricks/zerobus-sdk/purego/internal/transport"
)

func idSchema(metadata *arrow.Metadata) *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{{
		Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false,
	}}, metadata)
}

func idBatch(
	t *testing.T,
	allocator memory.Allocator,
	schema *arrow.Schema,
	values []int32,
) arrow.RecordBatch {
	t.Helper()
	builder := array.NewInt32Builder(allocator)
	builder.AppendValues(values, nil)
	column := builder.NewArray()
	builder.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{column}, int64(len(values)))
	column.Release()
	return record
}

func binaryBatch(
	t *testing.T,
	allocator memory.Allocator,
	rows int,
	valueBytes int,
) (*arrow.Schema, arrow.RecordBatch) {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "value", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
	idBuilder := array.NewInt32Builder(allocator)
	valueBuilder := array.NewStringBuilder(allocator)
	value := strings.Repeat("x", valueBytes)
	for i := range rows {
		idBuilder.Append(int32(i))
		valueBuilder.Append(value)
	}
	ids := idBuilder.NewArray()
	values := valueBuilder.NewArray()
	idBuilder.Release()
	valueBuilder.Release()
	record := array.NewRecordBatch(
		schema,
		[]arrow.Array{ids, values},
		int64(rows),
	)
	ids.Release()
	values.Release()
	return schema, record
}

func dictionaryBatch(
	t *testing.T,
	allocator memory.Allocator,
) (*arrow.Schema, arrow.RecordBatch) {
	t.Helper()
	dictionaryType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int8,
		ValueType: arrow.BinaryTypes.String,
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "category", Type: dictionaryType, Nullable: false},
	}, nil)

	idBuilder := array.NewInt32Builder(allocator)
	idBuilder.AppendValues([]int32{1, 2, 3, 4}, nil)
	ids := idBuilder.NewArray()
	idBuilder.Release()

	dictionaryBuilder := array.NewDictionaryBuilder(allocator, dictionaryType)
	stringBuilder := dictionaryBuilder.(*array.BinaryDictionaryBuilder)
	for _, value := range []string{"alpha", "beta", "alpha", "gamma"} {
		if err := stringBuilder.AppendString(value); err != nil {
			t.Fatalf("append dictionary value: %v", err)
		}
	}
	categories := dictionaryBuilder.NewDictionaryArray()
	dictionaryBuilder.Release()

	record := array.NewRecordBatch(
		schema,
		[]arrow.Array{ids, categories},
		4,
	)
	ids.Release()
	categories.Release()
	return schema, record
}

func chunkedDictionaryBatch(
	t *testing.T,
	allocator memory.Allocator,
	rows int,
	valueBytes int,
) (*arrow.Schema, arrow.RecordBatch) {
	t.Helper()
	dictionaryType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int8,
		ValueType: arrow.BinaryTypes.String,
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "category", Type: dictionaryType, Nullable: false},
		{Name: "value", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
	idBuilder := array.NewInt32Builder(allocator)
	dictionaryBuilder := array.NewDictionaryBuilder(allocator, dictionaryType)
	categoryBuilder := dictionaryBuilder.(*array.BinaryDictionaryBuilder)
	valueBuilder := array.NewStringBuilder(allocator)
	value := strings.Repeat("x", valueBytes)
	categories := []string{"alpha", "beta", "gamma"}
	for row := range rows {
		idBuilder.Append(int32(row))
		if err := categoryBuilder.AppendString(categories[row%len(categories)]); err != nil {
			t.Fatalf("append dictionary value: %v", err)
		}
		valueBuilder.Append(value)
	}
	ids := idBuilder.NewArray()
	categoryArray := dictionaryBuilder.NewDictionaryArray()
	values := valueBuilder.NewArray()
	idBuilder.Release()
	dictionaryBuilder.Release()
	valueBuilder.Release()
	record := array.NewRecordBatch(
		schema,
		[]arrow.Array{ids, categoryArray, values},
		int64(rows),
	)
	ids.Release()
	categoryArray.Release()
	values.Release()
	return schema, record
}

func serializeRecords(
	t *testing.T,
	schema *arrow.Schema,
	records ...arrow.RecordBatch,
) []byte {
	return serializeRecordsWithOptions(t, schema, nil, records...)
}

func serializeRecordsWithOptions(
	t *testing.T,
	schema *arrow.Schema,
	options []ipc.Option,
	records ...arrow.RecordBatch,
) []byte {
	t.Helper()
	var output bytes.Buffer
	writerOptions := append([]ipc.Option{ipc.WithSchema(schema)}, options...)
	writer := ipc.NewWriter(&output, writerOptions...)
	for _, record := range records {
		if err := writer.Write(record); err != nil {
			t.Fatalf("write IPC record: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close IPC writer: %v", err)
	}
	return bytes.Clone(output.Bytes())
}

func readIDs(t *testing.T, data []byte) []int32 {
	t.Helper()
	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("new IPC reader: %v", err)
	}
	defer reader.Release()
	if !reader.Next() {
		t.Fatalf("read IPC record: %v", reader.Err())
	}
	column, ok := reader.RecordBatch().Column(0).(*array.Int32)
	if !ok {
		t.Fatalf("first column type = %T, want *array.Int32", reader.RecordBatch().Column(0))
	}
	values := append([]int32(nil), column.Int32Values()...)
	if reader.Next() {
		t.Fatal("decoded IPC contains more than one record")
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("finish IPC reader: %v", err)
	}
	return values
}

type flightSliceReader struct {
	frames []*flight.FlightData
	next   int
}

type testFlightWire struct {
	sends      chan *flight.FlightData
	responses  chan *flight.PutResult
	recvErrors chan error
	closed     chan struct{}
	sendFailed chan struct{}
	closeOnce  sync.Once
	failOnce   sync.Once
	failAt     int
	failGate   <-chan struct{}
	sendNumber int

	closeSendErr error
}

func newTestFlightWire(failAt int, failGate <-chan struct{}) *testFlightWire {
	return &testFlightWire{
		sends:      make(chan *flight.FlightData, 32),
		responses:  make(chan *flight.PutResult, 32),
		recvErrors: make(chan error, 1),
		closed:     make(chan struct{}),
		sendFailed: make(chan struct{}),
		failAt:     failAt,
		failGate:   failGate,
	}
}

func (*testFlightWire) ServerID() string { return "test-flight" }

func (w *testFlightWire) closedSendError() error {
	if w.closeSendErr != nil {
		return w.closeSendErr
	}
	return io.ErrClosedPipe
}

func (w *testFlightWire) Send(frame *flight.FlightData) error {
	sendNumber := w.sendNumber
	w.sendNumber++
	select {
	case w.sends <- proto.Clone(frame).(*flight.FlightData):
	case <-w.closed:
		return w.closedSendError()
	}
	if sendNumber == w.failAt {
		select {
		case <-w.failGate:
			w.failOnce.Do(func() { close(w.sendFailed) })
			return io.ErrUnexpectedEOF
		case <-w.closed:
			return w.closedSendError()
		}
	}
	return nil
}

func (w *testFlightWire) Recv() (*flight.PutResult, error) {
	select {
	case response := <-w.responses:
		return proto.Clone(response).(*flight.PutResult), nil
	case err := <-w.recvErrors:
		return nil, err
	case <-w.closed:
		return nil, io.EOF
	}
}

func (w *testFlightWire) CloseSend() error {
	w.Close()
	return nil
}

func (w *testFlightWire) Close() {
	w.closeOnce.Do(func() { close(w.closed) })
}

func (r *flightSliceReader) Recv() (*flight.FlightData, error) {
	if r.next == len(r.frames) {
		return nil, io.EOF
	}
	frame := proto.Clone(r.frames[r.next]).(*flight.FlightData)
	r.next++
	return frame, nil
}

func readFlightIDs(
	t *testing.T,
	schema *flight.FlightData,
	frames []*flight.FlightData,
) []int32 {
	t.Helper()
	all := make([]*flight.FlightData, 0, len(frames)+1)
	all = append(all, schema)
	all = append(all, frames...)
	reader, err := flight.NewRecordReader(&flightSliceReader{frames: all})
	if err != nil {
		t.Fatalf("new Flight record reader: %v", err)
	}
	defer reader.Release()

	var values []int32
	for reader.Next() {
		column := reader.RecordBatch().Column(0).(*array.Int32)
		values = append(values, column.Int32Values()...)
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("read Flight records: %v", err)
	}
	return values
}

type classifiedFlightTestError struct {
	message   string
	retryable bool
}

func (e *classifiedFlightTestError) Error() string     { return e.message }
func (e *classifiedFlightTestError) IsRetryable() bool { return e.retryable }

func TestTypedRecordBatchRoundTripOwnsIPC(t *testing.T) {
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	schema := idSchema(nil)
	protocol, err := New(schema, Options{Allocator: allocator})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if allocator.CurrentAlloc() != 0 {
		t.Fatalf("schema construction retained %d allocator bytes", allocator.CurrentAlloc())
	}

	record := idBatch(t, allocator, schema, []int32{10, 20, 30})
	estimate, err := protocol.EstimateRecordBatchRetainedSize(record)
	if err != nil {
		t.Fatalf("EstimateRecordBatchRetainedSize: %v", err)
	}
	payload, err := protocol.EncodeRecordBatch(record)
	if err != nil {
		t.Fatalf("EncodeRecordBatch: %v", err)
	}
	record.Release()

	if got := payload.UnitCount(); got != 3 {
		t.Fatalf("UnitCount = %d, want 3", got)
	}
	if got := readIDs(t, payload.IPCBytes()); !equalInt32(got, []int32{10, 20, 30}) {
		t.Fatalf("round-trip ids = %v", got)
	}
	if payload.RetainedSize() < int64(len(payload.ipcBytes)) {
		t.Fatalf(
			"RetainedSize = %d, smaller than IPC bytes %d",
			payload.RetainedSize(),
			len(payload.ipcBytes),
		)
	}
	if payload.RetainedSize() > estimate {
		t.Fatalf(
			"actual retained size %d exceeds admission estimate %d",
			payload.RetainedSize(),
			estimate,
		)
	}

	first := protocol.Decode(payload)[0]
	first[0] ^= 0xff
	if bytes.Equal(first, protocol.Decode(payload)[0]) {
		t.Fatal("Decode returned bytes aliasing the retained payload")
	}
	allocator.AssertSize(t, 0)
}

func TestCanonicalPayloadTakesSerializerOwnership(t *testing.T) {
	protocol, err := New(idSchema(nil), Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	canonical := []byte{1, 2, 3, 4}
	payload, err := protocol.payloadFromCanonicalIPC(canonical, 1, []int64{1})
	if err != nil {
		t.Fatalf("payloadFromCanonicalIPC: %v", err)
	}
	if &payload.ipcBytes[0] != &canonical[0] {
		t.Fatal("canonical IPC was copied instead of ownership being transferred")
	}
}

func TestIPCValidationAndCanonicalOwnership(t *testing.T) {
	schema := idSchema(nil)
	protocol, err := New(schema, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	record := idBatch(t, memory.DefaultAllocator, schema, []int32{1, 2})
	defer record.Release()

	input := serializeRecords(t, schema, record)
	payload, err := protocol.EncodeIPC(input)
	if err != nil {
		t.Fatalf("EncodeIPC: %v", err)
	}
	for i := range input {
		input[i] = 0
	}
	if got := readIDs(t, payload.IPCBytes()); !equalInt32(got, []int32{1, 2}) {
		t.Fatalf("canonical ids = %v", got)
	}

	if _, err := protocol.EncodeIPC([]byte("not IPC")); err == nil {
		t.Fatal("invalid IPC accepted")
	}
}

func TestIPCRejectsNoEmptyAndMultipleBatches(t *testing.T) {
	schema := idSchema(nil)
	protocol, err := New(schema, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	empty := idBatch(t, memory.DefaultAllocator, schema, nil)
	defer empty.Release()
	one := idBatch(t, memory.DefaultAllocator, schema, []int32{1})
	defer one.Release()

	tests := []struct {
		name string
		data []byte
	}{
		{name: "no batch", data: serializeRecords(t, schema)},
		{name: "empty batch", data: serializeRecords(t, schema, empty)},
		{name: "multiple batches", data: serializeRecords(t, schema, one, one)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := protocol.EncodeIPC(test.data); err == nil {
				t.Fatalf("EncodeIPC accepted %s", test.name)
			}
		})
	}
}

func TestIPCDecodersRejectTrailingBytesAndConcatenatedStreams(t *testing.T) {
	schema := idSchema(nil)
	protocol, err := New(schema, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	record := idBatch(t, memory.DefaultAllocator, schema, []int32{1})
	defer record.Release()

	batchStream := serializeRecords(t, schema, record)
	schemaStream, err := EncodeSchemaIPC(schema)
	if err != nil {
		t.Fatalf("EncodeSchemaIPC: %v", err)
	}
	for _, test := range []struct {
		name   string
		input  []byte
		decode func([]byte) error
	}{
		{
			name:  "batch trailing bytes",
			input: append(bytes.Clone(batchStream), []byte("trailing")...),
			decode: func(data []byte) error {
				_, err := protocol.EncodeIPC(data)
				return err
			},
		},
		{
			name:  "batch concatenated stream",
			input: append(bytes.Clone(batchStream), batchStream...),
			decode: func(data []byte) error {
				_, err := protocol.EncodeIPC(data)
				return err
			},
		},
		{
			name:  "schema trailing bytes",
			input: append(bytes.Clone(schemaStream), []byte("trailing")...),
			decode: func(data []byte) error {
				_, err := DecodeSchemaIPC(data)
				return err
			},
		},
		{
			name:  "schema concatenated stream",
			input: append(bytes.Clone(schemaStream), schemaStream...),
			decode: func(data []byte) error {
				_, err := DecodeSchemaIPC(data)
				return err
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.decode(test.input); err == nil {
				t.Fatal("decoder accepted trailing IPC data")
			}
		})
	}
}

func TestExactSchemaMatchIncludesMetadata(t *testing.T) {
	expectedMetadata := arrow.NewMetadata([]string{"owner"}, []string{"expected"})
	actualMetadata := arrow.NewMetadata([]string{"owner"}, []string{"different"})
	expected := idSchema(&expectedMetadata)
	actual := idSchema(&actualMetadata)
	protocol, err := New(expected, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	record := idBatch(t, memory.DefaultAllocator, actual, []int32{1})
	defer record.Release()

	if _, err := protocol.EncodeRecordBatch(record); err == nil {
		t.Fatal("typed RecordBatch with different schema metadata accepted")
	}
	if _, err := protocol.EncodeIPC(serializeRecords(t, actual, record)); err == nil {
		t.Fatal("IPC RecordBatch with different schema metadata accepted")
	}
}

func TestProtocolOwnsPointerBackedSchemaTypes(t *testing.T) {
	dictionaryType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int8,
		ValueType: arrow.BinaryTypes.String,
	}
	schema := arrow.NewSchema([]arrow.Field{{
		Name: "category", Type: dictionaryType, Nullable: false,
	}}, nil)
	protocol, err := New(schema, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	dictionaryType.ValueType = arrow.BinaryTypes.Binary
	owned := protocol.schema.Field(0).Type.(*arrow.DictionaryType)
	if owned.ValueType.ID() != arrow.STRING {
		t.Fatalf("owned dictionary value type = %v, want string", owned.ValueType)
	}
}

func TestSliceReserializesUnacknowledgedSuffix(t *testing.T) {
	schema := idSchema(nil)
	protocol, err := New(schema, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	record := idBatch(t, memory.DefaultAllocator, schema, []int32{1, 2, 3, 4, 5})
	payload, err := protocol.EncodeRecordBatch(record)
	record.Release()
	if err != nil {
		t.Fatalf("EncodeRecordBatch: %v", err)
	}

	suffix, err := protocol.Slice(payload, 2)
	if err != nil {
		t.Fatalf("Slice: %v", err)
	}
	if suffix.UnitCount() != 3 {
		t.Fatalf("suffix units = %d, want 3", suffix.UnitCount())
	}
	if got := readIDs(t, suffix.IPCBytes()); !equalInt32(got, []int32{3, 4, 5}) {
		t.Fatalf("suffix ids = %v", got)
	}
	if got := readIDs(t, payload.IPCBytes()); !equalInt32(got, []int32{1, 2, 3, 4, 5}) {
		t.Fatalf("original payload changed after Slice: %v", got)
	}
	if _, err := protocol.Slice(payload, 5); err == nil {
		t.Fatal("Slice accepted a fully acknowledged prefix")
	}
}

func TestIPCCompressionOptionsRoundTrip(t *testing.T) {
	schema, record := binaryBatch(t, memory.DefaultAllocator, 2_000, 200)
	defer record.Release()

	sizes := make(map[Compression]int)
	for _, compression := range []Compression{
		CompressionNone,
		CompressionLZ4,
		CompressionZstd,
	} {
		protocol, err := New(schema, Options{Compression: compression})
		if err != nil {
			t.Fatalf("New compression %d: %v", compression, err)
		}
		payload, err := protocol.EncodeRecordBatch(record)
		if err != nil {
			t.Fatalf("EncodeRecordBatch compression %d: %v", compression, err)
		}
		sizes[compression] = len(payload.ipcBytes)
		reader, err := ipc.NewReader(bytes.NewReader(payload.IPCBytes()))
		if err != nil {
			t.Fatalf("read compression %d: %v", compression, err)
		}
		if !reader.Next() || reader.RecordBatch().NumRows() != 2_000 {
			t.Fatalf("compression %d did not round-trip: %v", compression, reader.Err())
		}
		reader.Release()
	}
	if sizes[CompressionLZ4] >= sizes[CompressionNone] {
		t.Errorf("LZ4 size %d >= uncompressed %d", sizes[CompressionLZ4], sizes[CompressionNone])
	}
	if sizes[CompressionZstd] >= sizes[CompressionNone] {
		t.Errorf("Zstd size %d >= uncompressed %d", sizes[CompressionZstd], sizes[CompressionNone])
	}
	if _, err := New(schema, Options{Compression: Compression(99)}); err == nil {
		t.Fatal("unsupported compression accepted")
	}
}

func TestCompressedIPCAdmissionUsesDeclaredUncompressedSizes(t *testing.T) {
	const (
		rows       = 4_096
		valueBytes = 2_048
		memoryCap  = 4 * 1024 * 1024
	)
	schema, record := binaryBatch(
		t,
		memory.DefaultAllocator,
		rows,
		valueBytes,
	)
	defer record.Release()

	for _, test := range []struct {
		name   string
		option ipc.Option
	}{
		{name: "LZ4", option: ipc.WithLZ4()},
		{name: "Zstd", option: ipc.WithZstd()},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := serializeRecordsWithOptions(
				t,
				schema,
				[]ipc.Option{test.option},
				record,
			)
			if len(input) >= rows*valueBytes/4 {
				t.Fatalf(
					"compressed IPC size = %d, input is not highly compressible",
					len(input),
				)
			}

			allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
			protocol, err := New(schema, Options{Allocator: allocator})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			estimate, err := protocol.EstimateIPCRetainedSize(input)
			if err != nil {
				t.Fatalf("EstimateIPCRetainedSize: %v", err)
			}
			if estimate <= memoryCap {
				t.Fatalf(
					"compressed IPC estimate = %d, want above %d-byte limit",
					estimate,
					memoryCap,
				)
			}

			cfg := stream.DefaultConfig()
			cfg.MaxBufferedPayloadBytes = memoryCap
			cfg.Recovery = stream.RecoveryDisabled
			cfg.RecoveryTimeout = time.Hour
			open := stream.OpenFunc[*Payload, *flight.PutResult](func(
				ctx context.Context,
				_ stream.StreamParams,
			) (stream.WireStream[*Payload, *flight.PutResult], error) {
				<-ctx.Done()
				return nil, ctx.Err()
			})
			core, err := stream.NewCoreStreamWithHooks(
				context.Background(),
				stream.StreamParams{},
				cfg,
				open,
				protocol.EncoderHooks(),
				AckModelHooks(),
				nil,
			)
			if err != nil {
				t.Fatalf("NewCoreStreamWithHooks: %v", err)
			}
			t.Cleanup(func() { _ = core.Terminate() })

			builderCalled := false
			offset, err := core.EnqueuePayloadBuilder(
				context.Background(),
				estimate,
				func() (*Payload, uint64, int64, error) {
					builderCalled = true
					payload, encodeErr := protocol.EncodeIPC(input)
					if encodeErr != nil {
						return nil, 0, 0, encodeErr
					}
					return payload, payload.UnitCount(), payload.RetainedSize(), nil
				},
			)
			if offset != -1 || !errors.Is(err, stream.ErrPayloadTooLarge) {
				t.Fatalf(
					"compressed admission = (%d,%v), want (-1, ErrPayloadTooLarge)",
					offset,
					err,
				)
			}
			if builderCalled {
				t.Fatal("Arrow decoder ran before compressed IPC admission")
			}
			allocator.AssertSize(t, 0)
		})
	}
}

func TestCompressedIPCDeclaredSizeOverflowIsRejected(t *testing.T) {
	builder := flatbuffers.NewBuilder(128)

	builder.StartObject(2)
	compression := builder.EndObject()

	builder.StartVector(16, 2, 8)
	for index := 1; index >= 0; index-- {
		builder.Prep(8, 16)
		builder.PrependInt64(8)
		builder.PrependInt64(int64(index * 8))
	}
	buffers := builder.EndVector(2)

	builder.StartObject(5)
	builder.PrependUOffsetTSlot(2, buffers, 0)
	builder.PrependUOffsetTSlot(3, compression, 0)
	recordBatch := builder.EndObject()
	builder.Finish(recordBatch)

	body := make([]byte, 16)
	binary.LittleEndian.PutUint64(body[:8], math.MaxInt64)
	binary.LittleEndian.PutUint64(body[8:], 1)
	if _, err := ipcCompressedExpansion(
		ipcRootTable(builder.FinishedBytes()),
		body,
	); err == nil || !strings.Contains(err.Error(), "overflow") {
		t.Fatalf("ipcCompressedExpansion overflow error = %v", err)
	}
}

func TestTypedAdmissionIncludesRecordBatchMetadata(t *testing.T) {
	schema := idSchema(nil)
	protocol, err := New(schema, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	plain := idBatch(t, memory.DefaultAllocator, schema, []int32{1})
	defer plain.Release()

	metadataValue := strings.Repeat("m", 512*1024)
	metadata := arrow.NewMetadata([]string{"large"}, []string{metadataValue})
	withMetadata := array.NewRecordBatchWithMetadata(
		schema,
		plain.Columns(),
		plain.NumRows(),
		metadata,
	)
	defer withMetadata.Release()

	plainEstimate, err := protocol.EstimateRecordBatchRetainedSize(plain)
	if err != nil {
		t.Fatalf("plain estimate: %v", err)
	}
	metadataEstimate, err := protocol.EstimateRecordBatchRetainedSize(withMetadata)
	if err != nil {
		t.Fatalf("metadata estimate: %v", err)
	}
	if increase := metadataEstimate - plainEstimate; increase < int64(len(metadataValue))*2 {
		t.Fatalf(
			"metadata increased estimate by %d bytes, want at least %d",
			increase,
			len(metadataValue)*2,
		)
	}
	payload, err := protocol.EncodeRecordBatch(withMetadata)
	if err != nil {
		t.Fatalf("EncodeRecordBatch: %v", err)
	}
	if payload.RetainedSize() > metadataEstimate {
		t.Fatalf(
			"metadata payload retained size %d exceeds estimate %d",
			payload.RetainedSize(),
			metadataEstimate,
		)
	}
	decoded, err := protocol.DecodeIPCRecordBatch(payload.IPCBytes())
	if err != nil {
		t.Fatalf("DecodeIPCRecordBatch: %v", err)
	}
	defer decoded.Release()
	decodedMetadata := decoded.(arrow.RecordBatchWithMetadata).Metadata()
	if !decodedMetadata.Equal(metadata) {
		t.Fatal("RecordBatch custom metadata did not round-trip")
	}
}

func TestFlightChunkingUsesActualProtoSizeAndSequentialMetadata(t *testing.T) {
	const rows = 10_500
	schema, record := binaryBatch(t, memory.DefaultAllocator, rows, 256)
	protocol, err := New(schema, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	payload, err := protocol.EncodeRecordBatch(record)
	record.Release()
	if err != nil {
		t.Fatalf("EncodeRecordBatch: %v", err)
	}

	frames, next, err := protocol.EncodeFlightData(payload, 0)
	if err != nil {
		t.Fatalf("EncodeFlightData: %v", err)
	}
	if len(frames) < 2 {
		t.Fatalf("Flight frames = %d, want at least 2", len(frames))
	}
	if next != int64(len(frames)) {
		t.Fatalf("next offset = %d, want %d", next, len(frames))
	}
	for offset, frame := range frames {
		if size := proto.Size(frame); size > TargetFlightDataBytes {
			t.Errorf("frame %d size = %d, exceeds %d", offset, size, TargetFlightDataBytes)
		}
		metadata, err := transport.ParseFlightBatchMetadata(frame.GetAppMetadata())
		if err != nil {
			t.Fatalf("frame %d metadata: %v", offset, err)
		}
		if metadata.OffsetID != int64(offset) {
			t.Errorf("frame %d offset = %d", offset, metadata.OffsetID)
		}
	}

	ids := readFlightIDs(t, protocol.SchemaFlightData(), frames)
	if len(ids) != rows {
		t.Fatalf("decoded Flight rows = %d, want %d", len(ids), rows)
	}
	for i, id := range ids {
		if id != int32(i) {
			t.Fatalf("decoded Flight id[%d] = %d", i, id)
		}
	}
}

func TestFlightChunkPlanningBoundsProbeWorkAcrossManyChunks(t *testing.T) {
	const rows = 40_000
	schema, record := binaryBatch(t, memory.DefaultAllocator, rows, 512)
	protocol, err := New(schema, Options{})
	if err != nil {
		record.Release()
		t.Fatalf("New: %v", err)
	}
	probes := 0
	protocol.chunkProbe = func(int64, int64) { probes++ }
	payload, err := protocol.EncodeRecordBatch(record)
	record.Release()
	if err != nil {
		t.Fatalf("EncodeRecordBatch: %v", err)
	}
	if chunks := len(payload.chunkRows); chunks < 8 {
		t.Fatalf("planned chunks = %d, want at least 8", chunks)
	} else if limit := chunks*6 + 20; probes > limit {
		t.Fatalf(
			"chunk probes = %d for %d chunks, exceeds local-search bound %d",
			probes,
			chunks,
			limit,
		)
	}
}

func TestFlightRejectsOneRowOverTarget(t *testing.T) {
	schema, record := binaryBatch(
		t,
		memory.DefaultAllocator,
		1,
		TargetFlightDataBytes+1024,
	)
	protocol, err := New(schema, Options{})
	if err != nil {
		record.Release()
		t.Fatalf("New: %v", err)
	}
	_, err = protocol.EncodeRecordBatch(record)
	record.Release()
	if err == nil ||
		!strings.Contains(err.Error(), "one-row FlightData exceeds") {
		t.Fatalf("EncodeRecordBatch oversize error = %v", err)
	}
}

func TestFlightPartialSendFailureReplaysOnlyUnackedRows(t *testing.T) {
	const rows = 10_500
	schema, record := binaryBatch(t, memory.DefaultAllocator, rows, 256)
	protocol, err := New(schema, Options{})
	if err != nil {
		record.Release()
		t.Fatalf("New: %v", err)
	}
	payload, err := protocol.EncodeRecordBatch(record)
	record.Release()
	if err != nil {
		t.Fatalf("EncodeRecordBatch: %v", err)
	}

	failLaterFrame := make(chan struct{})
	firstRaw := newTestFlightWire(1, failLaterFrame)
	secondRaw := newTestFlightWire(-1, nil)
	firstWire := &wireStream{protocol: protocol, raw: firstRaw}
	firstWire.highestFrame.Store(-1)
	secondWire := &wireStream{protocol: protocol, raw: secondRaw}
	secondWire.highestFrame.Store(-1)

	ackClassified := make(chan struct{})
	var ackOnce sync.Once
	acks := AckModelHooks()
	classify := acks.Classify
	acks.Classify = func(result *flight.PutResult) stream.ResponseClassification {
		classified := classify(result)
		ackOnce.Do(func() { close(ackClassified) })
		return classified
	}
	wires := []stream.WireStream[*Payload, *flight.PutResult]{
		firstWire,
		secondWire,
	}
	var openMu sync.Mutex
	nextWire := 0
	open := stream.OpenFunc[*Payload, *flight.PutResult](func(
		context.Context,
		stream.StreamParams,
	) (stream.WireStream[*Payload, *flight.PutResult], error) {
		openMu.Lock()
		defer openMu.Unlock()
		if nextWire >= len(wires) {
			return nil, fmt.Errorf("unexpected open %d", nextWire)
		}
		wire := wires[nextWire]
		nextWire++
		return wire, nil
	})
	cfg := stream.DefaultConfig()
	cfg.RecoveryRetries = 1
	cfg.RecoveryBackoff = time.Millisecond
	cfg.RecoveryTimeout = time.Second
	cfg.FlushTimeout = 2 * time.Second
	cfg.LackOfAckTimeout = 2 * time.Second
	core, err := stream.NewCoreStreamWithHooks(
		context.Background(),
		stream.StreamParams{},
		cfg,
		open,
		protocol.EncoderHooks(),
		acks,
		nil,
	)
	if err != nil {
		t.Fatalf("NewCoreStreamWithHooks: %v", err)
	}
	t.Cleanup(func() { _ = core.Terminate() })

	offset, err := core.EnqueuePayload(
		context.Background(),
		payload,
		payload.UnitCount(),
		payload.RetainedSize(),
	)
	if err != nil {
		t.Fatalf("EnqueuePayload: %v", err)
	}
	firstFrame := <-firstRaw.sends
	laterFailedFrame := <-firstRaw.sends
	firstMetadata, err := transport.ParseFlightBatchMetadata(
		firstFrame.GetAppMetadata(),
	)
	if err != nil {
		t.Fatalf("first frame metadata: %v", err)
	}
	laterMetadata, err := transport.ParseFlightBatchMetadata(
		laterFailedFrame.GetAppMetadata(),
	)
	if err != nil {
		t.Fatalf("later frame metadata: %v", err)
	}
	if firstMetadata.OffsetID != 0 || laterMetadata.OffsetID != 1 {
		t.Fatalf(
			"attempted frame offsets = (%d,%d), want (0,1)",
			firstMetadata.OffsetID,
			laterMetadata.OffsetID,
		)
	}
	firstRows := uint64(len(readFlightIDs(
		t,
		protocol.SchemaFlightData(),
		[]*flight.FlightData{firstFrame},
	)))
	if firstRows == 0 || firstRows >= rows {
		t.Fatalf("first frame rows = %d, want a strict non-empty prefix", firstRows)
	}
	close(failLaterFrame)
	select {
	case <-firstRaw.sendFailed:
	case <-time.After(time.Second):
		t.Fatal("later frame Send did not fail")
	}
	firstRaw.responses <- &flight.PutResult{AppMetadata: mustJSON(
		t,
		transport.FlightAckMetadata{
			AckUpToOffset:  firstMetadata.OffsetID,
			AckUpToRecords: firstRows,
		},
	)}
	select {
	case <-ackClassified:
	case <-time.After(time.Second):
		t.Fatal("first-frame ACK arriving after Send failure was not classified")
	}

	var replayed []int32
	var cumulative uint64
	for cumulative < uint64(rows)-firstRows {
		var frame *flight.FlightData
		select {
		case frame = <-secondRaw.sends:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for sliced replay frame")
		}
		frameIDs := readFlightIDs(
			t,
			protocol.SchemaFlightData(),
			[]*flight.FlightData{frame},
		)
		replayed = append(replayed, frameIDs...)
		cumulative += uint64(len(frameIDs))
		metadata, err := transport.ParseFlightBatchMetadata(frame.GetAppMetadata())
		if err != nil {
			t.Fatalf("replay frame metadata: %v", err)
		}
		secondRaw.responses <- &flight.PutResult{AppMetadata: mustJSON(
			t,
			transport.FlightAckMetadata{
				AckUpToOffset:  metadata.OffsetID,
				AckUpToRecords: cumulative,
			},
		)}
	}
	if len(replayed) != rows-int(firstRows) {
		t.Fatalf("replayed rows = %d, want %d", len(replayed), rows-int(firstRows))
	}
	for index, id := range replayed {
		want := int32(index) + int32(firstRows)
		if id != want {
			t.Fatalf("replayed id[%d] = %d, want %d", index, id, want)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := core.WaitForOffset(ctx, offset); err != nil {
		t.Fatalf("WaitForOffset after sliced partial-send replay: %v", err)
	}
}

func TestFlightAckBeforeRecvErrorReconcilesActiveSendReceipt(t *testing.T) {
	const rows = 10_500
	schema, record := binaryBatch(t, memory.DefaultAllocator, rows, 256)
	protocol, err := New(schema, Options{})
	if err != nil {
		record.Release()
		t.Fatalf("New: %v", err)
	}
	payload, err := protocol.EncodeRecordBatch(record)
	record.Release()
	if err != nil {
		t.Fatalf("EncodeRecordBatch: %v", err)
	}

	blockLaterFrame := make(chan struct{})
	firstRaw := newTestFlightWire(1, blockLaterFrame)
	firstRaw.closeSendErr = &classifiedFlightTestError{
		message:   "non-retryable send teardown",
		retryable: false,
	}
	secondRaw := newTestFlightWire(-1, nil)
	firstWire := &wireStream{protocol: protocol, raw: firstRaw}
	firstWire.highestFrame.Store(-1)
	secondWire := &wireStream{protocol: protocol, raw: secondRaw}
	secondWire.highestFrame.Store(-1)

	ackClassified := make(chan struct{})
	var ackOnce sync.Once
	acks := AckModelHooks()
	classify := acks.Classify
	acks.Classify = func(result *flight.PutResult) stream.ResponseClassification {
		classified := classify(result)
		ackOnce.Do(func() { close(ackClassified) })
		return classified
	}
	wires := []stream.WireStream[*Payload, *flight.PutResult]{
		firstWire,
		secondWire,
	}
	var openMu sync.Mutex
	nextWire := 0
	open := stream.OpenFunc[*Payload, *flight.PutResult](func(
		context.Context,
		stream.StreamParams,
	) (stream.WireStream[*Payload, *flight.PutResult], error) {
		openMu.Lock()
		defer openMu.Unlock()
		if nextWire >= len(wires) {
			return nil, fmt.Errorf("unexpected open %d", nextWire)
		}
		wire := wires[nextWire]
		nextWire++
		return wire, nil
	})
	cfg := stream.DefaultConfig()
	cfg.RecoveryRetries = 1
	cfg.RecoveryBackoff = time.Millisecond
	cfg.RecoveryTimeout = time.Second
	cfg.FlushTimeout = 2 * time.Second
	cfg.LackOfAckTimeout = 2 * time.Second
	core, err := stream.NewCoreStreamWithHooks(
		context.Background(),
		stream.StreamParams{},
		cfg,
		open,
		protocol.EncoderHooks(),
		acks,
		nil,
	)
	if err != nil {
		t.Fatalf("NewCoreStreamWithHooks: %v", err)
	}
	t.Cleanup(func() {
		close(blockLaterFrame)
		_ = core.Terminate()
	})

	offset, err := core.EnqueuePayload(
		context.Background(),
		payload,
		payload.UnitCount(),
		payload.RetainedSize(),
	)
	if err != nil {
		t.Fatalf("EnqueuePayload: %v", err)
	}
	firstFrame := <-firstRaw.sends
	laterBlockedFrame := <-firstRaw.sends
	firstMetadata, err := transport.ParseFlightBatchMetadata(
		firstFrame.GetAppMetadata(),
	)
	if err != nil {
		t.Fatalf("first frame metadata: %v", err)
	}
	laterMetadata, err := transport.ParseFlightBatchMetadata(
		laterBlockedFrame.GetAppMetadata(),
	)
	if err != nil {
		t.Fatalf("later frame metadata: %v", err)
	}
	if firstMetadata.OffsetID != 0 || laterMetadata.OffsetID != 1 {
		t.Fatalf(
			"attempted frame offsets = (%d,%d), want (0,1)",
			firstMetadata.OffsetID,
			laterMetadata.OffsetID,
		)
	}
	firstRows := uint64(len(readFlightIDs(
		t,
		protocol.SchemaFlightData(),
		[]*flight.FlightData{firstFrame},
	)))
	if firstRows == 0 || firstRows >= rows {
		t.Fatalf("first frame rows = %d, want a strict non-empty prefix", firstRows)
	}

	firstRaw.responses <- &flight.PutResult{AppMetadata: mustJSON(
		t,
		transport.FlightAckMetadata{
			AckUpToOffset:  firstMetadata.OffsetID,
			AckUpToRecords: firstRows,
		},
	)}
	select {
	case <-ackClassified:
	case <-time.After(time.Second):
		t.Fatal("first-frame ACK was not buffered while the later Send was blocked")
	}

	// The receiver error arrives while frame 1 is still blocked. Closing the
	// wire then makes Send return a receipt for frame 0. The receiver failure
	// is retryable while that Send error is not, so recovery also proves the
	// receiver error remains the authoritative run cause.
	firstRaw.recvErrors <- &classifiedFlightTestError{
		message:   "retryable receiver failure",
		retryable: true,
	}

	var replayed []int32
	var cumulative uint64
	for cumulative < uint64(rows)-firstRows {
		var frame *flight.FlightData
		select {
		case frame = <-secondRaw.sends:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for sliced replay frame")
		}
		frameIDs := readFlightIDs(
			t,
			protocol.SchemaFlightData(),
			[]*flight.FlightData{frame},
		)
		replayed = append(replayed, frameIDs...)
		cumulative += uint64(len(frameIDs))
		metadata, err := transport.ParseFlightBatchMetadata(frame.GetAppMetadata())
		if err != nil {
			t.Fatalf("replay frame metadata: %v", err)
		}
		secondRaw.responses <- &flight.PutResult{AppMetadata: mustJSON(
			t,
			transport.FlightAckMetadata{
				AckUpToOffset:  metadata.OffsetID,
				AckUpToRecords: cumulative,
			},
		)}
	}
	if len(replayed) != rows-int(firstRows) {
		t.Fatalf("replayed rows = %d, want %d", len(replayed), rows-int(firstRows))
	}
	for index, id := range replayed {
		want := int32(index) + int32(firstRows)
		if id != want {
			t.Fatalf("replayed id[%d] = %d, want %d", index, id, want)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := core.WaitForOffset(ctx, offset); err != nil {
		t.Fatalf("WaitForOffset after ACK/error receipt reconciliation: %v", err)
	}
}

func TestDictionaryPayloadsAreSelfContainedOnIPCAndFlight(t *testing.T) {
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	schema, record := dictionaryBatch(t, allocator)
	protocol, err := New(schema, Options{Allocator: allocator})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	payload, err := protocol.EncodeRecordBatch(record)
	record.Release()
	if err != nil {
		t.Fatalf("EncodeRecordBatch: %v", err)
	}

	// The connection-scoped writer emits the dictionary once, then skips it for
	// an unchanged dictionary in a later logical payload.
	raw := newTestFlightWire(-1, nil)
	wire := &wireStream{protocol: protocol, raw: raw}
	wire.highestFrame.Store(-1)
	if receipt, err := wire.SendWithReceipt(payload); err != nil ||
		receipt.SubmittedUnits != payload.UnitCount() {
		t.Fatalf("first SendWithReceipt = (%+v, %v)", receipt, err)
	}
	firstCount := len(raw.sends)
	if firstCount < 2 {
		t.Fatalf("first payload frames = %d, want dictionary plus record", firstCount)
	}
	if receipt, err := wire.SendWithReceipt(payload); err != nil ||
		receipt.SubmittedUnits != payload.UnitCount() {
		t.Fatalf("second SendWithReceipt = (%+v, %v)", receipt, err)
	}
	secondCount := len(raw.sends) - firstCount
	if secondCount != 1 {
		t.Fatalf("unchanged second payload emitted %d frames, want record only", secondCount)
	}
	allFrames := make([]*flight.FlightData, 0, len(raw.sends))
	for len(raw.sends) > 0 {
		allFrames = append(allFrames, <-raw.sends)
	}
	ids := readFlightIDs(t, protocol.SchemaFlightData(), allFrames)
	if !equalInt32(ids, []int32{1, 2, 3, 4, 1, 2, 3, 4}) {
		t.Fatalf("dictionary Flight ids = %v", ids)
	}
	wire.Close()
	if got := readIDs(t, payload.IPCBytes()); !equalInt32(got, []int32{1, 2, 3, 4}) {
		t.Fatalf("dictionary IPC ids = %v", got)
	}

	// The IPC input path materializes and reserializes the dictionary too. Its
	// Flight output remains usable after the caller's IPC bytes and RecordBatch
	// have gone away.
	sourceSchema, sourceRecord := dictionaryBatch(t, allocator)
	input := serializeRecords(t, sourceSchema, sourceRecord)
	sourceRecord.Release()
	fromIPC, err := protocol.EncodeIPC(input)
	if err != nil {
		t.Fatalf("EncodeIPC dictionary: %v", err)
	}
	clear(input)
	ipcFrames, _, err := protocol.EncodeFlightData(fromIPC, 0)
	if err != nil {
		t.Fatalf("EncodeFlightData dictionary IPC: %v", err)
	}
	if got := readFlightIDs(t, protocol.SchemaFlightData(), ipcFrames); !equalInt32(got, []int32{1, 2, 3, 4}) {
		t.Fatalf("dictionary IPC Flight ids = %v", got)
	}

	suffix, err := protocol.Slice(fromIPC, 2)
	if err != nil {
		t.Fatalf("Slice dictionary payload: %v", err)
	}
	suffixFrames, _, err := protocol.EncodeFlightData(suffix, 0)
	if err != nil {
		t.Fatalf("EncodeFlightData dictionary suffix: %v", err)
	}
	if got := readFlightIDs(t, protocol.SchemaFlightData(), suffixFrames); !equalInt32(got, []int32{3, 4}) {
		t.Fatalf("dictionary suffix ids = %v", got)
	}
	allocator.AssertSize(t, 0)
}

func TestChunkedFlightEmitsUnchangedDictionaryOnce(t *testing.T) {
	const rows = 6_000
	schema, record := chunkedDictionaryBatch(
		t,
		memory.DefaultAllocator,
		rows,
		512,
	)
	protocol, err := New(schema, Options{})
	if err != nil {
		record.Release()
		t.Fatalf("New: %v", err)
	}
	payload, err := protocol.EncodeRecordBatch(record)
	record.Release()
	if err != nil {
		t.Fatalf("EncodeRecordBatch: %v", err)
	}
	raw := newTestFlightWire(-1, nil)
	wire := &wireStream{protocol: protocol, raw: raw}
	wire.highestFrame.Store(-1)
	receipt, err := wire.SendWithReceipt(payload)
	if err != nil {
		t.Fatalf("SendWithReceipt: %v", err)
	}
	if receipt.SubmittedUnits != rows {
		t.Fatalf("submitted rows = %d, want %d", receipt.SubmittedUnits, rows)
	}

	var dictionaryFrames, recordFrames int
	allFrames := make([]*flight.FlightData, 0, len(raw.sends))
	for len(raw.sends) > 0 {
		frame := <-raw.sends
		allFrames = append(allFrames, frame)
		messageType, err := flightMessageType(frame)
		if err != nil {
			t.Fatalf("flightMessageType: %v", err)
		}
		switch messageType {
		case ipc.MessageDictionaryBatch:
			dictionaryFrames++
		case ipc.MessageRecordBatch:
			recordFrames++
		}
	}
	if dictionaryFrames != 1 || recordFrames < 2 {
		t.Fatalf(
			"chunked frames dictionaries=%d records=%d, want 1 dictionary and multiple records",
			dictionaryFrames,
			recordFrames,
		)
	}
	if ids := readFlightIDs(t, protocol.SchemaFlightData(), allFrames); len(ids) != rows {
		t.Fatalf("decoded chunked dictionary rows = %d, want %d", len(ids), rows)
	}
	wire.Close()
}

func TestSchemaSeparateAndOffsetsCanRestartPerConnection(t *testing.T) {
	schema := idSchema(nil)
	protocol, err := New(schema, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	schemaFrame := protocol.SchemaFlightData()
	if len(schemaFrame.GetDataHeader()) == 0 ||
		len(schemaFrame.GetDataBody()) != 0 ||
		len(schemaFrame.GetAppMetadata()) != 0 {
		t.Fatalf("schema frame = %+v", schemaFrame)
	}

	record := idBatch(t, memory.DefaultAllocator, schema, []int32{1})
	payload, err := protocol.EncodeRecordBatch(record)
	record.Release()
	if err != nil {
		t.Fatalf("EncodeRecordBatch: %v", err)
	}
	for connection := 0; connection < 2; connection++ {
		frames, _, err := protocol.EncodeFlightData(payload, 0)
		if err != nil {
			t.Fatalf("connection %d EncodeFlightData: %v", connection, err)
		}
		metadata, err := transport.ParseFlightBatchMetadata(frames[0].GetAppMetadata())
		if err != nil {
			t.Fatalf("connection %d metadata: %v", connection, err)
		}
		if metadata.OffsetID != 0 {
			t.Fatalf("connection %d first offset = %d", connection, metadata.OffsetID)
		}
	}
}

func TestFlightAckHooksUseRecordDurability(t *testing.T) {
	hooks := AckModelHooks()
	result := &flight.PutResult{AppMetadata: mustJSON(t, transport.FlightAckMetadata{
		AckUpToOffset:  1,
		AckUpToRecords: 3,
	})}
	classified := hooks.Classify(result)
	if classified.Status != stream.ResponseOK ||
		!classified.HasAck ||
		classified.LegacyOffset != 1 ||
		classified.HasPause {
		t.Fatalf("Classify = %+v, want ack-only offset 1", classified)
	}
	resolution, err := hooks.Resolve(result, stream.AckState{
		Ranges: []stream.SubmittedRange{{
			WireOffset: 0, LogicalOffset: 7, UnitStart: 0, UnitEnd: 5,
		}},
		SubmittedUnits: 5,
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if resolution.PartialOffset != 7 || resolution.PartialUnits != 3 {
		t.Fatalf("resolution = %+v", resolution)
	}
}

func TestFlightAckHooksKeepAckAndPauseOrthogonal(t *testing.T) {
	hooks := AckModelHooks()
	duration := uint64(25)
	inline := &flight.PutResult{AppMetadata: mustJSON(t, transport.FlightAckMetadata{
		AckUpToOffset:         1,
		AckUpToRecords:        3,
		CloseStreamDurationMS: &duration,
	})}
	classified := hooks.Classify(inline)
	if classified.Status != stream.ResponseOK ||
		!classified.HasAck ||
		classified.LegacyOffset != 1 ||
		!classified.HasPause ||
		classified.PauseDuration != 25*time.Millisecond {
		t.Fatalf("inline ACK+pause classification = %+v", classified)
	}

	closeOnly := &flight.PutResult{AppMetadata: mustJSON(t, transport.FlightAckMetadata{
		AckUpToOffset:         transport.FlightStreamReadyOffset,
		AckUpToRecords:        0,
		CloseStreamDurationMS: &duration,
	})}
	classified = hooks.Classify(closeOnly)
	if classified.Status != stream.ResponseOK ||
		classified.HasAck ||
		!classified.HasPause {
		t.Fatalf("close-only classification = %+v", classified)
	}

	repeatedReady := &flight.PutResult{AppMetadata: mustJSON(t, transport.FlightAckMetadata{
		AckUpToOffset:  transport.FlightStreamReadyOffset,
		AckUpToRecords: 0,
	})}
	if classified := hooks.Classify(repeatedReady); classified.Status != stream.ResponseMalformed {
		t.Fatalf("repeated ready classification = %+v", classified)
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return data
}

func equalInt32(left, right []int32) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
