package arrowproto

import (
	"bytes"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
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

	exported := payload.IPCBytes()
	exported[0] ^= 0xff
	if bytes.Equal(exported, payload.IPCBytes()) {
		t.Fatal("IPCBytes returned bytes aliasing the retained payload")
	}
	allocator.AssertSize(t, 0)
}

func TestCanonicalPayloadTakesSerializerOwnership(t *testing.T) {
	protocol, err := New(idSchema(nil), Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	canonical := []byte{1, 2, 3, 4}
	payload, err := protocol.payloadFromCanonicalIPC(canonical, 1)
	if err != nil {
		t.Fatalf("payloadFromCanonicalIPC: %v", err)
	}
	if &payload.ipcBytes[0] != &canonical[0] {
		t.Fatal("canonical IPC was copied instead of ownership being transferred")
	}
}

func TestExactSchemaMatchIncludesMetadata(t *testing.T) {
	expectedMetadata := arrow.NewMetadata([]string{"owner"}, []string{"expected"})
	actualMetadata := arrow.NewMetadata([]string{"owner"}, []string{"different"})
	protocol, err := New(idSchema(&expectedMetadata), Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	record := idBatch(t, memory.DefaultAllocator, idSchema(&actualMetadata), []int32{1})
	defer record.Release()

	if _, err := protocol.EncodeRecordBatch(record); err == nil {
		t.Fatal("typed RecordBatch with different schema metadata accepted")
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
	// The core never slices an unacknowledged payload, so a zero prefix is a bug
	// to surface, not a payload to pass through.
	if _, err := protocol.Slice(payload, 0); err == nil {
		t.Fatal("Slice accepted an unacknowledged prefix")
	}
}

// TestSliceRejectsPayloadWhoseRowCountDrifted keeps a corrupted invariant on the
// error path. Slicing against a bound past the decoded batch panics, and that
// panic would escape through recovery into the caller's goroutine.
func TestSliceRejectsPayloadWhoseRowCountDrifted(t *testing.T) {
	schema := idSchema(nil)
	protocol, err := New(schema, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	record := idBatch(t, memory.DefaultAllocator, schema, []int32{1, 2, 3, 4})
	defer record.Release()
	payload, err := protocol.EncodeRecordBatch(record)
	if err != nil {
		t.Fatalf("EncodeRecordBatch: %v", err)
	}

	drifted := &Payload{ipcBytes: payload.ipcBytes, rows: 64}
	_, err = protocol.Slice(drifted, 32)
	if err == nil || !strings.Contains(err.Error(), "row count changed") {
		t.Fatalf("Slice of a drifted payload = %v, want a row-count error", err)
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

// TestTypedAdmissionChargesSliceNotParentBuffers pins the reason admission sizes
// a batch from the rows it covers: a slice shares its parent's buffers, so a
// whole-buffer measurement charges a small slice for the entire parent.
func TestTypedAdmissionChargesSliceNotParentBuffers(t *testing.T) {
	const (
		rows       = 20_000
		valueBytes = 512
		sliceRows  = 10
	)
	schema, record := binaryBatch(t, memory.DefaultAllocator, rows, valueBytes)
	defer record.Release()
	protocol, err := New(schema, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	slice := record.NewSlice(rows-sliceRows, rows)
	defer slice.Release()
	estimate, err := protocol.EstimateRecordBatchRetainedSize(slice)
	if err != nil {
		t.Fatalf("EstimateRecordBatchRetainedSize: %v", err)
	}
	payload, err := protocol.EncodeRecordBatch(slice)
	if err != nil {
		t.Fatalf("EncodeRecordBatch: %v", err)
	}
	if estimate < payload.RetainedSize() {
		t.Fatalf(
			"slice estimate %d under-reserves its %d-byte payload",
			estimate,
			payload.RetainedSize(),
		)
	}
	parentEstimate, err := protocol.EstimateRecordBatchRetainedSize(record)
	if err != nil {
		t.Fatalf("EstimateRecordBatchRetainedSize(parent): %v", err)
	}
	if estimate > parentEstimate/100 {
		t.Fatalf(
			"slice estimate %d tracks the %d-byte parent rather than its own rows",
			estimate,
			parentEstimate,
		)
	}
}
