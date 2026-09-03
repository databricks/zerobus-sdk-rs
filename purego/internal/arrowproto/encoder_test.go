package arrowproto

import (
	"bytes"
	"encoding/binary"
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	flatbuffers "github.com/google/flatbuffers/go"
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

func listBatch(
	t *testing.T,
	allocator memory.Allocator,
	rows int,
	valuesPerRow int,
) (*arrow.Schema, arrow.RecordBatch) {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{
		Name: "value",
		Type: arrow.ListOf(arrow.PrimitiveTypes.Int64),
	}}, nil)
	builder := array.NewListBuilder(allocator, arrow.PrimitiveTypes.Int64)
	values, ok := builder.ValueBuilder().(*array.Int64Builder)
	if !ok {
		t.Fatalf("list value builder = %T, want *array.Int64Builder",
			builder.ValueBuilder())
	}
	for i := range rows {
		builder.Append(true)
		for j := range valuesPerRow {
			values.Append(int64(i*valuesPerRow + j))
		}
	}
	column := builder.NewArray()
	builder.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{column}, int64(rows))
	column.Release()
	return schema, record
}

// structBatch builds one struct column whose string field holds valueBytes(row)
// bytes, so a slice's cost is dominated by a nested variable-width child.
func structBatch(
	t *testing.T,
	allocator memory.Allocator,
	rows int,
	valueBytes func(row int) int,
) (*arrow.Schema, arrow.RecordBatch) {
	t.Helper()
	structType := arrow.StructOf(
		arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "value", Type: arrow.BinaryTypes.String},
	)
	schema := arrow.NewSchema(
		[]arrow.Field{{Name: "entry", Type: structType}},
		nil,
	)
	builder := array.NewStructBuilder(allocator, structType)
	ids, ok := builder.FieldBuilder(0).(*array.Int64Builder)
	if !ok {
		t.Fatalf("field 0 builder = %T, want *array.Int64Builder",
			builder.FieldBuilder(0))
	}
	values, ok := builder.FieldBuilder(1).(*array.StringBuilder)
	if !ok {
		t.Fatalf("field 1 builder = %T, want *array.StringBuilder",
			builder.FieldBuilder(1))
	}
	for i := range rows {
		builder.Append(true)
		ids.Append(int64(i))
		values.Append(strings.Repeat("x", valueBytes(i)))
	}
	column := builder.NewArray()
	builder.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{column}, int64(rows))
	column.Release()
	return schema, record
}

func dictionaryBatch(
	t *testing.T,
	allocator memory.Allocator,
	rows int,
	distinct int,
	valueBytes int,
) (*arrow.Schema, arrow.RecordBatch) {
	t.Helper()
	dictionaryType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int32,
		ValueType: arrow.BinaryTypes.String,
	}
	schema := arrow.NewSchema([]arrow.Field{{
		Name: "value", Type: dictionaryType, Nullable: false,
	}}, nil)
	builder, ok := array.NewBuilder(allocator, dictionaryType).(*array.BinaryDictionaryBuilder)
	if !ok {
		t.Fatal("dictionary builder is not a *array.BinaryDictionaryBuilder")
	}
	// Single-byte runes only, so each value is exactly valueBytes on the wire and
	// the caller can predict the dictionary's total size.
	const alphabet = "abcdefghijklmnopqrstuvwxyz" +
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+/"
	if distinct > len(alphabet) {
		t.Fatalf("distinct = %d, want at most %d", distinct, len(alphabet))
	}
	for i := range rows {
		letter := alphabet[i%distinct : i%distinct+1]
		if err := builder.AppendString(strings.Repeat(letter, valueBytes)); err != nil {
			t.Fatalf("append dictionary value: %v", err)
		}
	}
	column := builder.NewArray()
	builder.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{column}, int64(rows))
	column.Release()
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
// whole-buffer measurement charges a small slice for the entire parent. Nested
// layouts are covered because slicing rebases only the top-level node, leaving
// a struct's fields and a list's values spanning the whole parent.
func TestTypedAdmissionChargesSliceNotParentBuffers(t *testing.T) {
	const (
		rows       = 20_000
		valueBytes = 512
		sliceRows  = 10
	)
	cases := []struct {
		name  string
		build func(*testing.T) (*arrow.Schema, arrow.RecordBatch)
	}{{
		name: "string",
		build: func(t *testing.T) (*arrow.Schema, arrow.RecordBatch) {
			return binaryBatch(t, memory.DefaultAllocator, rows, valueBytes)
		},
	}, {
		name: "list",
		build: func(t *testing.T) (*arrow.Schema, arrow.RecordBatch) {
			return listBatch(t, memory.DefaultAllocator, rows, valueBytes/8)
		},
	}, {
		name: "struct",
		build: func(t *testing.T) (*arrow.Schema, arrow.RecordBatch) {
			return structBatch(t, memory.DefaultAllocator, rows,
				func(int) int { return valueBytes })
		},
	}}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			schema, record := testCase.build(t)
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
		})
	}
}

// TestNestedSliceAdmissionCoversHeavyTailRows pins that a nested child's window
// follows its parent's offset. Sizing the wrong rows of a variable-width child
// looks plausible on uniform data, but under-reserves once the sliced rows are
// the large ones.
func TestNestedSliceAdmissionCoversHeavyTailRows(t *testing.T) {
	const (
		rows      = 2_000
		sliceRows = 4
		smallLen  = 16
		largeLen  = 256 * 1024
	)
	schema, record := structBatch(
		t,
		memory.DefaultAllocator,
		rows,
		func(row int) int {
			if row >= rows-sliceRows {
				return largeLen
			}
			return smallLen
		},
	)
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
			"estimate %d under-reserves the %d-byte heavy-tail payload",
			estimate,
			payload.RetainedSize(),
		)
	}
}

// A batch's custom metadata is written into the IPC message header, where a
// column buffer walk cannot see it, so it is charged separately once it grows
// past the admission slop.
func TestTypedAdmissionChargesRecordBatchMetadata(t *testing.T) {
	const metadataBytes = 512 * 1024
	schema := idSchema(nil)
	protocol, err := New(schema, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	plain := idBatch(t, memory.DefaultAllocator, schema, []int32{1})
	defer plain.Release()
	metadata := arrow.NewMetadata(
		[]string{"trace"},
		[]string{strings.Repeat("m", metadataBytes)},
	)
	annotated := array.NewRecordBatchWithMetadata(
		schema,
		plain.Columns(),
		plain.NumRows(),
		metadata,
	)
	defer annotated.Release()

	plainEstimate, err := protocol.EstimateRecordBatchRetainedSize(plain)
	if err != nil {
		t.Fatalf("plain estimate: %v", err)
	}
	estimate, err := protocol.EstimateRecordBatchRetainedSize(annotated)
	if err != nil {
		t.Fatalf("annotated estimate: %v", err)
	}
	if growth := estimate - plainEstimate; growth < metadataBytes {
		t.Fatalf(
			"metadata grew the estimate by %d bytes, want at least %d",
			growth,
			metadataBytes,
		)
	}
	payload, err := protocol.EncodeRecordBatch(annotated)
	if err != nil {
		t.Fatalf("EncodeRecordBatch: %v", err)
	}
	if estimate < payload.RetainedSize() {
		t.Fatalf(
			"estimate %d under-reserves the %d-byte annotated payload",
			estimate,
			payload.RetainedSize(),
		)
	}
}

func TestEncodeIPCCanonicalizesAndCopiesInput(t *testing.T) {
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
	// The caller may reuse its buffer the moment EncodeIPC returns.
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

func TestEncodeIPCRejectsNoEmptyAndMultipleBatches(t *testing.T) {
	schema := idSchema(nil)
	protocol, err := New(schema, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	empty := idBatch(t, memory.DefaultAllocator, schema, nil)
	defer empty.Release()
	one := idBatch(t, memory.DefaultAllocator, schema, []int32{1})
	defer one.Release()

	for _, test := range []struct {
		name string
		data []byte
	}{
		{name: "no batch", data: serializeRecords(t, schema)},
		{name: "empty batch", data: serializeRecords(t, schema, empty)},
		{name: "multiple batches", data: serializeRecords(t, schema, one, one)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := protocol.EncodeIPC(test.data); err == nil {
				t.Fatalf("EncodeIPC accepted %s", test.name)
			}
		})
	}
}

// A caller supplying its own IPC bytes is the first place a payload's exactly-one-batch
// contract can be violated from outside, so anything past the single batch is rejected
// rather than silently dropped.
func TestEncodeIPCRejectsTrailingBytesAndConcatenatedStreams(t *testing.T) {
	schema := idSchema(nil)
	protocol, err := New(schema, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	record := idBatch(t, memory.DefaultAllocator, schema, []int32{1})
	defer record.Release()
	batchStream := serializeRecords(t, schema, record)

	for _, test := range []struct {
		name  string
		input []byte
	}{
		{
			name:  "trailing bytes",
			input: append(bytes.Clone(batchStream), []byte("trailing")...),
		},
		{
			name:  "concatenated stream",
			input: append(bytes.Clone(batchStream), batchStream...),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := protocol.EncodeIPC(test.input); err == nil {
				t.Fatalf("EncodeIPC accepted %s", test.name)
			}
			if _, err := protocol.EstimateIPCRetainedSize(test.input); err == nil {
				t.Fatalf("EstimateIPCRetainedSize accepted %s", test.name)
			}
		})
	}
}

func TestUncompressedIPCAdmissionCoversCanonicalPayload(t *testing.T) {
	schema, record := binaryBatch(t, memory.DefaultAllocator, 512, 64)
	defer record.Release()
	protocol, err := New(schema, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	input := serializeRecords(t, schema, record)
	estimate, err := protocol.EstimateIPCRetainedSize(input)
	if err != nil {
		t.Fatalf("EstimateIPCRetainedSize: %v", err)
	}
	payload, err := protocol.EncodeIPC(input)
	if err != nil {
		t.Fatalf("EncodeIPC: %v", err)
	}
	if estimate < payload.RetainedSize() {
		t.Fatalf(
			"estimate %d under-reserves the %d-byte canonical payload",
			estimate,
			payload.RetainedSize(),
		)
	}
}

// A compressed stream weighs a fraction of what Arrow allocates for it, so the
// estimate has to follow the sizes the buffers declare rather than the wire
// length — that is what lets a buffer limit below the expanded size refuse the
// stream. Reaching that number must not materialize anything.
func TestCompressedIPCAdmissionUsesDeclaredUncompressedSizes(t *testing.T) {
	const (
		rows       = 4_096
		valueBytes = 2_048
		valueTotal = int64(rows) * valueBytes
	)
	schema, record := binaryBatch(t, memory.DefaultAllocator, rows, valueBytes)
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
			if int64(len(input)) >= valueTotal/4 {
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
			// Doubling the compressed length cannot reach this, so only the
			// declared uncompressed sizes can account for the estimate.
			if estimate < valueTotal {
				t.Fatalf(
					"compressed IPC estimate = %d, below the %d bytes its buffers declare",
					estimate,
					valueTotal,
				)
			}
			allocator.AssertSize(t, 0)
		})
	}
}

// A dictionary arrives as its own IPC message, and its values — not the indices
// in the record batch — are the large buffers, so the preflight has to charge
// that message too or the bulk of a dictionary-encoded stream goes unmeasured.
func TestCompressedDictionaryIPCChargesDictionaryBatch(t *testing.T) {
	const (
		rows       = 4_096
		distinct   = 64
		valueBytes = 16 * 1024
		valueTotal = int64(distinct) * valueBytes
	)
	schema, record := dictionaryBatch(
		t,
		memory.DefaultAllocator,
		rows,
		distinct,
		valueBytes,
	)
	defer record.Release()
	protocol, err := New(schema, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	input := serializeRecordsWithOptions(
		t,
		schema,
		[]ipc.Option{ipc.WithLZ4()},
		record,
	)
	estimate, err := protocol.EstimateIPCRetainedSize(input)
	if err != nil {
		t.Fatalf("EstimateIPCRetainedSize: %v", err)
	}
	// The record batch holds only 4-byte indices, so nothing but the dictionary
	// message can account for this much.
	if estimate < valueTotal {
		t.Fatalf(
			"dictionary IPC estimate = %d, below the %d bytes its values declare",
			estimate,
			valueTotal,
		)
	}
	payload, err := protocol.EncodeIPC(input)
	if err != nil {
		t.Fatalf("EncodeIPC: %v", err)
	}
	if estimate < payload.RetainedSize() {
		t.Fatalf(
			"estimate %d under-reserves the %d-byte canonical payload",
			estimate,
			payload.RetainedSize(),
		)
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

// The preflight walks flatbuffer metadata by hand, so every malformed shape has
// to come back as an error rather than as a panic or a usable size.
func TestPreflightRejectsMalformedIPC(t *testing.T) {
	schema := idSchema(nil)
	record := idBatch(t, memory.DefaultAllocator, schema, []int32{1})
	defer record.Release()
	batchStream := serializeRecords(t, schema, record)

	for _, test := range []struct {
		name  string
		input []byte
	}{
		{name: "empty", input: nil},
		{name: "garbage", input: []byte("not IPC at all")},
		{name: "truncated", input: batchStream[:len(batchStream)/2]},
		{
			name:  "trailing bytes",
			input: append(bytes.Clone(batchStream), 'x'),
		},
		{name: "schema only", input: serializeRecords(t, schema)},
	} {
		t.Run(test.name, func(t *testing.T) {
			expanded, err := preflightIPCExpansion(test.input)
			if err == nil {
				t.Fatalf("preflight accepted %s", test.name)
			}
			if expanded != 0 {
				t.Fatalf("rejected input reported %d expanded bytes", expanded)
			}
		})
	}
}
