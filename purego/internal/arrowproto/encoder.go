// Package arrowproto implements the Arrow IPC payload for the Arrow ingestion
// path. A payload holds only self-contained IPC bytes, so caller-owned Arrow
// arrays are never retained by the stream core.
package arrowproto

import (
	"bytes"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// payloadOverheadBytes charges the Payload container so tiny batches still
// consume byte capacity.
const payloadOverheadBytes = int64(64)

// admissionSlopBytes is headroom for IPC framing a buffer sum cannot see:
// buffer padding, end markers, message headers. It bounds the estimate, not
// occupancy — a smaller MaxBufferedPayloadBytes rejects every batch.
const admissionSlopBytes = int64(64 * 1024)

// metadataEntryOverheadBytes covers the flatbuffer vector, table, and offset
// overhead one custom metadata entry costs beyond its UTF-8 contents.
const metadataEntryOverheadBytes = int64(32)

// Compression selects the compression used when serializing Arrow IPC batches.
type Compression uint8

const (
	CompressionNone Compression = iota
	CompressionLZ4
	CompressionZstd
)

// Options configures Arrow IPC materialization.
type Options struct {
	Compression Compression

	// Allocator is mainly a test seam. Nil uses Arrow's default.
	Allocator memory.Allocator
}

// Payload is one eagerly materialized, non-empty Arrow RecordBatch held as a
// canonical self-contained IPC stream: schema, dictionaries, batch, end marker.
type Payload struct {
	ipcBytes []byte
	rows     uint64
}

// UnitCount returns the number of row durability units in the payload.
func (p *Payload) UnitCount() uint64 {
	if p == nil {
		return 0
	}
	return p.rows
}

// RetainedSize returns a conservative heap charge for the payload.
func (p *Payload) RetainedSize() int64 {
	if p == nil {
		return 0
	}
	return payloadOverheadBytes + int64(cap(p.ipcBytes))
}

// IPCBytes returns a caller-owned copy of the self-contained IPC stream.
func (p *Payload) IPCBytes() []byte {
	if p == nil {
		return nil
	}
	return bytes.Clone(p.ipcBytes)
}

// Protocol owns one exact Arrow schema and the IPC encoding policy.
type Protocol struct {
	schema             *arrow.Schema
	compression        Compression
	allocator          memory.Allocator
	admissionBaseBytes int64
}

// New constructs an Arrow protocol encoder, copying schema so the caller's
// Schema object is not retained.
func New(schema *arrow.Schema, options Options) (*Protocol, error) {
	if schema == nil {
		return nil, fmt.Errorf("arrow protocol: schema is required")
	}
	if _, err := compressionOption(options.Compression); err != nil {
		return nil, err
	}
	allocator := options.Allocator
	if allocator == nil {
		allocator = memory.DefaultAllocator
	}
	ownedSchema, err := cloneSchemaThroughIPC(schema, allocator)
	if err != nil {
		return nil, err
	}
	schemaIPC, err := encodeSchemaIPC(ownedSchema)
	if err != nil {
		return nil, err
	}
	return &Protocol{
		schema:      ownedSchema,
		compression: options.Compression,
		allocator:   allocator,
		// Every payload repeats the schema, and materialization holds a second
		// copy of it while writing, so both are charged up front.
		admissionBaseBytes: admissionSlopBytes + 2*int64(len(schemaIPC)),
	}, nil
}

// EncodeRecordBatch serializes batch immediately. The returned payload owns no
// references to batch, its columns, or their buffers.
func (p *Protocol) EncodeRecordBatch(batch arrow.RecordBatch) (*Payload, error) {
	if batch == nil {
		return nil, fmt.Errorf("arrow protocol: RecordBatch is required")
	}
	if err := p.validateBatch(batch); err != nil {
		return nil, err
	}
	serialized, err := p.serialize(batch)
	if err != nil {
		return nil, fmt.Errorf("arrow protocol: serialize RecordBatch: %w", err)
	}
	return p.payloadFromCanonicalIPC(serialized, uint64(batch.NumRows()))
}

// EstimateRecordBatchRetainedSize returns a conservative pre-materialization
// reservation: the batch sized from the rows it covers plus its custom
// metadata, doubled to cover framing, compression, and buffer growth, plus the
// per-payload schema base. The core admits on this value before encoding, so an
// over-estimate rejects a batch that would have fit.
func (p *Protocol) EstimateRecordBatchRetainedSize(
	batch arrow.RecordBatch,
) (int64, error) {
	if batch == nil {
		return 0, fmt.Errorf("arrow protocol: RecordBatch is required")
	}
	if err := p.validateBatch(batch); err != nil {
		return 0, err
	}
	inputBytes, err := addInt64Saturating(
		totalRecordBufferSize(batch),
		recordBatchMetadataSize(batch),
	)
	if err != nil {
		return math.MaxInt64, nil
	}
	return p.admissionEstimate(inputBytes), nil
}

func (p *Protocol) admissionEstimate(inputBytes int64) int64 {
	if inputBytes < 0 ||
		inputBytes > math.MaxInt64/2 ||
		p.admissionBaseBytes > math.MaxInt64-payloadOverheadBytes {
		return math.MaxInt64
	}
	total := payloadOverheadBytes + p.admissionBaseBytes
	if scaled := inputBytes * 2; scaled <= math.MaxInt64-total {
		return total + scaled
	}
	return math.MaxInt64
}

// Slice drops an acknowledged row prefix and reserializes the remaining suffix
// as a standalone IPC payload.
func (p *Protocol) Slice(payload *Payload, acknowledgedPrefix uint64) (*Payload, error) {
	if payload == nil {
		return nil, fmt.Errorf("arrow protocol: payload is nil")
	}
	// The core only slices a partially acknowledged payload, so a prefix outside
	// (0,rows) means its accounting is wrong. Say so rather than invent a payload.
	if acknowledgedPrefix == 0 || acknowledgedPrefix >= payload.rows {
		return nil, fmt.Errorf(
			"arrow protocol: acknowledged prefix %d is invalid for %d rows",
			acknowledgedPrefix,
			payload.rows,
		)
	}

	batch, err := p.decodeOne(payload.ipcBytes)
	if err != nil {
		return nil, fmt.Errorf("arrow protocol: decode payload for slicing: %w", err)
	}
	defer batch.Release()
	// The prefix was checked against the header count, so a decoded batch that
	// disagrees would make NewSlice panic on an out-of-range bound.
	if uint64(batch.NumRows()) != payload.rows {
		return nil, fmt.Errorf(
			"arrow protocol: payload row count changed: header=%d decoded=%d",
			payload.rows,
			batch.NumRows(),
		)
	}

	suffix := batch.NewSlice(int64(acknowledgedPrefix), batch.NumRows())
	defer suffix.Release()
	serialized, err := p.serialize(suffix)
	if err != nil {
		return nil, fmt.Errorf("arrow protocol: serialize sliced suffix: %w", err)
	}
	return p.payloadFromCanonicalIPC(serialized, uint64(suffix.NumRows()))
}

func (p *Protocol) payloadFromCanonicalIPC(data []byte, rows uint64) (*Payload, error) {
	if len(data) == 0 || rows == 0 {
		return nil, fmt.Errorf("arrow protocol: canonical IPC payload is empty")
	}
	// data is the serializer's own buffer, which nothing else aliases, so the
	// payload can adopt it without a second full copy.
	return &Payload{ipcBytes: data, rows: rows}, nil
}

func (p *Protocol) validateBatch(batch arrow.RecordBatch) error {
	if !exactSchemaEqual(batch.Schema(), p.schema) {
		return fmt.Errorf(
			"arrow protocol: RecordBatch schema does not exactly match stream schema",
		)
	}
	if batch.NumRows() <= 0 {
		return fmt.Errorf("arrow protocol: RecordBatch must contain at least one row")
	}
	return nil
}

func exactSchemaEqual(left, right *arrow.Schema) bool {
	return left != nil && right != nil &&
		left.Equal(right) &&
		left.Metadata().Equal(right.Metadata())
}

// rowWindow is the absolute row range of one array node that a batch covers.
// Slicing rebases only the top-level nodes: a struct's fields stay parallel to
// the parent's offset and a list's values are reached through its offsets
// buffer, so a child still spans the parent's whole extent and cannot report
// its own covered rows.
type rowWindow struct {
	offset int64
	length int64
}

func windowOf(data arrow.ArrayData) rowWindow {
	return rowWindow{offset: int64(data.Offset()), length: int64(data.Len())}
}

// absentArrayData reports whether data is missing. array.Data returns a nil
// *Data as a non-nil interface for a node without a dictionary, so the typed
// nil has to be caught before any method call on it.
func absentArrayData(data arrow.ArrayData) bool {
	if data == nil {
		return true
	}
	concrete, ok := data.(*array.Data)
	return ok && concrete == nil
}

// totalRecordBufferSize sums the bytes a batch's columns hold. A slice shares
// its parent's buffers, so a buffer's own length reports the parent's whole
// extent: charging that would reject a ten-row slice of a million-row batch.
// Every node — nested children included — is therefore charged from the row
// window the batch covers rather than from buffer lengths. Layouts with no
// per-row rule (list-view, union, run-end-encoded) still fall back to whole
// buffers, an over-estimate that reconciliation corrects once the payload
// exists.
func totalRecordBufferSize(batch arrow.RecordBatch) int64 {
	seen := make(map[*memory.Buffer]struct{})
	var total int64
	for _, column := range batch.Columns() {
		data := column.Data()
		size, err := addInt64Saturating(
			total,
			arrayDataSize(data, windowOf(data), seen),
		)
		if err != nil {
			return math.MaxInt64
		}
		total = size
	}
	return total
}

func arrayDataSize(
	data arrow.ArrayData,
	window rowWindow,
	seen map[*memory.Buffer]struct{},
) int64 {
	if absentArrayData(data) {
		return 0
	}
	total := ownedBufferSize(data, window, seen)
	for _, child := range data.Children() {
		if absentArrayData(child) {
			continue
		}
		size, err := addInt64Saturating(
			total,
			arrayDataSize(child, childWindow(data, window, child), seen),
		)
		if err != nil {
			return math.MaxInt64
		}
		total = size
	}
	// A dictionary is shared whole rather than sliced per row, so it is charged
	// in full through the same recursion.
	dictionary := data.Dictionary()
	if absentArrayData(dictionary) {
		return total
	}
	size, err := addInt64Saturating(
		total,
		arrayDataSize(dictionary, windowOf(dictionary), seen),
	)
	if err != nil {
		return math.MaxInt64
	}
	return size
}

// childWindow maps a parent's covered rows onto one of its children. A child
// carries its own offset as well, so the parent's range is applied on top of
// it. An unrecognized nesting falls back to the child's whole extent.
func childWindow(
	parent arrow.ArrayData,
	window rowWindow,
	child arrow.ArrayData,
) rowWindow {
	base := int64(child.Offset())
	switch dataType := parent.DataType().(type) {
	case *arrow.StructType:
		return rowWindow{offset: base + window.offset, length: window.length}
	case *arrow.ListType, *arrow.MapType:
		if first, last, ok := offsetRange(parent, window, false); ok {
			return rowWindow{offset: base + first, length: last - first}
		}
	case *arrow.LargeListType:
		if first, last, ok := offsetRange(parent, window, true); ok {
			return rowWindow{offset: base + first, length: last - first}
		}
	case *arrow.FixedSizeListType:
		width := int64(dataType.Len())
		if width > 0 && window.offset <= math.MaxInt64/width &&
			window.length <= math.MaxInt64/width {
			return rowWindow{
				offset: base + window.offset*width,
				length: window.length * width,
			}
		}
	}
	return windowOf(child)
}

// ownedBufferSize charges one array node for the rows it actually covers.
func ownedBufferSize(
	data arrow.ArrayData,
	window rowWindow,
	seen map[*memory.Buffer]struct{},
) int64 {
	rows := window.length
	buffers := data.Buffers()
	if rows < 0 {
		return wholeBufferSize(buffers, seen)
	}
	var validityBytes int64
	if len(buffers) > 0 && buffers[0] != nil {
		validityBytes = bitmapBytes(rows)
	}
	switch dataType := data.DataType().(type) {
	case *arrow.StringType, *arrow.BinaryType:
		if valueBytes, ok := variableWidthValueBytes(data, window, false); ok {
			return validityBytes + (rows+1)*4 + valueBytes
		}
	case *arrow.LargeStringType, *arrow.LargeBinaryType:
		if valueBytes, ok := variableWidthValueBytes(data, window, true); ok {
			return validityBytes + (rows+1)*8 + valueBytes
		}
	case *arrow.ListType, *arrow.MapType:
		// The values are a child node, so only the offsets are charged here.
		if len(buffers) == 2 {
			return validityBytes + (rows+1)*4
		}
	case *arrow.LargeListType:
		if len(buffers) == 2 {
			return validityBytes + (rows+1)*8
		}
	case *arrow.StructType, *arrow.FixedSizeListType:
		// Both own a validity bitmap only; every value lives in a child.
		if len(buffers) <= 1 {
			return validityBytes
		}
	case arrow.FixedWidthDataType:
		// A wider layout would put row data in a buffer this arithmetic does not
		// know about, so only the canonical validity+values shape is derived.
		if len(buffers) <= 2 {
			return validityBytes + fixedWidthValueBytes(rows, dataType.BitWidth())
		}
	}
	return wholeBufferSize(buffers, seen)
}

// variableWidthValueBytes sizes exactly the values the covered rows reference.
func variableWidthValueBytes(
	data arrow.ArrayData,
	window rowWindow,
	large bool,
) (int64, bool) {
	if len(data.Buffers()) != 3 {
		return 0, false
	}
	first, last, ok := offsetRange(data, window, large)
	if !ok {
		return 0, false
	}
	return last - first, true
}

// offsetRange reads an offsets-buffer layout to find the first and last value
// index the covered rows reference.
func offsetRange(
	data arrow.ArrayData,
	window rowWindow,
	large bool,
) (first, last int64, ok bool) {
	buffers := data.Buffers()
	if len(buffers) < 2 || buffers[1] == nil {
		return 0, 0, false
	}
	start := window.offset
	end := start + window.length
	if start < 0 || end < start {
		return 0, 0, false
	}
	if large {
		offsets := arrow.Int64Traits.CastFromBytes(buffers[1].Bytes())
		if int64(len(offsets)) <= end {
			return 0, 0, false
		}
		first, last = offsets[start], offsets[end]
	} else {
		offsets := arrow.Int32Traits.CastFromBytes(buffers[1].Bytes())
		if int64(len(offsets)) <= end {
			return 0, 0, false
		}
		first, last = int64(offsets[start]), int64(offsets[end])
	}
	if first < 0 || last < first {
		return 0, 0, false
	}
	return first, last, true
}

func fixedWidthValueBytes(rows int64, bitWidth int) int64 {
	if bitWidth <= 0 {
		return 0
	}
	if bitWidth < 8 {
		return bitmapBytes(rows)
	}
	width := int64(bitWidth / 8)
	if rows > math.MaxInt64/width {
		return math.MaxInt64
	}
	return rows * width
}

func bitmapBytes(rows int64) int64 {
	return (rows + 7) / 8
}

func wholeBufferSize(
	buffers []*memory.Buffer,
	seen map[*memory.Buffer]struct{},
) int64 {
	var total int64
	for _, buffer := range buffers {
		if buffer == nil {
			continue
		}
		if _, exists := seen[buffer]; exists {
			continue
		}
		seen[buffer] = struct{}{}
		size, err := addInt64Saturating(total, int64(buffer.Len()))
		if err != nil {
			return math.MaxInt64
		}
		total = size
	}
	return total
}

// recordBatchMetadataSize charges a batch's custom metadata, which arrow-go
// writes into the IPC message header. A buffer walk cannot see it, so a batch
// carrying more of it than the slop covers would under-reserve.
func recordBatchMetadataSize(batch arrow.RecordBatch) int64 {
	withMetadata, ok := batch.(arrow.RecordBatchWithMetadata)
	if !ok {
		return 0
	}
	metadata := withMetadata.Metadata()
	values := metadata.Values()
	var total int64
	for index, key := range metadata.Keys() {
		entryBytes := metadataEntryOverheadBytes +
			int64(len(key)) + int64(len(values[index]))
		size, err := addInt64Saturating(total, entryBytes)
		if err != nil {
			return math.MaxInt64
		}
		total = size
	}
	return total
}

func addInt64Saturating(left, right int64) (int64, error) {
	if left < 0 || right < 0 || right > math.MaxInt64-left {
		return math.MaxInt64, fmt.Errorf("int64 size overflow")
	}
	return left + right, nil
}

// encodeSchemaIPC serializes schema as a schema-only Arrow IPC stream.
func encodeSchemaIPC(schema *arrow.Schema) ([]byte, error) {
	var output bytes.Buffer
	writer := ipc.NewWriter(&output, ipc.WithSchema(schema))
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("arrow protocol: serialize schema: %w", err)
	}
	return output.Bytes(), nil
}

// cloneSchemaThroughIPC rebuilds nested and dictionary DataTypes independently. A
// shallow field copy would still alias mutable pointer-backed types such as
// DictionaryType.
func cloneSchemaThroughIPC(
	schema *arrow.Schema,
	allocator memory.Allocator,
) (*arrow.Schema, error) {
	var output bytes.Buffer
	writer := ipc.NewWriter(
		&output,
		ipc.WithSchema(schema),
		ipc.WithAllocator(allocator),
	)
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("arrow protocol: serialize schema: %w", err)
	}
	reader, err := ipc.NewReader(
		bytes.NewReader(output.Bytes()),
		ipc.WithAllocator(allocator),
	)
	if err != nil {
		return nil, fmt.Errorf("arrow protocol: deserialize schema: %w", err)
	}
	defer reader.Release()
	decoded := reader.Schema()
	metadata := decoded.Metadata()
	return arrow.NewSchemaWithEndian(
		decoded.Fields(),
		&metadata,
		decoded.Endianness(),
	), nil
}

// decodeOne parses exactly one non-empty RecordBatch carrying the protocol
// schema, rejecting anything the payload contract does not allow.
func (p *Protocol) decodeOne(data []byte) (arrow.RecordBatch, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("arrow protocol: IPC input is empty")
	}
	source := bytes.NewReader(data)
	reader, err := ipc.NewReader(
		source,
		ipc.WithAllocator(p.allocator),
	)
	if err != nil {
		return nil, fmt.Errorf("arrow protocol: invalid IPC stream: %w", err)
	}
	defer reader.Release()
	if !exactSchemaEqual(reader.Schema(), p.schema) {
		return nil, fmt.Errorf("arrow protocol: IPC schema does not exactly match stream schema")
	}
	if !reader.Next() {
		if err := reader.Err(); err != nil {
			return nil, fmt.Errorf("arrow protocol: read IPC RecordBatch: %w", err)
		}
		return nil, fmt.Errorf("arrow protocol: IPC stream contains no RecordBatch")
	}
	batch := reader.RecordBatch()
	batch.Retain()
	if batch.NumRows() <= 0 {
		batch.Release()
		return nil, fmt.Errorf("arrow protocol: IPC RecordBatch must contain at least one row")
	}
	if reader.Next() {
		batch.Release()
		return nil, fmt.Errorf(
			"arrow protocol: IPC stream must contain exactly one RecordBatch",
		)
	}
	if err := reader.Err(); err != nil {
		batch.Release()
		return nil, fmt.Errorf("arrow protocol: read trailing IPC data: %w", err)
	}
	if source.Len() != 0 {
		batch.Release()
		return nil, fmt.Errorf(
			"arrow protocol: IPC stream contains %d trailing bytes",
			source.Len(),
		)
	}
	return batch, nil
}

func (p *Protocol) serialize(batch arrow.RecordBatch) ([]byte, error) {
	var output bytes.Buffer
	options, err := p.ipcOptions()
	if err != nil {
		return nil, err
	}
	options = append(options, ipc.WithSchema(p.schema))
	writer := ipc.NewWriter(&output, options...)
	if err := writer.Write(batch); err != nil {
		_ = writer.Close()
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func (p *Protocol) ipcOptions() ([]ipc.Option, error) {
	compression, err := compressionOption(p.compression)
	if err != nil {
		return nil, err
	}
	options := []ipc.Option{ipc.WithAllocator(p.allocator)}
	if compression != nil {
		options = append(options, compression)
	}
	return options, nil
}

func compressionOption(compression Compression) (ipc.Option, error) {
	switch compression {
	case CompressionNone:
		return nil, nil
	case CompressionLZ4:
		return ipc.WithLZ4(), nil
	case CompressionZstd:
		return ipc.WithZstd(), nil
	default:
		return nil, fmt.Errorf(
			"arrow protocol: unsupported IPC compression %d",
			compression,
		)
	}
}
