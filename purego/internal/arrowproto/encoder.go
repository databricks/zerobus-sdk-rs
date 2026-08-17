// Package arrowproto implements the Arrow IPC payload and the encoder that turns
// it into Arrow Flight frames. Payloads hold only self-contained IPC bytes, so
// caller-owned Arrow arrays are never retained by the stream core.
package arrowproto

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/protobuf/proto"

	"github.com/databricks/zerobus-sdk/purego/internal/stream"
	"github.com/databricks/zerobus-sdk/purego/internal/transport"
)

// TargetFlightDataBytes caps the protobuf size of a non-schema FlightData frame.
const TargetFlightDataBytes = 2 * 1024 * 1024

const payloadOverheadBytes = int64(64)
const admissionSlopBytes = int64(64 * 1024)

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
// chunkRows is the frame plan the sender replays after decoding that stream once.
type Payload struct {
	ipcBytes  []byte
	rows      uint64
	chunkRows []int64
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
	return payloadOverheadBytes + int64(cap(p.ipcBytes)) +
		int64(cap(p.chunkRows))*8
}

// IPCBytes returns a caller-owned copy of the self-contained IPC stream.
func (p *Payload) IPCBytes() []byte {
	if p == nil {
		return nil
	}
	return bytes.Clone(p.ipcBytes)
}

// Protocol owns one exact Arrow schema and the IPC/Flight encoding policy.
type Protocol struct {
	schema             *arrow.Schema
	compression        Compression
	allocator          memory.Allocator
	schemaFrame        *flight.FlightData
	admissionBaseBytes int64
	chunkProbe         func(rowStart, rowCount int64)
}

// EncodeSchemaIPC serializes schema as a schema-only Arrow IPC stream.
func EncodeSchemaIPC(schema *arrow.Schema) ([]byte, error) {
	if schema == nil {
		return nil, fmt.Errorf("arrow protocol: schema is required")
	}
	var output bytes.Buffer
	writer := ipc.NewWriter(&output, ipc.WithSchema(schema))
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("arrow protocol: serialize schema: %w", err)
	}
	return output.Bytes(), nil
}

// DecodeSchemaIPC parses a schema-only Arrow IPC stream and returns an
// independent schema. Record batches in the input are rejected.
func DecodeSchemaIPC(data []byte) (*arrow.Schema, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("arrow protocol: schema IPC input is empty")
	}
	source := bytes.NewReader(data)
	reader, err := ipc.NewReader(source)
	if err != nil {
		return nil, fmt.Errorf("arrow protocol: invalid schema IPC stream: %w", err)
	}
	defer reader.Release()
	if reader.Next() {
		return nil, fmt.Errorf("arrow protocol: schema IPC stream must not contain RecordBatches")
	}
	if err := reader.Err(); err != nil {
		return nil, fmt.Errorf("arrow protocol: read schema IPC stream: %w", err)
	}
	if source.Len() != 0 {
		return nil, fmt.Errorf(
			"arrow protocol: schema IPC stream contains %d trailing bytes",
			source.Len(),
		)
	}
	return cloneSchemaThroughIPC(reader.Schema(), memory.DefaultAllocator)
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
	p := &Protocol{
		schema:      ownedSchema,
		compression: options.Compression,
		allocator:   allocator,
	}
	frame, err := p.makeSchemaFrame()
	if err != nil {
		return nil, err
	}
	p.schemaFrame = frame
	p.admissionBaseBytes = admissionSlopBytes + 2*int64(proto.Size(frame))
	return p, nil
}

// SchemaFlightData returns a caller-owned schema frame for the first DoPut, with
// no body and no app metadata.
func (p *Protocol) SchemaFlightData() *flight.FlightData {
	if p == nil || p.schemaFrame == nil {
		return nil
	}
	return proto.Clone(p.schemaFrame).(*flight.FlightData)
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
	chunkRows, err := p.planChunks(batch)
	if err != nil {
		return nil, err
	}
	return p.payloadFromCanonicalIPC(
		serialized,
		uint64(batch.NumRows()),
		chunkRows,
	)
}

// EncodeIPC validates one non-empty RecordBatch against the exact schema and
// reserializes it, hydrating dictionary state into a standalone stream rather
// than depending on the caller's frames.
func (p *Protocol) EncodeIPC(data []byte) (*Payload, error) {
	batch, err := p.decodeOne(data)
	if err != nil {
		return nil, err
	}
	defer batch.Release()

	serialized, err := p.serialize(batch)
	if err != nil {
		return nil, fmt.Errorf("arrow protocol: canonicalize IPC RecordBatch: %w", err)
	}
	chunkRows, err := p.planChunks(batch)
	if err != nil {
		return nil, err
	}
	return p.payloadFromCanonicalIPC(
		serialized,
		uint64(batch.NumRows()),
		chunkRows,
	)
}

// EstimateRecordBatchRetainedSize returns a conservative pre-materialization
// reservation. Arrow's buffer total already covers nested children and
// dictionaries; doubling it covers framing, compression, and buffer growth.
func (p *Protocol) EstimateRecordBatchRetainedSize(
	batch arrow.RecordBatch,
) (int64, error) {
	if batch == nil {
		return 0, fmt.Errorf("arrow protocol: RecordBatch is required")
	}
	if err := p.validateBatch(batch); err != nil {
		return 0, err
	}
	rowPlanBytes := int64(batch.NumRows())
	if rowPlanBytes > math.MaxInt64/8 {
		return math.MaxInt64, nil
	}
	metadataBytes := recordBatchMetadataSize(batch)
	inputBytes, err := addInt64Saturating(
		totalRecordBufferSize(batch),
		metadataBytes,
	)
	if err != nil {
		return math.MaxInt64, nil
	}
	return p.admissionEstimate(inputBytes, 0, rowPlanBytes*8), nil
}

// EstimateIPCRetainedSize reserves for canonicalizing one caller-owned IPC
// stream. Compressed metadata is inspected before Arrow sees the input, so
// declared uncompressed sizes are part of the reservation.
func (p *Protocol) EstimateIPCRetainedSize(data []byte) (int64, error) {
	expandedBytes, err := preflightIPCExpansion(data)
	if err != nil {
		return 0, err
	}
	return p.admissionEstimate(int64(len(data)), expandedBytes, 0), nil
}

func (p *Protocol) admissionEstimate(
	inputBytes, expandedBytes, extraBytes int64,
) int64 {
	if inputBytes < 0 ||
		expandedBytes < 0 ||
		extraBytes < 0 ||
		p.admissionBaseBytes > math.MaxInt64-payloadOverheadBytes {
		return math.MaxInt64
	}
	total := payloadOverheadBytes + p.admissionBaseBytes
	for _, component := range []struct {
		value int64
		scale int64
	}{
		{value: inputBytes, scale: 2},
		{value: expandedBytes, scale: 2},
		{value: extraBytes, scale: 1},
	} {
		if component.value > math.MaxInt64/component.scale {
			return math.MaxInt64
		}
		scaled := component.value * component.scale
		if scaled > math.MaxInt64-total {
			return math.MaxInt64
		}
		total += scaled
	}
	return total
}

// DecodeIPCRecordBatch parses exactly one non-empty RecordBatch with the
// protocol schema. The caller owns the returned reference and must Release it.
func (p *Protocol) DecodeIPCRecordBatch(data []byte) (arrow.RecordBatch, error) {
	if p == nil {
		return nil, fmt.Errorf("arrow protocol: protocol is nil")
	}
	return p.decodeOne(data)
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
	chunkRows, err := p.planChunks(suffix)
	if err != nil {
		return nil, err
	}
	return p.payloadFromCanonicalIPC(
		serialized,
		uint64(suffix.NumRows()),
		chunkRows,
	)
}

// Decode returns the payload as caller-owned IPC bytes shaped for
// stream.EncoderHooks.
func (p *Protocol) Decode(payload *Payload) [][]byte {
	if payload == nil {
		return nil
	}
	return [][]byte{payload.IPCBytes()}
}

// EncoderHooks returns the stream-core hooks for Arrow payloads. The []byte entry
// points take one self-contained IPC RecordBatch; typed callers should prefer
// EncodeRecordBatch with CoreStream.EnqueuePayload.
func (p *Protocol) EncoderHooks() stream.EncoderHooks[*Payload] {
	return stream.EncoderHooks[*Payload]{
		EncodeRecord: p.EncodeIPC,
		EncodeBatch: func(records [][]byte) (*Payload, error) {
			if len(records) != 1 {
				return nil, fmt.Errorf(
					"arrow protocol: IPC batch input must contain exactly one payload, got %d",
					len(records),
				)
			}
			return p.EncodeIPC(records[0])
		},
		StampOffset: func(*Payload, int64) {},
		UnitCount: func(payload *Payload) uint64 {
			return payload.UnitCount()
		},
		Slice:  p.Slice,
		Decode: p.Decode,
		MaxWireSize: func(payload *Payload) int {
			if payload == nil {
				return 0
			}
			return TargetFlightDataBytes
		},
		RetainedSize: func(rawBytes, recordCount int) int64 {
			return payloadOverheadBytes + int64(rawBytes) + int64(recordCount)*8
		},
		// Canonicalizing re-serializes with this protocol's compression, so the
		// input length says nothing about what the payload retains: compressed
		// input expands, uncompressed input may shrink. Charge the real figure.
		ActualRetainedSize: func(payload *Payload) int64 {
			return payload.RetainedSize()
		},
	}
}

// EncodeFlightData materializes payload into caller-owned non-schema frames. The
// wire path drives the same emitter with a live sink, so it never buffers a whole
// payload before the first Send.
func (p *Protocol) EncodeFlightData(
	payload *Payload,
	startOffset int64,
) ([]*flight.FlightData, int64, error) {
	if startOffset < 0 {
		return nil, startOffset, fmt.Errorf(
			"arrow protocol: Flight frame offset must be non-negative, got %d",
			startOffset,
		)
	}
	var frames []*flight.FlightData
	emitter := &flightFrameEmitter{
		send: func(frame *flight.FlightData) error {
			frames = append(frames, proto.Clone(frame).(*flight.FlightData))
			return nil
		},
		nextOffset: startOffset,
		skipSchema: true,
	}
	options, err := p.flightWriterOptions()
	if err != nil {
		return nil, startOffset, err
	}
	writer := flight.NewRecordWriter(emitter, options...)
	if err := p.emitPayload(payload, writer, emitter); err != nil {
		_ = writer.Close()
		return nil, startOffset, err
	}
	if err := writer.Close(); err != nil {
		return nil, startOffset, fmt.Errorf("arrow protocol: close Flight encoder: %w", err)
	}
	return frames, emitter.nextOffset, nil
}

// flightFrameEmitter stamps sequential metadata and hands each frame straight to
// its sink. One connection-scoped ipc.Writer feeds it, so Arrow Go caches an
// unchanged dictionary instead of re-emitting it per chunk.
type flightFrameEmitter struct {
	send       func(*flight.FlightData) error
	nextOffset int64
	exhausted  bool
	skipSchema bool
	highest    *atomic.Int64

	currentRowEnd uint64
	receipt       *stream.SubmissionReceipt
}

func (e *flightFrameEmitter) Send(frame *flight.FlightData) error {
	if frame == nil {
		return fmt.Errorf("arrow protocol: Flight encoder emitted a nil frame")
	}
	messageType, err := flightMessageType(frame)
	if err != nil {
		return err
	}
	if messageType == ipc.MessageSchema {
		if !e.skipSchema {
			return fmt.Errorf("arrow protocol: Flight encoder emitted a duplicate schema frame")
		}
		e.skipSchema = false
		if len(frame.GetDataBody()) != 0 {
			return fmt.Errorf("arrow protocol: encoded Flight schema has a body")
		}
		return nil
	}
	if e.skipSchema {
		return fmt.Errorf("arrow protocol: Flight encoder omitted its schema frame")
	}
	if e.exhausted {
		return fmt.Errorf("arrow protocol: Flight frame offset space exhausted")
	}
	metadata, err := json.Marshal(transport.FlightBatchMetadata{
		OffsetID: e.nextOffset,
	})
	if err != nil {
		return fmt.Errorf("arrow protocol: marshal Flight frame metadata: %w", err)
	}
	frame.AppMetadata = metadata
	if size := proto.Size(frame); size > TargetFlightDataBytes {
		return fmt.Errorf(
			"arrow protocol: encoded FlightData is %d bytes, exceeds %d-byte target",
			size,
			TargetFlightDataBytes,
		)
	}
	if e.highest != nil {
		// Publish before Send: the server may respond as soon as gRPC accepts the
		// frame, concurrently with Send returning.
		e.highest.Store(e.nextOffset)
	}
	if err := e.send(frame); err != nil {
		return err
	}
	if messageType == ipc.MessageRecordBatch && e.receipt != nil {
		e.receipt.SubmittedUnits = e.currentRowEnd
	}
	if e.nextOffset == math.MaxInt64 {
		e.exhausted = true
	} else {
		e.nextOffset++
	}
	return nil
}

func flightMessageType(frame *flight.FlightData) (ipc.MessageType, error) {
	if len(frame.GetDataHeader()) == 0 {
		return ipc.MessageNone, fmt.Errorf("arrow protocol: Flight frame has no IPC header")
	}
	meta := memory.NewBufferBytes(frame.GetDataHeader())
	body := memory.NewBufferBytes(frame.GetDataBody())
	message := ipc.NewMessage(meta, body)
	meta.Release()
	body.Release()
	defer message.Release()
	return message.Type(), nil
}

func (p *Protocol) payloadFromCanonicalIPC(
	data []byte,
	rows uint64,
	chunkRows []int64,
) (*Payload, error) {
	if len(data) == 0 || rows == 0 || len(chunkRows) == 0 {
		return nil, fmt.Errorf("arrow protocol: canonical IPC payload is empty")
	}
	// A plan that does not cover exactly the rows the payload reports would
	// otherwise surface as a short submission mid-send, long after encoding.
	var planned int64
	for _, rowCount := range chunkRows {
		if rowCount <= 0 || planned > math.MaxInt64-rowCount {
			return nil, fmt.Errorf("arrow protocol: invalid Flight chunk plan")
		}
		planned += rowCount
	}
	if uint64(planned) != rows {
		return nil, fmt.Errorf(
			"arrow protocol: Flight chunk plan covers %d of %d rows",
			planned,
			rows,
		)
	}
	// data is the serializer's own buffer, which nothing else aliases, so the
	// payload can adopt it without a second full copy.
	return &Payload{ipcBytes: data, rows: rows, chunkRows: chunkRows}, nil
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

// totalRecordBufferSize sums the bytes a batch's columns hold. A slice shares
// its parent's buffers, so a buffer's own length reports the parent's whole
// extent: charging that would reject a ten-row slice of a million-row batch.
// Layouts with a derivable per-row extent are charged for their own rows only;
// the rest fall back to whole buffers, an over-estimate that reconciliation
// corrects once the payload exists.
func totalRecordBufferSize(batch arrow.RecordBatch) int64 {
	seen := make(map[*memory.Buffer]struct{})
	var total int64
	for _, column := range batch.Columns() {
		size, err := addInt64Saturating(total, arrayDataSize(column.Data(), seen))
		if err != nil {
			return math.MaxInt64
		}
		total = size
	}
	return total
}

func arrayDataSize(data arrow.ArrayData, seen map[*memory.Buffer]struct{}) int64 {
	if data == nil {
		return 0
	}
	if concrete, ok := data.(*array.Data); ok && concrete == nil {
		return 0
	}
	total := ownedBufferSize(data, seen)
	for _, child := range data.Children() {
		size, err := addInt64Saturating(total, arrayDataSize(child, seen))
		if err != nil {
			return math.MaxInt64
		}
		total = size
	}
	// A dictionary is shared whole rather than sliced per row, so it is charged
	// in full through the same recursion.
	size, err := addInt64Saturating(total, arrayDataSize(data.Dictionary(), seen))
	if err != nil {
		return math.MaxInt64
	}
	return size
}

// ownedBufferSize charges one array node for the rows it actually covers.
func ownedBufferSize(data arrow.ArrayData, seen map[*memory.Buffer]struct{}) int64 {
	rows := int64(data.Len())
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
		if valueBytes, ok := variableWidthValueBytes(data, rows, false); ok {
			return validityBytes + (rows+1)*4 + valueBytes
		}
	case *arrow.LargeStringType, *arrow.LargeBinaryType:
		if valueBytes, ok := variableWidthValueBytes(data, rows, true); ok {
			return validityBytes + (rows+1)*8 + valueBytes
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

// variableWidthValueBytes reads the offsets buffer to size exactly the values
// this array's rows reference.
func variableWidthValueBytes(
	data arrow.ArrayData,
	rows int64,
	large bool,
) (int64, bool) {
	buffers := data.Buffers()
	if len(buffers) != 3 || buffers[1] == nil {
		return 0, false
	}
	start := int64(data.Offset())
	end := start + rows
	if start < 0 || end < start {
		return 0, false
	}
	var first, last int64
	if large {
		offsets := arrow.Int64Traits.CastFromBytes(buffers[1].Bytes())
		if int64(len(offsets)) <= end {
			return 0, false
		}
		first, last = offsets[start], offsets[end]
	} else {
		offsets := arrow.Int32Traits.CastFromBytes(buffers[1].Bytes())
		if int64(len(offsets)) <= end {
			return 0, false
		}
		first, last = int64(offsets[start]), int64(offsets[end])
	}
	if first < 0 || last < first {
		return 0, false
	}
	return last - first, true
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

func recordBatchMetadataSize(batch arrow.RecordBatch) int64 {
	withMetadata, ok := batch.(arrow.RecordBatchWithMetadata)
	if !ok {
		return 0
	}
	metadata := withMetadata.Metadata()
	keys := metadata.Keys()
	values := metadata.Values()
	var total int64
	for index, key := range keys {
		value := values[index]
		// Count FlatBuffer vector/table/offset overhead on top of UTF-8 contents.
		// The full estimate doubles this for materialization.
		entryBytes, err := addInt64Saturating(int64(len(key)), int64(len(value)))
		if err == nil {
			entryBytes, err = addInt64Saturating(entryBytes, 32)
		}
		if err == nil {
			total, err = addInt64Saturating(total, entryBytes)
		}
		if err != nil {
			return math.MaxInt64
		}
	}
	return total
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

func (p *Protocol) decodeOne(data []byte) (arrow.RecordBatch, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("arrow protocol: IPC input is empty")
	}
	// Cross-check declared message and buffer sizes against the input before
	// Arrow allocates against them: every decode entry point runs through here.
	if _, err := preflightIPCExpansion(data); err != nil {
		return nil, err
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

func (p *Protocol) makeSchemaFrame() (*flight.FlightData, error) {
	collector := new(flightDataCollector)
	options, err := p.ipcOptions()
	if err != nil {
		return nil, err
	}
	options = append(options, ipc.WithSchema(p.schema))
	writer := flight.NewRecordWriter(collector, options...)
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("arrow protocol: encode Flight schema: %w", err)
	}
	if len(collector.frames) != 1 {
		return nil, fmt.Errorf(
			"arrow protocol: Flight schema encoding produced %d frames, want 1",
			len(collector.frames),
		)
	}
	frame := collector.frames[0]
	if len(frame.GetDataHeader()) == 0 ||
		len(frame.GetDataBody()) != 0 ||
		len(frame.GetAppMetadata()) != 0 {
		return nil, fmt.Errorf("arrow protocol: malformed encoded Flight schema frame")
	}
	return frame, nil
}

func (p *Protocol) emitPayload(
	payload *Payload,
	writer *flight.Writer,
	emitter *flightFrameEmitter,
) error {
	if payload == nil {
		return fmt.Errorf("arrow protocol: payload is nil")
	}
	batch, err := p.decodeOne(payload.ipcBytes)
	if err != nil {
		return fmt.Errorf("arrow protocol: decode Flight payload: %w", err)
	}
	defer batch.Release()
	if uint64(batch.NumRows()) != payload.rows {
		return fmt.Errorf(
			"arrow protocol: payload row count changed: header=%d decoded=%d",
			payload.rows,
			batch.NumRows(),
		)
	}

	rowStart := int64(0)
	for _, rowCount := range payload.chunkRows {
		if rowCount <= 0 || rowStart > batch.NumRows()-rowCount {
			return fmt.Errorf("arrow protocol: invalid Flight chunk plan")
		}
		rowEnd := rowStart + rowCount
		chunk := batch.NewSlice(rowStart, rowEnd)
		emitter.currentRowEnd = uint64(rowEnd)
		err = writer.Write(chunk)
		chunk.Release()
		if err != nil {
			return fmt.Errorf(
				"arrow protocol: encode/send Flight rows [%d,%d): %w",
				rowStart,
				rowEnd,
				err,
			)
		}
		rowStart = rowEnd
	}
	if rowStart != batch.NumRows() {
		return fmt.Errorf(
			"arrow protocol: Flight chunk plan covers %d of %d rows",
			rowStart,
			batch.NumRows(),
		)
	}
	return nil
}

type chunkSearch struct {
	bytesPerRow float64
	priorRows   int64
}

type chunkMeasurement struct {
	recordBytes int
}

func (p *Protocol) planChunks(batch arrow.RecordBatch) ([]int64, error) {
	average := float64(totalRecordBufferSize(batch)) / float64(batch.NumRows())
	if average < 1 {
		average = 1
	}
	search := chunkSearch{bytesPerRow: average}
	chunks := make([]int64, 0, 1)
	for rowStart := int64(0); rowStart < batch.NumRows(); {
		rowCount, err := p.findChunkRows(batch, rowStart, &search)
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, rowCount)
		rowStart += rowCount
	}
	return chunks, nil
}

func (p *Protocol) findChunkRows(
	batch arrow.RecordBatch,
	rowStart int64,
	search *chunkSearch,
) (int64, error) {
	remaining := batch.NumRows() - rowStart
	if remaining <= 0 {
		return 0, fmt.Errorf("arrow protocol: no rows remain for Flight chunk")
	}
	estimated := search.priorRows
	if estimated <= 0 {
		estimated = int64(float64(TargetFlightDataBytes-1024) / search.bytesPerRow)
	}
	if estimated < 1 {
		estimated = 1
	}
	if estimated > remaining {
		estimated = remaining
	}

	cache := make(map[int64]chunkMeasurement)
	measure := func(rows int64) (chunkMeasurement, error) {
		if measured, ok := cache[rows]; ok {
			return measured, nil
		}
		measured, err := p.measureChunk(batch, rowStart, rows)
		if err == nil {
			cache[rows] = measured
		}
		return measured, err
	}
	fits := func(measured chunkMeasurement) bool {
		return measured.recordBytes <= TargetFlightDataBytes
	}

	measured, err := measure(estimated)
	if err != nil {
		return 0, err
	}
	var best int64
	var low, high int64
	if fits(measured) {
		best = estimated
		if best == remaining {
			search.priorRows = best
			return best, nil
		}
		// Probe outward from the local prior with exponentially growing steps: this
		// brackets the failure without rescanning the whole batch per chunk.
		step := int64(1)
		for {
			candidate := estimated + step
			if candidate < estimated || candidate > remaining {
				candidate = remaining
			}
			candidateMeasurement, err := measure(candidate)
			if err != nil {
				return 0, err
			}
			if fits(candidateMeasurement) {
				best = candidate
				if candidate == remaining {
					low, high = 1, 0
					break
				}
				if step > math.MaxInt64/2 {
					step = remaining - estimated
				} else {
					step *= 2
				}
				continue
			}
			low, high = best+1, candidate-1
			break
		}
	} else {
		if estimated == 1 {
			return 0, fmt.Errorf(
				"arrow protocol: one-row FlightData exceeds %d-byte target",
				TargetFlightDataBytes,
			)
		}
		failed := estimated
		step := int64(1)
		for {
			candidate := estimated - step
			if candidate < 1 || candidate > estimated {
				candidate = 1
			}
			candidateMeasurement, err := measure(candidate)
			if err != nil {
				return 0, err
			}
			if fits(candidateMeasurement) {
				best = candidate
				low, high = candidate+1, failed-1
				break
			}
			if candidate == 1 {
				return 0, fmt.Errorf(
					"arrow protocol: one-row FlightData exceeds %d-byte target",
					TargetFlightDataBytes,
				)
			}
			failed = candidate
			if step > math.MaxInt64/2 {
				step = estimated - 1
			} else {
				step *= 2
			}
		}
	}

	// Actual compressed protobuf size is authoritative; search only the bracket.
	for low <= high {
		mid := low + (high-low)/2
		candidate, err := measure(mid)
		if err != nil {
			return 0, err
		}
		if fits(candidate) {
			best = mid
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	accepted, err := measure(best)
	if err != nil {
		return 0, err
	}
	if accepted.recordBytes > 0 {
		search.bytesPerRow = float64(accepted.recordBytes) / float64(best)
		if search.bytesPerRow < 1 {
			search.bytesPerRow = 1
		}
	}
	search.priorRows = best
	return best, nil
}

func (p *Protocol) measureChunk(
	batch arrow.RecordBatch,
	rowStart, rowCount int64,
) (chunkMeasurement, error) {
	if rowCount <= 0 || rowStart < 0 || rowStart > batch.NumRows()-rowCount {
		return chunkMeasurement{}, fmt.Errorf("arrow protocol: invalid Flight chunk range")
	}
	if p.chunkProbe != nil {
		p.chunkProbe(rowStart, rowCount)
	}
	sizer := &flightDataSizer{skipSchema: true}
	options, err := p.flightWriterOptions()
	if err != nil {
		return chunkMeasurement{}, err
	}
	writer := flight.NewRecordWriter(sizer, options...)
	chunk := batch.NewSlice(rowStart, rowStart+rowCount)
	err = writer.Write(chunk)
	chunk.Release()
	if err != nil {
		_ = writer.Close()
		return chunkMeasurement{}, fmt.Errorf("arrow protocol: size Flight row chunk: %w", err)
	}
	if err := writer.Close(); err != nil {
		return chunkMeasurement{}, fmt.Errorf("arrow protocol: close Flight chunk sizer: %w", err)
	}
	if sizer.recordFrames != 1 {
		return chunkMeasurement{}, fmt.Errorf(
			"arrow protocol: Flight row chunk produced %d record frames, want 1",
			sizer.recordFrames,
		)
	}
	// Slicing rows never shrinks a dictionary, so an oversized one is a property
	// of the batch. Report it here rather than letting the row search bottom out
	// at one row and blame the row.
	if sizer.dictionaryBytes > TargetFlightDataBytes {
		return chunkMeasurement{}, fmt.Errorf(
			"arrow protocol: dictionary FlightData is %d bytes, exceeds %d-byte target",
			sizer.dictionaryBytes,
			TargetFlightDataBytes,
		)
	}
	return chunkMeasurement{recordBytes: sizer.recordBytes}, nil
}

type flightDataSizer struct {
	skipSchema      bool
	recordBytes     int
	recordFrames    int
	dictionaryBytes int
}

func (s *flightDataSizer) Send(frame *flight.FlightData) error {
	messageType, err := flightMessageType(frame)
	if err != nil {
		return err
	}
	if messageType == ipc.MessageSchema {
		if !s.skipSchema {
			return fmt.Errorf("arrow protocol: chunk sizer received duplicate schema")
		}
		s.skipSchema = false
		return nil
	}
	frame.AppMetadata = []byte(`{"offset_id":9223372036854775807}`)
	size := proto.Size(frame)
	// The record frame alone decides the row count. A dictionary rides in its own
	// frame, and the live writer emits it once per connection rather than once
	// per chunk, so folding it in here would shrink every chunk for nothing.
	if messageType == ipc.MessageRecordBatch {
		s.recordFrames++
		s.recordBytes = size
	} else if size > s.dictionaryBytes {
		s.dictionaryBytes = size
	}
	return nil
}

func (p *Protocol) flightWriterOptions() ([]ipc.Option, error) {
	options, err := p.ipcOptions()
	if err != nil {
		return nil, err
	}
	return append(
		options,
		ipc.WithSchema(p.schema),
		ipc.WithDictionaryDeltas(true),
	), nil
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

type flightDataCollector struct {
	frames []*flight.FlightData
}

func (c *flightDataCollector) Send(frame *flight.FlightData) error {
	if frame == nil {
		return fmt.Errorf("arrow protocol: Flight encoder emitted a nil frame")
	}
	c.frames = append(c.frames, proto.Clone(frame).(*flight.FlightData))
	return nil
}
