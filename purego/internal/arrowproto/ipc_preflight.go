package arrowproto

import (
	"encoding/binary"
	"fmt"
	"math"

	flatbuffers "github.com/google/flatbuffers/go"
)

const (
	ipcContinuationToken = uint32(0xffffffff)

	ipcHeaderSchema          = byte(1)
	ipcHeaderDictionaryBatch = byte(2)
	ipcHeaderRecordBatch     = byte(3)
)

// preflightIPCExpansion walks encapsulated IPC message metadata without
// constructing Arrow arrays. For compressed record and dictionary batches it
// sums the per-buffer uncompressed-size prefixes that Arrow would otherwise use
// directly as allocation sizes.
func preflightIPCExpansion(data []byte) (expandedBytes int64, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			expandedBytes = 0
			err = fmt.Errorf("arrow protocol: invalid IPC metadata: %v", recovered)
		}
	}()
	if len(data) == 0 {
		return 0, fmt.Errorf("arrow protocol: IPC input is empty")
	}

	position := int64(0)
	sawSchema := false
	sawRecordBatch := false
	for position < int64(len(data)) {
		first, next, takeErr := takeIPCBytes(data, position, 4)
		if takeErr != nil {
			return 0, takeErr
		}
		position = next
		indicator := binary.LittleEndian.Uint32(first)

		var metadataLength uint32
		switch indicator {
		case 0:
			if position != int64(len(data)) {
				return 0, fmt.Errorf(
					"arrow protocol: IPC stream contains %d trailing bytes",
					int64(len(data))-position,
				)
			}
			position = int64(len(data))
			continue
		case ipcContinuationToken:
			lengthBytes, next, lengthErr := takeIPCBytes(data, position, 4)
			if lengthErr != nil {
				return 0, lengthErr
			}
			position = next
			metadataLength = binary.LittleEndian.Uint32(lengthBytes)
			if metadataLength == 0 {
				if position != int64(len(data)) {
					return 0, fmt.Errorf(
						"arrow protocol: IPC stream contains %d trailing bytes",
						int64(len(data))-position,
					)
				}
				position = int64(len(data))
				continue
			}
		default:
			metadataLength = indicator
		}
		if metadataLength < 4 {
			return 0, fmt.Errorf(
				"arrow protocol: invalid IPC message metadata length %d",
				metadataLength,
			)
		}
		metadata, next, metadataErr := takeIPCBytes(
			data,
			position,
			int64(metadataLength),
		)
		if metadataErr != nil {
			return 0, metadataErr
		}
		position = next

		message := ipcRootTable(metadata)
		headerType := message.GetByteSlot(6, 0)
		bodyLength := message.GetInt64Slot(10, 0)
		if bodyLength < 0 {
			return 0, fmt.Errorf(
				"arrow protocol: invalid IPC message body length %d",
				bodyLength,
			)
		}
		body, next, bodyErr := takeIPCBytes(data, position, bodyLength)
		if bodyErr != nil {
			return 0, bodyErr
		}
		position = next

		switch headerType {
		case ipcHeaderSchema:
			if sawSchema || sawRecordBatch {
				return 0, fmt.Errorf("arrow protocol: IPC stream contains an unexpected schema message")
			}
			sawSchema = true
		case ipcHeaderDictionaryBatch:
			if !sawSchema || sawRecordBatch {
				return 0, fmt.Errorf("arrow protocol: IPC dictionary message is out of order")
			}
			header, headerErr := ipcMessageHeader(message)
			if headerErr != nil {
				return 0, headerErr
			}
			recordBatch, recordErr := ipcDictionaryRecordBatch(header)
			if recordErr != nil {
				return 0, recordErr
			}
			expanded, expansionErr := ipcCompressedExpansion(recordBatch, body)
			if expansionErr != nil {
				return 0, expansionErr
			}
			expandedBytes, err = addInt64Saturating(expandedBytes, expanded)
			if err != nil {
				return math.MaxInt64, err
			}
		case ipcHeaderRecordBatch:
			if !sawSchema || sawRecordBatch {
				return 0, fmt.Errorf(
					"arrow protocol: IPC stream must contain exactly one RecordBatch",
				)
			}
			sawRecordBatch = true
			recordBatch, headerErr := ipcMessageHeader(message)
			if headerErr != nil {
				return 0, headerErr
			}
			expanded, expansionErr := ipcCompressedExpansion(recordBatch, body)
			if expansionErr != nil {
				return 0, expansionErr
			}
			expandedBytes, err = addInt64Saturating(expandedBytes, expanded)
			if err != nil {
				return math.MaxInt64, err
			}
		default:
			return 0, fmt.Errorf(
				"arrow protocol: unsupported IPC message header type %d",
				headerType,
			)
		}
	}
	if !sawSchema {
		return 0, fmt.Errorf("arrow protocol: IPC stream contains no schema")
	}
	if !sawRecordBatch {
		return 0, fmt.Errorf("arrow protocol: IPC stream contains no RecordBatch")
	}
	return expandedBytes, nil
}

func takeIPCBytes(data []byte, position, length int64) ([]byte, int64, error) {
	if position < 0 || length < 0 ||
		position > int64(len(data)) ||
		length > int64(len(data))-position {
		return nil, position, fmt.Errorf(
			"arrow protocol: IPC message extends beyond %d-byte input",
			len(data),
		)
	}
	end := position + length
	return data[int(position):int(end)], end, nil
}

func ipcRootTable(metadata []byte) flatbuffers.Table {
	root := flatbuffers.GetUOffsetT(metadata)
	return flatbuffers.Table{Bytes: metadata, Pos: root}
}

func ipcMessageHeader(message flatbuffers.Table) (flatbuffers.Table, error) {
	offset := flatbuffers.UOffsetT(message.Offset(8))
	if offset == 0 {
		return flatbuffers.Table{}, fmt.Errorf("arrow protocol: IPC message has no header")
	}
	var header flatbuffers.Table
	message.Union(&header, offset)
	return header, nil
}

func ipcDictionaryRecordBatch(
	dictionary flatbuffers.Table,
) (flatbuffers.Table, error) {
	offset := flatbuffers.UOffsetT(dictionary.Offset(6))
	if offset == 0 {
		return flatbuffers.Table{}, fmt.Errorf(
			"arrow protocol: IPC dictionary message has no RecordBatch header",
		)
	}
	position := dictionary.Indirect(offset + dictionary.Pos)
	return flatbuffers.Table{Bytes: dictionary.Bytes, Pos: position}, nil
}

func ipcCompressedExpansion(recordBatch flatbuffers.Table, body []byte) (int64, error) {
	compressionOffset := flatbuffers.UOffsetT(recordBatch.Offset(10))
	if compressionOffset == 0 {
		return 0, nil
	}
	compressionPosition := recordBatch.Indirect(compressionOffset + recordBatch.Pos)
	compression := flatbuffers.Table{
		Bytes: recordBatch.Bytes,
		Pos:   compressionPosition,
	}
	codec := compression.GetInt8Slot(4, 0)
	method := compression.GetInt8Slot(6, 0)
	if codec != 0 && codec != 1 {
		return 0, fmt.Errorf("arrow protocol: unsupported IPC compression codec %d", codec)
	}
	if method != 0 {
		return 0, fmt.Errorf("arrow protocol: unsupported IPC compression method %d", method)
	}

	buffersOffset := flatbuffers.UOffsetT(recordBatch.Offset(8))
	if buffersOffset == 0 {
		return 0, nil
	}
	buffersLength := recordBatch.VectorLen(buffersOffset)
	buffersStart := recordBatch.Vector(buffersOffset)
	var total int64
	for index := range buffersLength {
		bufferPosition := buffersStart + flatbuffers.UOffsetT(index*16)
		offset := recordBatch.GetInt64(bufferPosition)
		length := recordBatch.GetInt64(bufferPosition + 8)
		if offset < 0 || length < 0 || offset > int64(len(body)) ||
			length > int64(len(body))-offset {
			return 0, fmt.Errorf(
				"arrow protocol: compressed IPC buffer %d range [%d,%d) exceeds %d-byte body",
				index,
				offset,
				offset+length,
				len(body),
			)
		}
		if length == 0 {
			continue
		}
		if length < 8 {
			return 0, fmt.Errorf(
				"arrow protocol: compressed IPC buffer %d is %d bytes, smaller than its size prefix",
				index,
				length,
			)
		}
		prefix := int64(binary.LittleEndian.Uint64(
			body[int(offset) : int(offset)+8],
		))
		if prefix == -1 {
			continue
		}
		if prefix < 0 {
			return 0, fmt.Errorf(
				"arrow protocol: compressed IPC buffer %d declares invalid uncompressed size %d",
				index,
				prefix,
			)
		}
		var err error
		total, err = addInt64Saturating(total, prefix)
		if err != nil {
			return math.MaxInt64, fmt.Errorf(
				"arrow protocol: declared uncompressed IPC buffer sizes overflow: %w",
				err,
			)
		}
	}
	return total, nil
}

func addInt64Saturating(left, right int64) (int64, error) {
	if left < 0 || right < 0 || right > math.MaxInt64-left {
		return math.MaxInt64, fmt.Errorf("int64 size overflow")
	}
	return left + right, nil
}
