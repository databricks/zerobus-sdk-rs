package transport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"time"
)

// FlightStreamReadyOffset is the sentinel in the first DoPut PutResult. It
// confirms that authentication, table access, and schema validation succeeded
// before any record batches are sent.
const FlightStreamReadyOffset int64 = -1

// FlightBatchMetadata is carried in FlightData.AppMetadata for a data frame.
// OffsetID is connection-local and must be assigned sequentially from zero.
type FlightBatchMetadata struct {
	OffsetID int64 `json:"offset_id"`
}

// FlightAckMetadata is carried in PutResult.AppMetadata.
type FlightAckMetadata struct {
	// AckUpToOffset is the highest connection-local frame offset made durable.
	// FlightStreamReadyOffset is reserved for the setup-ready response.
	AckUpToOffset int64 `json:"ack_up_to_offset"`
	// AckUpToRecords is the cumulative number of rows made durable.
	AckUpToRecords uint64 `json:"ack_up_to_records"`
	// CloseStreamDurationMS requests connection rotation after this grace period.
	CloseStreamDurationMS *uint64 `json:"close_stream_duration_ms,omitempty"`
}

// IsStreamReady reports whether this metadata is the setup-ready response.
func (m FlightAckMetadata) IsStreamReady() bool {
	return m.AckUpToOffset == FlightStreamReadyOffset
}

// ParseFlightBatchMetadata decodes one strict FlightData metadata object.
func ParseFlightBatchMetadata(data []byte) (FlightBatchMetadata, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return FlightBatchMetadata{}, fmt.Errorf("parse Flight batch metadata: metadata is empty")
	}
	var metadata FlightBatchMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return FlightBatchMetadata{}, fmt.Errorf("parse Flight batch metadata: %w", err)
	}
	return metadata, nil
}

// ParseFlightAckMetadata decodes one strict PutResult metadata object.
func ParseFlightAckMetadata(data []byte) (FlightAckMetadata, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return FlightAckMetadata{}, fmt.Errorf("parse Flight ack metadata: metadata is empty")
	}
	var metadata FlightAckMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return FlightAckMetadata{}, fmt.Errorf("parse Flight ack metadata: %w", err)
	}
	return metadata, nil
}

// UnmarshalJSON rejects missing, duplicate, null, and incorrectly typed fields
// while ignoring unknown fields for additive protocol compatibility. A
// permissive zero-value decode would otherwise turn malformed metadata such as
// {} into a real offset-zero frame.
func (m *FlightBatchMetadata) UnmarshalJSON(data []byte) error {
	var parsed FlightBatchMetadata
	seen, err := decodeStrictJSONObject(data, map[string]func(json.RawMessage) error{
		"offset_id": func(raw json.RawMessage) error {
			return decodeRequiredJSONNumber(raw, &parsed.OffsetID)
		},
	})
	if err != nil {
		return err
	}
	if !seen["offset_id"] {
		return fmt.Errorf("missing required field %q", "offset_id")
	}
	if parsed.OffsetID < 0 {
		return fmt.Errorf("field %q must be non-negative", "offset_id")
	}
	*m = parsed
	return nil
}

// UnmarshalJSON applies the same strict wire validation as
// FlightBatchMetadata.UnmarshalJSON while permitting the optional close signal.
func (m *FlightAckMetadata) UnmarshalJSON(data []byte) error {
	var parsed FlightAckMetadata
	seen, err := decodeStrictJSONObject(data, map[string]func(json.RawMessage) error{
		"ack_up_to_offset": func(raw json.RawMessage) error {
			return decodeRequiredJSONNumber(raw, &parsed.AckUpToOffset)
		},
		"ack_up_to_records": func(raw json.RawMessage) error {
			return decodeRequiredJSONNumber(raw, &parsed.AckUpToRecords)
		},
		"close_stream_duration_ms": func(raw json.RawMessage) error {
			var duration uint64
			if err := decodeRequiredJSONNumber(raw, &duration); err != nil {
				return err
			}
			parsed.CloseStreamDurationMS = &duration
			return nil
		},
	})
	if err != nil {
		return err
	}
	for _, required := range []string{"ack_up_to_offset", "ack_up_to_records"} {
		if !seen[required] {
			return fmt.Errorf("missing required field %q", required)
		}
	}
	if parsed.AckUpToOffset < FlightStreamReadyOffset {
		return fmt.Errorf(
			"field %q must be at least %d",
			"ack_up_to_offset",
			FlightStreamReadyOffset,
		)
	}
	if parsed.CloseStreamDurationMS != nil {
		maxMillis := uint64(math.MaxInt64 / int64(time.Millisecond))
		if *parsed.CloseStreamDurationMS > maxMillis {
			return fmt.Errorf(
				"field %q exceeds maximum representable duration",
				"close_stream_duration_ms",
			)
		}
	}
	*m = parsed
	return nil
}

// decodeStrictJSONObject decodes exactly one JSON object. Known field values
// remain raw until their field-specific decoder checks the concrete numeric
// type; unknown fields are consumed and ignored for additive compatibility.
// Duplicate names are rejected even when the field is unknown.
func decodeStrictJSONObject(
	data []byte,
	decoders map[string]func(json.RawMessage) error,
) (map[string]bool, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	start, err := decoder.Token()
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("metadata is empty")
		}
		return nil, err
	}
	if delim, ok := start.(json.Delim); !ok || delim != '{' {
		return nil, fmt.Errorf("metadata must be a JSON object")
	}

	seen := make(map[string]bool, len(decoders))
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok {
			return nil, fmt.Errorf("metadata field name is not a string")
		}
		if seen[key] {
			return nil, fmt.Errorf("duplicate field %q", key)
		}
		seen[key] = true

		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return nil, fmt.Errorf("field %q: %w", key, err)
		}
		if decode, ok := decoders[key]; ok {
			if err := decode(raw); err != nil {
				return nil, fmt.Errorf("field %q: %w", key, err)
			}
		}
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("metadata contains trailing JSON")
		}
		return nil, fmt.Errorf("metadata contains trailing data: %w", err)
	}
	return seen, nil
}

func decodeRequiredJSONNumber[T int64 | uint64](raw json.RawMessage, dst *T) error {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return fmt.Errorf("must not be null")
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return err
	}
	return nil
}
