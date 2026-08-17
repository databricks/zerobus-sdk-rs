package transport

import (
	"strings"
	"testing"
)

func TestParseFlightBatchMetadata(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantOffset int64
		wantErr    string
	}{
		{name: "offset", input: `{"offset_id":7}`, wantOffset: 7},
		{name: "zero offset", input: `{"offset_id":0}`},
		{
			name:       "unknown fields stay additive",
			input:      `{"offset_id":3,"future_field":"x"}`,
			wantOffset: 3,
		},
		{name: "empty object", input: `{}`, wantErr: "missing required field"},
		{name: "empty input", input: ``, wantErr: "metadata is empty"},
		{name: "null offset", input: `{"offset_id":null}`, wantErr: "must not be null"},
		{
			name:    "negative offset",
			input:   `{"offset_id":-1}`,
			wantErr: "must be non-negative",
		},
		{
			name:    "duplicate field",
			input:   `{"offset_id":1,"offset_id":2}`,
			wantErr: "duplicate field",
		},
		{name: "not an object", input: `[1]`, wantErr: "must be a JSON object"},
		{
			name:    "trailing json",
			input:   `{"offset_id":1} {"offset_id":2}`,
			wantErr: "invalid character",
		},
		{
			name:    "string offset",
			input:   `{"offset_id":"1"}`,
			wantErr: "offset_id",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metadata, err := ParseFlightBatchMetadata([]byte(test.input))
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("error = %v, want it to mention %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseFlightBatchMetadata: %v", err)
			}
			if metadata.OffsetID != test.wantOffset {
				t.Fatalf("OffsetID = %d, want %d", metadata.OffsetID, test.wantOffset)
			}
		})
	}
}

func TestParseFlightAckMetadata(t *testing.T) {
	const closeMillis = uint64(2_500)
	tests := []struct {
		name        string
		input       string
		wantOffset  int64
		wantRecords uint64
		wantClose   *uint64
		wantReady   bool
		wantErr     string
	}{
		{
			name:        "ack",
			input:       `{"ack_up_to_offset":4,"ack_up_to_records":900}`,
			wantOffset:  4,
			wantRecords: 900,
		},
		{
			name:        "ack with rotation request",
			input:       `{"ack_up_to_offset":4,"ack_up_to_records":900,"close_stream_duration_ms":2500}`,
			wantOffset:  4,
			wantRecords: 900,
			wantClose:   &[]uint64{closeMillis}[0],
		},
		{
			// An absent optional is written as an explicit null by many encoders,
			// and dropping the acknowledgment over it would fail the stream.
			name:        "explicit null rotation request is absent",
			input:       `{"ack_up_to_offset":4,"ack_up_to_records":900,"close_stream_duration_ms":null}`,
			wantOffset:  4,
			wantRecords: 900,
		},
		{
			name:       "stream ready sentinel",
			input:      `{"ack_up_to_offset":-1,"ack_up_to_records":0}`,
			wantOffset: FlightStreamReadyOffset,
			wantReady:  true,
		},
		{
			name:    "offset below the sentinel",
			input:   `{"ack_up_to_offset":-2,"ack_up_to_records":0}`,
			wantErr: "must be at least",
		},
		{
			name:    "missing record count",
			input:   `{"ack_up_to_offset":4}`,
			wantErr: "missing required field",
		},
		{
			name:    "null record count",
			input:   `{"ack_up_to_offset":4,"ack_up_to_records":null}`,
			wantErr: "must not be null",
		},
		{
			name:    "negative record count",
			input:   `{"ack_up_to_offset":4,"ack_up_to_records":-1}`,
			wantErr: "ack_up_to_records",
		},
		{
			name:    "rotation request overflows a duration",
			input:   `{"ack_up_to_offset":4,"ack_up_to_records":1,"close_stream_duration_ms":9223372036854775807}`,
			wantErr: "maximum representable duration",
		},
		{name: "empty input", input: ``, wantErr: "metadata is empty"},
		{name: "empty object", input: `{}`, wantErr: "missing required field"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metadata, err := ParseFlightAckMetadata([]byte(test.input))
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("error = %v, want it to mention %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseFlightAckMetadata: %v", err)
			}
			if metadata.AckUpToOffset != test.wantOffset {
				t.Errorf(
					"AckUpToOffset = %d, want %d",
					metadata.AckUpToOffset,
					test.wantOffset,
				)
			}
			if metadata.AckUpToRecords != test.wantRecords {
				t.Errorf(
					"AckUpToRecords = %d, want %d",
					metadata.AckUpToRecords,
					test.wantRecords,
				)
			}
			if metadata.IsStreamReady() != test.wantReady {
				t.Errorf("IsStreamReady = %v, want %v",
					metadata.IsStreamReady(), test.wantReady)
			}
			switch {
			case test.wantClose == nil && metadata.CloseStreamDurationMS != nil:
				t.Errorf(
					"CloseStreamDurationMS = %d, want absent",
					*metadata.CloseStreamDurationMS,
				)
			case test.wantClose != nil && metadata.CloseStreamDurationMS == nil:
				t.Errorf("CloseStreamDurationMS absent, want %d", *test.wantClose)
			case test.wantClose != nil &&
				*metadata.CloseStreamDurationMS != *test.wantClose:
				t.Errorf(
					"CloseStreamDurationMS = %d, want %d",
					*metadata.CloseStreamDurationMS,
					*test.wantClose,
				)
			}
		})
	}
}
