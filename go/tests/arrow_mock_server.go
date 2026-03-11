package tests

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
)

// arrowBatchMeta matches FlightBatchMetadata JSON in the Rust SDK.
type arrowBatchMeta struct {
	OffsetID int64 `json:"offset_id"`
}

// arrowAckMeta matches FlightAckMetadata JSON in the Rust SDK.
type arrowAckMeta struct {
	AckUpToOffset  int64  `json:"ack_up_to_offset"`
	AckUpToRecords uint64 `json:"ack_up_to_records"`
}

// MockArrowFlightServer is a minimal Arrow Flight DoPut server for testing.
// It sends a stream-ready ack after the schema message, then acks each data
// batch using a configurable per-offset row count.
type MockArrowFlightServer struct {
	flight.BaseFlightServer

	mu              sync.Mutex
	rowsPerOffset   map[int64]uint64 // configured rows for each logical offset
	defaultRows     uint64           // fallback when offset not configured
	batchesReceived int
	maxOffsetSeen   int64
}

// NewMockArrowFlightServer creates a server with defaultRows=1.
func NewMockArrowFlightServer() *MockArrowFlightServer {
	return &MockArrowFlightServer{
		rowsPerOffset: make(map[int64]uint64),
		defaultRows:   1,
		maxOffsetSeen: -1,
	}
}

// ConfigureRowsForOffset sets the row count for a specific logical offset.
func (s *MockArrowFlightServer) ConfigureRowsForOffset(offset int64, rows uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rowsPerOffset[offset] = rows
}

// SetDefaultRows sets the fallback row count used for unconfigured offsets.
func (s *MockArrowFlightServer) SetDefaultRows(rows uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.defaultRows = rows
}

// GetBatchesReceived returns the total number of data batches (not schema) received.
func (s *MockArrowFlightServer) GetBatchesReceived() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.batchesReceived
}

// GetMaxOffsetSeen returns the highest logical offset received so far.
func (s *MockArrowFlightServer) GetMaxOffsetSeen() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxOffsetSeen
}

// Reset clears server state for reuse between tests.
func (s *MockArrowFlightServer) ArrowReset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rowsPerOffset = make(map[int64]uint64)
	s.defaultRows = 1
	s.batchesReceived = 0
	s.maxOffsetSeen = -1
}

// DoPut implements flight.FlightServer. It handles one Arrow Flight DoPut call:
//  1. Reads the schema FlightData (first message, no AppMetadata).
//  2. Sends the stream-ready ack {"ack_up_to_offset":-1,"ack_up_to_records":0}.
//  3. For each subsequent data FlightData, reads offset_id from AppMetadata,
//     accumulates the configured row count, and sends an ack.
func (s *MockArrowFlightServer) DoPut(stream flight.FlightService_DoPutServer) error {
	// First message is the schema (FlightDataEncoderBuilder idx=0, no AppMetadata).
	if _, err := stream.Recv(); err != nil {
		return err
	}

	// Send stream-ready ack to unblock the Rust SDK's stream creation wait.
	readyBytes, _ := json.Marshal(arrowAckMeta{AckUpToOffset: -1, AckUpToRecords: 0})
	if err := stream.Send(&flight.PutResult{AppMetadata: readyBytes}); err != nil {
		return err
	}

	var cumulativeRecords uint64
	for {
		data, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		var meta arrowBatchMeta
		if err := json.Unmarshal(data.AppMetadata, &meta); err != nil {
			return fmt.Errorf("arrow mock: invalid batch AppMetadata: %w", err)
		}

		s.mu.Lock()
		rows, ok := s.rowsPerOffset[meta.OffsetID]
		if !ok {
			rows = s.defaultRows
		}
		s.batchesReceived++
		if meta.OffsetID > s.maxOffsetSeen {
			s.maxOffsetSeen = meta.OffsetID
		}
		s.mu.Unlock()

		cumulativeRecords += rows

		ackBytes, _ := json.Marshal(arrowAckMeta{
			AckUpToOffset:  meta.OffsetID,
			AckUpToRecords: cumulativeRecords,
		})
		if err := stream.Send(&flight.PutResult{AppMetadata: ackBytes}); err != nil {
			return err
		}
	}
}

// StartMockArrowServer creates and starts a mock Arrow Flight gRPC server on a
// random local port. Returns the server, its URL, a stop function, and any error.
func StartMockArrowServer() (*MockArrowFlightServer, string, func(), error) {
	mockServer := NewMockArrowFlightServer()

	srv := flight.NewServerWithMiddleware(nil)
	srv.RegisterFlightService(mockServer)

	if err := srv.Init("127.0.0.1:0"); err != nil {
		return nil, "", nil, fmt.Errorf("failed to init Arrow Flight server: %w", err)
	}

	go func() {
		srv.Serve() //nolint:errcheck
	}()

	// Let the server finish binding before returning.
	time.Sleep(100 * time.Millisecond)

	serverURL := fmt.Sprintf("http://%s", srv.Addr().String())
	stop := func() { srv.Shutdown() }

	return mockServer, serverURL, stop, nil
}
