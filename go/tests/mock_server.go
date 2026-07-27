package tests

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	pb "github.com/databricks/zerobus-sdk/go/tests/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// MockResponse represents a response that can be injected into the mock server
type MockResponse struct {
	// Type of response
	Type MockResponseType
	// For CreateStream responses
	StreamID string
	DelayMs  int64
	// For RecordAck responses
	AckUpToOffset int64
	// For CloseStreamSignal responses
	DurationSeconds int64
	// For Error responses
	GrpcStatus *grpcstatus.Status
}

type MockResponseType int

const (
	MockResponseCreateStream MockResponseType = iota
	MockResponseRecordAck
	MockResponseCloseStreamSignal
	MockResponseError
)

// MockZerobusServer is a mock gRPC server for testing
type MockZerobusServer struct {
	pb.UnimplementedZerobusServer

	// Responses to inject for each table
	responses      map[string][]MockResponse
	responsesMutex sync.Mutex

	// Counter for generating unique stream IDs
	streamCounter int32
	counterMutex  sync.Mutex

	// Track the maximum offset sent by clients
	maxOffsetSent int64
	offsetMutex   sync.Mutex

	// Track number of writes received
	writeCount      uint64
	writeCountMutex sync.Mutex

	// Track response index across multiple connection attempts
	responseIndices      map[string]int
	responseIndicesMutex sync.Mutex

	// Last user-agent observed on an incoming stream.
	lastUserAgent      string
	lastUserAgentMutex sync.RWMutex
}

// NewMockZerobusServer creates a new mock server
func NewMockZerobusServer() *MockZerobusServer {
	return &MockZerobusServer{
		responses:       make(map[string][]MockResponse),
		responseIndices: make(map[string]int),
		maxOffsetSent:   -1,
	}
}

// InjectResponses configures responses for a specific table
func (m *MockZerobusServer) InjectResponses(tableName string, responses []MockResponse) {
	m.responsesMutex.Lock()
	defer m.responsesMutex.Unlock()

	m.responses[tableName] = responses

	m.responseIndicesMutex.Lock()
	defer m.responseIndicesMutex.Unlock()
	m.responseIndices[tableName] = 0
}

// GetMaxOffsetSent returns the maximum offset sent by clients
func (m *MockZerobusServer) GetMaxOffsetSent() int64 {
	m.offsetMutex.Lock()
	defer m.offsetMutex.Unlock()
	return m.maxOffsetSent
}

// GetWriteCount returns the number of writes received
func (m *MockZerobusServer) GetWriteCount() uint64 {
	m.writeCountMutex.Lock()
	defer m.writeCountMutex.Unlock()
	return m.writeCount
}

// GetLastUserAgent returns the most recent user-agent observed on a stream.
func (m *MockZerobusServer) GetLastUserAgent() string {
	m.lastUserAgentMutex.RLock()
	defer m.lastUserAgentMutex.RUnlock()
	return m.lastUserAgent
}

// Reset resets the server state
func (m *MockZerobusServer) Reset() {
	m.responsesMutex.Lock()
	m.responses = make(map[string][]MockResponse)
	m.responsesMutex.Unlock()

	m.responseIndicesMutex.Lock()
	m.responseIndices = make(map[string]int)
	m.responseIndicesMutex.Unlock()

	m.offsetMutex.Lock()
	m.maxOffsetSent = -1
	m.offsetMutex.Unlock()

	m.writeCountMutex.Lock()
	m.writeCount = 0
	m.writeCountMutex.Unlock()

	m.counterMutex.Lock()
	m.streamCounter = 0
	m.counterMutex.Unlock()

	m.lastUserAgentMutex.Lock()
	m.lastUserAgent = ""
	m.lastUserAgentMutex.Unlock()
}

// EphemeralStream implements the bidirectional streaming RPC
func (m *MockZerobusServer) EphemeralStream(stream pb.Zerobus_EphemeralStreamServer) error {
	if incoming, ok := metadata.FromIncomingContext(stream.Context()); ok {
		if values := incoming.Get("user-agent"); len(values) > 0 {
			m.lastUserAgentMutex.Lock()
			m.lastUserAgent = values[0]
			m.lastUserAgentMutex.Unlock()
		}
	}

	// Send initial headers/metadata to signal readiness
	// This triggers the server to send HTTP/2 HEADERS frame
	// This is CRITICAL: Without this, the Rust SDK will wait indefinitely
	if err := stream.SendHeader(make(map[string][]string)); err != nil {
		return err
	}

	var tableName string
	var streamResponses []MockResponse
	var responseIndex int

	// Read the first message (CreateStream request)
	req, err := stream.Recv()
	if err != nil {
		return err
	}

	createReq := req.GetCreateStream()
	if createReq == nil {
		return grpcstatus.Error(codes.InvalidArgument, "first message must be CreateStream")
	}

	tableName = createReq.GetTableName()

	// Get stream ID
	m.counterMutex.Lock()
	m.streamCounter++
	streamID := fmt.Sprintf("test_stream_%d", m.streamCounter)
	m.counterMutex.Unlock()

	// Get configured responses for this table
	m.responsesMutex.Lock()
	if responses, ok := m.responses[tableName]; ok {
		streamResponses = make([]MockResponse, len(responses))
		copy(streamResponses, responses)
	}
	m.responsesMutex.Unlock()

	// Get current response index
	m.responseIndicesMutex.Lock()
	responseIndex = m.responseIndices[tableName]
	m.responseIndicesMutex.Unlock()

	// Search for the next CreateStream response starting from responseIndex
	createStreamFound := false
	for idx := responseIndex; idx < len(streamResponses); idx++ {
		mockResp := &streamResponses[idx]
		if mockResp.Type == MockResponseCreateStream {
			responseIndex = idx
			createStreamFound = true

			if mockResp.DelayMs > 0 {
				time.Sleep(time.Duration(mockResp.DelayMs) * time.Millisecond)
			}

			customID := mockResp.StreamID
			if customID == "" {
				customID = streamID
			}

			resp := &pb.EphemeralStreamResponse{
				Payload: &pb.EphemeralStreamResponse_CreateStreamResponse{
					CreateStreamResponse: &pb.CreateIngestStreamResponse{
						StreamId: &customID,
					},
				},
			}

			if err := stream.Send(resp); err != nil {
				return err
			}

			responseIndex++
			m.responseIndicesMutex.Lock()
			m.responseIndices[tableName] = responseIndex
			m.responseIndicesMutex.Unlock()
			break
		} else if mockResp.Type == MockResponseError {
			if mockResp.DelayMs > 0 {
				time.Sleep(time.Duration(mockResp.DelayMs) * time.Millisecond)
			}

			m.responseIndicesMutex.Lock()
			m.responseIndices[tableName] = idx + 1
			m.responseIndicesMutex.Unlock()

			return mockResp.GrpcStatus.Err()
		}
	}

	// If no CreateStream response was configured, send default
	if !createStreamFound {
		resp := &pb.EphemeralStreamResponse{
			Payload: &pb.EphemeralStreamResponse_CreateStreamResponse{
				CreateStreamResponse: &pb.CreateIngestStreamResponse{
					StreamId: &streamID,
				},
			},
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}

	// Process subsequent requests
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch payload := req.Payload.(type) {
		case *pb.EphemeralStreamRequest_IngestRecord:
			if err := m.handleIngestRecord(stream, payload.IngestRecord, &streamResponses, &responseIndex, tableName); err != nil {
				return err
			}

		case *pb.EphemeralStreamRequest_IngestRecordBatch:
			if err := m.handleIngestRecordBatch(stream, payload.IngestRecordBatch, &streamResponses, &responseIndex, tableName); err != nil {
				return err
			}
		}
	}
}

func (m *MockZerobusServer) handleIngestRecord(stream pb.Zerobus_EphemeralStreamServer, req *pb.IngestRecordRequest, streamResponses *[]MockResponse, responseIndex *int, tableName string) error {
	// Update max offset
	if req.OffsetId != nil {
		m.offsetMutex.Lock()
		if *req.OffsetId > m.maxOffsetSent {
			m.maxOffsetSent = *req.OffsetId
		}
		m.offsetMutex.Unlock()
	}

	// Increment write count
	m.writeCountMutex.Lock()
	m.writeCount++
	m.writeCountMutex.Unlock()

	// Process mock response
	if *responseIndex < len(*streamResponses) {
		shouldContinue, indexIncrement := m.handleMockResponse(stream, &(*streamResponses)[*responseIndex], req.OffsetId, tableName)
		*responseIndex += indexIncrement

		m.responseIndicesMutex.Lock()
		m.responseIndices[tableName] = *responseIndex
		m.responseIndicesMutex.Unlock()

		if !shouldContinue {
			return grpcstatus.Error(codes.Internal, "mock response indicated stop")
		}
	}

	return nil
}

func (m *MockZerobusServer) handleIngestRecordBatch(stream pb.Zerobus_EphemeralStreamServer, req *pb.IngestRecordBatchRequest, streamResponses *[]MockResponse, responseIndex *int, tableName string) error {
	// Count records in the batch
	recordCount := 0
	if req.Batch != nil {
		switch batch := req.Batch.(type) {
		case *pb.IngestRecordBatchRequest_ProtoEncodedBatch:
			recordCount = len(batch.ProtoEncodedBatch.Records)
		case *pb.IngestRecordBatchRequest_JsonBatch:
			recordCount = len(batch.JsonBatch.Records)
		}
	}

	// Update max offset
	if req.OffsetId != nil {
		m.offsetMutex.Lock()
		if *req.OffsetId > m.maxOffsetSent {
			m.maxOffsetSent = *req.OffsetId
		}
		m.offsetMutex.Unlock()
	}

	// Increment write count by number of records
	m.writeCountMutex.Lock()
	m.writeCount += uint64(recordCount)
	m.writeCountMutex.Unlock()

	// Process mock response
	if *responseIndex < len(*streamResponses) {
		shouldContinue, indexIncrement := m.handleMockResponse(stream, &(*streamResponses)[*responseIndex], req.OffsetId, tableName)
		*responseIndex += indexIncrement

		m.responseIndicesMutex.Lock()
		m.responseIndices[tableName] = *responseIndex
		m.responseIndicesMutex.Unlock()

		if !shouldContinue {
			return grpcstatus.Error(codes.Internal, "mock response indicated stop")
		}
	}

	return nil
}

func (m *MockZerobusServer) handleMockResponse(stream pb.Zerobus_EphemeralStreamServer, mockResp *MockResponse, offset *int64, tableName string) (bool, int) {
	switch mockResp.Type {
	case MockResponseRecordAck:
		if offset != nil && *offset == mockResp.AckUpToOffset {
			if mockResp.DelayMs > 0 {
				time.Sleep(time.Duration(mockResp.DelayMs) * time.Millisecond)
			}

			resp := &pb.EphemeralStreamResponse{
				Payload: &pb.EphemeralStreamResponse_IngestRecordResponse{
					IngestRecordResponse: &pb.IngestRecordResponse{
						DurabilityAckUpToOffset: &mockResp.AckUpToOffset,
					},
				},
			}

			if err := stream.Send(resp); err != nil {
				return false, 0
			}
			return true, 1
		}
		return true, 0

	case MockResponseCloseStreamSignal:
		if mockResp.DelayMs > 0 {
			time.Sleep(time.Duration(mockResp.DelayMs) * time.Millisecond)
		}

		resp := &pb.EphemeralStreamResponse{
			Payload: &pb.EphemeralStreamResponse_CloseStreamSignal{
				CloseStreamSignal: &pb.CloseStreamSignal{
					Duration: durationpb.New(time.Duration(mockResp.DurationSeconds) * time.Second),
				},
			},
		}

		if err := stream.Send(resp); err != nil {
			return false, 0
		}
		return true, 1

	case MockResponseError:
		if mockResp.DelayMs > 0 {
			time.Sleep(time.Duration(mockResp.DelayMs) * time.Millisecond)
		}
		return false, 1

	case MockResponseCreateStream:
		return true, 1
	}

	return true, 0
}

// StartMockServer starts a mock gRPC server and returns the server instance and its URL
func StartMockServer() (*MockZerobusServer, string, *grpc.Server, error) {
	mockServer := NewMockZerobusServer()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, "", nil, fmt.Errorf("failed to listen: %w", err)
	}

	// Create gRPC server with no options (HTTP/2 is default)
	grpcServer := grpc.NewServer()
	pb.RegisterZerobusServer(grpcServer, mockServer)

	serverURL := fmt.Sprintf("http://%s", lis.Addr().String())

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			// Server stopped
		}
	}()

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	return mockServer, serverURL, grpcServer, nil
}
