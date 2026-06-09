package tests

import (
	"strings"
	"testing"
	"time"

	zerobus "github.com/databricks/zerobus-sdk/go"
)

// openStream creates an SDK with the given options and opens a stream so
// the mock server captures the user-agent metadata.
func openStream(t *testing.T, serverURL string, opts ...zerobus.SdkOption) {
	t.Helper()
	sdk, err := zerobus.NewZerobusSdk(serverURL, "https://mock-uc.com", opts...)
	if err != nil {
		t.Fatalf("Failed to create SDK: %v", err)
	}
	defer sdk.Free()

	stream, err := sdk.CreateStreamWithHeadersProvider(
		zerobus.TableProperties{
			TableName:       testTableName,
			DescriptorProto: CreateTestDescriptorProto(),
		},
		&TestHeadersProvider{},
		&zerobus.StreamConfigurationOptions{MaxInflightRequests: 100, Recovery: false},
	)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}
	defer stream.Close()
}

// TestDefaultUserAgentIdentifiesAsGo verifies the wire user-agent is
// `zerobus-sdk-go/<version>`, not the Rust default.
func TestDefaultUserAgentIdentifiesAsGo(t *testing.T) {
	mockServer, serverURL, grpcServer, err := StartMockServer()
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer grpcServer.Stop()

	mockServer.InjectResponses(testTableName, []MockResponse{
		CreateStreamResponse("test_stream_1", 0),
	})
	time.Sleep(200 * time.Millisecond)

	openStream(t, serverURL)

	ua := mockServer.GetLastUserAgent()
	if !strings.Contains(ua, "zerobus-sdk-go/") {
		t.Fatalf("expected user-agent to contain `zerobus-sdk-go/`, got %q", ua)
	}
	if strings.Contains(ua, "zerobus-sdk-rs/") {
		t.Fatalf("user-agent must not advertise the Rust SDK prefix, got %q", ua)
	}
}

// TestApplicationNameAppendedToUserAgent verifies WithApplicationName appends
// the caller identifier after the SDK identifier.
func TestApplicationNameAppendedToUserAgent(t *testing.T) {
	mockServer, serverURL, grpcServer, err := StartMockServer()
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer grpcServer.Stop()

	mockServer.InjectResponses(testTableName, []MockResponse{
		CreateStreamResponse("test_stream_1", 0),
	})
	time.Sleep(200 * time.Millisecond)

	openStream(t, serverURL, zerobus.WithApplicationName("my-app/1.0"))

	ua := mockServer.GetLastUserAgent()
	if !strings.Contains(ua, "zerobus-sdk-go/") {
		t.Fatalf("expected user-agent to keep `zerobus-sdk-go/` prefix, got %q", ua)
	}
	if !strings.Contains(ua, "my-app/1.0") {
		t.Fatalf("expected user-agent to contain application name `my-app/1.0`, got %q", ua)
	}
}

// TestEmptyApplicationNameIsIgnored verifies WithApplicationName("") is a
// no-op (no trailing space, no extra token in the user-agent).
func TestEmptyApplicationNameIsIgnored(t *testing.T) {
	mockServer, serverURL, grpcServer, err := StartMockServer()
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer grpcServer.Stop()

	mockServer.InjectResponses(testTableName, []MockResponse{
		CreateStreamResponse("test_stream_1", 0),
	})
	time.Sleep(200 * time.Millisecond)

	openStream(t, serverURL, zerobus.WithApplicationName(""))

	ua := mockServer.GetLastUserAgent()
	if !strings.Contains(ua, "zerobus-sdk-go/") {
		t.Fatalf("expected user-agent to contain `zerobus-sdk-go/`, got %q", ua)
	}
	if strings.Contains(ua, " my-app/") {
		t.Fatalf("did not expect application name token in user-agent: %q", ua)
	}
}

// TestWithNoTLSStreamCreationSucceeds verifies WithNoTLS() is forwarded
// through Go → FFI → Rust builder and the stream opens successfully.
func TestWithNoTLSStreamCreationSucceeds(t *testing.T) {
	mockServer, serverURL, grpcServer, err := StartMockServer()
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	defer grpcServer.Stop()

	mockServer.InjectResponses(testTableName, []MockResponse{
		CreateStreamResponse("test_stream_1", 0),
	})
	time.Sleep(200 * time.Millisecond)

	openStream(t, serverURL, zerobus.WithNoTLS())
}
