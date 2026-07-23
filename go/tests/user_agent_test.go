package tests

import (
	"strings"
	"testing"

	zerobus "github.com/databricks/zerobus-sdk/go"
)

func openStreamForUserAgent(
	t *testing.T,
	serverURL string,
	options ...zerobus.SdkOption,
) {
	t.Helper()

	var sdk *zerobus.ZerobusSdk
	var err error
	if len(options) == 0 {
		// Exercise the original constructor as a compatibility and default
		// user-agent regression check.
		sdk, err = zerobus.NewZerobusSdk(serverURL, "https://mock-uc.com")
	} else {
		sdk, err = zerobus.NewZerobusSdkWithOptions(
			serverURL,
			"https://mock-uc.com",
			options...,
		)
	}
	if err != nil {
		t.Fatalf("failed to create SDK: %v", err)
	}
	defer sdk.Free()

	stream, err := sdk.CreateStreamWithHeadersProvider(
		zerobus.TableProperties{
			TableName:       testTableName,
			DescriptorProto: CreateTestDescriptorProto(),
		},
		&TestHeadersProvider{},
		&zerobus.StreamConfigurationOptions{
			MaxInflightRequests: 100,
			Recovery:            false,
		},
	)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.Close()
}

func openArrowStreamForUserAgent(
	t *testing.T,
	serverURL string,
	options ...zerobus.SdkOption,
) {
	t.Helper()

	var sdk *zerobus.ZerobusSdk
	var err error
	if len(options) == 0 {
		sdk, err = zerobus.NewZerobusSdk(serverURL, "https://mock-uc.com")
	} else {
		sdk, err = zerobus.NewZerobusSdkWithOptions(
			serverURL,
			"https://mock-uc.com",
			options...,
		)
	}
	if err != nil {
		t.Fatalf("failed to create SDK: %v", err)
	}
	defer sdk.Free()

	schema := testArrowSchema()
	stream, err := sdk.CreateArrowStreamWithHeadersProvider(
		arrowTestTable,
		makeSchemaIPC(schema),
		&TestHeadersProvider{},
		arrowOpts(),
	)
	if err != nil {
		t.Fatalf("failed to create Arrow stream: %v", err)
	}
	defer stream.Close()
}

func startUserAgentTestServer(t *testing.T) (*MockZerobusServer, string) {
	t.Helper()

	mockServer, serverURL, grpcServer, err := StartMockServer()
	if err != nil {
		t.Fatalf("failed to start mock server: %v", err)
	}
	t.Cleanup(grpcServer.Stop)
	mockServer.InjectResponses(testTableName, []MockResponse{
		CreateStreamResponse("user_agent_stream", 0),
	})
	return mockServer, serverURL
}

func TestDefaultUserAgentIdentifiesGoSdk(t *testing.T) {
	mockServer, serverURL := startUserAgentTestServer(t)

	openStreamForUserAgent(t, serverURL)

	userAgent := mockServer.GetLastUserAgent()
	if !strings.Contains(userAgent, "zerobus-sdk-go/1.3.0") {
		t.Fatalf("expected Go SDK user-agent, got %q", userAgent)
	}
	if strings.Contains(userAgent, "zerobus-sdk-rs/") {
		t.Fatalf("user-agent must not advertise the Rust SDK: %q", userAgent)
	}
}

func TestApplicationNameIsAppendedToUserAgent(t *testing.T) {
	mockServer, serverURL := startUserAgentTestServer(t)

	openStreamForUserAgent(
		t,
		serverURL,
		zerobus.WithApplicationName("my-app/1.0"),
	)

	userAgent := mockServer.GetLastUserAgent()
	if !strings.Contains(userAgent, "zerobus-sdk-go/1.3.0 my-app/1.0") {
		t.Fatalf("expected application name in user-agent, got %q", userAgent)
	}
}

func TestEmptyApplicationNameIsIgnored(t *testing.T) {
	mockServer, serverURL := startUserAgentTestServer(t)

	openStreamForUserAgent(
		t,
		serverURL,
		zerobus.WithApplicationName("   "),
	)

	userAgent := mockServer.GetLastUserAgent()
	if !strings.Contains(userAgent, "zerobus-sdk-go/1.3.0") {
		t.Fatalf("expected default Go SDK user-agent, got %q", userAgent)
	}
}

func TestArrowDefaultUserAgentIdentifiesGoSdk(t *testing.T) {
	mockServer, serverURL, stop, err := StartMockArrowServer()
	if err != nil {
		t.Fatalf("failed to start Arrow mock server: %v", err)
	}
	t.Cleanup(stop)

	openArrowStreamForUserAgent(t, serverURL)

	userAgent := mockServer.GetLastUserAgent()
	if !strings.Contains(userAgent, "zerobus-sdk-go/1.3.0") {
		t.Fatalf("expected Go SDK user-agent on Arrow DoPut, got %q", userAgent)
	}
	if strings.Contains(userAgent, "zerobus-sdk-rs/") {
		t.Fatalf("Arrow user-agent must not advertise the Rust SDK: %q", userAgent)
	}
}

func TestArrowApplicationNameIsAppendedToUserAgent(t *testing.T) {
	mockServer, serverURL, stop, err := StartMockArrowServer()
	if err != nil {
		t.Fatalf("failed to start Arrow mock server: %v", err)
	}
	t.Cleanup(stop)

	openArrowStreamForUserAgent(
		t,
		serverURL,
		zerobus.WithApplicationName("my-app/1.0"),
	)

	userAgent := mockServer.GetLastUserAgent()
	if !strings.Contains(userAgent, "zerobus-sdk-go/1.3.0 my-app/1.0") {
		t.Fatalf("expected application name in Arrow user-agent, got %q", userAgent)
	}
}
