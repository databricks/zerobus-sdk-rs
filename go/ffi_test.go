package zerobus

import (
	"runtime/cgo"
	"testing"
	"unsafe"
)

// TestStreamHandleRegistry tests the stream handle registry
func TestStreamHandleRegistry(t *testing.T) {
	// Create a test handle
	testProvider := &mockHeadersProvider{}
	handle := cgo.NewHandle(testProvider)

	// Create a real allocated pointer (not a fake one)
	dummyStream := struct{ id int }{1234}
	dummyStreamPtr := unsafe.Pointer(&dummyStream)

	// Store in registry
	streamHandleRegistryMu.Lock()
	streamHandleRegistry[dummyStreamPtr] = handle
	streamHandleRegistryMu.Unlock()

	// Verify it's stored
	streamHandleRegistryMu.Lock()
	storedHandle, exists := streamHandleRegistry[dummyStreamPtr]
	streamHandleRegistryMu.Unlock()

	if !exists {
		t.Fatal("Handle not found in registry")
	}

	if storedHandle != handle {
		t.Fatal("Retrieved handle doesn't match stored handle")
	}

	// Clean up
	streamHandleRegistryMu.Lock()
	delete(streamHandleRegistry, dummyStreamPtr)
	streamHandleRegistryMu.Unlock()
	handle.Delete()
}

// TestStreamHandleCleanup tests that handles are properly cleaned up
func TestStreamHandleCleanup(t *testing.T) {
	testProvider := &mockHeadersProvider{}
	handle := cgo.NewHandle(testProvider)

	dummyStream := struct{ id int }{5678}
	dummyStreamPtr := unsafe.Pointer(&dummyStream)

	// Store in registry
	streamHandleRegistryMu.Lock()
	streamHandleRegistry[dummyStreamPtr] = handle
	streamHandleRegistryMu.Unlock()

	// Simulate streamFree cleanup logic
	streamHandleRegistryMu.Lock()
	if h, exists := streamHandleRegistry[dummyStreamPtr]; exists {
		h.Delete()
		delete(streamHandleRegistry, dummyStreamPtr)
	}
	streamHandleRegistryMu.Unlock()

	// Verify it's removed
	streamHandleRegistryMu.Lock()
	_, exists := streamHandleRegistry[dummyStreamPtr]
	streamHandleRegistryMu.Unlock()

	if exists {
		t.Fatal("Handle should have been removed from registry")
	}
}

// TestHandleConcurrency tests concurrent access to the handle registry
func TestHandleConcurrency(t *testing.T) {
	const numGoroutines = 10

	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			testProvider := &mockHeadersProvider{}
			handle := cgo.NewHandle(testProvider)
			dummyStream := struct{ id int }{1000 + id}
			ptr := unsafe.Pointer(&dummyStream)

			// Store
			streamHandleRegistryMu.Lock()
			streamHandleRegistry[ptr] = handle
			streamHandleRegistryMu.Unlock()

			// Retrieve
			streamHandleRegistryMu.Lock()
			_, exists := streamHandleRegistry[ptr]
			streamHandleRegistryMu.Unlock()

			if !exists {
				t.Errorf("Handle %d not found", id)
			}

			// Clean up
			streamHandleRegistryMu.Lock()
			delete(streamHandleRegistry, ptr)
			streamHandleRegistryMu.Unlock()
			handle.Delete()

			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}

// Mock HeadersProvider for testing
type mockHeadersProvider struct {
	headers map[string]string
	err     error
}

func (m *mockHeadersProvider) GetHeaders() (map[string]string, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.headers == nil {
		return map[string]string{
			"Authorization":   "Bearer test-token",
			"X-Custom-Header": "test-value",
		}, nil
	}
	return m.headers, nil
}

// TestMockHeadersProvider tests the mock provider
func TestMockHeadersProvider(t *testing.T) {
	provider := &mockHeadersProvider{}

	headers, err := provider.GetHeaders()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(headers) != 2 {
		t.Fatalf("Expected 2 headers, got %d", len(headers))
	}

	if headers["Authorization"] != "Bearer test-token" {
		t.Errorf("Unexpected Authorization header: %s", headers["Authorization"])
	}
}

// TestMockHeadersProviderWithError tests the mock provider error handling
func TestMockHeadersProviderWithError(t *testing.T) {
	testErr := &ZerobusError{Message: "test error", IsRetryable: false}
	provider := &mockHeadersProvider{err: testErr}

	_, err := provider.GetHeaders()
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	if err != testErr {
		t.Errorf("Expected error %v, got %v", testErr, err)
	}
}

// TestSdkIdentifierFormat verifies the SDK identifier is `zerobus-sdk-go/<version>`
// — regression guard against the wrapper reporting `zerobus-sdk-rs/<version>`.
func TestSdkIdentifierFormat(t *testing.T) {
	got := sdkIdentifier()
	want := "zerobus-sdk-go/" + sdkVersion
	if got != want {
		t.Errorf("sdkIdentifier() = %q, want %q", got, want)
	}
}

// TestWithApplicationName verifies the option mutates resolved options.
func TestWithApplicationName(t *testing.T) {
	var o sdkOptions
	WithApplicationName("my-app/1.0")(&o)
	if o.applicationName != "my-app/1.0" {
		t.Errorf("WithApplicationName did not set applicationName, got %q", o.applicationName)
	}
}

// TestNewZerobusSdkSkipsNilOption verifies nil entries in the variadic opts
// list are skipped rather than panicking.
func TestNewZerobusSdkSkipsNilOption(t *testing.T) {
	sdk, err := NewZerobusSdk(
		"https://workspace.zerobus.databricks.com",
		"https://workspace.cloud.databricks.com",
		nil,
		WithApplicationName("ok"),
		nil,
	)
	if err != nil {
		t.Fatalf("NewZerobusSdk should not error on nil opts, got: %v", err)
	}
	if sdk == nil {
		t.Fatal("expected non-nil SDK")
	}
	sdk.Free()
}

// TestZerobusError tests the ZerobusError type
func TestZerobusError(t *testing.T) {
	err := &ZerobusError{
		Message:     "test error message",
		IsRetryable: true,
	}

	errStr := err.Error()
	if errStr != "ZerobusError (retryable): test error message" {
		t.Errorf("Expected 'ZerobusError (retryable): test error message', got '%s'", errStr)
	}

	if !err.IsRetryable {
		t.Error("Expected error to be retryable")
	}

	// Test non-retryable error
	err2 := &ZerobusError{
		Message:     "permanent error",
		IsRetryable: false,
	}

	errStr2 := err2.Error()
	if errStr2 != "ZerobusError: permanent error" {
		t.Errorf("Expected 'ZerobusError: permanent error', got '%s'", errStr2)
	}
}
