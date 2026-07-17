package zerobus

import (
	"runtime/cgo"
	"testing"
	"unsafe"
)

// TestGoFreeHeadersProviderReleasesHandle verifies the FFI-owned destroy path:
// goFreeHeadersProvider must delete the cgo.Handle it is handed, releasing the
// Go provider. This is the Go side of the ownership transfer that closes the
// recovery-vs-teardown use-after-free.
func TestGoFreeHeadersProviderReleasesHandle(t *testing.T) {
	handle := cgo.NewHandle(&mockHeadersProvider{})
	handlePtr := *(*unsafe.Pointer)(unsafe.Pointer(&handle))

	// The handle resolves to the provider while live.
	if _, ok := handle.Value().(HeadersProvider); !ok {
		t.Fatal("handle should resolve to the provider before free")
	}

	// The destroy callback releases it; the handle must no longer be valid.
	goFreeHeadersProvider(handlePtr)

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected handle.Value() to panic after goFreeHeadersProvider")
		}
	}()
	_ = handle.Value() // panics: handle was deleted
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
