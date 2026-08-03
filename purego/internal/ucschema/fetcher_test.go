package ucschema

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newUCServer(t *testing.T, tokenStatus, schemaStatus int) *httptest.Server {
	t.Helper()
	return httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/oidc/v1/token":
			if tokenStatus != http.StatusOK {
				w.WriteHeader(tokenStatus)
				_, _ = w.Write([]byte(`{"error":"bad_client"}`))
				return
			}
			_ = r.ParseForm()
			if got := r.Form.Get("grant_type"); got != "client_credentials" {
				t.Fatalf("grant_type = %q, want client_credentials", got)
			}
			if got := r.Form.Get("scope"); got != "all-apis" {
				t.Fatalf("scope = %q, want all-apis", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "abc"})
			return
		default:
			if !strings.HasPrefix(r.URL.Path, "/api/2.1/unity-catalog/tables/") {
				http.NotFound(w, r)
				return
			}
			if schemaStatus != http.StatusOK {
				w.WriteHeader(schemaStatus)
				_, _ = w.Write([]byte(`{"error":"boom"}`))
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name":         "orders",
				"catalog_name": "main",
				"schema_name":  "sales",
				"columns": []map[string]any{
					{
						"name":      "id",
						"type_name": "LONG",
						"position":  0,
						"nullable":  false,
					},
				},
			})
		}
	}))
}

func TestFetchTableSchema_Success(t *testing.T) {
	srv := newUCServer(t, http.StatusOK, http.StatusOK)
	defer srv.Close()

	fetcher, err := New(Config{
		WorkspaceEndpoint: srv.URL,
		ClientID:          "id",
		ClientSecret:      "secret",
		HTTPClient:        srv.Client(),
		RequestTimeout:    2 * time.Second,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	got, err := fetcher.FetchTableSchema(context.Background(), "main.sales.orders")
	if err != nil {
		t.Fatalf("FetchTableSchema() error = %v", err)
	}
	if got.Name != "orders" {
		t.Fatalf("name = %q, want orders", got.Name)
	}
	if len(got.Columns) != 1 {
		t.Fatalf("columns = %d, want 1", len(got.Columns))
	}
}

func TestFetchTableSchema_TokenFailure(t *testing.T) {
	srv := newUCServer(t, http.StatusUnauthorized, http.StatusOK)
	defer srv.Close()

	fetcher, err := New(Config{
		WorkspaceEndpoint: srv.URL,
		ClientID:          "id",
		ClientSecret:      "secret",
		HTTPClient:        srv.Client(),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	_, err = fetcher.FetchTableSchema(context.Background(), "main.sales.orders")
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), "FetchToken") {
		t.Fatalf("error = %v, want FetchToken context", err)
	}
	var fe *FetchError
	if !errors.As(err, &fe) {
		t.Fatalf("expected FetchError")
	}
}

func TestFetchTableSchema_ServerErrorRetryable(t *testing.T) {
	srv := newUCServer(t, http.StatusOK, http.StatusInternalServerError)
	defer srv.Close()

	fetcher, err := New(Config{
		WorkspaceEndpoint: srv.URL,
		ClientID:          "id",
		ClientSecret:      "secret",
		HTTPClient:        srv.Client(),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	_, err = fetcher.FetchTableSchema(context.Background(), "main.sales.orders")
	if err == nil {
		t.Fatalf("expected error")
	}
	fe, ok := err.(*FetchError)
	if !ok {
		t.Fatalf("error type = %T, want *FetchError", err)
	}
	if !fe.IsRetryable() {
		t.Fatalf("retryable = false, want true")
	}
}
