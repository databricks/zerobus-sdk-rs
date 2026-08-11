package auth

import (
	"context"
	"testing"
)

func TestStaticHeadersProviderTrimsValues(t *testing.T) {
	p := NewStaticHeadersProvider(map[string]string{"authorization": "  Bearer tok  "})
	got, err := p.GetHeaders(context.Background(), "c.s.t")
	if err != nil {
		t.Fatalf("GetHeaders: %v", err)
	}
	if got["authorization"] != "Bearer tok" {
		t.Fatalf("want trimmed %q, got %q", "Bearer tok", got["authorization"])
	}
}

func TestStaticHeadersProviderReturnsIndependentCopy(t *testing.T) {
	p := NewStaticHeadersProvider(map[string]string{"authorization": "Bearer tok"})

	got, err := p.GetHeaders(context.Background(), "c.s.t")
	if err != nil {
		t.Fatalf("GetHeaders: %v", err)
	}
	// Mutating the returned map must not corrupt the provider's own copy.
	got["authorization"] = "mutated"

	got2, err := p.GetHeaders(context.Background(), "c.s.t")
	if err != nil {
		t.Fatalf("GetHeaders: %v", err)
	}
	if got2["authorization"] != "Bearer tok" {
		t.Fatalf("caller mutation leaked into provider: got %q", got2["authorization"])
	}
}

func TestStaticHeadersProviderEmptyIsError(t *testing.T) {
	for name, headers := range map[string]map[string]string{
		"nil":   nil,
		"empty": {},
	} {
		t.Run(name, func(t *testing.T) {
			p := NewStaticHeadersProvider(headers)
			if _, err := p.GetHeaders(context.Background(), "c.s.t"); err == nil {
				t.Fatal("want error for empty static headers, got nil")
			}
		})
	}
}

func TestStaticHeadersProviderInvalidateIsNoOp(t *testing.T) {
	p := NewStaticHeadersProvider(map[string]string{"authorization": "Bearer tok"})
	p.Invalidate(context.Background(), "c.s.t") // must not panic

	got, err := p.GetHeaders(context.Background(), "c.s.t")
	if err != nil {
		t.Fatalf("GetHeaders after Invalidate: %v", err)
	}
	if got["authorization"] != "Bearer tok" {
		t.Fatalf("Invalidate should not alter static headers, got %q", got["authorization"])
	}
}
