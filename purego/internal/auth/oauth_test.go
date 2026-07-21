package auth

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// tokenServer is a minimal OAuth 2.0 token endpoint stub.
type tokenServer struct {
	accessToken string
	expiresIn   int    // 0 means omit expires_in
	rawExpires  string // when non-empty, emitted verbatim as expires_in (overrides expiresIn)
	statusCode  int    // 0 defaults to 200
	calls       atomic.Int32
}

func (s *tokenServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.calls.Add(1)
	code := s.statusCode
	if code == 0 {
		code = http.StatusOK
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if code != http.StatusOK {
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "test_error"})
		return
	}
	if s.rawExpires != "" {
		// Emit a hand-built body so expires_in can be a non-integer JSON type.
		_, _ = w.Write([]byte(`{"access_token":"` + s.accessToken + `","expires_in":` + s.rawExpires + `}`))
		return
	}
	resp := map[string]any{"access_token": s.accessToken}
	if s.expiresIn > 0 {
		resp["expires_in"] = s.expiresIn
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func newTestProvider(t *testing.T, srv *tokenServer) (*OAuthTokenProvider, *httptest.Server) {
	t.Helper()
	ts := httptest.NewServer(srv)
	p, err := NewOAuthTokenProvider("clientID", "clientSecret", "https://ws123.zerobus.databricks.com", ts.URL,
		WithHTTPClient(ts.Client()),
	)
	if err != nil {
		t.Fatalf("NewOAuthTokenProvider: %v", err)
	}
	return p, ts
}

func TestOAuthTokenProviderHappyPath(t *testing.T) {
	srv := &tokenServer{accessToken: "eyJtb2NrfQ", expiresIn: 3600}
	p, ts := newTestProvider(t, srv)
	defer ts.Close()

	tok, err := p.Token(context.Background(), "cat.sch.tbl")
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if tok != "eyJtb2NrfQ" {
		t.Fatalf("want %q, got %q", "eyJtb2NrfQ", tok)
	}
}

func TestOAuthTokenProviderCachesToken(t *testing.T) {
	srv := &tokenServer{accessToken: "tok", expiresIn: 3600}
	p, ts := newTestProvider(t, srv)
	defer ts.Close()

	if _, err := p.Token(context.Background(), "cat.sch.tbl"); err != nil {
		t.Fatalf("first Token: %v", err)
	}
	if _, err := p.Token(context.Background(), "cat.sch.tbl"); err != nil {
		t.Fatalf("second Token: %v", err)
	}
	if got := srv.calls.Load(); got != 1 {
		t.Fatalf("want 1 server call (cached), got %d", got)
	}
}

func TestOAuthTokenProviderInvalidateForcesRemint(t *testing.T) {
	srv := &tokenServer{accessToken: "tok", expiresIn: 3600}
	p, ts := newTestProvider(t, srv)
	defer ts.Close()

	if _, err := p.Token(context.Background(), "cat.sch.tbl"); err != nil {
		t.Fatalf("first Token: %v", err)
	}
	p.Invalidate(context.Background(), "cat.sch.tbl")
	if _, err := p.Token(context.Background(), "cat.sch.tbl"); err != nil {
		t.Fatalf("Token after Invalidate: %v", err)
	}
	if got := srv.calls.Load(); got != 2 {
		t.Fatalf("want 2 server calls after invalidate, got %d", got)
	}
}

func TestOAuthTokenProvider5xxIsRetryable(t *testing.T) {
	srv := &tokenServer{statusCode: http.StatusInternalServerError}
	p, ts := newTestProvider(t, srv)
	defer ts.Close()

	_, err := p.Token(context.Background(), "cat.sch.tbl")
	if err == nil {
		t.Fatal("want error for 500, got nil")
	}
	var te *TokenError
	if !asTokenError(err, &te) || !te.IsRetryable() {
		t.Fatalf("want retryable TokenError, got %T: %v", err, err)
	}
}

func TestOAuthTokenProvider4xxIsNonRetryable(t *testing.T) {
	srv := &tokenServer{statusCode: http.StatusUnauthorized}
	p, ts := newTestProvider(t, srv)
	defer ts.Close()

	_, err := p.Token(context.Background(), "cat.sch.tbl")
	if err == nil {
		t.Fatal("want error for 401, got nil")
	}
	var te *TokenError
	if !asTokenError(err, &te) || te.IsRetryable() {
		t.Fatalf("want non-retryable TokenError, got %T (retryable=%v): %v", err, te != nil && te.IsRetryable(), err)
	}
}

func TestOAuthTokenProviderContextCancellation(t *testing.T) {
	// unblock is closed when the test ends so the hanging handler exits before
	// httptest.Server.Close() tries to drain in-flight requests.
	unblock := make(chan struct{})
	hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-unblock:
		case <-r.Context().Done():
		}
		http.Error(w, "cancelled", http.StatusServiceUnavailable)
	}))
	defer hang.Close()   // executed second: server is now idle
	defer close(unblock) // executed first: unblocks the handler

	p, err := NewOAuthTokenProvider("id", "secret", "https://ws.zerobus.databricks.com", hang.URL,
		WithHTTPClient(hang.Client()),
	)
	if err != nil {
		t.Fatalf("NewOAuthTokenProvider: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = p.Token(ctx, "c.s.t")
	if err == nil {
		t.Fatal("want error on ctx cancellation, got nil")
	}
	if elapsed := time.Since(start); elapsed > 1*time.Second {
		t.Fatalf("Token took %v, expected it to return near 100ms deadline", elapsed)
	}
}

func TestOAuthTokenProviderConnectionRefusedIsRetryable(t *testing.T) {
	// Point the provider at a closed port so Do() fails with a dial error, which
	// must be classified as a retryable transport failure.
	ts := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	client := ts.Client()
	url := ts.URL
	ts.Close() // close immediately so connections are refused

	p, err := NewOAuthTokenProvider("id", "secret", "https://ws.zerobus.databricks.com", url,
		WithHTTPClient(client),
	)
	if err != nil {
		t.Fatalf("NewOAuthTokenProvider: %v", err)
	}
	_, err = p.Token(context.Background(), "c.s.t")
	if err == nil {
		t.Fatal("want error for refused connection, got nil")
	}
	var te *TokenError
	if !asTokenError(err, &te) || !te.IsRetryable() {
		t.Fatalf("want retryable TokenError for dial failure, got %T (retryable=%v): %v",
			err, te != nil && te.IsRetryable(), err)
	}
}

func TestOAuthTokenProviderClientTimeoutIsRetryable(t *testing.T) {
	// A handler slower than the client's Timeout drives Do() into a client-level
	// timeout. http.Client.Timeout surfaces as an error wrapping
	// context.DeadlineExceeded, but the caller's own context is still live, so it
	// is the request that timed out, not the caller cancelling. That is a
	// transient fault and must be classified retryable so a proactive refresh can
	// fall back to a still-valid cached token instead of failing the open.
	// release is closed before ts.Close() so the parked handler returns and Close
	// does not block waiting on the outstanding request.
	release := make(chan struct{})
	srv := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	ts := httptest.NewServer(srv)
	defer ts.Close()
	defer close(release)

	client := ts.Client()
	client.Timeout = 50 * time.Millisecond

	p, err := NewOAuthTokenProvider("id", "secret", "https://ws.zerobus.databricks.com", ts.URL,
		WithHTTPClient(client),
	)
	if err != nil {
		t.Fatalf("NewOAuthTokenProvider: %v", err)
	}
	// Pass a context with no deadline so the timeout can only come from the
	// client, not the caller — the case the classification must get right.
	_, err = p.Token(context.Background(), "c.s.t")
	if err == nil {
		t.Fatal("want error for client timeout, got nil")
	}
	var te *TokenError
	if !asTokenError(err, &te) || !te.IsRetryable() {
		t.Fatalf("want retryable TokenError for client timeout, got %T (retryable=%v): %v",
			err, te != nil && te.IsRetryable(), err)
	}
}

func TestOAuthTokenProviderClientTimeoutRefreshServesCachedToken(t *testing.T) {
	// End-to-end: a token that is still valid but due for proactive refresh, whose
	// refresh mint trips the client timeout, must fall back to the cached token
	// rather than failing. This is the payoff of classifying a client timeout as
	// retryable: transport.Open (which adds its own header budget) still succeeds.
	release := make(chan struct{})
	srv := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	ts := httptest.NewServer(srv)
	defer ts.Close()
	defer close(release)

	client := ts.Client()
	client.Timeout = 50 * time.Millisecond

	p, err := NewOAuthTokenProvider("id", "secret", "https://ws.zerobus.databricks.com", ts.URL,
		WithHTTPClient(client),
	)
	if err != nil {
		t.Fatalf("NewOAuthTokenProvider: %v", err)
	}
	// Seed a still-valid-but-refresh-due token under the provider's real audience.
	seedRefreshable(p.cache, "id", "secret", "c.s.t", p.tokenAudience(), "cached-valid")

	tok, err := p.Token(context.Background(), "c.s.t")
	if err != nil {
		t.Fatalf("want fallback to cached token on client-timeout refresh, got error: %v", err)
	}
	if tok != "cached-valid" {
		t.Fatalf("want cached token served, got %q", tok)
	}
}

func TestOAuthTokenProviderSharedCacheKeyedByWorkspace(t *testing.T) {
	// Two providers with the same credentials and table but different workspaces
	// share a cache. Because a minted token is bound to its workspace audience,
	// they must NOT collide on one entry — the second must mint its own token
	// rather than being served the first's workspace-scoped one.
	var mu sync.Mutex
	seen := map[string]string{} // resource audience -> token minted for it
	srv := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		res := r.FormValue("resource")
		mu.Lock()
		tok, ok := seen[res]
		if !ok {
			tok = "tok-for-" + res
			seen[res] = tok
		}
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": tok, "expires_in": 3600})
	})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	shared := NewSharedTokenCache()
	p1, err := NewOAuthTokenProvider("id", "secret", "https://ws1.zerobus.databricks.com", ts.URL,
		WithHTTPClient(ts.Client()), WithSharedTokenCache(shared))
	if err != nil {
		t.Fatalf("provider 1: %v", err)
	}
	p2, err := NewOAuthTokenProvider("id", "secret", "https://ws2.zerobus.databricks.com", ts.URL,
		WithHTTPClient(ts.Client()), WithSharedTokenCache(shared))
	if err != nil {
		t.Fatalf("provider 2: %v", err)
	}

	t1, err := p1.Token(context.Background(), "c.s.t")
	if err != nil {
		t.Fatalf("p1.Token: %v", err)
	}
	t2, err := p2.Token(context.Background(), "c.s.t")
	if err != nil {
		t.Fatalf("p2.Token: %v", err)
	}
	if t1 == t2 {
		t.Fatalf("providers for different workspaces shared a cached token %q; audience must be part of the key", t1)
	}
}

func TestOAuthTokenProvider429IsNonRetryable(t *testing.T) {
	srv := &tokenServer{statusCode: http.StatusTooManyRequests}
	p, ts := newTestProvider(t, srv)
	defer ts.Close()

	_, err := p.Token(context.Background(), "cat.sch.tbl")
	if err == nil {
		t.Fatal("want error for 429, got nil")
	}
	var te *TokenError
	if !asTokenError(err, &te) || te.IsRetryable() {
		t.Fatalf("want non-retryable TokenError for 429, got %T (retryable=%v): %v",
			err, te != nil && te.IsRetryable(), err)
	}
}

func TestOAuthTokenProvider408IsNonRetryable(t *testing.T) {
	srv := &tokenServer{statusCode: http.StatusRequestTimeout}
	p, ts := newTestProvider(t, srv)
	defer ts.Close()

	_, err := p.Token(context.Background(), "cat.sch.tbl")
	if err == nil {
		t.Fatal("want error for 408, got nil")
	}
	var te *TokenError
	if !asTokenError(err, &te) || te.IsRetryable() {
		t.Fatalf("want non-retryable TokenError for 408, got %T (retryable=%v): %v",
			err, te != nil && te.IsRetryable(), err)
	}
}

func TestTokenErrorUnwrapsContextCancellation(t *testing.T) {
	// A cancelled context must be visible via errors.Is on the returned error,
	// and must NOT be classified as retryable (else a cancelled proactive
	// refresh would silently serve a stale token).
	unblock := make(chan struct{})
	hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-unblock:
		case <-r.Context().Done():
		}
	}))
	defer hang.Close()
	defer close(unblock)

	p, err := NewOAuthTokenProvider("id", "secret", "https://ws.zerobus.databricks.com", hang.URL,
		WithHTTPClient(hang.Client()),
	)
	if err != nil {
		t.Fatalf("NewOAuthTokenProvider: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err = p.Token(ctx, "c.s.t")
	if err == nil {
		t.Fatal("want error on ctx cancellation, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want errors.Is(err, DeadlineExceeded), got %v", err)
	}
	var te *TokenError
	if asTokenError(err, &te) && te.IsRetryable() {
		t.Fatal("context deadline must not be classified as retryable")
	}
}

func TestOAuthTokenProviderNilOptionsKeepDefaults(t *testing.T) {
	// A nil client / cache passed via options must be ignored so the provider's
	// defaults survive rather than being overwritten with nil (which would panic
	// on the first Token call).
	srv := &tokenServer{accessToken: "tok", expiresIn: 3600}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	p, err := NewOAuthTokenProvider("id", "secret", "https://ws.zerobus.databricks.com", ts.URL,
		WithHTTPClient(nil),
		WithSharedTokenCache(nil),
	)
	if err != nil {
		t.Fatalf("NewOAuthTokenProvider: %v", err)
	}
	if p.client == nil {
		t.Fatal("WithHTTPClient(nil) overwrote the default client")
	}
	if p.cache == nil {
		t.Fatal("WithSharedTokenCache(nil) overwrote the default cache")
	}
	// It must not panic and must actually work end to end.
	if _, err := p.Token(context.Background(), "c.s.t"); err != nil {
		t.Fatalf("Token with nil options: %v", err)
	}
}

func TestClassifyHTTPErrorPreservesBody(t *testing.T) {
	srv := &tokenServer{statusCode: http.StatusUnauthorized}
	p, ts := newTestProvider(t, srv)
	defer ts.Close()

	_, err := p.Token(context.Background(), "cat.sch.tbl")
	if err == nil {
		t.Fatal("want error for 401, got nil")
	}
	// The structured OAuth error field (`{"error":"test_error"}`) must survive
	// into the message so on-call has something to debug with; the raw body is
	// retained separately on TokenError.ResponseBody.
	if !strings.Contains(err.Error(), "test_error") {
		t.Fatalf("error message dropped OAuth error field: %q", err.Error())
	}
	if !strings.Contains(err.Error(), "401") {
		t.Fatalf("error message dropped status code: %q", err.Error())
	}
	// The raw body must be retained on ResponseBody for diagnostics — and kept
	// out of Error() so a %+v log of the wrapped error can't re-expose it.
	var te *TokenError
	if !errors.As(err, &te) {
		t.Fatalf("want *TokenError, got %T", err)
	}
	if !strings.Contains(te.ResponseBody, "test_error") {
		t.Fatalf("raw body not retained on ResponseBody: %q", te.ResponseBody)
	}
}

func TestNewOAuthTokenProviderValidation(t *testing.T) {
	cases := []struct {
		name                                                 string
		clientID, secret, zerobusEndpoint, ucEndpoint, table string
	}{
		{"empty clientID", "", "s", "https://ws.zerobus.databricks.com", "https://host", "c.s.t"},
		{"empty secret", "id", "", "https://ws.zerobus.databricks.com", "https://host", "c.s.t"},
		{"empty zerobus endpoint", "id", "s", "", "https://host", "c.s.t"},
		{"empty ucEndpoint", "id", "s", "https://ws.zerobus.databricks.com", "", "c.s.t"},
		{"bad table (2 parts)", "id", "s", "https://ws.zerobus.databricks.com", "https://host", "c.s"},
		{"bad table (empty schema)", "id", "s", "https://ws.zerobus.databricks.com", "https://host", "c..t"},
		{"bad zerobus endpoint host", "id", "s", "https://", "https://host", "c.s.t"},
		{"dotless zerobus host (no workspace subdomain)", "id", "s", "https://ws1", "https://host", "c.s.t"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := NewOAuthTokenProvider(tc.clientID, tc.secret, tc.zerobusEndpoint, tc.ucEndpoint)
			if err == nil {
				if _, tokErr := p.Token(context.Background(), tc.table); tokErr == nil {
					t.Fatal("want constructor or token validation error, got nil")
				}
			}
		})
	}
}

func TestSecondsToDuration(t *testing.T) {
	if d, ok := secondsToDuration(3600); !ok || d != time.Hour {
		t.Fatalf("secondsToDuration(3600) = (%v, %v), want (1h, true)", d, ok)
	}
	if _, ok := secondsToDuration(0); ok {
		t.Fatal("secondsToDuration(0) should report no usable TTL")
	}
	if _, ok := secondsToDuration(-5); ok {
		t.Fatal("secondsToDuration(negative) should report no usable TTL")
	}
	// An absurd value must clamp to a positive duration, never wrap negative.
	d, ok := secondsToDuration(1 << 60)
	if !ok || d <= 0 {
		t.Fatalf("secondsToDuration(1<<60) = (%v, %v), want a positive clamped duration", d, ok)
	}
}

func TestParseExpiresIn(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{`3600`, 3600},   // RFC 6749 integer
		{`"3600"`, 3600}, // quoted string
		{`3600.0`, 3600}, // float
		{`3600.9`, 3600}, // float truncates toward zero
		{`0`, 0},         // zero → no TTL
		{`-5`, -5},       // negative int passes through; secondsToDuration rejects it
		{`null`, 0},      // JSON null
		{``, 0},          // absent
		{`"nope"`, 0},    // unparseable string
		{`{}`, 0},        // wrong type
		{` "42" `, 42},   // surrounding whitespace
		{`"-5"`, -5},     // quoted negative
	}
	for _, tc := range cases {
		if got := parseExpiresIn(json.RawMessage(tc.in)); got != tc.want {
			t.Errorf("parseExpiresIn(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

// A non-integer but parseable expires_in (quoted string, float) must not fail
// the mint, and must be cached like a regular integer TTL.
func TestOAuthTokenProviderToleratesNonIntExpiresIn(t *testing.T) {
	for _, raw := range []string{`"3600"`, `3600.0`} {
		t.Run(raw, func(t *testing.T) {
			srv := &tokenServer{accessToken: "tok", rawExpires: raw}
			p, ts := newTestProvider(t, srv)
			defer ts.Close()

			tok, err := p.Token(context.Background(), "cat.sch.tbl")
			if err != nil {
				t.Fatalf("Token: %v", err)
			}
			if tok != "tok" {
				t.Fatalf("want %q, got %q", "tok", tok)
			}
			// A parseable TTL caches; a second call is served from cache.
			if _, err := p.Token(context.Background(), "cat.sch.tbl"); err != nil {
				t.Fatalf("second Token: %v", err)
			}
			if got := srv.calls.Load(); got != 1 {
				t.Fatalf("want 1 server call (cached), got %d", got)
			}
		})
	}
}

// An unparseable expires_in keeps the token but skips caching, so every call
// mints afresh instead of failing.
func TestOAuthTokenProviderUnparseableExpiresInIsNotCached(t *testing.T) {
	srv := &tokenServer{accessToken: "tok", rawExpires: `"not-a-number"`}
	p, ts := newTestProvider(t, srv)
	defer ts.Close()

	if _, err := p.Token(context.Background(), "cat.sch.tbl"); err != nil {
		t.Fatalf("first Token: %v", err)
	}
	if _, err := p.Token(context.Background(), "cat.sch.tbl"); err != nil {
		t.Fatalf("second Token: %v", err)
	}
	if got := srv.calls.Load(); got != 2 {
		t.Fatalf("want 2 server calls (uncached), got %d", got)
	}
}

// A whitespace-only access_token is unusable: it must be rejected rather than
// shipped as "Bearer    ".
func TestOAuthTokenProviderRejectsBlankAccessToken(t *testing.T) {
	srv := &tokenServer{accessToken: "   ", expiresIn: 3600}
	p, ts := newTestProvider(t, srv)
	defer ts.Close()

	if _, err := p.Token(context.Background(), "cat.sch.tbl"); err == nil {
		t.Fatal("want error for whitespace-only access_token, got nil")
	}
}

// Omitting expires_in entirely returns the token but leaves the cache empty, so
// a second call mints again.
func TestOAuthTokenProviderOmittedExpiresInIsNotCached(t *testing.T) {
	srv := &tokenServer{accessToken: "tok"} // expiresIn 0 → omitted
	p, ts := newTestProvider(t, srv)
	defer ts.Close()

	tok, err := p.Token(context.Background(), "cat.sch.tbl")
	if err != nil {
		t.Fatalf("first Token: %v", err)
	}
	if tok != "tok" {
		t.Fatalf("want %q, got %q", "tok", tok)
	}
	if _, err := p.Token(context.Background(), "cat.sch.tbl"); err != nil {
		t.Fatalf("second Token: %v", err)
	}
	if got := srv.calls.Load(); got != 2 {
		t.Fatalf("want 2 server calls (uncached), got %d", got)
	}
}

func TestValidateEndpoint(t *testing.T) {
	ok := []string{
		"https://workspace.databricks.com",
		"http://localhost:8080",
		"http://LOCALHOST:8080", // hostnames are case-insensitive
		"http://127.0.0.1:1234",
		"http://[::1]:9000",
	}
	for _, e := range ok {
		if err := validateEndpoint(e); err != nil {
			t.Errorf("validateEndpoint(%q): unexpected error: %v", e, err)
		}
	}

	bad := []string{
		"http://workspace.databricks.com", // plaintext to a remote host
		"ftp://host",
		"://nonsense",
		"https://",  // scheme but no host
		"https:///", // no host, only a path
		"https://workspace.databricks.com?tenant=x",  // query would swallow the /oidc/v1/token suffix
		"https://workspace.databricks.com#frag",      // fragment would drop the suffix
		"https://user:pass@workspace.databricks.com", // userinfo would collide with basic-auth creds
	}
	for _, e := range bad {
		if err := validateEndpoint(e); err == nil {
			t.Errorf("validateEndpoint(%q): want error, got nil", e)
		}
	}
}

func TestDeriveWorkspaceIDFromEndpoint(t *testing.T) {
	ok := []struct {
		in, want string
	}{
		{"https://ws.zerobus.databricks.com", "ws"},
		{"ws.zerobus.databricks.com", "ws"},         // scheme supplied by default
		{"HTTPS://ws.zerobus.databricks.com", "ws"}, // scheme is case-insensitive
		{"HTTP://ws.zerobus.databricks.com", "ws"},
	}
	for _, tc := range ok {
		got, err := deriveWorkspaceIDFromEndpoint(tc.in)
		if err != nil {
			t.Errorf("deriveWorkspaceIDFromEndpoint(%q): unexpected error: %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("deriveWorkspaceIDFromEndpoint(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}

	bad := []string{
		"",            // required
		"https://",    // no host
		"https://ws1", // dotless host, no workspace subdomain
	}
	for _, in := range bad {
		if _, err := deriveWorkspaceIDFromEndpoint(in); err == nil {
			t.Errorf("deriveWorkspaceIDFromEndpoint(%q): want error, got nil", in)
		}
	}
}

func TestOAuthTokenProviderCacheDisabledAlwaysMints(t *testing.T) {
	srv := &tokenServer{accessToken: "tok", expiresIn: 3600}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	p, err := NewOAuthTokenProvider("id", "secret", "https://ws.zerobus.databricks.com", ts.URL,
		WithHTTPClient(ts.Client()),
		WithTokenCacheEnabled(false),
	)
	if err != nil {
		t.Fatalf("NewOAuthTokenProvider: %v", err)
	}
	if _, err := p.Token(context.Background(), "c.s.t"); err != nil {
		t.Fatalf("first Token: %v", err)
	}
	if _, err := p.Token(context.Background(), "c.s.t"); err != nil {
		t.Fatalf("second Token: %v", err)
	}
	if got := srv.calls.Load(); got != 2 {
		t.Fatalf("want 2 server calls with caching disabled, got %d", got)
	}
}

func TestOAuthTokenProviderCustomRefreshBuffer(t *testing.T) {
	// WithRefreshBuffer must reach the provider's own cache. (The refresh-timing
	// behavior itself is covered by the tokenCache unit tests; a large buffer no
	// longer forces a re-mint on every call because it is clamped to ttl/2.)
	p, err := NewOAuthTokenProvider("id", "secret", "https://ws.zerobus.databricks.com", "https://host",
		WithRefreshBuffer(90*time.Second),
	)
	if err != nil {
		t.Fatalf("NewOAuthTokenProvider: %v", err)
	}
	if p.cache.refreshBuffer != 90*time.Second {
		t.Fatalf("WithRefreshBuffer not applied: got %v", p.cache.refreshBuffer)
	}
}

func TestOAuthTokenProviderSharedCacheIgnoresProviderCacheOpts(t *testing.T) {
	// A shared cache owns its own config; per-provider cache options are ignored
	// so the shared cache still caches even though the provider asked to disable.
	srv := &tokenServer{accessToken: "tok", expiresIn: 3600}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	shared := NewSharedTokenCache()
	p, err := NewOAuthTokenProvider("id", "secret", "https://ws.zerobus.databricks.com", ts.URL,
		WithHTTPClient(ts.Client()),
		WithSharedTokenCache(shared),
		WithTokenCacheEnabled(false),
	)
	if err != nil {
		t.Fatalf("NewOAuthTokenProvider: %v", err)
	}
	if _, err := p.Token(context.Background(), "c.s.t"); err != nil {
		t.Fatalf("first Token: %v", err)
	}
	if _, err := p.Token(context.Background(), "c.s.t"); err != nil {
		t.Fatalf("second Token: %v", err)
	}
	if got := srv.calls.Load(); got != 1 {
		t.Fatalf("shared cache should cache (1 call), got %d", got)
	}
}

func TestSharedTokenCacheDisabled(t *testing.T) {
	srv := &tokenServer{accessToken: "tok", expiresIn: 3600}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	shared := NewSharedTokenCache(CacheEnabled(false))
	p, err := NewOAuthTokenProvider("id", "secret", "https://ws.zerobus.databricks.com", ts.URL,
		WithHTTPClient(ts.Client()),
		WithSharedTokenCache(shared),
	)
	if err != nil {
		t.Fatalf("NewOAuthTokenProvider: %v", err)
	}
	if _, err := p.Token(context.Background(), "c.s.t"); err != nil {
		t.Fatalf("first Token: %v", err)
	}
	if _, err := p.Token(context.Background(), "c.s.t"); err != nil {
		t.Fatalf("second Token: %v", err)
	}
	if got := srv.calls.Load(); got != 2 {
		t.Fatalf("want 2 calls with disabled shared cache, got %d", got)
	}
}

func TestWithSharedTokenCacheZeroValueIsIgnored(t *testing.T) {
	// A zero-value SharedTokenCache holds no usable cache. It must be ignored
	// (falling back to the provider's own cache) rather than installed, which
	// would previously panic on a nil map at the first Token call.
	srv := &tokenServer{accessToken: "tok", expiresIn: 3600}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	p, err := NewOAuthTokenProvider("id", "secret", "https://ws.zerobus.databricks.com", ts.URL,
		WithHTTPClient(ts.Client()),
		WithSharedTokenCache(&SharedTokenCache{}),
	)
	if err != nil {
		t.Fatalf("NewOAuthTokenProvider: %v", err)
	}
	if p.cache == nil {
		t.Fatal("zero-value shared cache left provider without a cache")
	}
	if _, err := p.Token(context.Background(), "c.s.t"); err != nil {
		t.Fatalf("Token with zero-value shared cache: %v", err)
	}
}

func TestFetchTokenValidatesTableName(t *testing.T) {
	// FetchToken must reject a malformed table name before contacting UC, just
	// like Token does.
	srv := &tokenServer{accessToken: "tok", expiresIn: 3600}
	p, ts := newTestProvider(t, srv)
	defer ts.Close()

	if _, err := p.FetchToken(context.Background(), "c..t"); err == nil {
		t.Fatal("FetchToken with invalid table name: want error, got nil")
	}
	if got := srv.calls.Load(); got != 0 {
		t.Fatalf("FetchToken must not contact UC for an invalid table (0 calls), got %d", got)
	}
}

func TestOAuthTokenProviderFetchTokenBypassesCache(t *testing.T) {
	srv := &tokenServer{accessToken: "tok", expiresIn: 3600}
	p, ts := newTestProvider(t, srv)
	defer ts.Close()

	// Warm the cache.
	if _, err := p.Token(context.Background(), "cat.sch.tbl"); err != nil {
		t.Fatalf("Token: %v", err)
	}
	// FetchToken must mint fresh regardless of the warm cache…
	if _, err := p.FetchToken(context.Background(), "cat.sch.tbl"); err != nil {
		t.Fatalf("FetchToken: %v", err)
	}
	if got := srv.calls.Load(); got != 2 {
		t.Fatalf("FetchToken should bypass cache (2 calls), got %d", got)
	}
	// …and must not populate/replace the cached entry the next Token call uses.
	if _, err := p.Token(context.Background(), "cat.sch.tbl"); err != nil {
		t.Fatalf("Token after FetchToken: %v", err)
	}
	if got := srv.calls.Load(); got != 2 {
		t.Fatalf("Token after FetchToken should hit cache (still 2 calls), got %d", got)
	}
}

func TestOAuthHeadersProviderGetHeadersShape(t *testing.T) {
	srv := &tokenServer{accessToken: "tok", expiresIn: 3600}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	p, err := NewOAuthHeadersProvider("id", "secret", "https://ws.zerobus.databricks.com", ts.URL,
		WithHTTPClient(ts.Client()),
	)
	if err != nil {
		t.Fatalf("NewOAuthHeadersProvider: %v", err)
	}
	h, err := p.GetHeaders(context.Background(), "c.s.t")
	if err != nil {
		t.Fatalf("GetHeaders: %v", err)
	}
	if h["authorization"] != "Bearer tok" {
		t.Fatalf("want %q, got %q", "Bearer tok", h["authorization"])
	}
	if h["x-databricks-zerobus-table-name"] != "c.s.t" {
		t.Fatalf("want table header %q, got %q", "c.s.t", h["x-databricks-zerobus-table-name"])
	}
}

func TestOAuthHeadersProviderInvalidateForcesRemint(t *testing.T) {
	srv := &tokenServer{accessToken: "tok", expiresIn: 3600}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	p, err := NewOAuthHeadersProvider("id", "secret", "https://ws.zerobus.databricks.com", ts.URL,
		WithHTTPClient(ts.Client()),
	)
	if err != nil {
		t.Fatalf("NewOAuthHeadersProvider: %v", err)
	}

	if _, err := p.GetHeaders(context.Background(), "c.s.t"); err != nil {
		t.Fatalf("GetHeaders: %v", err)
	}
	// Second call hits the cache (still one mint).
	if _, err := p.GetHeaders(context.Background(), "c.s.t"); err != nil {
		t.Fatalf("GetHeaders: %v", err)
	}
	if got := srv.calls.Load(); got != 1 {
		t.Fatalf("want 1 mint before invalidate, got %d", got)
	}

	// Invalidate must delegate to the underlying cache, forcing a re-mint.
	p.Invalidate(context.Background(), "c.s.t")
	if _, err := p.GetHeaders(context.Background(), "c.s.t"); err != nil {
		t.Fatalf("GetHeaders after Invalidate: %v", err)
	}
	if got := srv.calls.Load(); got != 2 {
		t.Fatalf("want 2 mints after invalidate, got %d", got)
	}
}

func TestOAuthTokenProviderLoggerReceivesMint(t *testing.T) {
	var buf strings.Builder
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	srv := &tokenServer{accessToken: "tok", expiresIn: 3600}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	p, err := NewOAuthTokenProvider("id", "secret", "https://ws.zerobus.databricks.com", ts.URL,
		WithHTTPClient(ts.Client()),
		WithLogger(logger),
	)
	if err != nil {
		t.Fatalf("NewOAuthTokenProvider: %v", err)
	}
	if _, err := p.Token(context.Background(), "c.s.t"); err != nil {
		t.Fatalf("Token: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "minted UC OAuth token") || !strings.Contains(out, "reason=cold_miss") {
		t.Fatalf("logger did not receive expected mint line: %q", out)
	}
}

func TestAuthorizationDetailsContent(t *testing.T) {
	var captured []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err == nil {
			captured = []byte(r.FormValue("authorization_details"))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "tok", "expires_in": 3600})
	}))
	defer ts.Close()

	p, err := NewOAuthTokenProvider("id", "secret", "https://ws.zerobus.databricks.com", ts.URL,
		WithHTTPClient(ts.Client()),
	)
	if err != nil {
		t.Fatalf("NewOAuthTokenProvider: %v", err)
	}
	if _, err := p.Token(context.Background(), "mycat.mysch.mytbl"); err != nil {
		t.Fatalf("Token: %v", err)
	}

	var details []authorizationDetailsEntry
	if err := json.Unmarshal(captured, &details); err != nil {
		t.Fatalf("parse authorization_details: %v", err)
	}
	if len(details) != 3 {
		t.Fatalf("want 3 authorization_details entries, got %d", len(details))
	}

	catalog := details[0]
	if catalog.ObjectFullPath != "mycat" {
		t.Errorf("catalog entry: want ObjectFullPath %q, got %q", "mycat", catalog.ObjectFullPath)
	}

	schema := details[1]
	if schema.ObjectFullPath != "mycat.mysch" {
		t.Errorf("schema entry: want ObjectFullPath %q, got %q", "mycat.mysch", schema.ObjectFullPath)
	}

	table := details[2]
	if table.ObjectFullPath != "mycat.mysch.mytbl" {
		t.Errorf("table entry: want ObjectFullPath %q, got %q", "mycat.mysch.mytbl", table.ObjectFullPath)
	}
	if len(table.Operations) != 1 || table.Operations[0] != "zerobuswrite" {
		t.Errorf("table entry: want Operations [zerobuswrite], got %v", table.Operations)
	}
}

func TestIsUsableAsHeader(t *testing.T) {
	cases := []struct {
		token string
		want  bool
	}{
		{"eyJhbGciOiJSUzI1NiJ9.payload.sig", true},
		{"", false},
		{"bad\ntoken", false},
		{"bad\x00token", false},
		{strings.Repeat("a", 1000), true},
	}
	for _, tc := range cases {
		if got := isUsableAsHeader(tc.token); got != tc.want {
			t.Errorf("isUsableAsHeader(%q) = %v, want %v", tc.token, got, tc.want)
		}
	}
}

func TestParseTableName(t *testing.T) {
	good := []struct{ in, c, s, tbl string }{
		{"a.b.c", "a", "b", "c"},
		{"cat.schema.table", "cat", "schema", "table"},
	}
	for _, tc := range good {
		c, s, tbl, err := parseTableName(tc.in)
		if err != nil {
			t.Errorf("parseTableName(%q): unexpected error: %v", tc.in, err)
			continue
		}
		if c != tc.c || s != tc.s || tbl != tc.tbl {
			t.Errorf("parseTableName(%q) = (%q,%q,%q), want (%q,%q,%q)", tc.in, c, s, tbl, tc.c, tc.s, tc.tbl)
		}
	}

	bad := []string{"", "a", "a.b", "a.b.c.d", ".b.c", "a..c", "a.b."}
	for _, tc := range bad {
		if _, _, _, err := parseTableName(tc); err == nil {
			t.Errorf("parseTableName(%q): want error, got nil", tc)
		}
	}
}

// asTokenError is a test helper to check if err is or wraps a *TokenError.
// TokenError implements Unwrap, so errors.As handles both single- and
// multi-error chains.
func asTokenError(err error, target **TokenError) bool {
	return errors.As(err, target)
}
