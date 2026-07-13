package auth

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// retryErr is a test error that reports itself as retryable.
type retryErr struct{ msg string }

func (e *retryErr) Error() string     { return e.msg }
func (e *retryErr) IsRetryable() bool { return true }

// fatalErr is a test error that is non-retryable.
type fatalErr struct{ msg string }

func (e *fatalErr) Error() string     { return e.msg }
func (e *fatalErr) IsRetryable() bool { return false }

// denyingWrapper reports IsRetryable() == false yet wraps another error: it
// exercises that isRetryable keeps walking past a non-retryable node instead of
// short-circuiting on it.
type denyingWrapper struct{ cause error }

func (e *denyingWrapper) Error() string     { return "denied: " + e.cause.Error() }
func (e *denyingWrapper) IsRetryable() bool { return false }
func (e *denyingWrapper) Unwrap() error     { return e.cause }

func makeMint(token string, ttlSecs int) func(context.Context, mintReason) (fetchedToken, error) {
	return func(_ context.Context, _ mintReason) (fetchedToken, error) {
		ft := fetchedToken{token: token}
		if ttlSecs > 0 {
			d := time.Duration(ttlSecs) * time.Second
			ft.expiresIn = &d
		}
		return ft, nil
	}
}

func getOrFetch(t *testing.T, c *tokenCache, clientID, secret, table string,
	mint func(context.Context, mintReason) (fetchedToken, error),
) string {
	t.Helper()
	tok, err := c.getOrFetch(context.Background(), clientID, secret, table, mint)
	if err != nil {
		t.Fatalf("getOrFetch: %v", err)
	}
	return tok
}

func TestTokenCacheCachesAcrossCalls(t *testing.T) {
	c := newTokenCache()
	var calls atomic.Int64
	mint := func(_ context.Context, _ mintReason) (fetchedToken, error) {
		calls.Add(1)
		return fetchedToken{token: "tok", expiresIn: dur(3600)}, nil
	}

	a := getOrFetch(t, c, "id", "secret", "c.s.t", mint)
	b := getOrFetch(t, c, "id", "secret", "c.s.t", mint)

	if a != "tok" || b != "tok" {
		t.Fatalf("want tok/tok, got %q/%q", a, b)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("want 1 mint, got %d", n)
	}
}

func TestTokenCacheSeparateTablesGetSeparateEntries(t *testing.T) {
	c := newTokenCache()
	var calls atomic.Int64
	mint := func(_ context.Context, _ mintReason) (fetchedToken, error) {
		n := calls.Add(1)
		return fetchedToken{token: "tok" + string(rune('0'+n)), expiresIn: dur(3600)}, nil
	}

	a := getOrFetch(t, c, "id", "secret", "c.s.t1", mint)
	b := getOrFetch(t, c, "id", "secret", "c.s.t2", mint)

	if a == b {
		t.Fatal("different tables must get different tokens")
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("want 2 mints, got %d", n)
	}
}

func TestTokenCacheRotatedSecretGetsFreshEntry(t *testing.T) {
	c := newTokenCache()
	var calls atomic.Int64
	mint := func(_ context.Context, _ mintReason) (fetchedToken, error) {
		calls.Add(1)
		return fetchedToken{token: "tok", expiresIn: dur(3600)}, nil
	}

	getOrFetch(t, c, "id", "secret-v1", "c.s.t", mint)
	getOrFetch(t, c, "id", "secret-v2", "c.s.t", mint)

	if n := calls.Load(); n != 2 {
		t.Fatalf("want 2 mints for different secrets, got %d", n)
	}
}

func TestTokenCacheNoTTLIsNotCached(t *testing.T) {
	c := newTokenCache()
	var calls atomic.Int64
	mint := func(_ context.Context, _ mintReason) (fetchedToken, error) {
		calls.Add(1)
		return fetchedToken{token: "tok"}, nil // no expiresIn
	}

	a := getOrFetch(t, c, "id", "secret", "c.s.t", mint)
	b := getOrFetch(t, c, "id", "secret", "c.s.t", mint)

	if a != "tok" || b != "tok" {
		t.Fatalf("want tok/tok, got %q/%q", a, b)
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("want 2 mints for no-TTL token (not cached), got %d", n)
	}
}

func TestTokenCacheInvalidateForcesMintOnNextCall(t *testing.T) {
	c := newTokenCache()
	var calls atomic.Int64
	mint := func(_ context.Context, _ mintReason) (fetchedToken, error) {
		calls.Add(1)
		return fetchedToken{token: "tok", expiresIn: dur(3600)}, nil
	}

	getOrFetch(t, c, "id", "secret", "c.s.t", mint)
	c.invalidate("id", "secret", "c.s.t")
	getOrFetch(t, c, "id", "secret", "c.s.t", mint)

	if n := calls.Load(); n != 2 {
		t.Fatalf("want 2 mints after invalidate, got %d", n)
	}
}

func TestTokenCacheWithinRefreshBufferRemints(t *testing.T) {
	c := newTokenCache()
	// A cached token past its refresh point is re-minted on the next call, and
	// the fresh long-TTL result is then cached and reused.
	seedRefreshable(c, "id", "secret", "c.s.t", "stale")

	var calls atomic.Int64
	mint := func(_ context.Context, _ mintReason) (fetchedToken, error) {
		calls.Add(1)
		return fetchedToken{token: "fresh", expiresIn: dur(3600)}, nil
	}

	a := getOrFetch(t, c, "id", "secret", "c.s.t", mint) // refresh due -> re-mints
	b := getOrFetch(t, c, "id", "secret", "c.s.t", mint) // fresh token cached -> hit

	if a != "fresh" || b != "fresh" {
		t.Fatalf("want fresh/fresh, got %q/%q", a, b)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("want 1 mint (refresh, then cache hit), got %d", n)
	}
}

func TestTokenCacheRetryableRefreshFailureFallsBack(t *testing.T) {
	c := newTokenCache()
	// Seed a token that is valid but due for proactive refresh.
	seedRefreshable(c, "id", "secret", "c.s.t", "valid")

	// Retryable refresh error: should fall back to the still-valid cached token.
	tok, err := c.getOrFetch(context.Background(), "id", "secret", "c.s.t",
		func(_ context.Context, _ mintReason) (fetchedToken, error) {
			return fetchedToken{}, &retryErr{"transient"}
		},
	)
	if err != nil {
		t.Fatalf("want fallback to cached token, got error: %v", err)
	}
	if tok != "valid" {
		t.Fatalf("want %q, got %q", "valid", tok)
	}
}

func TestTokenCacheNonRetryableRefreshErrorPropagates(t *testing.T) {
	c := newTokenCache()
	seedRefreshable(c, "id", "secret", "c.s.t", "valid")

	_, err := c.getOrFetch(context.Background(), "id", "secret", "c.s.t",
		func(_ context.Context, _ mintReason) (fetchedToken, error) {
			return fetchedToken{}, &fatalErr{"revoked"}
		},
	)
	if err == nil {
		t.Fatal("want error for non-retryable refresh failure, got nil")
	}
	var fe *fatalErr
	if !errors.As(err, &fe) {
		t.Fatalf("want fatalErr, got %T: %v", err, err)
	}
}

func TestTokenCacheColdMissFailureLeavesNoEntry(t *testing.T) {
	c := newTokenCache()

	// A failed cold-miss mint must not cache anything: the error propagates and a
	// retry re-mints rather than serving a phantom token.
	_, err := c.getOrFetch(context.Background(), "id", "secret", "c.s.t",
		func(_ context.Context, _ mintReason) (fetchedToken, error) {
			return fetchedToken{}, &fatalErr{"boom"}
		},
	)
	if err == nil {
		t.Fatal("want error from failed cold-miss mint, got nil")
	}

	var calls atomic.Int64
	tok := getOrFetch(t, c, "id", "secret", "c.s.t",
		func(_ context.Context, _ mintReason) (fetchedToken, error) {
			calls.Add(1)
			return fetchedToken{token: "tok", expiresIn: dur(3600)}, nil
		},
	)
	if tok != "tok" || calls.Load() != 1 {
		t.Fatalf("want a fresh mint after cold-miss failure, got %q with %d calls", tok, calls.Load())
	}
}

func TestTokenCacheNoTTLResponseKeepsExistingCachedToken(t *testing.T) {
	c := newTokenCache()
	// Seed a token that is due for refresh (refreshAt already in the past).
	seedRefreshable(c, "id", "secret", "c.s.t", "valid")

	// A refresh returns a token with no TTL; caller gets the fresh token but the
	// existing valid cached token is retained (Rust parity).
	fresh := getOrFetch(t, c, "id", "secret", "c.s.t",
		func(_ context.Context, _ mintReason) (fetchedToken, error) {
			return fetchedToken{token: "nottl"}, nil
		},
	)
	if fresh != "nottl" {
		t.Fatalf("want %q, got %q", "nottl", fresh)
	}

	// A subsequent retryable refresh failure should still fall back to the
	// original valid cached token, proving it was retained.
	tok, err := c.getOrFetch(context.Background(), "id", "secret", "c.s.t",
		func(_ context.Context, _ mintReason) (fetchedToken, error) {
			return fetchedToken{}, &retryErr{"blip"}
		},
	)
	if err != nil {
		t.Fatalf("want fallback to retained cached token, got error: %v", err)
	}
	if tok != "valid" {
		t.Fatalf("want retained cached token %q, got %q", "valid", tok)
	}
}

func TestTokenCacheSingleFlightMintOnce(t *testing.T) {
	c := newTokenCache()
	var calls atomic.Int64

	const goroutines = 16
	var wg sync.WaitGroup
	wg.Add(goroutines)
	results := make([]string, goroutines)

	for i := range goroutines {
		go func(i int) {
			defer wg.Done()
			tok, err := c.getOrFetch(context.Background(), "id", "secret", "c.s.t",
				func(_ context.Context, _ mintReason) (fetchedToken, error) {
					calls.Add(1)
					time.Sleep(20 * time.Millisecond) // hold lock so others queue up
					return fetchedToken{token: "tok", expiresIn: dur(3600)}, nil
				},
			)
			if err != nil {
				t.Errorf("goroutine %d: %v", i, err)
				return
			}
			results[i] = tok
		}(i)
	}
	wg.Wait()

	for i, tok := range results {
		if tok != "tok" {
			t.Errorf("goroutine %d: want %q, got %q", i, "tok", tok)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("single-flight: want 1 mint, got %d", n)
	}
}

func TestTokenCacheConcurrentMintFailureSharesError(t *testing.T) {
	c := newTokenCache()
	var calls atomic.Int64

	// The leader deterministically claims the in-flight slot and blocks until
	// released, so every waiter is guaranteed to arrive while the mint is in
	// flight. While inflight is held no waiter can start a second mint, so this
	// exercises single-flight without racing a wall-clock sleep.
	leaderMinting := make(chan struct{})
	release := make(chan struct{})

	const waiters = 15
	var wg sync.WaitGroup
	errs := make([]error, waiters+1)

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, errs[0] = c.getOrFetch(context.Background(), "id", "secret", "c.s.t",
			func(_ context.Context, _ mintReason) (fetchedToken, error) {
				calls.Add(1)
				close(leaderMinting)
				<-release
				return fetchedToken{}, &fatalErr{"boom"}
			},
		)
	}()

	<-leaderMinting // leader holds the flight; inflight stays set until release

	wg.Add(waiters)
	for i := range waiters {
		go func(i int) {
			defer wg.Done()
			_, errs[i+1] = c.getOrFetch(context.Background(), "id", "secret", "c.s.t",
				func(_ context.Context, _ mintReason) (fetchedToken, error) {
					calls.Add(1) // a waiter reaching here means single-flight broke
					return fetchedToken{}, &fatalErr{"boom"}
				},
			)
		}(i)
	}

	// Let the waiters reach getOrFetch and park on the leader's in-flight mint.
	// The leader has no deadline — it blocks on release — so this only needs to
	// outlast goroutine scheduling, not race a wall clock as the old sleep did.
	// Any waiter that parks before release shares the leader's failure and never
	// mints; the calls==1 assertion below catches it if one slips through.
	time.Sleep(100 * time.Millisecond)
	close(release) // fail the leader's mint; parked waiters share its error
	wg.Wait()

	// One mint total — the leader's. Every waiter shared its failure.
	if n := calls.Load(); n != 1 {
		t.Fatalf("concurrent failure should mint once, got %d mints", n)
	}
	for i, err := range errs {
		var fe *fatalErr
		if !errors.As(err, &fe) {
			t.Errorf("goroutine %d: want shared fatalErr, got %T: %v", i, err, err)
		}
	}
}

func TestTokenCacheContextDeadlineRespectedDuringMint(t *testing.T) {
	c := newTokenCache()
	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)

	// Leader holds the mint open until release is closed.
	go func() {
		_, _ = c.getOrFetch(context.Background(), "id", "s", "c.s.t",
			func(_ context.Context, _ mintReason) (fetchedToken, error) {
				close(started)
				<-release
				return fetchedToken{token: "tok", expiresIn: dur(3600)}, nil
			},
		)
	}()
	<-started

	// A second caller with a short deadline must not block on the leader's mint.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := c.getOrFetch(ctx, "id", "s", "c.s.t", makeMint("tok2", 3600))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 1*time.Second {
		t.Fatalf("waiter blocked %v on in-progress mint, want ~100ms", elapsed)
	}
}

// TestTokenCacheInvalidateDuringInFlightMint runs invalidate concurrently with
// an in-flight mint on the same key. The leader's fresh token is what should
// win: invalidate clears cached mid-mint, and the leader then repopulates it.
func TestTokenCacheInvalidateDuringInFlightMint(t *testing.T) {
	c := newTokenCache()
	// Seed so invalidate has an entry to clear.
	seedRefreshable(c, "id", "secret", "c.s.t", "old")

	mintStarted := make(chan struct{})
	releaseMint := make(chan struct{})

	var out string
	var err error
	done := make(chan struct{})
	go func() {
		defer close(done)
		out, err = c.getOrFetch(context.Background(), "id", "secret", "c.s.t",
			func(_ context.Context, _ mintReason) (fetchedToken, error) {
				close(mintStarted)
				<-releaseMint
				return fetchedToken{token: "fresh", expiresIn: dur(3600)}, nil
			},
		)
	}()

	<-mintStarted
	c.invalidate("id", "secret", "c.s.t") // clears cached mid-mint
	close(releaseMint)
	<-done

	if err != nil || out != "fresh" {
		t.Fatalf("want fresh/nil, got %q/%v", out, err)
	}
	// Leader's fresh token should be the one now cached.
	entry := c.slot(newTokenKey("id", "secret", "c.s.t"))
	entry.mu.Lock()
	cached := entry.cached
	entry.mu.Unlock()
	if cached == nil || cached.value != "fresh" {
		t.Fatalf("want fresh cached after leader completes, got %+v", cached)
	}
}

func TestTokenCacheJoinedRetryableErrorFallsBack(t *testing.T) {
	c := newTokenCache()
	seedRefreshable(c, "id", "secret", "c.s.t", "valid")

	// A retryable error buried inside errors.Join must still be detected so the
	// cache falls back to the still-valid token.
	tok, err := c.getOrFetch(context.Background(), "id", "secret", "c.s.t",
		func(_ context.Context, _ mintReason) (fetchedToken, error) {
			joined := errors.Join(errors.New("context note"), &retryErr{"transient"})
			return fetchedToken{}, joined
		},
	)
	if err != nil {
		t.Fatalf("want fallback to cached token, got error: %v", err)
	}
	if tok != "valid" {
		t.Fatalf("want %q, got %q", "valid", tok)
	}
}

func TestTokenCacheRetryableCauseUnderNonRetryableWrapperFallsBack(t *testing.T) {
	c := newTokenCache()
	seedRefreshable(c, "id", "secret", "c.s.t", "valid")

	// The outermost error reports IsRetryable() == false but unwraps a retryable
	// cause. isRetryable must keep walking and detect the cause, so the cache
	// falls back to the still-valid token rather than propagating the failure.
	tok, err := c.getOrFetch(context.Background(), "id", "secret", "c.s.t",
		func(_ context.Context, _ mintReason) (fetchedToken, error) {
			return fetchedToken{}, &denyingWrapper{cause: &retryErr{"transient"}}
		},
	)
	if err != nil {
		t.Fatalf("want fallback to cached token, got error: %v", err)
	}
	if tok != "valid" {
		t.Fatalf("want %q, got %q", "valid", tok)
	}
}

// TestTokenCacheWaiterRemintsWhenLeaderContextCancelled verifies that a waiter
// with a live context does not inherit the leader's context cancellation: it
// re-attempts and mints for itself instead.
func TestTokenCacheWaiterRemintsWhenLeaderContextCancelled(t *testing.T) {
	c := newTokenCache()

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	mintStarted := make(chan struct{})
	var mints atomic.Int64

	// Leader: parks in mint until its own context is cancelled, then returns that
	// cancellation as its error (as an OAuth mint bounded by ctx would).
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		_, _ = c.getOrFetch(leaderCtx, "id", "secret", "c.s.t",
			func(ctx context.Context, _ mintReason) (fetchedToken, error) {
				mints.Add(1)
				close(mintStarted)
				<-ctx.Done()
				return fetchedToken{}, ctx.Err()
			},
		)
	}()

	<-mintStarted // leader holds the in-flight slot

	// Waiter with a healthy context parks on the leader's flight.
	waiterResult := make(chan string, 1)
	waiterErr := make(chan error, 1)
	go func() {
		tok, err := c.getOrFetch(context.Background(), "id", "secret", "c.s.t",
			func(_ context.Context, _ mintReason) (fetchedToken, error) {
				mints.Add(1)
				return fetchedToken{token: "waiter-tok", expiresIn: dur(3600)}, nil
			},
		)
		waiterErr <- err
		waiterResult <- tok
	}()

	// Give the waiter time to park before cancelling the leader.
	time.Sleep(50 * time.Millisecond)
	cancelLeader() // leader's mint fails with context.Canceled

	<-leaderDone
	if err := <-waiterErr; err != nil {
		t.Fatalf("waiter with live ctx should not inherit leader cancel, got %v", err)
	}
	if tok := <-waiterResult; tok != "waiter-tok" {
		t.Fatalf("waiter should have re-minted its own token, got %q", tok)
	}
	// Leader minted once (and was cancelled); waiter re-minted once.
	if n := mints.Load(); n != 2 {
		t.Fatalf("want 2 mints (leader cancelled + waiter re-mint), got %d", n)
	}
}

func TestTokenCacheDisabledAlwaysMints(t *testing.T) {
	c := newTokenCache(CacheEnabled(false))
	var calls atomic.Int64
	reasons := make(chan mintReason, 2)
	mint := func(_ context.Context, r mintReason) (fetchedToken, error) {
		calls.Add(1)
		reasons <- r
		return fetchedToken{token: "tok", expiresIn: dur(3600)}, nil
	}

	getOrFetch(t, c, "id", "secret", "c.s.t", mint)
	getOrFetch(t, c, "id", "secret", "c.s.t", mint)

	if n := calls.Load(); n != 2 {
		t.Fatalf("want 2 mints with caching disabled, got %d", n)
	}
	close(reasons)
	for r := range reasons {
		if r != mintReasonCacheDisabled {
			t.Fatalf("want mintReasonCacheDisabled, got %v", r)
		}
	}
}

func TestTokenCacheCustomRefreshBuffer(t *testing.T) {
	// A custom 20-minute buffer against a 1-hour token places the refresh point
	// ~40 minutes out (20m < ttl/2, so no clamping) — proving the option took
	// effect. Compare against the default 5-minute buffer, which would place it
	// ~55 minutes out.
	const buffer = 20 * time.Minute
	c := newTokenCache(CacheRefreshBuffer(buffer))

	before := time.Now()
	getOrFetch(t, c, "id", "secret", "c.s.t", makeMint("tok", 3600))
	after := time.Now()

	entry := c.slot(newTokenKey("id", "secret", "c.s.t"))
	entry.mu.Lock()
	cached := entry.cached
	entry.mu.Unlock()
	if cached == nil {
		t.Fatal("token should be cached")
	}

	// refreshAt should sit buffer before expiresAt, i.e. ttl-buffer after mint.
	wantMin := before.Add(time.Hour - buffer)
	wantMax := after.Add(time.Hour - buffer)
	if cached.refreshAt.Before(wantMin) || cached.refreshAt.After(wantMax) {
		t.Fatalf("refreshAt %v outside expected window [%v, %v] for 20m buffer",
			cached.refreshAt, wantMin, wantMax)
	}
}

func TestNewCachedToken(t *testing.T) {
	now := time.Now()

	// Normal case: buffer smaller than ttl/2 is applied verbatim.
	ct := newCachedToken("v", time.Hour, 5*time.Minute, now)
	if !ct.expiresAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("expiresAt = %v, want +1h", ct.expiresAt)
	}
	if !ct.refreshAt.Equal(now.Add(time.Hour - 5*time.Minute)) {
		t.Fatalf("refreshAt = %v, want +55m", ct.refreshAt)
	}

	// Buffer larger than ttl/2 is clamped so at least half the TTL is reusable.
	ct = newCachedToken("v", 10*time.Minute, time.Hour, now)
	if !ct.refreshAt.Equal(now.Add(5 * time.Minute)) {
		t.Fatalf("clamped refreshAt = %v, want +5m (ttl/2)", ct.refreshAt)
	}
	if !ct.refreshAt.After(now) {
		t.Fatal("clamped refreshAt must stay in the future so the token is reusable")
	}
}

func TestTokenCacheNonPositiveRefreshBufferKeepsDefault(t *testing.T) {
	c := newTokenCache(CacheRefreshBuffer(-1))
	if c.refreshBuffer != defaultRefreshBuffer {
		t.Fatalf("non-positive buffer should be ignored, got %v", c.refreshBuffer)
	}
}

// dur returns a pointer to a Duration of d seconds, for test readability.
func dur(secs int) *time.Duration {
	d := time.Duration(secs) * time.Second
	return &d
}

// seedRefreshable directly installs a cached token that is still valid but past
// its refresh point (refreshAt in the past, expiresAt in the future). This is
// the "due for proactive refresh yet safe to fall back to" state; seeding it
// explicitly avoids depending on TTL/buffer arithmetic that clamping changes.
func seedRefreshable(c *tokenCache, clientID, secret, table, value string) {
	key := newTokenKey(clientID, secret, table)
	entry := c.slot(key)
	now := time.Now()
	entry.mu.Lock()
	entry.cached = &cachedToken{
		value:     value,
		expiresAt: now.Add(time.Hour), // still valid
		refreshAt: now.Add(-time.Second),
	}
	entry.mu.Unlock()
}
