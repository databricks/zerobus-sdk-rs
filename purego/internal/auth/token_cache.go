package auth

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"time"
)

// defaultRefreshBuffer is the lead time before a token's expiry at which the
// cache proactively re-mints; 5 minutes is the SDK-wide standard.
const defaultRefreshBuffer = 5 * time.Minute

// fetchedToken is the raw result of a mint: the token string plus its
// server-reported lifetime (nil when the OAuth server omits expires_in).
type fetchedToken struct {
	token     string
	expiresIn *time.Duration
}

// mintReason is passed to the mint callback so it can distinguish a cold start
// from a proactive refresh from caching being off (e.g. for logging or metrics).
type mintReason int

const (
	mintReasonColdMiss      mintReason = iota // no usable entry in cache
	mintReasonRefresh                         // token is within the refresh buffer
	mintReasonCacheDisabled                   // caching is off, so every call mints
	mintReasonDirect                          // minted outside the cache via FetchToken
)

func (r mintReason) String() string {
	switch r {
	case mintReasonColdMiss:
		return "cold_miss"
	case mintReasonRefresh:
		return "refresh"
	case mintReasonCacheDisabled:
		return "cache_disabled"
	case mintReasonDirect:
		return "direct"
	default:
		return "unknown"
	}
}

// tokenKey identifies a cache entry. The client secret is stored as its
// SHA-256 digest so the raw credential is never kept in the map: distinct
// secrets still yield distinct keys (collision resistance), and a rotated
// secret gets a fresh entry.
//
// audience scopes the entry to the token's target audience (the workspace the
// minted token is bound to). Two providers with the same credentials and table
// but different workspaces mint tokens that are not interchangeable, so they
// must not share an entry; without audience in the key the second provider would
// be served the first's workspace-scoped token and rejected on audience mismatch.
type tokenKey struct {
	clientID     string
	secretDigest [sha256.Size]byte
	tableName    string
	audience     string
}

func newTokenKey(clientID, clientSecret, tableName, audience string) tokenKey {
	return tokenKey{
		clientID:     clientID,
		secretDigest: sha256.Sum256([]byte(clientSecret)),
		tableName:    tableName,
		audience:     audience,
	}
}

// cachedToken is one live entry in the cache. refreshAt is precomputed from the
// TTL and the cache's refresh buffer so needsRefresh is a plain timestamp
// comparison; it never sits after expiresAt.
type cachedToken struct {
	value     string
	expiresAt time.Time
	refreshAt time.Time
}

func (c *cachedToken) isExpired() bool {
	return time.Now().After(c.expiresAt)
}

// newCachedToken builds an entry for a token whose TTL started at mintedAt
// (response-receipt time). ttl must be positive.
//
// The effective refresh lead time is clamped to at most half the TTL: with the
// default 5-minute buffer a 10-minute token would otherwise be due for refresh
// the instant it is cached, collapsing to a re-mint on every call. Clamping
// guarantees at least ttl/2 of reuse before proactive refresh kicks in.
func newCachedToken(value string, ttl, refreshBuffer time.Duration, mintedAt time.Time) *cachedToken {
	if maxBuffer := ttl / 2; refreshBuffer > maxBuffer {
		refreshBuffer = maxBuffer
	}
	expiresAt := mintedAt.Add(ttl)
	return &cachedToken{
		value:     value,
		expiresAt: expiresAt,
		refreshAt: expiresAt.Add(-refreshBuffer),
	}
}

// tokenCacheEntry is the per-key slot. Its mutex is held only for short state
// transitions, never across the mint call, so callers single-flight the mint
// yet each waiter can still abandon on its own context. Different keys never
// block each other.
type tokenCacheEntry struct {
	mu       sync.Mutex
	cached   *cachedToken // nil when no valid entry has been stored yet
	inflight *tokenFlight // non-nil while a mint is in progress
	minted   bool         // true after the first mint completes; guards pruning
}

// tokenFlight is a single in-progress mint. The leader records its resolved
// outcome (the same token or error it returns to its own caller) before closing
// done, so every waiter parked on the flight shares that outcome instead of
// re-minting — matching golang.org/x/sync/singleflight and bounding a failing
// token endpoint to one mint per burst rather than N serial attempts.
type tokenFlight struct {
	done  chan struct{} // closed when the mint completes
	token string        // resolved token (empty on error)
	err   error         // resolved error (nil on success)
}

// tokenCache caches OAuth tokens per (clientID, secret, tableName).
//
// It is safe for concurrent use. Construct one with [newTokenCache]; the
// methods do not guard against a nil receiver.
type tokenCache struct {
	mu            sync.Mutex
	entries       map[tokenKey]*tokenCacheEntry
	refreshBuffer time.Duration
	disabled      bool // when true, every getOrFetch mints without caching
}

// CacheOption configures a token cache at construction. See [CacheEnabled] and
// [CacheRefreshBuffer].
type CacheOption func(*tokenCache)

// CacheEnabled toggles token caching. When disabled, every token request mints
// a fresh token instead of consulting the cache. Caching is enabled by default.
func CacheEnabled(enabled bool) CacheOption {
	return func(c *tokenCache) { c.disabled = !enabled }
}

// CacheRefreshBuffer sets the lead time before a token's expiry at which it is
// proactively re-minted; it defaults to 5 minutes. A non-positive value is
// ignored so the default holds.
func CacheRefreshBuffer(d time.Duration) CacheOption {
	return func(c *tokenCache) {
		if d > 0 {
			c.refreshBuffer = d
		}
	}
}

func newTokenCache(opts ...CacheOption) *tokenCache {
	c := &tokenCache{
		entries:       make(map[tokenKey]*tokenCacheEntry),
		refreshBuffer: defaultRefreshBuffer,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// getOrFetch returns a valid token for the given credentials and table, minting
// (via mint, bounded by ctx) only when the cache is empty or within the refresh
// window. A retryable refresh error falls back to the still-valid cached token;
// a non-retryable error propagates.
//
// mint runs at most once per key at a time; other callers wait for it and share
// the result, or return ctx.Err() if their own ctx expires while waiting. If the
// leader's mint fails because the leader's own context was cancelled, a waiter
// whose context is still live re-attempts (and usually mints for itself) rather
// than inheriting a cancellation it never requested.
func (c *tokenCache) getOrFetch(
	ctx context.Context,
	clientID, clientSecret, tableName, audience string,
	mint func(ctx context.Context, reason mintReason) (fetchedToken, error),
) (string, error) {
	if c.disabled {
		fetched, err := mint(ctx, mintReasonCacheDisabled)
		if err != nil {
			return "", err
		}
		return fetched.token, nil
	}

	key := newTokenKey(clientID, clientSecret, tableName, audience)

	for {
		token, err, retry := c.tryGetOrFetch(ctx, key, mint)
		if retry {
			// The leader we parked on failed because its own ctx was cancelled,
			// but ours is still live. Re-attempt rather than inheriting a cancel
			// we never asked for; typically we become the next leader and mint.
			continue
		}
		return token, err
	}
}

// tryGetOrFetch makes one attempt to serve or mint a token for key. It returns
// retry == true only when the caller parked on another goroutine's in-flight
// mint that failed due to that leader's context cancellation while the caller's
// own context is still live; the caller then loops to re-attempt.
func (c *tokenCache) tryGetOrFetch(
	ctx context.Context,
	key tokenKey,
	mint func(ctx context.Context, reason mintReason) (fetchedToken, error),
) (string, error, bool) {
	entry := c.slot(key)

	entry.mu.Lock()

	if entry.cached != nil && !c.needsRefresh(entry.cached) {
		token := entry.cached.value
		entry.mu.Unlock()
		return token, nil, false
	}

	// A mint is already in progress: wait for it (or our own ctx) and share the
	// leader's resolved outcome rather than launching a second mint. This bounds
	// a failing token endpoint to one mint per burst instead of N serial retries.
	if entry.inflight != nil {
		flight := entry.inflight
		entry.mu.Unlock()
		select {
		case <-flight.done:
			// If the leader failed solely because its own caller cancelled its
			// context, don't propagate that cancel to a waiter whose context is
			// still live; signal a re-attempt so this caller can mint for itself.
			//
			// A leader mint that timed out on its own (a slow endpoint tripping a
			// client/transport deadline) is reported retryable, not a cancellation,
			// so it is excluded here: the waiter shares that one outcome instead of
			// each re-minting, which is what single-flight is for.
			if flight.err != nil && isContextError(flight.err) && !isRetryable(flight.err) && ctx.Err() == nil {
				return "", nil, true
			}
			return flight.token, flight.err, false
		case <-ctx.Done():
			return "", ctx.Err(), false
		}
	}

	// We are the leader: claim the mint. The reason is a refresh only when a
	// still-valid token is being re-minted early; an expired entry is a cold
	// miss operationally, not a refresh.
	reason := mintReasonColdMiss
	if entry.cached != nil && !entry.cached.isExpired() {
		reason = mintReasonRefresh
	}
	flight := &tokenFlight{done: make(chan struct{})}
	entry.inflight = flight
	entry.mu.Unlock()

	fetched, err := mint(ctx, reason)

	entry.mu.Lock()
	entry.inflight = nil
	entry.minted = true

	if err != nil {
		token, resErr := "", err
		if entry.cached != nil && !entry.cached.isExpired() && isRetryable(err) {
			// Proactive refresh failed transiently; serve the still-valid token.
			token, resErr = entry.cached.value, nil
		}
		// Publish to waiters: writes before close(done) happen-before <-done. The
		// mutex just brackets the entry-state transition, not the flight publish.
		// A context error published here is what lets a live-ctx waiter re-attempt
		// instead of inheriting our cancellation.
		flight.token, flight.err = token, resErr
		close(flight.done)
		entry.mu.Unlock()
		return token, resErr, false
	}

	token := fetched.token

	// Cache only tokens with a usable TTL. If refresh returned no expires_in,
	// keep any existing still-valid token rather than discarding it.
	if fetched.expiresIn != nil && *fetched.expiresIn > 0 {
		mintedAt := time.Now()
		entry.cached = newCachedToken(token, *fetched.expiresIn, c.refreshBuffer, mintedAt)
	} else {
		keepExisting := entry.cached != nil && !entry.cached.isExpired()
		if !keepExisting {
			entry.cached = nil
		}
	}
	flight.token, flight.err = token, nil
	close(flight.done)
	entry.mu.Unlock()

	return token, nil, false
}

// isContextError reports whether err is (or wraps) a context cancellation or
// deadline. This alone does not say where the cancellation came from: an
// http.Client.Timeout or transport deadline surfaces as a wrapped context error
// just as a caller's own cancellation does. Use [isCallerCancellation] to tell
// the two apart when the request context is in hand.
func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// isCallerCancellation reports whether err is a context error that originated
// from ctx — the caller cancelled or its deadline fired — rather than from the
// request itself. It is the caller's signal to stop: such a failure must not be
// retried or masked by serving a cached token. A context error seen while ctx is
// still live came from the request (a client/transport timeout) and is a
// transient, retryable fault instead.
func isCallerCancellation(ctx context.Context, err error) bool {
	return isContextError(err) && ctx.Err() != nil
}

// invalidate drops the cached token for the given credentials, table, and
// audience. The next getOrFetch call will re-mint from scratch.
func (c *tokenCache) invalidate(clientID, clientSecret, tableName, audience string) {
	key := newTokenKey(clientID, clientSecret, tableName, audience)

	c.mu.Lock()
	entry, ok := c.entries[key]
	c.mu.Unlock()

	if !ok {
		return
	}
	entry.mu.Lock()
	entry.cached = nil
	entry.mu.Unlock()
}

// slot returns the per-key entry, creating it on first access. The outer lock
// is held only for the map lookup/insert, keeping contention off the hot path.
func (c *tokenCache) slot(key tokenKey) *tokenCacheEntry {
	c.mu.Lock()
	defer c.mu.Unlock()

	if e, ok := c.entries[key]; ok {
		return e
	}
	// Sweep expired entries on a miss to prevent unbounded map growth in
	// clients that ingest into many tables over the lifetime of one process.
	c.pruneExpiredLocked()
	e := &tokenCacheEntry{}
	c.entries[key] = e
	return e
}

// pruneExpiredLocked drops cache entries whose token is fully expired and that
// have no mint in progress. Must be called with c.mu held.
//
// Only entries that have completed at least one mint (minted == true) are
// eligible for eviction: a freshly created entry (minted == false) could still
// have a goroutine racing to acquire entry.mu and become its first leader.
// Evicting it would cause a second entry to be created for the same key,
// briefly duplicating a mint. The minted flag closes that window.
func (c *tokenCache) pruneExpiredLocked() {
	for k, e := range c.entries {
		if e.mu.TryLock() {
			// Never evict a mint in flight, and never evict an entry whose first
			// mint has not yet completed — its leader writes back to this entry.
			pruneable := e.minted && e.inflight == nil && (e.cached == nil || e.cached.isExpired())
			e.mu.Unlock()
			if pruneable {
				delete(c.entries, k)
			}
		}
		// A locked entry is mid-transition; leave it alone.
	}
}

func (c *tokenCache) needsRefresh(cached *cachedToken) bool {
	return time.Now().After(cached.refreshAt)
}

// isRetryable reports whether the error tree holds a retryable [retryableError],
// walking both single- and multi-error (errors.Join) unwrap chains. A
// retryableError that reports IsRetryable() == false does not short-circuit the
// walk: a wrapper may deny retry while wrapping a retryable cause, so we keep
// unwrapping until we either find a retryable node or exhaust the tree.
func isRetryable(err error) bool {
	for err != nil {
		if re, ok := err.(retryableError); ok && re.IsRetryable() {
			return true
		}
		switch x := err.(type) {
		case interface{ Unwrap() error }:
			err = x.Unwrap()
		case interface{ Unwrap() []error }:
			for _, e := range x.Unwrap() {
				if isRetryable(e) {
					return true
				}
			}
			return false
		default:
			return false
		}
	}
	return false
}

// retryableError is implemented by errors from the mint path that are safe to
// suppress when a cached token is still valid.
type retryableError interface {
	IsRetryable() bool
}
