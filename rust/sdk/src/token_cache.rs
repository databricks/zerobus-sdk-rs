//! Per-table OAuth token cache for the default OAuth authentication path.
//!
//! Unity Catalog sets each token's lifetime (currently one hour), while a single
//! stream lives at most ~15 minutes. Without caching, every stream creation (and
//! every recovery) mints a fresh token, putting the Unity Catalog token endpoint
//! under unnecessary load when a client churns through many streams. The cache
//! does not assume a fixed lifetime — it serves a token until it nears the
//! `expires_in` the server reported.
//!
//! [`TokenCache`] caches one token per `(client_id, secret, table_name)` key on
//! the [`ZerobusSdk`](crate::ZerobusSdk) instance and serves it until it nears
//! expiry, refreshing lazily on access. Tokens are downscoped to a single table
//! (the authorization details embed the catalog/schema/table), so the table
//! name is part of the cache key.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::{debug, warn};

use crate::default_token_factory::{FetchedToken, MintReason};
use crate::{ZerobusError, ZerobusResult};

/// Default lead time before expiry at which a cached token is refreshed.
pub(crate) const DEFAULT_REFRESH_BUFFER: Duration = Duration::from_secs(300);

/// Post-failure backoff bounds, as fractions of the refresh window
/// (`refresh_buffer`) so they scale with it rather than being a fixed value.
/// `MAX_BACKOFF_WINDOW_FRACTION` gives the normal backoff,
/// `MIN_BACKOFF_WINDOW_FRACTION` a near-expiry floor that keeps the `remaining/2`
/// shrink from converging to zero.
const MAX_BACKOFF_WINDOW_FRACTION: u32 = 60;
const MIN_BACKOFF_WINDOW_FRACTION: u32 = 300;

/// A cached token and the instant at which it expires.
struct CachedToken {
    value: String,
    expires_at: Instant,
    /// Defer the next proactive refresh until this instant, set after a refresh
    /// fell back to this token (a failed mint or a dead-on-arrival token). Always
    /// `<= expires_at`, so it never serves an expired token.
    refresh_retry_at: Option<Instant>,
}

impl CachedToken {
    fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }

    /// Whether the token is inside its post-fallback backoff window, during which
    /// the next proactive refresh is deferred.
    fn in_backoff_window(&self) -> bool {
        self.refresh_retry_at
            .is_some_and(|retry_at| Instant::now() < retry_at)
    }

    /// Arms the post-fallback backoff so a burst of callers reuses this cached
    /// token instead of each re-attempting a refresh that just fell back. The delay
    /// is half the remaining validity, clamped to the window-scaled
    /// `[min_backoff, max_backoff]` and capped at expiry.
    fn arm_refresh_backoff(&mut self, refresh_buffer: Duration) {
        let now = Instant::now();
        let remaining = self.expires_at.saturating_duration_since(now);
        let max_backoff = refresh_buffer / MAX_BACKOFF_WINDOW_FRACTION;
        let min_backoff = refresh_buffer / MIN_BACKOFF_WINDOW_FRACTION;
        let backoff = (remaining / 2).clamp(min_backoff, max_backoff);
        let retry_at = now
            .checked_add(backoff)
            .unwrap_or(self.expires_at)
            .min(self.expires_at);
        self.refresh_retry_at = Some(retry_at);
    }
}

/// Identifies a cache entry. The client secret is keyed by its SHA-256 digest,
/// not plaintext: the digest is collision-resistant (distinct secrets cannot in
/// practice share a token) and keeps the raw secret out of the cache map. A
/// rotated secret yields a different digest, hence a fresh entry.
#[derive(Clone, PartialEq, Eq, Hash)]
struct TokenKey {
    client_id: String,
    secret_digest: [u8; 32],
    table_name: String,
}

impl TokenKey {
    fn new(client_id: &str, client_secret: &str, table_name: &str) -> Self {
        let secret_digest = Sha256::digest(client_secret.as_bytes()).into();
        Self {
            client_id: client_id.to_string(),
            secret_digest,
            table_name: table_name.to_string(),
        }
    }
}

/// Per-entry slot. Each key has its own mutex so that a cold-cache burst of
/// concurrent stream creations for the same table mints a single token
/// (single-flight) while creations for different tables never block each other.
type Slot = Arc<Mutex<Option<CachedToken>>>;

/// Caches OAuth tokens per table for the lifetime of a [`ZerobusSdk`].
///
/// Safe for concurrent use across streams created from the same SDK instance.
pub(crate) struct TokenCache {
    entries: Mutex<HashMap<TokenKey, Slot>>,
    refresh_buffer: Duration,
    enabled: bool,
}

impl TokenCache {
    pub(crate) fn new(enabled: bool, refresh_buffer: Duration) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            refresh_buffer,
            enabled,
        }
    }

    /// Returns a valid token, leaving a proactive refresh unbounded. Used where
    /// there is no stream-setup budget to bound by, such as the standalone
    /// `OAuthHeadersProvider::new`. Callers with a setup deadline use
    /// [`get_or_fetch_within`](Self::get_or_fetch_within) instead.
    pub(crate) async fn get_or_fetch<F, Fut>(
        &self,
        client_id: &str,
        client_secret: &str,
        table_name: &str,
        fetch: F,
    ) -> ZerobusResult<String>
    where
        F: FnOnce(MintReason) -> Fut,
        Fut: std::future::Future<Output = ZerobusResult<FetchedToken>>,
    {
        self.get_or_fetch_bounded(client_id, client_secret, table_name, None, fetch)
            .await
    }

    /// Returns a valid token, bounding a proactive refresh at `refresh_timeout` so a
    /// stall surfaces as a retryable error and the still-valid cached token is served
    /// before the outer setup deadline. The bound is skipped when too little of the
    /// token's life remains to fall back on, and on a cold miss.
    pub(crate) async fn get_or_fetch_within<F, Fut>(
        &self,
        client_id: &str,
        client_secret: &str,
        table_name: &str,
        refresh_timeout: Duration,
        fetch: F,
    ) -> ZerobusResult<String>
    where
        F: FnOnce(MintReason) -> Fut,
        Fut: std::future::Future<Output = ZerobusResult<FetchedToken>>,
    {
        self.get_or_fetch_bounded(
            client_id,
            client_secret,
            table_name,
            Some(refresh_timeout),
            fetch,
        )
        .await
    }

    /// Shared implementation. `refresh_timeout` bounds a proactive-refresh mint
    /// when `Some`; `None` leaves it unbounded.
    async fn get_or_fetch_bounded<F, Fut>(
        &self,
        client_id: &str,
        client_secret: &str,
        table_name: &str,
        refresh_timeout: Option<Duration>,
        fetch: F,
    ) -> ZerobusResult<String>
    where
        F: FnOnce(MintReason) -> Fut,
        Fut: std::future::Future<Output = ZerobusResult<FetchedToken>>,
    {
        if !self.enabled {
            return fetch(MintReason::CacheDisabled)
                .await
                .map(|fetched| fetched.token);
        }

        let key = TokenKey::new(client_id, client_secret, table_name);

        let slot = {
            let mut entries = self.entries.lock().await;
            // Sweep only on a miss, keeping the cost off the hot lookup path.
            if !entries.contains_key(&key) {
                Self::prune_expired(&mut entries);
            }
            Arc::clone(entries.entry(key).or_default())
        };

        // Hold the per-entry lock across the fetch so concurrent callers for the
        // same key reuse a single mint instead of stampeding the token endpoint.
        let mut guard = slot.lock().await;

        if let Some(cached) = guard.as_ref() {
            if !self.needs_refresh(cached) {
                // Distinguish a healthy hit from serving a token whose refresh is in
                // post-fallback backoff, since they are operationally different states.
                if cached.in_backoff_window() {
                    debug!(table = %table_name, "serving cached token; proactive refresh in backoff after a recent fallback");
                } else {
                    debug!(table = %table_name, "token cache hit, reusing cached token");
                }
                return Ok(cached.value.clone());
            }
        }

        // Anchor the lifetime to request start, not response arrival, so a slow
        // response can't make the token look valid longer than the issuer allows.
        let fetch_started_at = Instant::now();

        // The cached token's remaining validity (`None` if there is no usable
        // token), measured once from the request start so the mint reason and the
        // refresh bound below share a single reading.
        let cached_remaining = guard.as_ref().map(|cached| {
            cached
                .expires_at
                .saturating_duration_since(fetch_started_at)
        });

        // A still-valid token in the refresh window is a proactive refresh; an empty
        // or already-expired slot is a cold miss (`MintReason::ColdMiss`). Surfaced
        // on the mint log.
        let reason = if cached_remaining.is_some_and(|remaining| !remaining.is_zero()) {
            MintReason::Refresh
        } else {
            MintReason::ColdMiss
        };

        let fetch_result = match (reason, refresh_timeout) {
            // Bound a proactive refresh only when the cached token outlasts the
            // budget: a stall then surfaces as a retryable error and the fallback
            // below serves the still-valid token before the setup deadline. Otherwise
            // there's nothing to fall back on, so run unbounded like a cold miss.
            (MintReason::Refresh, Some(budget))
                if cached_remaining.is_some_and(|remaining| remaining > budget) =>
            {
                match tokio::time::timeout(budget, fetch(reason)).await {
                    Ok(result) => result,
                    Err(_) => Err(ZerobusError::TokenFetchError(
                        "proactive token refresh timed out".to_string(),
                    )),
                }
            }
            _ => fetch(reason).await,
        };

        let fetched = match fetch_result {
            Ok(fetched) => fetched,
            Err(err) => {
                // On any refresh error, serve the still-valid cached token if we have
                // one, arming the backoff; otherwise surface the error.
                if let Some(value) =
                    Self::serve_valid_cached_fallback(&mut guard, self.refresh_buffer)
                {
                    warn!(table = %table_name, error = %err, "token refresh failed; serving still-valid cached token");
                    return Ok(value);
                }
                return Err(err);
            }
        };

        // Cache only tokens with a usable TTL. `checked_add` also drops an absurd
        // `expires_in` that would overflow the clock instead of panicking.
        let expires_at = fetched
            .expires_in
            .and_then(|ttl| fetch_started_at.checked_add(ttl));

        // A token already past its (start-anchored) expiry is dead on arrival: serve
        // an older still-valid cached token if there is one and arm the backoff,
        // otherwise surface a retryable error.
        if expires_at.is_some_and(|deadline| deadline <= Instant::now()) {
            if let Some(value) = Self::serve_valid_cached_fallback(&mut guard, self.refresh_buffer)
            {
                warn!(table = %table_name, "fetched OAuth token expired on arrival; serving still-valid cached token");
                return Ok(value);
            }
            return Err(ZerobusError::TokenFetchError(
                "fetched OAuth token expired before arrival".to_string(),
            ));
        }

        match expires_at {
            Some(expires_at) => {
                *guard = Some(CachedToken {
                    value: fetched.token.clone(),
                    expires_at,
                    refresh_retry_at: None,
                });
            }
            None => {
                // No usable TTL: keep an existing still-valid token rather than
                // discarding it.
                let keep_existing = guard.as_ref().is_some_and(|cached| !cached.is_expired());
                if !keep_existing {
                    *guard = None;
                }
            }
        }

        Ok(fetched.token)
    }

    /// Drops any cached token for the given credentials and table so the next
    /// fetch re-mints. Called when the server rejects the token (e.g. it was
    /// revoked at the IdP), so the re-mint re-checks grants at UC. No-op when
    /// caching is disabled or no entry exists.
    pub(crate) async fn invalidate(&self, client_id: &str, client_secret: &str, table_name: &str) {
        if !self.enabled {
            return;
        }
        let key = TokenKey::new(client_id, client_secret, table_name);
        if self.entries.lock().await.remove(&key).is_some() {
            debug!(table = %table_name, "token cache entry invalidated after auth rejection");
        }
    }

    fn needs_refresh(&self, cached: &CachedToken) -> bool {
        // Within a post-fallback backoff window, don't refresh yet (see
        // `arm_refresh_backoff`).
        if cached.in_backoff_window() {
            return false;
        }
        // `checked_add` avoids a panic on an absurd refresh buffer (e.g.
        // `Duration::MAX`); an overflowing deadline means "always refresh".
        match Instant::now().checked_add(self.refresh_buffer) {
            Some(deadline) => deadline >= cached.expires_at,
            None => true,
        }
    }

    /// If a still-valid token is cached, arm its backoff and return it. This is the
    /// fallback when a proactive refresh can't produce a usable token; `None` when
    /// there is none to fall back to.
    fn serve_valid_cached_fallback(
        slot: &mut Option<CachedToken>,
        refresh_buffer: Duration,
    ) -> Option<String> {
        let cached = slot.as_mut()?;
        if cached.is_expired() {
            return None;
        }
        cached.arm_refresh_backoff(refresh_buffer);
        Some(cached.value.clone())
    }

    /// Drops entries whose token has fully expired. Locked (in-flight) entries,
    /// still-valid tokens, and empty slots are kept — keeping empty slots is
    /// what preserves single-flight for a key being minted concurrently.
    fn prune_expired(entries: &mut HashMap<TokenKey, Slot>) {
        entries.retain(|_, slot| match slot.try_lock() {
            Ok(guard) => match guard.as_ref() {
                Some(cached) => !cached.is_expired(),
                None => true,
            },
            Err(_) => true,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn fetched(token: &str, ttl_secs: Option<u64>) -> FetchedToken {
        FetchedToken {
            token: token.to_string(),
            expires_in: ttl_secs.map(Duration::from_secs),
        }
    }

    #[tokio::test]
    async fn caches_token_across_calls() {
        let cache = TokenCache::new(true, Duration::from_secs(60));
        let calls = AtomicUsize::new(0);

        let make = |_reason| async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(fetched("tok", Some(3600)))
        };

        let a = cache
            .get_or_fetch("id", "secret", "c.s.t", make)
            .await
            .unwrap();
        let b = cache
            .get_or_fetch("id", "secret", "c.s.t", make)
            .await
            .unwrap();

        assert_eq!(a, "tok");
        assert_eq!(b, "tok");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "second call should hit cache"
        );
    }

    #[tokio::test]
    async fn refetches_when_within_refresh_buffer() {
        // TTL (1s) is smaller than the refresh buffer (60s), so the token is
        // always considered due for refresh and every call mints anew.
        let cache = TokenCache::new(true, Duration::from_secs(60));
        let calls = AtomicUsize::new(0);

        let make = |_reason| async {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            Ok(fetched(&format!("tok{n}"), Some(1)))
        };

        let a = cache
            .get_or_fetch("id", "secret", "c.s.t", make)
            .await
            .unwrap();
        let b = cache
            .get_or_fetch("id", "secret", "c.s.t", make)
            .await
            .unwrap();

        assert_eq!(a, "tok0");
        assert_eq!(b, "tok1");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn refresh_installs_new_expiry() {
        // A proactive refresh must re-stabilize the cache: once it returns a
        // token with a healthy TTL, the following call should hit rather than
        // refresh again. The first mint uses a within-buffer TTL (30s < 60s
        // buffer) to force one refresh; later mints return a healthy TTL.
        let cache = TokenCache::new(true, Duration::from_secs(60));
        let calls = AtomicUsize::new(0);

        let make = |_reason| async {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            let ttl = if n == 0 { 30 } else { 3600 };
            Ok(fetched(&format!("tok{n}"), Some(ttl)))
        };

        // Call 1 mints tok0 (within-buffer, immediately due for refresh).
        let a = cache
            .get_or_fetch("id", "secret", "c.s.t", make)
            .await
            .unwrap();
        // Call 2 refreshes to tok1 with a healthy TTL.
        let b = cache
            .get_or_fetch("id", "secret", "c.s.t", make)
            .await
            .unwrap();
        // Call 3 must be a cache hit on tok1: no further mint.
        let c = cache
            .get_or_fetch("id", "secret", "c.s.t", make)
            .await
            .unwrap();

        assert_eq!(a, "tok0");
        assert_eq!(b, "tok1");
        assert_eq!(c, "tok1", "the refreshed token should be served from cache");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "refresh should install a new expiry so the third call hits cache"
        );
    }

    #[tokio::test]
    async fn separate_tables_get_separate_entries() {
        let cache = TokenCache::new(true, Duration::from_secs(60));
        let calls = AtomicUsize::new(0);

        let make = |_reason| async {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            Ok(fetched(&format!("tok{n}"), Some(3600)))
        };

        let a = cache
            .get_or_fetch("id", "secret", "c.s.t1", make)
            .await
            .unwrap();
        let b = cache
            .get_or_fetch("id", "secret", "c.s.t2", make)
            .await
            .unwrap();

        assert_ne!(a, b);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn rotated_secret_gets_new_entry() {
        let cache = TokenCache::new(true, Duration::from_secs(60));
        let calls = AtomicUsize::new(0);

        let make = |_reason| async {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            Ok(fetched(&format!("tok{n}"), Some(3600)))
        };

        cache
            .get_or_fetch("id", "secret-v1", "c.s.t", make)
            .await
            .unwrap();
        cache
            .get_or_fetch("id", "secret-v2", "c.s.t", make)
            .await
            .unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn token_without_ttl_is_not_cached() {
        let cache = TokenCache::new(true, Duration::from_secs(60));
        let calls = AtomicUsize::new(0);

        let make = |_reason| async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(fetched("tok", None))
        };

        cache
            .get_or_fetch("id", "secret", "c.s.t", make)
            .await
            .unwrap();
        cache
            .get_or_fetch("id", "secret", "c.s.t", make)
            .await
            .unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 2, "no TTL means no caching");
    }

    #[tokio::test]
    async fn invalidate_forces_remint_on_next_call() {
        let cache = TokenCache::new(true, Duration::from_secs(60));
        let calls = AtomicUsize::new(0);

        let make = |_reason| async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(fetched("tok", Some(3600)))
        };

        cache
            .get_or_fetch("id", "secret", "c.s.t", make)
            .await
            .unwrap();
        // Without invalidation a second call would hit the cache; invalidating
        // the entry forces the next call to re-mint.
        cache.invalidate("id", "secret", "c.s.t").await;
        cache
            .get_or_fetch("id", "secret", "c.s.t", make)
            .await
            .unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn invalidate_affects_only_its_own_key() {
        let cache = TokenCache::new(true, Duration::from_secs(60));
        let calls = AtomicUsize::new(0);

        let make = |_reason| async {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            Ok(fetched(&format!("tok{n}"), Some(3600)))
        };

        // Seed two different tables (tok0 and tok1).
        cache
            .get_or_fetch("id", "secret", "c.s.t1", make)
            .await
            .unwrap();
        cache
            .get_or_fetch("id", "secret", "c.s.t2", make)
            .await
            .unwrap();

        // Invalidating t1 must not disturb t2.
        cache.invalidate("id", "secret", "c.s.t1").await;

        // t1 re-mints (tok2); t2 still hits its original cached token (tok1).
        let t1 = cache
            .get_or_fetch("id", "secret", "c.s.t1", make)
            .await
            .unwrap();
        let t2 = cache
            .get_or_fetch("id", "secret", "c.s.t2", make)
            .await
            .unwrap();

        assert_eq!(t1, "tok2", "invalidated table should re-mint");
        assert_eq!(t2, "tok1", "untouched table should still hit cache");
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn invalidate_unknown_key_is_a_noop() {
        let cache = TokenCache::new(true, Duration::from_secs(60));
        let calls = AtomicUsize::new(0);

        let make = |_reason| async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(fetched("tok", Some(3600)))
        };

        cache
            .get_or_fetch("id", "secret", "c.s.t", make)
            .await
            .unwrap();

        // Invalidating a key that was never cached must leave the existing
        // entry intact, so the next call still hits.
        cache.invalidate("id", "secret", "other.table.here").await;
        cache
            .get_or_fetch("id", "secret", "c.s.t", make)
            .await
            .unwrap();

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "invalidating an unknown key must not evict the cached token"
        );
    }

    #[tokio::test]
    async fn invalidate_on_disabled_cache_is_a_noop() {
        // A disabled cache never stores anything, so invalidate has nothing to
        // do; it must simply not panic, and fetching must keep working.
        let cache = TokenCache::new(false, Duration::from_secs(60));
        let calls = AtomicUsize::new(0);

        let make = |_reason| async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(fetched("tok", Some(3600)))
        };

        cache.invalidate("id", "secret", "c.s.t").await;
        let token = cache
            .get_or_fetch("id", "secret", "c.s.t", make)
            .await
            .unwrap();

        assert_eq!(token, "tok");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn disabled_cache_always_fetches() {
        let cache = TokenCache::new(false, Duration::from_secs(60));
        let calls = AtomicUsize::new(0);

        let make = |_reason| async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(fetched("tok", Some(3600)))
        };

        cache
            .get_or_fetch("id", "secret", "c.s.t", make)
            .await
            .unwrap();
        cache
            .get_or_fetch("id", "secret", "c.s.t", make)
            .await
            .unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn fetch_error_leaves_no_cached_entry() {
        let cache = TokenCache::new(true, Duration::from_secs(60));

        let err = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Err(crate::ZerobusError::TokenFetchError("boom".to_string()))
            })
            .await;
        assert!(err.is_err());

        // A subsequent successful fetch should still succeed and cache.
        let ok = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Ok(fetched("tok", Some(3600)))
            })
            .await
            .unwrap();
        assert_eq!(ok, "tok");
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_failure_serves_still_valid_token() {
        // A refresh failure serves the still-valid cached token regardless of error
        // kind: this covers both a retryable and a non-retryable (revoked-creds) one.
        let cache = TokenCache::new(true, Duration::from_secs(60));

        let seeded = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Ok(fetched("valid", Some(30)))
            })
            .await
            .unwrap();
        assert_eq!(seeded, "valid");

        // Count attempts to prove each failing refresh actually ran (not suppressed).
        let refresh_attempts = AtomicUsize::new(0);

        let served_retryable = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                refresh_attempts.fetch_add(1, Ordering::SeqCst);
                Err(ZerobusError::TokenFetchError("blip".to_string()))
            })
            .await
            .unwrap();
        assert_eq!(served_retryable, "valid");
        assert_eq!(refresh_attempts.load(Ordering::SeqCst), 1);

        // Clear the armed backoff so the next call refreshes again.
        tokio::time::advance(Duration::from_secs(2)).await;

        let served_non_retryable = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                refresh_attempts.fetch_add(1, Ordering::SeqCst);
                Err(ZerobusError::InvalidUCTokenError("revoked".to_string()))
            })
            .await
            .unwrap();
        assert_eq!(served_non_retryable, "valid");
        assert_eq!(refresh_attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn fetch_error_surfaces_unchanged_when_no_valid_token() {
        // With nothing to fall back to, the fetch error surfaces unchanged, so its
        // retryability is preserved for callers (both a retryable and a non-retryable).
        let cache = TokenCache::new(true, Duration::from_secs(60));

        let retryable = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Err(ZerobusError::TokenFetchError("blip".to_string()))
            })
            .await
            .unwrap_err();
        assert!(matches!(retryable, ZerobusError::TokenFetchError(_)));
        assert!(retryable.is_retryable());

        let non_retryable = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Err(ZerobusError::InvalidUCTokenError("revoked".to_string()))
            })
            .await
            .unwrap_err();
        assert!(matches!(
            non_retryable,
            ZerobusError::InvalidUCTokenError(_)
        ));
        assert!(!non_retryable.is_retryable());
    }

    #[tokio::test(start_paused = true)]
    async fn cold_miss_dead_on_arrival_token_surfaces_error() {
        let cache = TokenCache::new(true, Duration::from_secs(60));

        // The fetch outlasts the token's TTL, so it returns already expired (dead on
        // arrival). A cold miss has nothing to fall back to, so a retryable error surfaces.
        let result = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                tokio::time::sleep(Duration::from_secs(2)).await;
                Ok(fetched("doa", Some(1)))
            })
            .await;
        assert!(matches!(result, Err(ZerobusError::TokenFetchError(_))));
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_dead_on_arrival_keeps_cached_token() {
        // A refresh returning a token already past its start-anchored expiry is dead
        // on arrival, so the cached token is served and the backoff armed.
        let cache = TokenCache::new(true, Duration::from_secs(60));
        let mints = AtomicUsize::new(0);

        let seeded = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                mints.fetch_add(1, Ordering::SeqCst);
                Ok(fetched("valid", Some(30)))
            })
            .await
            .unwrap();
        assert_eq!(seeded, "valid");

        let served = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                mints.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_secs(2)).await;
                Ok(fetched("doa", Some(1)))
            })
            .await
            .unwrap();
        assert_eq!(served, "valid");

        // The armed backoff suppresses the next refresh, so the mint count stays at 2.
        let reused = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                mints.fetch_add(1, Ordering::SeqCst);
                Ok(fetched("unexpected", Some(3600)))
            })
            .await
            .unwrap();
        assert_eq!(reused, "valid");
        assert_eq!(mints.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn expired_cached_entry_re_mints_as_cold_miss() {
        // An expired cached entry is a cold miss, not a proactive refresh, so the
        // re-mint runs unbounded: with a 1s budget, a 2s fetch still succeeds (a
        // budget-capped refresh would time out and fail).
        let cache = TokenCache::new(true, Duration::from_secs(60));

        let seeded = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Ok(fetched("stale", Some(1)))
            })
            .await
            .unwrap();
        assert_eq!(seeded, "stale");
        tokio::time::advance(Duration::from_secs(2)).await;

        let minted = cache
            .get_or_fetch_within(
                "id",
                "secret",
                "c.s.t",
                Duration::from_secs(1),
                |_reason| async {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    Ok(fetched("fresh", Some(3600)))
                },
            )
            .await
            .unwrap();
        assert_eq!(minted, "fresh");
    }

    #[tokio::test(start_paused = true)]
    async fn failed_refresh_backoff_suppresses_repeat_mint() {
        // A failed refresh arms a backoff that suppresses the next refresh, so the
        // cached token is reused without re-minting (the mint count stays at 2).
        let cache = TokenCache::new(true, Duration::from_secs(60));
        let mints = AtomicUsize::new(0);

        let seeded = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                mints.fetch_add(1, Ordering::SeqCst);
                Ok(fetched("valid", Some(30)))
            })
            .await
            .unwrap();
        assert_eq!(seeded, "valid");

        let served = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                mints.fetch_add(1, Ordering::SeqCst);
                Err(ZerobusError::TokenFetchError("blip".to_string()))
            })
            .await
            .unwrap();
        assert_eq!(served, "valid");

        let reused = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                mints.fetch_add(1, Ordering::SeqCst);
                Ok(fetched("unexpected", Some(3600)))
            })
            .await
            .unwrap();
        assert_eq!(reused, "valid");
        assert_eq!(mints.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn near_expiry_backoff_shrinks_to_retry_before_cold_miss() {
        // With only 3s of token life left, a flat 5s backoff (the 300s buffer's cap)
        // would suppress every retry until the token died. The remaining/2 backoff
        // (1.5s) instead retries while the token is still valid.
        let cache = TokenCache::new(true, Duration::from_secs(300));
        let mints = AtomicUsize::new(0);

        let seeded = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                mints.fetch_add(1, Ordering::SeqCst);
                Ok(fetched("valid", Some(3)))
            })
            .await
            .unwrap();
        assert_eq!(seeded, "valid");

        let served = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                mints.fetch_add(1, Ordering::SeqCst);
                Err(ZerobusError::TokenFetchError("blip".to_string()))
            })
            .await
            .unwrap();
        assert_eq!(served, "valid");

        // Still inside the ~1.5s backoff: reused, so the mint count stays at 2.
        tokio::time::advance(Duration::from_secs(1)).await;
        let reused = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                mints.fetch_add(1, Ordering::SeqCst);
                Ok(fetched("unexpected", Some(3600)))
            })
            .await
            .unwrap();
        assert_eq!(reused, "valid");
        assert_eq!(mints.load(Ordering::SeqCst), 2);

        // Past the backoff and still valid: the proactive retry runs and succeeds.
        tokio::time::advance(Duration::from_millis(600)).await;
        let refreshed = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                mints.fetch_add(1, Ordering::SeqCst);
                Ok(fetched("fresh", Some(3600)))
            })
            .await
            .unwrap();
        assert_eq!(refreshed, "fresh");
        assert_eq!(mints.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_refresh_serves_cached_token() {
        // A hung refresh is cut off by the 1s budget (elapsed in virtual time), so it
        // becomes a retryable error and the still-valid cached token is served.
        let cache = TokenCache::new(true, Duration::from_secs(60));

        let seeded = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Ok(fetched("valid", Some(30)))
            })
            .await
            .unwrap();
        assert_eq!(seeded, "valid");

        let served = cache
            .get_or_fetch_within(
                "id",
                "secret",
                "c.s.t",
                Duration::from_secs(1),
                |_reason| async { std::future::pending::<ZerobusResult<FetchedToken>>().await },
            )
            .await
            .unwrap();
        assert_eq!(served, "valid");
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_runs_unbounded_when_token_would_expire_before_budget() {
        // The token has less life left (3s) than the refresh budget (5s), so bounding
        // is pointless: it would expire before the budget fires, with nothing to fall
        // back to. The refresh runs unbounded instead, so an 8s mint still succeeds.
        let cache = TokenCache::new(true, Duration::from_secs(60));

        let seeded = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Ok(fetched("valid", Some(3)))
            })
            .await
            .unwrap();
        assert_eq!(seeded, "valid");

        let minted = cache
            .get_or_fetch_within(
                "id",
                "secret",
                "c.s.t",
                Duration::from_secs(5),
                |_reason| async {
                    tokio::time::sleep(Duration::from_secs(8)).await;
                    Ok(fetched("fresh", Some(3600)))
                },
            )
            .await
            .unwrap();
        assert_eq!(minted, "fresh");
    }

    #[tokio::test]
    async fn no_ttl_response_does_not_evict_valid_token() {
        let cache = TokenCache::new(true, Duration::from_secs(60));

        // Seed a still-valid (within-buffer) token.
        cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Ok(fetched("valid", Some(30)))
            })
            .await
            .unwrap();

        // A refresh returns a token with no TTL: the caller gets the fresh token,
        // but the cached valid token must not be discarded.
        let fresh = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Ok(fetched("nottl", None))
            })
            .await
            .unwrap();
        assert_eq!(fresh, "nottl");

        // A later refresh failure still finds the original valid token, proving
        // it was retained.
        let served = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Err(crate::ZerobusError::TokenFetchError("blip".to_string()))
            })
            .await
            .unwrap();
        assert_eq!(served, "valid");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn single_flight_mints_once_for_concurrent_callers() {
        let cache = Arc::new(TokenCache::new(true, Duration::from_secs(60)));
        let calls = Arc::new(AtomicUsize::new(0));
        const FOLLOWERS: usize = 15;

        // Keep the leader's mint in flight (blocked on `gate`) while the
        // followers pile in. A broken single-flight — one that didn't hold the
        // per-entry lock across the mint — would let a follower mint too and push
        // `calls` above 1.
        let gate = Arc::new(tokio::sync::Notify::new());

        // Leader: occupies the slot and blocks inside the mint on `gate`.
        let leader = {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            let gate = Arc::clone(&gate);
            tokio::spawn(async move {
                cache
                    .get_or_fetch("id", "secret", "c.s.t", |_reason| async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        gate.notified().await;
                        Ok(fetched("tok", Some(3600)))
                    })
                    .await
                    .unwrap()
            })
        };

        // Wait until the leader is inside the mint (one call recorded), so the
        // slot exists in the map and the leader holds its lock.
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }

        // Take the test's own clone of the occupied slot; its Arc strong count
        // then reports how many callers have reached it.
        let slot = {
            let entries = cache.entries.lock().await;
            Arc::clone(
                entries
                    .get(&TokenKey::new("id", "secret", "c.s.t"))
                    .unwrap(),
            )
        };

        // Followers: each calls get_or_fetch and contends for the held lock.
        let mut followers = Vec::new();
        for _ in 0..FOLLOWERS {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            followers.push(tokio::spawn(async move {
                cache
                    .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(fetched("tok", Some(3600)))
                    })
                    .await
                    .unwrap()
            }));
        }

        // Release the leader only once every follower has cloned the occupied
        // slot — i.e. entered get_or_fetch and reached the per-entry lock. The
        // strong count is the map, the leader, this test handle, and each
        // follower. The timeout only guards against a stuck follower (e.g. a
        // single-flight regression) hanging the test; it is generous so a loaded
        // CI box scheduling 15 tasks can't trip it spuriously.
        tokio::time::timeout(Duration::from_secs(10), async {
            while Arc::strong_count(&slot) < FOLLOWERS + 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("followers did not reach the occupied slot");

        gate.notify_one();

        assert_eq!(leader.await.unwrap(), "tok");
        for handle in followers {
            assert_eq!(handle.await.unwrap(), "tok");
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "single-flight must mint exactly once for concurrent same-key callers"
        );
    }

    #[tokio::test]
    async fn cancelled_mint_leaves_cache_usable() {
        let cache = Arc::new(TokenCache::new(true, Duration::from_secs(60)));
        let calls = Arc::new(AtomicUsize::new(0));

        // Signals that the leader has entered the mint (and so is holding the
        // per-entry lock) so we can cancel it at a known point, without relying
        // on wall-clock timing.
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();

        let task = {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            tokio::spawn(async move {
                cache
                    .get_or_fetch("id", "secret", "c.s.t", move |_reason| async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        let _ = started_tx.send(());
                        // Never completes: the task is aborted while awaiting
                        // here, dropping the get_or_fetch future mid-mint.
                        std::future::pending::<ZerobusResult<FetchedToken>>().await
                    })
                    .await
            })
        };

        // Wait until the mint is in flight, then cancel it. Awaiting the aborted
        // task guarantees its future (and the slot guard) has been dropped.
        started_rx.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());

        // The cancelled leader must have released the lock and left no
        // half-written entry, so the next caller mints cleanly...
        let minted = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(fetched("tok", Some(3600)))
            })
            .await
            .unwrap();
        assert_eq!(minted, "tok");

        // ...and that freshly minted token is cached, not a phantom entry: a
        // follow-up call hits without minting again.
        let cached = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(fetched("other", Some(3600)))
            })
            .await
            .unwrap();
        assert_eq!(cached, "tok");

        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "one aborted mint plus one real mint; the final call must hit cache"
        );
    }
}
