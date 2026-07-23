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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Duration;

use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::{debug, warn};

use crate::default_token_factory::{FetchedToken, MintReason};
use crate::ZerobusResult;

/// Default lead time before expiry at which a cached token is refreshed.
pub(crate) const DEFAULT_REFRESH_BUFFER: Duration = Duration::from_secs(300);

/// Delay before retrying a failed proactive refresh. This suppresses a burst of
/// waiting cache users from each repeating the same failed token request.
const DEFAULT_REFRESH_FAILURE_BACKOFF: Duration = Duration::from_secs(5);

/// A cached token and the instant at which it expires.
struct CachedToken {
    value: String,
    expires_at: Instant,
    refresh_retry_at: Option<Instant>,
    generation: u64,
}

impl CachedToken {
    fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }

    fn defer_refresh(&mut self, backoff: Duration) {
        let retry_at = Instant::now()
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
struct CacheSlot {
    token: Mutex<Option<CachedToken>>,
    invalidated: AtomicBool,
    current_generation: AtomicU64,
    next_generation: AtomicU64,
}

impl Default for CacheSlot {
    fn default() -> Self {
        Self {
            token: Mutex::new(None),
            invalidated: AtomicBool::new(false),
            current_generation: AtomicU64::new(0),
            next_generation: AtomicU64::new(1),
        }
    }
}

type Slot = Arc<CacheSlot>;

/// Identifies the exact cache generation that supplied a token.
#[derive(Clone)]
pub(crate) struct TokenGeneration {
    slot: Slot,
    id: u64,
}

/// A token returned to the built-in headers provider, with the cache generation
/// to invalidate if Zerobus rejects it.
pub(crate) struct TokenResult {
    pub(crate) value: String,
    pub(crate) generation: Option<TokenGeneration>,
}

/// Caches OAuth tokens per table for the lifetime of a [`ZerobusSdk`].
///
/// Safe for concurrent use across streams created from the same SDK instance.
pub(crate) struct TokenCache {
    entries: Mutex<HashMap<TokenKey, Slot>>,
    refresh_buffer: Duration,
    refresh_failure_backoff: Duration,
    enabled: bool,
}

impl TokenCache {
    pub(crate) fn new(enabled: bool, refresh_buffer: Duration) -> Self {
        Self::with_refresh_failure_backoff(enabled, refresh_buffer, DEFAULT_REFRESH_FAILURE_BACKOFF)
    }

    fn with_refresh_failure_backoff(
        enabled: bool,
        refresh_buffer: Duration,
        refresh_failure_backoff: Duration,
    ) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            refresh_buffer,
            refresh_failure_backoff,
            enabled,
        }
    }

    /// Returns a valid token for the given credentials and table, fetching a new
    /// one only if the cache is empty, the token has entered the refresh window
    /// without an active failure backoff, or caching is disabled.
    ///
    /// `fetch` is invoked to mint a fresh token. It is only ever called once per
    /// key at a time thanks to the per-entry lock.
    #[cfg(test)]
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
        self.get_or_fetch_with_timeout(client_id, client_secret, table_name, None, fetch)
            .await
            .map(|result| result.value)
    }

    /// Caching-aware token lookup used by the built-in OAuth headers provider.
    /// A proactive refresh can be bounded independently of the enclosing stream
    /// connection timeout so a stalled token endpoint still falls back to the
    /// unexpired cached token.
    pub(crate) async fn get_or_fetch_with_timeout<F, Fut>(
        &self,
        client_id: &str,
        client_secret: &str,
        table_name: &str,
        refresh_timeout: Option<Duration>,
        fetch: F,
    ) -> ZerobusResult<TokenResult>
    where
        F: FnOnce(MintReason) -> Fut,
        Fut: std::future::Future<Output = ZerobusResult<FetchedToken>>,
    {
        if !self.enabled {
            return fetch(MintReason::CacheDisabled)
                .await
                .map(|fetched| TokenResult {
                    value: fetched.token,
                    generation: None,
                });
        }

        let key = TokenKey::new(client_id, client_secret, table_name);
        let mut fetch = Some(fetch);

        loop {
            let slot = {
                let mut entries = self.entries.lock().await;
                // Sweep only on a miss, keeping the cost off the hot lookup path.
                if !entries.contains_key(&key) {
                    Self::prune_expired(&mut entries);
                }
                Arc::clone(entries.entry(key.clone()).or_default())
            };

            // Hold the per-entry lock across the fetch so concurrent callers for
            // the same key reuse a single mint instead of stampeding the endpoint.
            let mut guard = slot.token.lock().await;
            if slot.invalidated.load(AtomicOrdering::Acquire) {
                continue;
            }

            if let Some(cached) = guard.as_ref() {
                if slot.current_generation.load(AtomicOrdering::Acquire) != cached.generation {
                    continue;
                }
                if !self.needs_refresh(cached) {
                    debug!(table = %table_name, "token cache hit, reusing cached token");
                    return Ok(TokenResult {
                        value: cached.value.clone(),
                        generation: Some(TokenGeneration {
                            slot: Arc::clone(&slot),
                            id: cached.generation,
                        }),
                    });
                }
            }

            // A present-but-stale token means we are refreshing; an empty slot is
            // a cold miss. The reason is surfaced on the mint log.
            let reason = if guard.is_some() {
                MintReason::Refresh
            } else {
                MintReason::ColdMiss
            };

            // `expires_in` starts at token issuance, not after a slow response has
            // reached the client. Starting the local TTL before the fetch avoids
            // extending the issuer's hard-expiry boundary by request latency.
            let fetch_started_at = Instant::now();
            let fetch_future = fetch.take().expect("fetch closure used once")(reason);
            let fetch_result = match (reason, refresh_timeout) {
                (MintReason::Refresh, Some(timeout)) => {
                    match tokio::time::timeout(timeout, fetch_future).await {
                        Ok(result) => result,
                        Err(_) => Err(crate::ZerobusError::TokenFetchError(format!(
                            "Proactive token refresh timed out after {}ms",
                            timeout.as_millis()
                        ))),
                    }
                }
                _ => fetch_future.await,
            };

            let fetched = match fetch_result {
                Ok(fetched) => fetched,
                Err(err) => {
                    // A proactive refresh failure says nothing about the validity
                    // of the cached access token. Keep serving it until its actual
                    // expiry regardless of how the mint error was classified. An
                    // authentication rejection from Zerobus invalidates the exact
                    // generation that supplied the rejected token.
                    if !slot.invalidated.load(AtomicOrdering::Acquire) {
                        if let Some(cached) = guard.as_mut() {
                            if !cached.is_expired()
                                && slot.current_generation.load(AtomicOrdering::Acquire)
                                    == cached.generation
                            {
                                cached.defer_refresh(self.refresh_failure_backoff);
                                warn!(
                                    table = %table_name,
                                    retryable = err.is_retryable(),
                                    "token refresh failed; serving still-valid cached token"
                                );
                                return Ok(TokenResult {
                                    value: cached.value.clone(),
                                    generation: Some(TokenGeneration {
                                        slot: Arc::clone(&slot),
                                        id: cached.generation,
                                    }),
                                });
                            }
                        }
                    }
                    return Err(err);
                }
            };

            // If an auth rejection invalidated this generation while its fetch
            // was in flight, neither return nor store that detached result. A
            // retry will use the replacement map generation instead.
            let prior_generation = guard.as_ref().map(|cached| cached.generation);
            let prior_generation_invalidated = prior_generation.is_some_and(|generation| {
                slot.current_generation.load(AtomicOrdering::Acquire) != generation
            });
            if slot.invalidated.load(AtomicOrdering::Acquire) || prior_generation_invalidated {
                return Err(crate::ZerobusError::TokenFetchError(
                    "Token cache generation was invalidated during refresh".to_string(),
                ));
            }

            let token = fetched.token.clone();

            // Cache only tokens with a usable TTL. `checked_add` also drops an
            // absurd `expires_in` that would overflow the clock instead of panicking.
            let expires_at = fetched
                .expires_in
                .and_then(|ttl| fetch_started_at.checked_add(ttl));
            let generation = match expires_at {
                Some(expires_at) if expires_at > Instant::now() => {
                    let generation = slot.next_generation.fetch_add(1, AtomicOrdering::Relaxed);
                    let expected = prior_generation.unwrap_or(0);
                    if slot
                        .current_generation
                        .compare_exchange(
                            expected,
                            generation,
                            AtomicOrdering::AcqRel,
                            AtomicOrdering::Acquire,
                        )
                        .is_err()
                    {
                        return Err(crate::ZerobusError::TokenFetchError(
                            "Token cache generation changed during refresh".to_string(),
                        ));
                    }
                    *guard = Some(CachedToken {
                        value: fetched.token,
                        expires_at,
                        refresh_retry_at: None,
                        generation,
                    });
                    Some(TokenGeneration {
                        slot: Arc::clone(&slot),
                        id: generation,
                    })
                }
                Some(_) | None => {
                    // No usable TTL: keep an existing still-valid token rather
                    // than discarding it. The newly returned token is not tied
                    // to that older cache generation.
                    let keep_existing = guard.as_ref().is_some_and(|cached| !cached.is_expired());
                    if !keep_existing {
                        *guard = None;
                    }
                    None
                }
            };

            return Ok(TokenResult {
                value: token,
                generation,
            });
        }
    }

    /// Drops any cached token for the given credentials and table so the next
    /// `get_or_fetch` re-mints. Called when the server rejects the token (e.g.
    /// it was revoked at the IdP), so the re-mint re-checks grants at UC. No-op
    /// when caching is disabled or no entry exists.
    #[cfg(test)]
    pub(crate) async fn invalidate(&self, client_id: &str, client_secret: &str, table_name: &str) {
        if !self.enabled {
            return;
        }
        let key = TokenKey::new(client_id, client_secret, table_name);
        if let Some(slot) = self.entries.lock().await.remove(&key) {
            slot.invalidated.store(true, AtomicOrdering::Release);
            slot.current_generation.store(0, AtomicOrdering::Release);
            debug!(table = %table_name, "token cache entry invalidated after auth rejection");
        }
    }

    /// Invalidates the exact cache generation that supplied rejected headers.
    /// A late rejection from an older stream cannot remove a replacement token.
    pub(crate) async fn invalidate_generation(
        &self,
        client_id: &str,
        client_secret: &str,
        table_name: &str,
        generation: &TokenGeneration,
    ) {
        if !self.enabled {
            return;
        }

        let key = TokenKey::new(client_id, client_secret, table_name);
        if generation
            .slot
            .current_generation
            .compare_exchange(
                generation.id,
                0,
                AtomicOrdering::AcqRel,
                AtomicOrdering::Acquire,
            )
            .is_err()
        {
            return;
        }

        generation
            .slot
            .invalidated
            .store(true, AtomicOrdering::Release);
        let mut entries = self.entries.lock().await;
        let is_current = entries
            .get(&key)
            .is_some_and(|slot| Arc::ptr_eq(slot, &generation.slot));
        if is_current {
            entries.remove(&key);
            debug!(table = %table_name, "token cache entry invalidated after auth rejection");
        }
    }

    fn needs_refresh(&self, cached: &CachedToken) -> bool {
        let now = Instant::now();
        if now >= cached.expires_at {
            return true;
        }
        if cached
            .refresh_retry_at
            .is_some_and(|retry_at| now < retry_at)
        {
            return false;
        }

        // `checked_add` avoids a panic on an absurd refresh buffer (e.g.
        // `Duration::MAX`); an overflowing deadline means "always refresh".
        match now.checked_add(self.refresh_buffer) {
            Some(deadline) => deadline >= cached.expires_at,
            None => true,
        }
    }

    /// Drops entries whose token has fully expired. Locked (in-flight) entries,
    /// still-valid tokens, and empty slots are kept — keeping empty slots is
    /// what preserves single-flight for a key being minted concurrently.
    fn prune_expired(entries: &mut HashMap<TokenKey, Slot>) {
        entries.retain(|_, slot| match slot.token.try_lock() {
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

    #[tokio::test]
    async fn refresh_failure_serves_still_valid_token() {
        let cache = TokenCache::new(true, Duration::from_secs(60));

        // Seed a token that is within the refresh buffer (ttl < buffer) but not
        // yet expired, so the next call is due for a refresh.
        let seeded = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Ok(fetched("valid", Some(30)))
            })
            .await
            .unwrap();
        assert_eq!(seeded, "valid");

        // The refresh mint fails; the still-valid cached token is served instead
        // of surfacing the error.
        let served = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Err(crate::ZerobusError::TokenFetchError("blip".to_string()))
            })
            .await
            .unwrap();
        assert_eq!(served, "valid");
    }

    #[tokio::test]
    async fn non_retryable_refresh_failure_serves_still_valid_token() {
        let cache = TokenCache::new(true, Duration::from_secs(60));

        // Seed a token that is within the refresh buffer but not yet expired.
        cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Ok(fetched("valid", Some(30)))
            })
            .await
            .unwrap();

        // Even a non-retryable mint error does not invalidate the access token
        // already in hand. Continue serving it until its actual expiry.
        let served = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Err(crate::ZerobusError::InvalidUCTokenError(
                    "revoked".to_string(),
                ))
            })
            .await
            .unwrap();
        assert_eq!(served, "valid");
    }

    #[tokio::test]
    async fn concurrent_callers_share_refresh_failure_backoff() {
        let cache = Arc::new(TokenCache::with_refresh_failure_backoff(
            true,
            Duration::from_secs(60),
            Duration::from_secs(3600),
        ));
        cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Ok(fetched("valid", Some(30)))
            })
            .await
            .unwrap();

        let calls = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..16 {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            handles.push(tokio::spawn(async move {
                cache
                    .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                        calls.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        Err(crate::ZerobusError::InvalidUCTokenError(
                            "rate limited".to_string(),
                        ))
                    })
                    .await
                    .unwrap()
            }));
        }

        for handle in handles {
            assert_eq!(handle.await.unwrap(), "valid");
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "waiting callers must not repeat the failed refresh"
        );
    }

    #[tokio::test]
    async fn refresh_is_retried_after_failure_backoff() {
        let cache = TokenCache::with_refresh_failure_backoff(
            true,
            Duration::from_secs(60),
            Duration::from_secs(3600),
        );
        cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Ok(fetched("valid", Some(30)))
            })
            .await
            .unwrap();

        let calls = AtomicUsize::new(0);
        let served = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(crate::ZerobusError::TokenFetchError("blip".to_string()))
            })
            .await
            .unwrap();
        assert_eq!(served, "valid");

        let during_backoff = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(fetched("too-early", Some(3600)))
            })
            .await
            .unwrap();
        assert_eq!(during_backoff, "valid");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let key = TokenKey::new("id", "secret", "c.s.t");
        let slot = {
            let entries = cache.entries.lock().await;
            Arc::clone(entries.get(&key).unwrap())
        };
        slot.token.lock().await.as_mut().unwrap().refresh_retry_at = Some(Instant::now());

        let refreshed = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(fetched("fresh", Some(3600)))
            })
            .await
            .unwrap();
        assert_eq!(refreshed, "fresh");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn refresh_failure_backoff_never_extends_token_expiry() {
        let cache = TokenCache::with_refresh_failure_backoff(
            true,
            Duration::from_secs(60),
            Duration::from_secs(3600),
        );
        cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Ok(fetched("short-lived", Some(30)))
            })
            .await
            .unwrap();

        let served = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Err(crate::ZerobusError::TokenFetchError("blip".to_string()))
            })
            .await
            .unwrap();
        assert_eq!(served, "short-lived");

        let key = TokenKey::new("id", "secret", "c.s.t");
        let slot = {
            let entries = cache.entries.lock().await;
            Arc::clone(entries.get(&key).unwrap())
        };
        {
            let mut guard = slot.token.lock().await;
            let cached = guard.as_mut().unwrap();
            assert_eq!(cached.refresh_retry_at, Some(cached.expires_at));
            cached.expires_at = Instant::now();
        }

        let result = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Err(crate::ZerobusError::InvalidUCTokenError(
                    "still unavailable".to_string(),
                ))
            })
            .await;
        assert!(matches!(
            result,
            Err(crate::ZerobusError::InvalidUCTokenError(_))
        ));
    }

    #[tokio::test]
    async fn invalidation_bypasses_refresh_failure_backoff() {
        let cache = TokenCache::with_refresh_failure_backoff(
            true,
            Duration::from_secs(60),
            Duration::from_secs(3600),
        );
        cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Ok(fetched("rejected", Some(30)))
            })
            .await
            .unwrap();
        cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Err(crate::ZerobusError::TokenFetchError("blip".to_string()))
            })
            .await
            .unwrap();

        cache.invalidate("id", "secret", "c.s.t").await;
        let reminted = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Ok(fetched("fresh", Some(3600)))
            })
            .await
            .unwrap();
        assert_eq!(reminted, "fresh");
    }

    #[tokio::test]
    async fn invalidated_in_flight_generation_cannot_replace_or_remove_successor() {
        let cache = Arc::new(TokenCache::new(true, Duration::from_secs(60)));
        let initial = cache
            .get_or_fetch_with_timeout("id", "secret", "c.s.t", None, |_reason| async {
                Ok(fetched("rejected", Some(30)))
            })
            .await
            .unwrap();
        let initial_generation = initial.generation.unwrap();

        let refresh_started = Arc::new(tokio::sync::Notify::new());
        let release_refresh = Arc::new(tokio::sync::Notify::new());
        let refresh = {
            let cache = Arc::clone(&cache);
            let refresh_started = Arc::clone(&refresh_started);
            let release_refresh = Arc::clone(&release_refresh);
            tokio::spawn(async move {
                cache
                    .get_or_fetch_with_timeout(
                        "id",
                        "secret",
                        "c.s.t",
                        None,
                        move |_reason| async move {
                            refresh_started.notify_one();
                            release_refresh.notified().await;
                            Err(crate::ZerobusError::InvalidUCTokenError(
                                "rate limited".to_string(),
                            ))
                        },
                    )
                    .await
            })
        };

        refresh_started.notified().await;
        cache
            .invalidate_generation("id", "secret", "c.s.t", &initial_generation)
            .await;
        let replacement = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Ok(fetched("replacement", Some(3600)))
            })
            .await
            .unwrap();
        assert_eq!(replacement, "replacement");

        release_refresh.notify_one();
        assert!(matches!(
            refresh.await.unwrap(),
            Err(crate::ZerobusError::InvalidUCTokenError(_))
        ));

        // A late rejection from the old stream must not evict the replacement.
        cache
            .invalidate_generation("id", "secret", "c.s.t", &initial_generation)
            .await;
        let still_cached = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                panic!("old generation invalidation removed the replacement token")
            })
            .await
            .unwrap();
        assert_eq!(still_cached, "replacement");
    }

    #[tokio::test]
    async fn late_rejection_cannot_invalidate_refresh_in_same_slot() {
        let cache = TokenCache::new(true, Duration::from_secs(60));
        let initial = cache
            .get_or_fetch_with_timeout("id", "secret", "c.s.t", None, |_reason| async {
                Ok(fetched("old", Some(30)))
            })
            .await
            .unwrap();
        let initial_generation = initial.generation.unwrap();

        let refreshed = cache
            .get_or_fetch_with_timeout("id", "secret", "c.s.t", None, |_reason| async {
                Ok(fetched("new", Some(3600)))
            })
            .await
            .unwrap();
        let refreshed_generation = refreshed.generation.unwrap();
        assert!(Arc::ptr_eq(
            &initial_generation.slot,
            &refreshed_generation.slot
        ));
        assert_ne!(initial_generation.id, refreshed_generation.id);

        cache
            .invalidate_generation("id", "secret", "c.s.t", &initial_generation)
            .await;
        let still_cached = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                panic!("late rejection removed a newer token in the same slot")
            })
            .await
            .unwrap();
        assert_eq!(still_cached, "new");
    }

    #[tokio::test]
    async fn proactive_refresh_timeout_serves_still_valid_token() {
        let cache = TokenCache::with_refresh_failure_backoff(
            true,
            Duration::from_secs(60),
            Duration::from_secs(3600),
        );
        cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Ok(fetched("valid", Some(30)))
            })
            .await
            .unwrap();

        let served = cache
            .get_or_fetch_with_timeout(
                "id",
                "secret",
                "c.s.t",
                Some(Duration::from_millis(20)),
                |_reason| async {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    Ok(fetched("too-late", Some(3600)))
                },
            )
            .await
            .unwrap();
        assert_eq!(served.value, "valid");

        let during_backoff = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                panic!("timed-out refresh did not arm failure backoff")
            })
            .await
            .unwrap();
        assert_eq!(during_backoff, "valid");
    }

    #[tokio::test]
    async fn slow_fetch_does_not_extend_reported_token_lifetime() {
        let cache = TokenCache::new(true, Duration::ZERO);
        let first = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(FetchedToken {
                    token: "already-expired".to_string(),
                    expires_in: Some(Duration::from_millis(10)),
                })
            })
            .await
            .unwrap();
        assert_eq!(first, "already-expired");

        let second = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Ok(fetched("fresh", Some(3600)))
            })
            .await
            .unwrap();
        assert_eq!(second, "fresh");
    }

    #[tokio::test]
    async fn refresh_failure_surfaces_after_cached_token_expires() {
        let cache = TokenCache::new(true, Duration::from_secs(60));

        cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Ok(FetchedToken {
                    token: "expired".to_string(),
                    expires_in: Some(Duration::from_millis(1)),
                })
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        let result = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Err(crate::ZerobusError::InvalidUCTokenError(
                    "revoked".to_string(),
                ))
            })
            .await;
        assert!(matches!(
            result,
            Err(crate::ZerobusError::InvalidUCTokenError(_))
        ));
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

    #[tokio::test]
    async fn single_flight_mints_once_for_concurrent_callers() {
        let cache = Arc::new(TokenCache::new(true, Duration::from_secs(60)));
        let calls = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..16 {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            handles.push(tokio::spawn(async move {
                cache
                    .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                        calls.fetch_add(1, Ordering::SeqCst);
                        // Hold the slot briefly so the other callers pile up
                        // behind the single-flight lock rather than racing.
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        Ok(fetched("tok", Some(3600)))
                    })
                    .await
                    .unwrap()
            }));
        }

        for handle in handles {
            assert_eq!(handle.await.unwrap(), "tok");
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "single-flight must mint exactly once for concurrent same-key callers"
        );
    }
}
