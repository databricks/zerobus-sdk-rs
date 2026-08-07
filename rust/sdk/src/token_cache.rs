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
use crate::ZerobusResult;

/// Default lead time before expiry at which a cached token is refreshed.
pub(crate) const DEFAULT_REFRESH_BUFFER: Duration = Duration::from_secs(300);

/// A cached token and the instant at which it expires.
struct CachedToken {
    value: String,
    expires_at: Instant,
}

impl CachedToken {
    fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
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

    /// Returns a valid token for the given credentials and table, fetching a new
    /// one only if the cache is empty, the token has entered the refresh window,
    /// or caching is disabled.
    ///
    /// `fetch` is invoked to mint a fresh token. It is only ever called once per
    /// key at a time thanks to the per-entry lock.
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
                debug!(table = %table_name, "token cache hit, reusing cached token");
                return Ok(cached.value.clone());
            }
        }

        // A present-but-stale token means we are refreshing; an empty slot is a
        // cold miss. The reason is surfaced on the mint log.
        let reason = if guard.is_some() {
            MintReason::Refresh
        } else {
            MintReason::ColdMiss
        };

        let fetched = match fetch(reason).await {
            Ok(fetched) => fetched,
            Err(err) => {
                // On a retryable failure, serve the still-valid cached token;
                // let non-retryable errors (bad/revoked creds) surface.
                if err.is_retryable() {
                    if let Some(cached) = guard.as_ref() {
                        if !cached.is_expired() {
                            warn!(table = %table_name, "token refresh failed (retryable); serving still-valid cached token");
                            return Ok(cached.value.clone());
                        }
                    }
                }
                return Err(err);
            }
        };

        let token = fetched.token.clone();

        // Cache only tokens with a usable TTL. `checked_add` also drops an absurd
        // `expires_in` that would overflow the clock instead of panicking.
        let expires_at = fetched
            .expires_in
            .and_then(|ttl| Instant::now().checked_add(ttl));
        match expires_at {
            Some(expires_at) => {
                *guard = Some(CachedToken {
                    value: fetched.token,
                    expires_at,
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

        Ok(token)
    }

    /// Drops any cached token for the given credentials and table so the next
    /// `get_or_fetch` re-mints. Called when the server rejects the token (e.g.
    /// it was revoked at the IdP), so the re-mint re-checks grants at UC. No-op
    /// when caching is disabled or no entry exists.
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
        // `checked_add` avoids a panic on an absurd refresh buffer (e.g.
        // `Duration::MAX`); an overflowing deadline means "always refresh".
        match Instant::now().checked_add(self.refresh_buffer) {
            Some(deadline) => deadline >= cached.expires_at,
            None => true,
        }
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
    async fn refresh_failure_does_not_serve_expired_token() {
        let cache = TokenCache::new(true, Duration::from_secs(60));

        // Seed a token with a zero TTL: `expires_at` becomes the mint instant. By
        // the second await below the monotonic clock has reached or passed it, and
        // `is_expired` (`Instant::now() >= expires_at`) treats equality as expired,
        // so the token reads as expired.
        cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Ok(fetched("stale", Some(0)))
            })
            .await
            .unwrap();

        // A retryable refresh failure would serve a still-valid cached token, but
        // this one has expired, so the error must surface rather than handing the
        // caller a dead token.
        let result = cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Err(crate::ZerobusError::TokenFetchError("blip".to_string()))
            })
            .await;
        assert!(matches!(
            result,
            Err(crate::ZerobusError::TokenFetchError(_))
        ));
    }

    #[tokio::test]
    async fn refresh_failure_propagates_non_retryable_error() {
        let cache = TokenCache::new(true, Duration::from_secs(60));

        // Seed a token that is within the refresh buffer but not yet expired.
        cache
            .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                Ok(fetched("valid", Some(30)))
            })
            .await
            .unwrap();

        // A non-retryable refresh error (e.g. revoked or invalid credentials)
        // must surface rather than being masked by the still-valid cached token.
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn single_flight_mints_once_for_concurrent_callers() {
        let cache = Arc::new(TokenCache::new(true, Duration::from_secs(60)));
        let calls = Arc::new(AtomicUsize::new(0));
        const FOLLOWERS: usize = 15;

        // Keep the leader's mint in flight (blocked on `gate`) while the
        // followers pile in.
        let gate = Arc::new(tokio::sync::Notify::new());
        let (queued_tx, mut queued_rx) = tokio::sync::mpsc::unbounded_channel();

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

        // Wait until the leader is inside the mint (one call recorded) before
        // launching followers, so they cannot win the slot first.
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }

        // Followers: each signals that it has started, then calls get_or_fetch
        // and contends for the same per-entry lock the leader holds.
        let mut followers = Vec::new();
        for _ in 0..FOLLOWERS {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            let queued_tx = queued_tx.clone();
            followers.push(tokio::spawn(async move {
                queued_tx.send(()).unwrap();
                cache
                    .get_or_fetch("id", "secret", "c.s.t", |_reason| async {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(fetched("tok", Some(3600)))
                    })
                    .await
                    .unwrap()
            }));
        }

        // Once all followers report they have started, release the leader's mint
        // so it caches the single token.
        for _ in 0..FOLLOWERS {
            queued_rx.recv().await.unwrap();
        }
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
