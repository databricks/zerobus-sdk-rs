//! Integration tests for OAuth token caching.
//!
//! These exercise the real minting path end to end: the SDK's `reqwest` client
//! POSTs to a loopback mock Unity Catalog endpoint (`mock_oauth`), the response
//! flows through `DefaultTokenFactory` into the shared `TokenCache`, and back
//! out through `OAuthHeadersProvider`.
//!
//! Behavior is induced from the server side — the mock varies `expires_in`, the
//! HTTP status, and response timing — and asserted from the client side via the
//! returned token and `mock_oauth`'s mint counter (how many times the SDK hit
//! the endpoint rather than serving from cache).

// The shared gRPC mock is only partially used here (stream creation and error
// injection), so silence dead-code warnings for its unused response variants.
#[allow(dead_code)]
mod mock_grpc;
mod mock_oauth;
mod utils;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use databricks_zerobus_ingest_sdk::{
    HeadersProvider, NoTlsConfig, OAuthHeadersProvider, ZerobusError, ZerobusSdk,
};

use futures::future::join_all;
use mock_grpc::{start_mock_server, MockResponse};
use mock_oauth::{start_mock_oauth_server, MockTokenResponse};
use utils::setup_tracing;

const TABLE_NAME: &str = "catalog.schema.orders";
const OTHER_TABLE: &str = "catalog.schema.events";
const CLIENT_ID: &str = "test-client-id";
const CLIENT_SECRET: &str = "test-client-secret";
const WORKSPACE_ID: &str = "test-workspace";

/// Builds an `OAuthHeadersProvider` with its own cache, pointed at the mock UC
/// endpoint. This standalone provider is enough for the pure token-cache tests
/// (no stream, no gRPC server).
fn oauth_provider(uc_url: String) -> OAuthHeadersProvider {
    OAuthHeadersProvider::new(
        CLIENT_ID.to_string(),
        CLIENT_SECRET.to_string(),
        TABLE_NAME.to_string(),
        WORKSPACE_ID.to_string(),
        uc_url,
    )
}

/// Extracts the bearer token from a set of provider headers.
fn bearer_token(headers: &HashMap<&'static str, String>) -> String {
    headers
        .get("authorization")
        .and_then(|value| value.strip_prefix("Bearer "))
        .expect("headers must carry a Bearer authorization")
        .to_string()
}

/// Token minting and caching through the real HTTP path, without a stream.
mod token_minting_and_caching_tests {
    use super::*;

    #[tokio::test]
    async fn mints_once_then_serves_from_cache() {
        setup_tracing();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        oauth
            .set_responses(vec![MockTokenResponse::ok("token-1")])
            .await;
        let provider = oauth_provider(uc_url);

        let first = bearer_token(&provider.get_headers().await.unwrap());
        let second = bearer_token(&provider.get_headers().await.unwrap());

        assert_eq!(first, "token-1");
        assert_eq!(
            second, "token-1",
            "the second call must serve the cached token"
        );
        assert_eq!(
            oauth.mint_count(),
            1,
            "a cached token must not be re-minted"
        );
    }

    #[tokio::test]
    async fn quoted_expires_in_is_cached() {
        setup_tracing();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        // UC returns expires_in as a quoted string; caching must still kick in.
        oauth
            .set_responses(vec![
                MockTokenResponse::ok("token-1").with_expires_in(Some("\"3600\""))
            ])
            .await;
        let provider = oauth_provider(uc_url);

        provider.get_headers().await.unwrap();
        provider.get_headers().await.unwrap();

        assert_eq!(
            oauth.mint_count(),
            1,
            "a quoted expires_in must parse to a TTL and enable caching"
        );
    }

    #[tokio::test]
    async fn missing_expires_in_is_not_cached() {
        setup_tracing();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        oauth
            .set_responses(vec![
                MockTokenResponse::ok("token-1").with_expires_in(None),
                MockTokenResponse::ok("token-2"),
            ])
            .await;
        let provider = oauth_provider(uc_url);

        let first = bearer_token(&provider.get_headers().await.unwrap());
        let second = bearer_token(&provider.get_headers().await.unwrap());

        assert_eq!(first, "token-1");
        assert_eq!(
            second, "token-2",
            "a token with no TTL is not cacheable, so the next call re-mints"
        );
        assert_eq!(oauth.mint_count(), 2);
    }

    #[tokio::test]
    async fn server_error_surfaces_as_retryable() {
        setup_tracing();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        oauth
            .set_responses(vec![MockTokenResponse::error(503, "upstream unavailable")])
            .await;
        let provider = oauth_provider(uc_url);

        let err = provider
            .get_headers()
            .await
            .expect_err("a 5xx response must fail the mint");
        assert!(
            matches!(err, ZerobusError::TokenFetchError(_)),
            "got {err:?}"
        );
        assert!(err.is_retryable(), "a 5xx token error is retryable");
    }

    #[tokio::test]
    async fn client_error_surfaces_as_non_retryable() {
        setup_tracing();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        oauth
            .set_responses(vec![MockTokenResponse::error(401, "invalid_client")])
            .await;
        let provider = oauth_provider(uc_url);

        let err = provider
            .get_headers()
            .await
            .expect_err("a 4xx response must fail the mint");
        assert!(
            matches!(err, ZerobusError::InvalidUCTokenError(_)),
            "got {err:?}"
        );
        assert!(!err.is_retryable(), "a 4xx token error is not retryable");
    }

    #[tokio::test]
    async fn invalidate_forces_a_remint() {
        setup_tracing();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        oauth
            .set_responses(vec![
                MockTokenResponse::ok("token-1"),
                MockTokenResponse::ok("token-2"),
            ])
            .await;
        let provider = oauth_provider(uc_url);

        let first = bearer_token(&provider.get_headers().await.unwrap());
        provider.invalidate().await;
        let second = bearer_token(&provider.get_headers().await.unwrap());

        assert_eq!(first, "token-1");
        assert_eq!(
            second, "token-2",
            "invalidate must drop the rejected token so the next call re-mints"
        );
        assert_eq!(oauth.mint_count(), 2);
    }

    #[tokio::test]
    async fn proactive_refresh_replaces_the_cached_token() {
        setup_tracing();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        // token-1 is short-lived (60s < the 300s refresh window) so the second call
        // proactively refreshes; token-2 is long-lived so the third call is a plain
        // cache hit. This is the successful-refresh path (the hung/backoff tests only
        // exercise a *failing* refresh).
        oauth
            .set_responses(vec![
                MockTokenResponse::ok("token-1").with_expires_in(Some("60")),
                MockTokenResponse::ok("token-2"),
            ])
            .await;
        let provider = oauth_provider(uc_url);

        let first = bearer_token(&provider.get_headers().await.unwrap());
        let second = bearer_token(&provider.get_headers().await.unwrap());
        let third = bearer_token(&provider.get_headers().await.unwrap());

        assert_eq!(first, "token-1", "cold miss mints token-1");
        assert_eq!(
            second, "token-2",
            "the in-window token is proactively refreshed to token-2"
        );
        assert_eq!(
            third, "token-2",
            "the refreshed token is then cached and reused"
        );
        assert_eq!(
            oauth.mint_count(),
            2,
            "a cold mint plus one refresh, and no mint on the third call"
        );
    }

    #[tokio::test]
    async fn dead_on_arrival_token_surfaces_retryable_error() {
        setup_tracing();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        // A 1s token delivered ~1.2s late is already past its start-anchored expiry.
        // On a cold miss (no token to fall back to) it must surface a retryable error
        // rather than be cached.
        oauth
            .set_responses(vec![MockTokenResponse::ok("token-1")
                .with_expires_in(Some("1"))
                .with_delay(Duration::from_millis(1200))])
            .await;
        let provider = oauth_provider(uc_url);

        let err = provider
            .get_headers()
            .await
            .expect_err("a dead-on-arrival token must fail the cold miss");
        assert!(
            matches!(err, ZerobusError::TokenFetchError(_)),
            "got {err:?}"
        );
        assert!(
            err.is_retryable(),
            "a dead-on-arrival token is a retryable fetch error"
        );
        assert_eq!(oauth.mint_count(), 1);
    }

    #[tokio::test]
    async fn dead_on_arrival_refresh_falls_back_to_cached() {
        setup_tracing();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        // Seed a still-valid token in its refresh window; the refresh then returns a
        // dead-on-arrival token (1s TTL, ~1.2s late), so the cache must fall back to
        // the still-valid seed rather than install and serve the DOA token.
        oauth
            .set_responses(vec![
                MockTokenResponse::ok("token-1").with_expires_in(Some("60")),
                MockTokenResponse::ok("token-2")
                    .with_expires_in(Some("1"))
                    .with_delay(Duration::from_millis(1200)),
            ])
            .await;
        let provider = oauth_provider(uc_url);

        let first = bearer_token(&provider.get_headers().await.unwrap());
        let second = bearer_token(&provider.get_headers().await.unwrap());

        assert_eq!(first, "token-1");
        assert_eq!(
            second, "token-1",
            "a dead-on-arrival refresh must fall back to the cached token, not serve the DOA one"
        );
        assert_eq!(
            oauth.mint_count(),
            2,
            "the refresh was attempted, then discarded as dead on arrival"
        );
    }

    #[tokio::test]
    async fn unusable_token_is_rejected() {
        setup_tracing();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        // A token carrying a control character cannot be an HTTP header value, so the
        // factory must reject it before it is ever cached. (The raw string embeds a
        // JSON `\n` escape, so the response parses but the token contains a newline.)
        oauth
            .set_responses(vec![MockTokenResponse::ok(r"bad\ntoken")])
            .await;
        let provider = oauth_provider(uc_url);

        let err = provider
            .get_headers()
            .await
            .expect_err("an unusable token must fail the mint");
        assert!(
            matches!(err, ZerobusError::InvalidUCTokenError(_)),
            "got {err:?}"
        );
        assert!(
            !err.is_retryable(),
            "an unusable token is a non-retryable error"
        );
    }
}

/// The whole chain: SDK stream creation minting from mock UC and connecting to
/// the mock gRPC data plane.
mod stream_creation_tests {
    use super::*;

    /// Number of builds launched together in the concurrency tests. Bumping it
    /// stresses single-flight / backoff harder; the asserted mint counts do not
    /// depend on it (the per-key slot lock serializes the callers).
    const CONCURRENT_BUILDS: usize = 3;

    fn build_sdk(grpc_url: String, uc_url: String) -> ZerobusSdk {
        ZerobusSdk::builder()
            .endpoint(grpc_url)
            .unity_catalog_url(uc_url)
            .tls_config(Arc::new(NoTlsConfig))
            .build()
            .expect("SDK should build")
    }

    /// One `CreateStream` success for each of `count` streams.
    fn create_stream_responses(count: usize) -> Vec<MockResponse> {
        (0..count)
            .map(|i| MockResponse::CreateStream {
                stream_id: format!("s{i}"),
                delay_ms: 0,
            })
            .collect()
    }

    #[tokio::test]
    async fn shared_cache_reuses_token_across_streams() {
        setup_tracing();
        let (grpc, grpc_url) = start_mock_server().await.unwrap();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        oauth
            .set_responses(vec![MockTokenResponse::ok("token-1")])
            .await;
        grpc.inject_responses(
            TABLE_NAME,
            vec![
                MockResponse::CreateStream {
                    stream_id: "s1".to_string(),
                    delay_ms: 0,
                },
                MockResponse::CreateStream {
                    stream_id: "s2".to_string(),
                    delay_ms: 0,
                },
            ],
        )
        .await;
        let sdk = build_sdk(grpc_url, uc_url);

        let first = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .oauth(CLIENT_ID, CLIENT_SECRET)
            .json()
            .recovery(false)
            .build()
            .await;
        assert!(first.is_ok(), "first stream: {:?}", first.err());

        let second = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .oauth(CLIENT_ID, CLIENT_SECRET)
            .json()
            .recovery(false)
            .build()
            .await;
        assert!(second.is_ok(), "second stream: {:?}", second.err());

        assert_eq!(
            oauth.mint_count(),
            1,
            "both streams from one SDK must share a single cached token"
        );
    }

    #[tokio::test]
    async fn auth_rejection_invalidates_and_remints() {
        setup_tracing();
        let (grpc, grpc_url) = start_mock_server().await.unwrap();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        oauth
            .set_responses(vec![
                MockTokenResponse::ok("token-1"),
                MockTokenResponse::ok("token-2"),
            ])
            .await;
        // The server rejects the first attempt's credential and accepts the retry's.
        grpc.inject_responses(
            TABLE_NAME,
            vec![
                MockResponse::Error {
                    status: tonic::Status::unauthenticated("stale token"),
                    delay_ms: 0,
                },
                MockResponse::CreateStream {
                    stream_id: "s1".to_string(),
                    delay_ms: 0,
                },
            ],
        )
        .await;
        let sdk = build_sdk(grpc_url, uc_url);

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .oauth(CLIENT_ID, CLIENT_SECRET)
            .json()
            .recovery(true)
            .recovery_retries(1)
            .recovery_backoff_ms(0)
            .build()
            .await;

        assert!(
            stream.is_ok(),
            "the one-shot auth retry should succeed: {:?}",
            stream.err()
        );
        assert_eq!(
            oauth.mint_count(),
            2,
            "the auth rejection must invalidate the token and re-mint on the retry"
        );
    }

    // Real (not virtual) time: the mint is real socket I/O, so a paused clock
    // would auto-advance to the stream-creation timeout and fire it before the
    // real mint could complete. This mirrors the repo's `test_timeouted_stream_creation`,
    // which also bounds a real connection with a real millisecond budget.
    #[tokio::test]
    async fn hung_refresh_falls_back_to_cached_token() {
        setup_tracing();
        let (grpc, grpc_url) = start_mock_server().await.unwrap();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        // The first mint yields a short-lived token (60s < the 300s refresh
        // buffer, so it is immediately in its refresh window); the proactive
        // refresh triggered by the second stream then hangs and never replies.
        oauth
            .set_responses(vec![
                MockTokenResponse::ok("token-1").with_expires_in(Some("60")),
                MockTokenResponse::Hang,
            ])
            .await;
        grpc.inject_responses(
            TABLE_NAME,
            vec![
                MockResponse::CreateStream {
                    stream_id: "s1".to_string(),
                    delay_ms: 0,
                },
                MockResponse::CreateStream {
                    stream_id: "s2".to_string(),
                    delay_ms: 0,
                },
            ],
        )
        .await;
        // refresh_timeout = recovery_timeout_ms / 2 = 500ms. The 60s cached token
        // outlives that cap, so the bounded refresh applies and can fall back to
        // it; the 1s outer budget leaves room for the fallback plus the connect.
        let sdk = ZerobusSdk::builder()
            .endpoint(grpc_url)
            .unity_catalog_url(uc_url)
            .tls_config(Arc::new(NoTlsConfig))
            .build()
            .expect("SDK should build");

        // First stream: a cold mint of the short-lived token.
        let first = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .oauth(CLIENT_ID, CLIENT_SECRET)
            .json()
            .recovery(false)
            .recovery_timeout_ms(1000)
            .build()
            .await;
        assert!(first.is_ok(), "first stream: {:?}", first.err());
        assert_eq!(oauth.mint_count(), 1);

        // Second stream: the cached token is in its refresh window, so a
        // proactive refresh fires and hangs. The 500ms cap must make the SDK
        // fall back to the still-valid cached token rather than hang.
        let second = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .oauth(CLIENT_ID, CLIENT_SECRET)
            .json()
            .recovery(false)
            .recovery_timeout_ms(1000)
            .build()
            .await;
        assert!(
            second.is_ok(),
            "a hung refresh must fall back to the cached token, not hang: {:?}",
            second.err()
        );
        assert_eq!(
            oauth.mint_count(),
            2,
            "the refresh must be attempted before the fallback"
        );
    }

    #[tokio::test]
    async fn retryable_token_error_is_retried_then_succeeds() {
        setup_tracing();
        let (grpc, grpc_url) = start_mock_server().await.unwrap();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        // The first mint hits a 5xx (retryable); the retry mints successfully.
        oauth
            .set_responses(vec![
                MockTokenResponse::error(503, "token endpoint unavailable"),
                MockTokenResponse::ok("token-1"),
            ])
            .await;
        grpc.inject_responses(
            TABLE_NAME,
            vec![MockResponse::CreateStream {
                stream_id: "s1".to_string(),
                delay_ms: 0,
            }],
        )
        .await;
        let sdk = build_sdk(grpc_url, uc_url);

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .oauth(CLIENT_ID, CLIENT_SECRET)
            .json()
            .recovery(true)
            .recovery_retries(1)
            .recovery_backoff_ms(0)
            .build()
            .await;

        assert!(
            stream.is_ok(),
            "a retryable token error should be retried into a successful mint: {:?}",
            stream.err()
        );
        assert_eq!(
            oauth.mint_count(),
            2,
            "the failed mint and its retry are both hits on the endpoint"
        );
    }

    #[tokio::test]
    async fn non_retryable_token_error_fails_without_retry() {
        setup_tracing();
        // The mint fails before any connection is attempted, so the gRPC endpoint is
        // never contacted and needs no scripted response (the server is kept only to
        // give the SDK a valid endpoint).
        let (_grpc, grpc_url) = start_mock_server().await.unwrap();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        // A 4xx (bad credentials) is non-retryable and is not a server auth
        // rejection, so the retry loop must not re-mint even with a budget.
        oauth
            .set_responses(vec![MockTokenResponse::error(401, "invalid_client")])
            .await;
        let sdk = build_sdk(grpc_url, uc_url);

        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .oauth(CLIENT_ID, CLIENT_SECRET)
            .json()
            .recovery(true)
            .recovery_retries(3)
            .recovery_backoff_ms(0)
            .build()
            .await;

        let err = stream
            .err()
            .expect("a 4xx mint error must fail stream creation");
        assert!(
            matches!(err, ZerobusError::InvalidUCTokenError(_)),
            "got {err:?}"
        );
        assert_eq!(
            oauth.mint_count(),
            1,
            "a non-retryable token error must not be retried"
        );
    }

    #[tokio::test]
    async fn concurrent_stream_creation_mints_once() {
        setup_tracing();
        let (grpc, grpc_url) = start_mock_server().await.unwrap();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        // One token available; the concurrent creations must share it via
        // single-flight rather than each minting one.
        oauth
            .set_responses(vec![MockTokenResponse::ok("token-1")])
            .await;
        grpc.inject_responses(TABLE_NAME, create_stream_responses(CONCURRENT_BUILDS))
            .await;
        let sdk = build_sdk(grpc_url, uc_url);

        // Builds polled together on one task: whichever reaches the per-key slot
        // first mints; the others wait on it and reuse the result.
        let builds: Vec<_> = (0..CONCURRENT_BUILDS)
            .map(|_| {
                sdk.stream_builder()
                    .table(TABLE_NAME)
                    .oauth(CLIENT_ID, CLIENT_SECRET)
                    .json()
                    .recovery(false)
                    .build()
            })
            .collect();
        let results = join_all(builds).await;

        for (i, result) in results.iter().enumerate() {
            assert!(
                result.is_ok(),
                "concurrent stream {i}: {:?}",
                result.as_ref().err()
            );
        }
        assert_eq!(
            oauth.mint_count(),
            1,
            "concurrent creations must single-flight into one mint"
        );
    }

    #[tokio::test]
    async fn refresh_backoff_suppresses_a_concurrent_mint_stampede() {
        setup_tracing();
        let (grpc, grpc_url) = start_mock_server().await.unwrap();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        // Seed a short-lived token (already in its 300s refresh window), then fail
        // the refresh. Only the slot-winner's refresh reaches the endpoint; the rest
        // of the burst is suppressed by the post-fallback backoff. One 503 suffices:
        // if the backoff failed to hold, the extra refreshers would fall through to
        // the mock's default OK response and still push mint_count past 2.
        oauth
            .set_responses(vec![
                MockTokenResponse::ok("token-1").with_expires_in(Some("60")),
                MockTokenResponse::error(503, "token endpoint down"),
            ])
            .await;
        // One CreateStream for the seed, plus one per concurrent build.
        grpc.inject_responses(TABLE_NAME, create_stream_responses(1 + CONCURRENT_BUILDS))
            .await;
        let sdk = build_sdk(grpc_url, uc_url);

        // Seed the cache with the short-lived token.
        let seed = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .oauth(CLIENT_ID, CLIENT_SECRET)
            .json()
            .recovery(false)
            .build()
            .await;
        assert!(seed.is_ok(), "seed stream: {:?}", seed.err());
        assert_eq!(oauth.mint_count(), 1);

        // Concurrent burst: whichever build wins the slot refreshes (503 → falls
        // back to the cached token and arms a ~5s backoff); the others wake inside
        // the backoff window and serve the cached token without refreshing.
        let builds: Vec<_> = (0..CONCURRENT_BUILDS)
            .map(|_| {
                sdk.stream_builder()
                    .table(TABLE_NAME)
                    .oauth(CLIENT_ID, CLIENT_SECRET)
                    .json()
                    .recovery(false)
                    .build()
            })
            .collect();
        let results = join_all(builds).await;

        for (i, result) in results.iter().enumerate() {
            assert!(
                result.is_ok(),
                "burst stream {i}: {:?}",
                result.as_ref().err()
            );
        }
        assert_eq!(
            oauth.mint_count(),
            2,
            "one cold mint plus exactly one failed refresh; backoff must suppress the rest of the burst"
        );
    }

    #[tokio::test]
    async fn repeated_auth_rejection_stops_after_one_remint() {
        setup_tracing();
        let (grpc, grpc_url) = start_mock_server().await.unwrap();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        // Every minted token is rejected. The initial-setup auth retry is one-shot,
        // so the second rejection fails the build rather than re-minting again: the
        // two scripted tokens are the initial mint and its single retry, and the two
        // rejections are all the server ever sends.
        oauth
            .set_responses(vec![
                MockTokenResponse::ok("token-1"),
                MockTokenResponse::ok("token-2"),
            ])
            .await;
        grpc.inject_responses(
            TABLE_NAME,
            vec![
                MockResponse::Error {
                    status: tonic::Status::unauthenticated("stale token"),
                    delay_ms: 0,
                },
                MockResponse::Error {
                    status: tonic::Status::unauthenticated("stale token"),
                    delay_ms: 0,
                },
            ],
        )
        .await;
        let sdk = build_sdk(grpc_url, uc_url);

        // recovery_retries(3) leaves plenty of generic retry budget; the one-shot
        // auth cap — not the budget — is what must stop the retries.
        let stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .oauth(CLIENT_ID, CLIENT_SECRET)
            .json()
            .recovery(true)
            .recovery_retries(3)
            .recovery_backoff_ms(0)
            .build()
            .await;

        let err = stream
            .err()
            .expect("repeated auth rejection must fail stream creation");
        assert!(
            !err.is_retryable(),
            "the surfaced auth rejection is non-retryable: {err:?}"
        );
        assert_eq!(
            oauth.mint_count(),
            2,
            "one-shot auth retry: the initial mint plus exactly one re-mint, then it gives up"
        );
    }

    #[tokio::test]
    async fn rejected_cached_token_forces_next_stream_to_remint() {
        setup_tracing();
        let (grpc, grpc_url) = start_mock_server().await.unwrap();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        oauth
            .set_responses(vec![
                MockTokenResponse::ok("token-1"),
                MockTokenResponse::ok("token-2"),
            ])
            .await;
        // Stream 1 succeeds; stream 2 (reusing the cached token) is rejected as if
        // the token were revoked mid-lifetime; stream 3 succeeds with a fresh token.
        grpc.inject_responses(
            TABLE_NAME,
            vec![
                MockResponse::CreateStream {
                    stream_id: "s1".to_string(),
                    delay_ms: 0,
                },
                MockResponse::Error {
                    status: tonic::Status::unauthenticated("token revoked"),
                    delay_ms: 0,
                },
                MockResponse::CreateStream {
                    stream_id: "s3".to_string(),
                    delay_ms: 0,
                },
            ],
        )
        .await;
        let sdk = build_sdk(grpc_url, uc_url);

        // 1) A successful create mints and caches token-1.
        let first = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .oauth(CLIENT_ID, CLIENT_SECRET)
            .json()
            .recovery(false)
            .build()
            .await;
        assert!(first.is_ok(), "first stream: {:?}", first.err());
        assert_eq!(oauth.mint_count(), 1);

        // 2) The next create reuses the cached token (no mint), but the server
        //    rejects it, so the connection invalidates the shared cache. With
        //    recovery off the stream fails without retrying. `mint_count` staying at
        //    1 is what proves this stream reused the cached token rather than minting.
        let second = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .oauth(CLIENT_ID, CLIENT_SECRET)
            .json()
            .recovery(false)
            .build()
            .await;
        let err = second.err().expect("the rejected stream must fail");
        assert!(
            !err.is_retryable(),
            "auth rejection is non-retryable: {err:?}"
        );
        assert_eq!(
            oauth.mint_count(),
            1,
            "stream 2 must reuse the cached token, not mint a new one"
        );

        // 3) Because the reused token was invalidated, the next create must re-mint.
        let third = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .oauth(CLIENT_ID, CLIENT_SECRET)
            .json()
            .recovery(false)
            .build()
            .await;
        assert!(
            third.is_ok(),
            "third stream should re-mint and succeed: {:?}",
            third.err()
        );
        assert_eq!(
            oauth.mint_count(),
            2,
            "the rejection of the reused cached token must force the next stream to re-mint"
        );
    }

    #[tokio::test]
    async fn rejection_is_scoped_to_its_table() {
        setup_tracing();
        let (grpc, grpc_url) = start_mock_server().await.unwrap();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        // Two tables share one SDK cache (distinct keys). A rejection on table A must
        // invalidate only A's entry, leaving table B's cached token intact.
        oauth
            .set_responses(vec![
                MockTokenResponse::ok("token-a"),
                MockTokenResponse::ok("token-b"),
            ])
            .await;
        grpc.inject_responses(
            TABLE_NAME,
            vec![
                MockResponse::CreateStream {
                    stream_id: "a1".to_string(),
                    delay_ms: 0,
                },
                MockResponse::Error {
                    status: tonic::Status::unauthenticated("token revoked"),
                    delay_ms: 0,
                },
            ],
        )
        .await;
        grpc.inject_responses(
            OTHER_TABLE,
            vec![
                MockResponse::CreateStream {
                    stream_id: "b1".to_string(),
                    delay_ms: 0,
                },
                MockResponse::CreateStream {
                    stream_id: "b2".to_string(),
                    delay_ms: 0,
                },
            ],
        )
        .await;
        let sdk = build_sdk(grpc_url, uc_url);

        // Seed one cached token per table.
        let a1 = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .oauth(CLIENT_ID, CLIENT_SECRET)
            .json()
            .recovery(false)
            .build()
            .await;
        assert!(a1.is_ok(), "table A stream 1: {:?}", a1.err());
        let b1 = sdk
            .stream_builder()
            .table(OTHER_TABLE)
            .oauth(CLIENT_ID, CLIENT_SECRET)
            .json()
            .recovery(false)
            .build()
            .await;
        assert!(b1.is_ok(), "table B stream 1: {:?}", b1.err());
        assert_eq!(oauth.mint_count(), 2, "one mint per table");

        // A stream on table A reuses A's cached token, is rejected, and invalidates
        // only A's cache entry.
        let a2 = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .oauth(CLIENT_ID, CLIENT_SECRET)
            .json()
            .recovery(false)
            .build()
            .await;
        assert!(a2.is_err(), "table A stream 2 must be rejected");

        // Table B's cached token must be untouched, so a later B stream reuses it.
        let b2 = sdk
            .stream_builder()
            .table(OTHER_TABLE)
            .oauth(CLIENT_ID, CLIENT_SECRET)
            .json()
            .recovery(false)
            .build()
            .await;
        assert!(
            b2.is_ok(),
            "table B stream 2 should reuse B's cached token: {:?}",
            b2.err()
        );
        assert_eq!(
            oauth.mint_count(),
            2,
            "table B must not re-mint: a rejection on table A is scoped to A's cache entry"
        );
    }
}
