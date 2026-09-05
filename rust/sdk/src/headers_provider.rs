use crate::default_token_factory::DefaultTokenFactory;
use crate::token_cache::{TokenCache, DEFAULT_REFRESH_BUFFER};
use crate::ZerobusResult;
use async_trait::async_trait;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// A trait for providing custom headers for gRPC requests.
///
/// This trait allows you to implement custom logic for generating authentication headers,
/// such as fetching tokens from different OAuth providers or using alternative
/// authentication mechanisms.
///
/// The HTTP `user-agent` header is set by the SDK on the underlying tonic
/// `Endpoint` and cannot be overridden by values returned from `get_headers`.
/// Use [`ZerobusSdkBuilder::application_name`](crate::ZerobusSdkBuilder::application_name)
/// to customize it.
///
/// # Examples
///
/// ```no_run
/// # use databricks_zerobus_ingest_sdk::{HeadersProvider, ZerobusResult};
/// # use std::collections::HashMap;
/// # use async_trait::async_trait;
///
/// struct MyCustomAuthProvider;
///
/// #[async_trait]
/// impl HeadersProvider for MyCustomAuthProvider {
///     async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
///         let mut headers = HashMap::new();
///         headers.insert("some_key", "some_value".to_string());
///         Ok(headers)
///     }
/// }
/// ```
#[async_trait]
pub trait HeadersProvider: Send + Sync {
    /// Asynchronously gets the headers for a request.
    ///
    /// # Returns
    ///
    /// A `ZerobusResult` containing a `HashMap` of header names and values.
    ///
    /// # Errors
    ///
    /// Returns a `ZerobusError` if header generation fails (e.g., token request fails).
    async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>>;

    /// Invalidates cached authentication state that the server just rejected.
    ///
    /// The SDK calls this when the server rejects the supplied credentials with an
    /// authentication error during stream creation. The default is a no-op, which is
    /// correct for providers that hold no cache. The built-in OAuth provider clears
    /// the rejected token from its cache, so the next `get_headers` re-mints — unless
    /// a concurrent refresh already replaced it with a newer token, which is kept and
    /// served without re-minting.
    async fn invalidate(&self) {}
}

/// The default headers provider that uses OAuth 2.0 with Unity Catalog.
///
/// This provider implements the OAuth 2.0 client credentials flow to obtain
/// access tokens for authenticating with the Zerobus service.
pub struct OAuthHeadersProvider {
    client_id: String,
    client_secret: String,
    table_name: String,
    workspace_id: String,
    unity_catalog_url: String,
    token_cache: Arc<TokenCache>,
    /// How long a proactive refresh may run before it's treated as failed and the
    /// cached token is served; `None` leaves it unbounded.
    refresh_timeout: Option<Duration>,
    /// Highest token generation `get_headers` has served, so `invalidate` rejects a
    /// token this provider actually used. Stays 0 until a cacheable token is served,
    /// and `invalidate` treats 0 as nothing to reject.
    last_served_generation: AtomicU64,
}

impl OAuthHeadersProvider {
    /// Creates a new `OAuthHeadersProvider`.
    ///
    /// This standalone constructor caches tokens for the lifetime of the
    /// returned provider only. When streams are created via
    /// [`ZerobusSdk::stream_builder`](crate::ZerobusSdk::stream_builder) the SDK
    /// supplies a shared cache so tokens are reused across streams; see
    /// [`with_cache`](Self::with_cache).
    pub fn new(
        client_id: String,
        client_secret: String,
        table_name: String,
        workspace_id: String,
        unity_catalog_url: String,
    ) -> Self {
        Self::with_cache(
            client_id,
            client_secret,
            table_name,
            workspace_id,
            unity_catalog_url,
            Arc::new(TokenCache::new(true, DEFAULT_REFRESH_BUFFER)),
            None,
        )
    }

    /// Creates a new `OAuthHeadersProvider` backed by a shared token cache.
    ///
    /// Used internally so all streams created from one `ZerobusSdk` reuse cached
    /// tokens rather than minting a fresh one per stream.
    pub(crate) fn with_cache(
        client_id: String,
        client_secret: String,
        table_name: String,
        workspace_id: String,
        unity_catalog_url: String,
        token_cache: Arc<TokenCache>,
        refresh_timeout: Option<Duration>,
    ) -> Self {
        Self {
            client_id,
            client_secret,
            table_name,
            workspace_id,
            unity_catalog_url,
            token_cache,
            refresh_timeout,
            last_served_generation: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl HeadersProvider for OAuthHeadersProvider {
    async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
        let fetch = |reason| {
            DefaultTokenFactory::fetch_token(
                &self.unity_catalog_url,
                &self.table_name,
                &self.client_id,
                &self.client_secret,
                &self.workspace_id,
                reason,
            )
        };
        let (token, generation) = match self.refresh_timeout {
            Some(refresh_timeout) => {
                self.token_cache
                    .get_or_fetch_within(
                        &self.client_id,
                        &self.client_secret,
                        &self.table_name,
                        refresh_timeout,
                        fetch,
                    )
                    .await?
            }
            None => {
                self.token_cache
                    .get_or_fetch(
                        &self.client_id,
                        &self.client_secret,
                        &self.table_name,
                        fetch,
                    )
                    .await?
            }
        };
        // Remember the highest served generation so invalidate() rejects it.
        self.last_served_generation
            .fetch_max(generation, Ordering::SeqCst);
        let mut headers = HashMap::new();
        headers.insert("authorization", format!("Bearer {}", token));
        headers.insert("x-databricks-zerobus-table-name", self.table_name.clone());
        Ok(headers)
    }

    async fn invalidate(&self) {
        let rejected_generation = self.last_served_generation.load(Ordering::SeqCst);
        self.token_cache
            .invalidate(
                &self.client_id,
                &self.client_secret,
                &self.table_name,
                rejected_generation,
            )
            .await;
    }
}

/// An async callback that yields the current external IdP token (for example an
/// Entra ID / OIDC JWT).
///
/// The federated auth mode takes a supplier callback rather than a bare token
/// on purpose: an external IdP token is short-lived (typically ~1 hour), so a
/// bare token would strand the stream at the token's expiry with no way to
/// refresh. The supplier is invoked only when a fresh Databricks token must be
/// minted (a cache miss or a proactive refresh), never on every request, so a
/// cache hit incurs neither the callback nor the exchange. A caller that truly
/// holds a static token can wrap it in a trivial closure.
pub type IdpTokenSupplier =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ZerobusResult<String>> + Send>> + Send + Sync>;

/// A headers provider that federates an external IdP token into a Zerobus-scoped
/// Databricks token via the RFC 8693 token-exchange grant.
///
/// This is the first-class implementation of external-IdP (e.g. Entra ID)
/// federation. It supports the two supported federation modes through a single
/// `client_id` toggle:
///
/// * **Account-level federation** (`client_id = None`): no Databricks-managed
///   service principal. The exchanged token's subject is resolved to an
///   identity synced into Databricks via Automatic Identity Management (SCIM).
/// * **Workload identity federation** (`client_id = Some(sp_id)`): a Databricks
///   service principal with a client_id and no secret, with a federation policy
///   attached. The exchange request names the service principal via `client_id`.
///
/// It obtains the current IdP token from an [`IdpTokenSupplier`], performs the
/// exchange with the same request shaping as the client-credentials path, and
/// caches the exchanged Databricks token in the shared [`TokenCache`] keyed by
/// `(client_id-or-none, table)` so account-level and workload modes (and
/// distinct service principals) cache independently.
pub struct FederatedTokenProvider {
    /// The Databricks service principal client_id for workload identity
    /// federation, or `None` for account-level federation.
    client_id: Option<String>,
    idp_token_supplier: IdpTokenSupplier,
    table_name: String,
    workspace_id: String,
    unity_catalog_url: String,
    token_cache: Arc<TokenCache>,
    /// Opaque partition for the shared cache under account-level federation, so
    /// two distinct identities used from one SDK instance do not collide on the
    /// `("", "", table)` key and serve each other's token. `None` falls back to
    /// sharing by table. Unused for workload identity federation, which already
    /// keys by `client_id`.
    cache_key: Option<String>,
    /// How long a proactive refresh may run before it's treated as failed and the
    /// cached token is served; `None` leaves it unbounded.
    refresh_timeout: Option<Duration>,
    /// Highest token generation `get_headers` has served, so `invalidate` rejects a
    /// token this provider actually used. Stays 0 until a cacheable token is served,
    /// and `invalidate` treats 0 as nothing to reject.
    last_served_generation: AtomicU64,
}

impl FederatedTokenProvider {
    /// Creates a new `FederatedTokenProvider`.
    ///
    /// This standalone constructor caches tokens for the lifetime of the
    /// returned provider only. When streams are created via
    /// [`ZerobusSdk::stream_builder`](crate::ZerobusSdk::stream_builder) the SDK
    /// supplies a shared cache so tokens are reused across streams; see
    /// [`with_cache`](Self::with_cache).
    pub fn new(
        client_id: Option<String>,
        idp_token_supplier: IdpTokenSupplier,
        table_name: String,
        workspace_id: String,
        unity_catalog_url: String,
    ) -> Self {
        Self::with_cache(
            client_id,
            idp_token_supplier,
            table_name,
            workspace_id,
            unity_catalog_url,
            Arc::new(TokenCache::new(true, DEFAULT_REFRESH_BUFFER)),
            None,
            None,
        )
    }

    /// Creates a new `FederatedTokenProvider` backed by a shared token cache.
    ///
    /// Used internally so all streams created from one `ZerobusSdk` reuse cached
    /// exchanged tokens rather than re-exchanging per stream.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn with_cache(
        client_id: Option<String>,
        idp_token_supplier: IdpTokenSupplier,
        table_name: String,
        workspace_id: String,
        unity_catalog_url: String,
        token_cache: Arc<TokenCache>,
        refresh_timeout: Option<Duration>,
        cache_key: Option<String>,
    ) -> Self {
        Self {
            client_id,
            idp_token_supplier,
            table_name,
            workspace_id,
            unity_catalog_url,
            token_cache,
            cache_key,
            refresh_timeout,
            last_served_generation: AtomicU64::new(0),
        }
    }

    /// The cache key's client-id component. For workload identity federation it
    /// is the service principal id, so distinct principals cache independently.
    /// For account-level federation there is no client_id, so it is the caller's
    /// `cache_key` when set (isolating distinct identities used from one SDK
    /// instance), or the empty string otherwise (sharing by table). There is no
    /// secret in either mode, so the secret component of the key is always empty.
    fn cache_client_id(&self) -> &str {
        match self.client_id.as_deref() {
            Some(client_id) => client_id,
            None => self.cache_key.as_deref().unwrap_or(""),
        }
    }
}

#[async_trait]
impl HeadersProvider for FederatedTokenProvider {
    async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
        let fetch = |reason| async move {
            // Only reached on a cache miss/refresh: fetch the current IdP token,
            // then exchange it for a Zerobus-scoped Databricks token.
            let idp_token = (self.idp_token_supplier)().await?;
            DefaultTokenFactory::fetch_exchanged_token(
                &self.unity_catalog_url,
                &self.table_name,
                self.client_id.as_deref(),
                &idp_token,
                &self.workspace_id,
                reason,
            )
            .await
        };
        let (token, generation) = match self.refresh_timeout {
            Some(refresh_timeout) => {
                self.token_cache
                    .get_or_fetch_within(
                        self.cache_client_id(),
                        "",
                        &self.table_name,
                        refresh_timeout,
                        fetch,
                    )
                    .await?
            }
            None => {
                self.token_cache
                    .get_or_fetch(self.cache_client_id(), "", &self.table_name, fetch)
                    .await?
            }
        };
        // Remember the highest served generation so invalidate() rejects it.
        self.last_served_generation
            .fetch_max(generation, Ordering::SeqCst);
        let mut headers = HashMap::new();
        headers.insert("authorization", format!("Bearer {}", token));
        headers.insert("x-databricks-zerobus-table-name", self.table_name.clone());
        Ok(headers)
    }

    async fn invalidate(&self) {
        let rejected_generation = self.last_served_generation.load(Ordering::SeqCst);
        self.token_cache
            .invalidate(
                self.cache_client_id(),
                "",
                &self.table_name,
                rejected_generation,
            )
            .await;
    }
}

/// A headers provider that returns no headers.
///
/// Intended only for local testing against a Zerobus endpoint not enforcing authentication.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "testing")] {
/// use databricks_zerobus_ingest_sdk::NoAuthHeadersProvider;
/// use std::sync::Arc;
///
/// // Pass directly to `headers_provider()`, or use the `.no_auth()` shorthand on `StreamBuilder`.
/// let _provider: Arc<NoAuthHeadersProvider> = Arc::new(NoAuthHeadersProvider);
/// # }
/// ```
#[cfg(feature = "testing")]
pub struct NoAuthHeadersProvider;

#[cfg(feature = "testing")]
#[async_trait]
impl HeadersProvider for NoAuthHeadersProvider {
    async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
        Ok(HashMap::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A minimal blocking HTTP mock of the UC `/oidc/v1/token` endpoint. It runs
    /// on its own OS thread (blocking std IO) so the async test can drive the
    /// reqwest-based exchange against it. Each request is answered with a fresh
    /// `dbx-token-<n>` so tests can tell a real mint from a cache hit, and every
    /// request body is captured for assertions on the request shape.
    struct MockTokenEndpoint {
        base_url: String,
        request_bodies: Arc<std::sync::Mutex<Vec<String>>>,
        mint_count: Arc<AtomicUsize>,
    }

    impl MockTokenEndpoint {
        fn start() -> Self {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let base_url = format!("http://{}", listener.local_addr().unwrap());
            let request_bodies = Arc::new(std::sync::Mutex::new(Vec::new()));
            let mint_count = Arc::new(AtomicUsize::new(0));

            let bodies = Arc::clone(&request_bodies);
            let count = Arc::clone(&mint_count);
            // Detached daemon thread: it serves connections for the lifetime of
            // the test process. Bounded accept is deliberately avoided so that a
            // caching regression (too many mints) fails an assertion rather than
            // deadlocking on a missing connection.
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let Ok(mut stream) = stream else { continue };
                    let body = read_http_body(&mut stream);
                    let n = count.fetch_add(1, Ordering::SeqCst);
                    bodies.lock().unwrap().push(body);
                    write_json_response(&mut stream, &format!("dbx-token-{n}"));
                }
            });

            Self {
                base_url,
                request_bodies,
                mint_count,
            }
        }

        fn mint_count(&self) -> usize {
            self.mint_count.load(Ordering::SeqCst)
        }

        fn last_request_body(&self) -> String {
            self.request_bodies.lock().unwrap().last().cloned().unwrap()
        }
    }

    fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack.windows(needle.len()).position(|w| w == needle)
    }

    fn read_http_body(stream: &mut std::net::TcpStream) -> String {
        use std::io::Read;
        let mut buf = Vec::new();
        let mut tmp = [0u8; 2048];
        loop {
            let n = stream.read(&mut tmp).unwrap_or(0);
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
            if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
                let header_str = String::from_utf8_lossy(&buf[..pos]).to_string();
                let content_length = header_str
                    .lines()
                    .find_map(|line| {
                        let lower = line.to_ascii_lowercase();
                        lower
                            .strip_prefix("content-length:")
                            .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                    })
                    .unwrap_or(0);
                let body_start = pos + 4;
                while buf.len() < body_start + content_length {
                    let n = stream.read(&mut tmp).unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                }
                let end = (body_start + content_length).min(buf.len());
                return String::from_utf8_lossy(&buf[body_start..end]).to_string();
            }
        }
        String::new()
    }

    fn write_json_response(stream: &mut std::net::TcpStream, access_token: &str) {
        use std::io::Write;
        let body = format!(r#"{{"access_token":"{access_token}","expires_in":3600}}"#);
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = stream.write_all(response.as_bytes());
        let _ = stream.flush();
    }

    /// Builds an [`IdpTokenSupplier`] that returns `token` and counts its calls,
    /// so tests can assert the supplier is invoked only on a real mint.
    fn counting_supplier(token: &'static str, calls: Arc<AtomicUsize>) -> IdpTokenSupplier {
        Arc::new(move || {
            let calls = Arc::clone(&calls);
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(token.to_string())
            })
        })
    }

    const TOKEN_EXCHANGE_GRANT_ENCODED: &str =
        "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange";

    #[tokio::test]
    async fn account_level_exchanges_and_returns_headers() {
        let mock = MockTokenEndpoint::start();
        let idp_calls = Arc::new(AtomicUsize::new(0));
        let provider = FederatedTokenProvider::new(
            None,
            counting_supplier("entra-jwt", Arc::clone(&idp_calls)),
            "cat.sch.tbl".to_string(),
            "12345".to_string(),
            mock.base_url.clone(),
        );

        let headers = provider.get_headers().await.unwrap();

        assert_eq!(headers.get("authorization").unwrap(), "Bearer dbx-token-0");
        assert_eq!(
            headers.get("x-databricks-zerobus-table-name").unwrap(),
            "cat.sch.tbl"
        );
        assert_eq!(mock.mint_count(), 1);
        assert_eq!(idp_calls.load(Ordering::SeqCst), 1);

        // The exchange request carried the RFC 8693 grant and the IdP token, and
        // omitted client_id (account-level federation).
        let body = mock.last_request_body();
        assert!(body.contains(TOKEN_EXCHANGE_GRANT_ENCODED), "body: {body}");
        assert!(body.contains("subject_token=entra-jwt"), "body: {body}");
        assert!(!body.contains("client_id="), "body: {body}");
    }

    #[tokio::test]
    async fn workload_identity_sends_client_id() {
        let mock = MockTokenEndpoint::start();
        let idp_calls = Arc::new(AtomicUsize::new(0));
        let provider = FederatedTokenProvider::new(
            Some("sp-uuid".to_string()),
            counting_supplier("entra-jwt", Arc::clone(&idp_calls)),
            "cat.sch.tbl".to_string(),
            "12345".to_string(),
            mock.base_url.clone(),
        );

        provider.get_headers().await.unwrap();

        let body = mock.last_request_body();
        assert!(body.contains(TOKEN_EXCHANGE_GRANT_ENCODED), "body: {body}");
        assert!(body.contains("client_id=sp-uuid"), "body: {body}");
    }

    #[tokio::test]
    async fn caches_exchanged_token_across_calls() {
        let mock = MockTokenEndpoint::start();
        let idp_calls = Arc::new(AtomicUsize::new(0));
        let provider = FederatedTokenProvider::new(
            None,
            counting_supplier("entra-jwt", Arc::clone(&idp_calls)),
            "cat.sch.tbl".to_string(),
            "12345".to_string(),
            mock.base_url.clone(),
        );

        let first = provider.get_headers().await.unwrap();
        let second = provider.get_headers().await.unwrap();

        assert_eq!(first.get("authorization"), second.get("authorization"));
        assert_eq!(mock.mint_count(), 1, "second call must reuse cached token");
        assert_eq!(
            idp_calls.load(Ordering::SeqCst),
            1,
            "IdP supplier must not be called on a cache hit"
        );
    }

    #[tokio::test]
    async fn invalidate_forces_remint() {
        let mock = MockTokenEndpoint::start();
        let idp_calls = Arc::new(AtomicUsize::new(0));
        let provider = FederatedTokenProvider::new(
            None,
            counting_supplier("entra-jwt", Arc::clone(&idp_calls)),
            "cat.sch.tbl".to_string(),
            "12345".to_string(),
            mock.base_url.clone(),
        );

        let first = provider.get_headers().await.unwrap();
        provider.invalidate().await;
        let second = provider.get_headers().await.unwrap();

        assert_eq!(first.get("authorization").unwrap(), "Bearer dbx-token-0");
        assert_eq!(second.get("authorization").unwrap(), "Bearer dbx-token-1");
        assert_eq!(mock.mint_count(), 2, "invalidate must force a re-mint");
        assert_eq!(idp_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn account_level_and_workload_cache_independently() {
        let mock = MockTokenEndpoint::start();
        // A shared cache, as the SDK supplies to every stream from one instance.
        let cache = Arc::new(TokenCache::new(true, DEFAULT_REFRESH_BUFFER));

        let account_level = FederatedTokenProvider::with_cache(
            None,
            counting_supplier("entra-jwt", Arc::new(AtomicUsize::new(0))),
            "cat.sch.tbl".to_string(),
            "12345".to_string(),
            mock.base_url.clone(),
            Arc::clone(&cache),
            None,
            None,
        );
        let workload = FederatedTokenProvider::with_cache(
            Some("sp-uuid".to_string()),
            counting_supplier("entra-jwt", Arc::new(AtomicUsize::new(0))),
            "cat.sch.tbl".to_string(),
            "12345".to_string(),
            mock.base_url.clone(),
            Arc::clone(&cache),
            None,
            None,
        );

        // Same table, same shared cache, but different client_id => two mints.
        account_level.get_headers().await.unwrap();
        workload.get_headers().await.unwrap();
        assert_eq!(
            mock.mint_count(),
            2,
            "account-level and workload modes must key independently"
        );

        // Each then serves its own cached token.
        account_level.get_headers().await.unwrap();
        workload.get_headers().await.unwrap();
        assert_eq!(mock.mint_count(), 2, "both must now be cache hits");
    }

    #[tokio::test]
    async fn account_level_isolates_by_cache_key() {
        let mock = MockTokenEndpoint::start();
        // One shared SDK cache, two account-level identities (no client_id) on the
        // SAME table. Distinct cache_keys must keep them from colliding on the
        // ("", "", table) slot, so the second is not served the first's token.
        let cache = Arc::new(TokenCache::new(true, DEFAULT_REFRESH_BUFFER));
        let identity_a = FederatedTokenProvider::with_cache(
            None,
            counting_supplier("entra-jwt-a", Arc::new(AtomicUsize::new(0))),
            "cat.sch.tbl".to_string(),
            "12345".to_string(),
            mock.base_url.clone(),
            Arc::clone(&cache),
            None,
            Some("identity-a".to_string()),
        );
        let identity_b = FederatedTokenProvider::with_cache(
            None,
            counting_supplier("entra-jwt-b", Arc::new(AtomicUsize::new(0))),
            "cat.sch.tbl".to_string(),
            "12345".to_string(),
            mock.base_url.clone(),
            Arc::clone(&cache),
            None,
            Some("identity-b".to_string()),
        );

        // Different cache_keys => two independent mints, no cross-identity hit.
        identity_a.get_headers().await.unwrap();
        identity_b.get_headers().await.unwrap();
        assert_eq!(
            mock.mint_count(),
            2,
            "distinct account-level identities must not share a cached token"
        );

        // A third provider reusing identity-a's cache_key shares the cached token.
        let identity_a_again = FederatedTokenProvider::with_cache(
            None,
            counting_supplier("entra-jwt-a", Arc::new(AtomicUsize::new(0))),
            "cat.sch.tbl".to_string(),
            "12345".to_string(),
            mock.base_url.clone(),
            Arc::clone(&cache),
            None,
            Some("identity-a".to_string()),
        );
        identity_a_again.get_headers().await.unwrap();
        assert_eq!(
            mock.mint_count(),
            2,
            "same cache_key must reuse the cached token (no new mint)"
        );
    }

    #[tokio::test]
    async fn supplier_error_propagates_and_is_not_cached() {
        // A supplier whose token fetch fails (e.g. the external IdP rejected the
        // credentials). The error must surface from get_headers, and because
        // nothing was cached, a subsequent call must invoke the supplier again
        // rather than serving a stale/absent token. No network is used: the
        // supplier fails before the exchange is ever attempted.
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_in_cb = Arc::clone(&calls);
        let supplier: IdpTokenSupplier = Arc::new(move || {
            let calls = Arc::clone(&calls_in_cb);
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(crate::ZerobusError::InvalidUCTokenError(
                    "external IdP token fetch failed".to_string(),
                ))
            })
        });
        let provider = FederatedTokenProvider::new(
            None,
            supplier,
            "cat.sch.tbl".to_string(),
            "12345".to_string(),
            "http://127.0.0.1:1".to_string(),
        );

        assert!(
            provider.get_headers().await.is_err(),
            "first call must error"
        );
        assert!(
            provider.get_headers().await.is_err(),
            "second call must error too"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "a failed mint must not be cached; the supplier is retried each call"
        );
    }
}
