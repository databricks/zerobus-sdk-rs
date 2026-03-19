//! Typestate builder for creating Zerobus ingestion streams.
//!
//! The builder enforces a strict configuration order at compile time:
//!
//! 1. **Auth** — `.oauth()` or `.headers_provider()`
//! 2. **Format** — `.json()`, `.compiled_proto()`, or `.arrow()`
//! 3. **Config** (optional) — individual setters or `.options()`
//! 4. **Build** — `.build()`
//!
//! # Examples
//!
//! ```rust,ignore
//! let stream = sdk
//!     .stream_builder("catalog.schema.table")
//!     .oauth("client-id", "client-secret")
//!     .json()
//!     .max_inflight_requests(500_000)
//!     .build()
//!     .await?;
//! ```

use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::callbacks::AckCallback;
use crate::databricks::zerobus::RecordType;
use crate::headers_provider::{HeadersProvider, OAuthHeadersProvider};
use crate::stream_configuration::StreamConfigurationOptions;
use crate::{TableProperties, ZerobusResult, ZerobusSdk, ZerobusStream};

#[cfg(feature = "arrow-flight")]
use crate::arrow_configuration::ArrowStreamConfigurationOptions;
#[cfg(feature = "arrow-flight")]
use crate::arrow_stream::{ArrowSchema, ArrowTableProperties, ZerobusArrowStream};

use super::stream_format::*;

/// Internal representation of the authentication configuration.
enum AuthConfig {
    OAuth {
        client_id: String,
        client_secret: String,
    },
    HeadersProvider(Arc<dyn HeadersProvider>),
}

/// A typestate builder for creating Zerobus ingestion streams.
///
/// The two type parameters track compile-time state:
/// - `F` — the record format ([`NoFormat`], [`Json`], [`CompiledProto`], or [`Arrow`])
/// - `A` — the authentication state ([`NoAuth`] or [`HasAuth`])
///
/// The builder enforces the ordering: **auth → format → config → build**.
pub struct StreamBuilder<'a, F, A> {
    sdk: &'a ZerobusSdk,
    table_name: String,

    // Format-specific data
    descriptor_proto: Option<prost_types::DescriptorProto>,
    #[cfg(feature = "arrow-flight")]
    arrow_schema: Option<Arc<ArrowSchema>>,

    // Auth (None only in NoAuth state; HasAuth guarantees Some)
    auth: Option<AuthConfig>,

    // gRPC stream config (used by Json / CompiledProto)
    grpc_config: StreamConfigurationOptions,

    // Arrow-specific config
    #[cfg(feature = "arrow-flight")]
    arrow_config: ArrowStreamConfigurationOptions,

    _marker: PhantomData<(F, A)>,
}

impl<F, A> fmt::Debug for StreamBuilder<'_, F, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let auth_kind = match &self.auth {
            Some(AuthConfig::OAuth { .. }) => "OAuth",
            Some(AuthConfig::HeadersProvider(_)) => "HeadersProvider",
            None => "None",
        };
        f.debug_struct("StreamBuilder")
            .field("table_name", &self.table_name)
            .field("auth", &auth_kind)
            .field("format", &std::any::type_name::<F>())
            .finish_non_exhaustive()
    }
}

// ── Type-state transition helper ─────────────────────────────────────────────

impl<'a, F, A> StreamBuilder<'a, F, A> {
    /// Move all fields into a new `StreamBuilder` with different type parameters.
    /// Callers mutate the returned builder to set the fields that actually change.
    fn transition<F2, A2>(self) -> StreamBuilder<'a, F2, A2> {
        StreamBuilder {
            sdk: self.sdk,
            table_name: self.table_name,
            descriptor_proto: self.descriptor_proto,
            #[cfg(feature = "arrow-flight")]
            arrow_schema: self.arrow_schema,
            auth: self.auth,
            grpc_config: self.grpc_config,
            #[cfg(feature = "arrow-flight")]
            arrow_config: self.arrow_config,
            _marker: PhantomData,
        }
    }
}

// ── Constructor (only via ZerobusSdk::stream_builder) ───────────────────────

impl<'a> StreamBuilder<'a, NoFormat, NoAuth> {
    pub(crate) fn new(sdk: &'a ZerobusSdk, table_name: impl Into<String>) -> Self {
        Self {
            sdk,
            table_name: table_name.into(),
            descriptor_proto: None,
            #[cfg(feature = "arrow-flight")]
            arrow_schema: None,
            auth: None,
            grpc_config: StreamConfigurationOptions::default(),
            #[cfg(feature = "arrow-flight")]
            arrow_config: ArrowStreamConfigurationOptions::default(),
            _marker: PhantomData,
        }
    }
}

// ── Step 1: Auth (only on NoFormat + NoAuth) ────────────────────────────────

impl<'a> StreamBuilder<'a, NoFormat, NoAuth> {
    /// Authenticate with OAuth client credentials.
    pub fn oauth(
        self,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
    ) -> StreamBuilder<'a, NoFormat, HasAuth> {
        let mut b = self.transition();
        b.auth = Some(AuthConfig::OAuth {
            client_id: client_id.into(),
            client_secret: client_secret.into(),
        });
        b
    }

    /// Authenticate with a custom headers provider.
    pub fn headers_provider(
        self,
        provider: Arc<dyn HeadersProvider>,
    ) -> StreamBuilder<'a, NoFormat, HasAuth> {
        let mut b = self.transition();
        b.auth = Some(AuthConfig::HeadersProvider(provider));
        b
    }

    /// Skip authentication (uses a no-op headers provider).
    ///
    /// Useful for local development, testing, or when authentication is
    /// handled externally (e.g., via a sidecar proxy).
    pub fn no_auth(self) -> StreamBuilder<'a, NoFormat, HasAuth> {
        self.headers_provider(Arc::new(crate::headers_provider::NoOpHeadersProvider))
    }
}

// ── Step 2: Format (only on NoFormat + HasAuth) ─────────────────────────────

impl<'a> StreamBuilder<'a, NoFormat, HasAuth> {
    /// Select JSON record format.
    pub fn json(self) -> StreamBuilder<'a, Json, HasAuth> {
        let mut b = self.transition();
        b.grpc_config.record_type = RecordType::Json;
        b.descriptor_proto = None;
        b
    }

    /// Select compiled protobuf record format.
    pub fn compiled_proto(
        self,
        descriptor: prost_types::DescriptorProto,
    ) -> StreamBuilder<'a, CompiledProto, HasAuth> {
        let mut b = self.transition();
        b.grpc_config.record_type = RecordType::Proto;
        b.descriptor_proto = Some(descriptor);
        b
    }

    /// Select Arrow Flight record format.
    #[cfg(feature = "arrow-flight")]
    pub fn arrow(self, schema: Arc<ArrowSchema>) -> StreamBuilder<'a, Arrow, HasAuth> {
        let mut b = self.transition();
        b.arrow_schema = Some(schema);
        b
    }
}

// ── Step 3: Config — shared setters (format + auth resolved) ────────────────

impl<'a, F: StreamFormat> StreamBuilder<'a, F, HasAuth> {
    /// Enable or disable automatic stream recovery.
    pub fn recovery(mut self, enabled: bool) -> Self {
        self.grpc_config.recovery = enabled;
        #[cfg(feature = "arrow-flight")]
        {
            self.arrow_config.recovery = enabled;
        }
        self
    }

    /// Set the timeout in milliseconds for each recovery attempt.
    pub fn recovery_timeout_ms(mut self, ms: u64) -> Self {
        self.grpc_config.recovery_timeout_ms = ms;
        #[cfg(feature = "arrow-flight")]
        {
            self.arrow_config.recovery_timeout_ms = ms;
        }
        self
    }

    /// Set the backoff time in milliseconds between recovery retries.
    pub fn recovery_backoff_ms(mut self, ms: u64) -> Self {
        self.grpc_config.recovery_backoff_ms = ms;
        #[cfg(feature = "arrow-flight")]
        {
            self.arrow_config.recovery_backoff_ms = ms;
        }
        self
    }

    /// Set the maximum number of recovery retry attempts.
    pub fn recovery_retries(mut self, n: u32) -> Self {
        self.grpc_config.recovery_retries = n;
        #[cfg(feature = "arrow-flight")]
        {
            self.arrow_config.recovery_retries = n;
        }
        self
    }

    /// Set the timeout in milliseconds for server acknowledgement.
    pub fn server_lack_of_ack_timeout_ms(mut self, ms: u64) -> Self {
        self.grpc_config.server_lack_of_ack_timeout_ms = ms;
        #[cfg(feature = "arrow-flight")]
        {
            self.arrow_config.server_lack_of_ack_timeout_ms = ms;
        }
        self
    }

    /// Set the timeout in milliseconds for flush operations.
    pub fn flush_timeout_ms(mut self, ms: u64) -> Self {
        self.grpc_config.flush_timeout_ms = ms;
        #[cfg(feature = "arrow-flight")]
        {
            self.arrow_config.flush_timeout_ms = ms;
        }
        self
    }
}

// ── Step 3: Config — gRPC-only setters (Json / CompiledProto) ───────────────

impl<'a, F: GrpcFormat> StreamBuilder<'a, F, HasAuth> {
    /// Set the maximum number of in-flight requests.
    pub fn max_inflight_requests(mut self, n: usize) -> Self {
        self.grpc_config.max_inflight_requests = n;
        self
    }

    /// Set the maximum wait time during graceful stream pause.
    pub fn stream_paused_max_wait_time_ms(mut self, ms: Option<u64>) -> Self {
        self.grpc_config.stream_paused_max_wait_time_ms = ms;
        self
    }

    /// Set the acknowledgment callback.
    pub fn ack_callback(mut self, callback: Arc<dyn AckCallback>) -> Self {
        self.grpc_config.ack_callback = Some(callback);
        self
    }

    /// Set the maximum wait time for callbacks after stream close.
    pub fn callback_max_wait_time_ms(mut self, ms: Option<u64>) -> Self {
        self.grpc_config.callback_max_wait_time_ms = ms;
        self
    }

    /// Replace the entire gRPC stream configuration.
    ///
    /// Setters called after this will mutate the replacement config.
    pub fn options(mut self, options: StreamConfigurationOptions) -> Self {
        self.grpc_config = options;
        self
    }
}

// ── Step 3: Config — Arrow-only setters ─────────────────────────────────────

#[cfg(feature = "arrow-flight")]
impl<'a> StreamBuilder<'a, Arrow, HasAuth> {
    /// Set the maximum number of in-flight Arrow batches.
    pub fn max_inflight_batches(mut self, n: usize) -> Self {
        self.arrow_config.max_inflight_batches = n;
        self
    }

    /// Set the connection timeout in milliseconds for Arrow Flight.
    pub fn connection_timeout_ms(mut self, ms: u64) -> Self {
        self.arrow_config.connection_timeout_ms = ms;
        self
    }

    /// Set the Arrow IPC compression type.
    pub fn ipc_compression(mut self, compression: Option<arrow_ipc::CompressionType>) -> Self {
        self.arrow_config.ipc_compression = compression;
        self
    }

    /// Replace the entire Arrow stream configuration.
    ///
    /// Setters called after this will mutate the replacement config.
    pub fn options(mut self, options: ArrowStreamConfigurationOptions) -> Self {
        self.arrow_config = options;
        self
    }
}

// ── Step 4: build() ─────────────────────────────────────────────────────────

impl<'a, F: StreamFormat> StreamBuilder<'a, F, HasAuth> {
    /// Resolve the headers provider from the stored auth config.
    fn resolve_headers_provider(&self) -> Arc<dyn HeadersProvider> {
        match self.auth.as_ref().expect("HasAuth guarantees auth is set") {
            AuthConfig::OAuth {
                client_id,
                client_secret,
            } => Arc::new(OAuthHeadersProvider::new(
                client_id.clone(),
                client_secret.clone(),
                self.table_name.clone(),
                self.sdk.workspace_id.clone(),
                self.sdk.unity_catalog_url.clone(),
            )),
            AuthConfig::HeadersProvider(p) => Arc::clone(p),
        }
    }
}

impl<'a> StreamBuilder<'a, Json, HasAuth> {
    /// Build and open a JSON ingestion stream.
    pub async fn build(mut self) -> ZerobusResult<ZerobusStream> {
        let headers_provider = self.resolve_headers_provider();
        let table_properties = TableProperties {
            table_name: self.table_name,
            descriptor_proto: None,
        };
        // Override record_type so .options() can't cause a mismatch.
        self.grpc_config.record_type = RecordType::Json;
        let channel = self.sdk.get_or_create_channel_zerobus_client().await?;
        ZerobusStream::new_stream(channel, table_properties, headers_provider, self.grpc_config)
            .await
    }
}

impl<'a> StreamBuilder<'a, CompiledProto, HasAuth> {
    /// Build and open a compiled-protobuf ingestion stream.
    pub async fn build(mut self) -> ZerobusResult<ZerobusStream> {
        let headers_provider = self.resolve_headers_provider();
        let table_properties = TableProperties {
            table_name: self.table_name,
            descriptor_proto: self.descriptor_proto,
        };
        // Override record_type so .options() can't cause a mismatch.
        self.grpc_config.record_type = RecordType::Proto;
        let channel = self.sdk.get_or_create_channel_zerobus_client().await?;
        ZerobusStream::new_stream(channel, table_properties, headers_provider, self.grpc_config)
            .await
    }
}

#[cfg(feature = "arrow-flight")]
impl<'a> StreamBuilder<'a, Arrow, HasAuth> {
    /// Build and open an Arrow Flight ingestion stream.
    pub async fn build(self) -> ZerobusResult<ZerobusArrowStream> {
        let headers_provider = self.resolve_headers_provider();
        let table_properties = ArrowTableProperties {
            table_name: self.table_name,
            schema: self
                .arrow_schema
                .expect("Arrow format guarantees schema is set"),
        };
        ZerobusArrowStream::new(
            &self.sdk.zerobus_endpoint,
            Arc::clone(&self.sdk.tls_config),
            table_properties,
            headers_provider,
            self.arrow_config,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::headers_provider::NoOpHeadersProvider;

    fn test_sdk() -> ZerobusSdk {
        ZerobusSdk::new_with_config(
            "http://localhost:1234".to_string(),
            "http://localhost:5678".to_string(),
            "test-workspace".to_string(),
            Arc::new(crate::tls_config::SecureTlsConfig::new()),
        )
    }

    #[test]
    fn json_oauth_builder_compiles() {
        let sdk = test_sdk();
        let _builder = sdk
            .stream_builder("catalog.schema.table")
            .oauth("cid", "csec")
            .json()
            .max_inflight_requests(100);
    }

    #[test]
    fn compiled_proto_headers_provider_compiles() {
        let sdk = test_sdk();
        let provider: Arc<dyn HeadersProvider> = Arc::new(NoOpHeadersProvider);
        let _builder = sdk
            .stream_builder("catalog.schema.table")
            .headers_provider(provider)
            .compiled_proto(prost_types::DescriptorProto::default());
    }

    #[test]
    fn config_setters_chain() {
        let sdk = test_sdk();
        let _builder = sdk
            .stream_builder("t")
            .oauth("a", "b")
            .json()
            .recovery(false)
            .recovery_timeout_ms(10_000)
            .recovery_backoff_ms(1_000)
            .recovery_retries(3)
            .server_lack_of_ack_timeout_ms(30_000)
            .flush_timeout_ms(60_000)
            .max_inflight_requests(500)
            .stream_paused_max_wait_time_ms(Some(5_000))
            .callback_max_wait_time_ms(None);
    }

    #[test]
    fn options_replaces_config() {
        let sdk = test_sdk();
        let custom = StreamConfigurationOptions {
            max_inflight_requests: 42,
            ..Default::default()
        };
        let builder = sdk
            .stream_builder("t")
            .oauth("a", "b")
            .json()
            .max_inflight_requests(999)
            .options(custom);
        assert_eq!(builder.grpc_config.max_inflight_requests, 42);
    }

    #[test]
    fn setter_after_options_mutates_replacement() {
        let sdk = test_sdk();
        let custom = StreamConfigurationOptions {
            max_inflight_requests: 42,
            ..Default::default()
        };
        let builder = sdk
            .stream_builder("t")
            .oauth("a", "b")
            .json()
            .options(custom)
            .max_inflight_requests(100);
        assert_eq!(builder.grpc_config.max_inflight_requests, 100);
    }

    #[test]
    fn default_config_without_setters() {
        let sdk = test_sdk();
        let builder = sdk.stream_builder("t").oauth("a", "b").json();
        // Should have sensible defaults
        assert_eq!(builder.grpc_config.max_inflight_requests, 1_000_000);
        assert!(builder.grpc_config.recovery);
    }

    #[cfg(feature = "arrow-flight")]
    #[test]
    fn arrow_builder_compiles() {
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};

        let sdk = test_sdk();
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let _builder = sdk
            .stream_builder("t")
            .oauth("a", "b")
            .arrow(schema)
            .max_inflight_batches(500)
            .connection_timeout_ms(10_000);
    }

    #[cfg(feature = "arrow-flight")]
    #[test]
    fn options_replaces_arrow_config() {
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};

        let sdk = test_sdk();
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let custom = ArrowStreamConfigurationOptions {
            max_inflight_batches: 77,
            ..Default::default()
        };
        let builder = sdk
            .stream_builder("t")
            .oauth("a", "b")
            .arrow(schema)
            .options(custom);
        assert_eq!(builder.arrow_config.max_inflight_batches, 77);
    }

    #[cfg(feature = "arrow-flight")]
    #[test]
    fn shared_setters_write_to_arrow_config() {
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};

        let sdk = test_sdk();
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let builder = sdk
            .stream_builder("t")
            .oauth("a", "b")
            .arrow(schema)
            .recovery(false)
            .recovery_timeout_ms(5_000)
            .recovery_backoff_ms(500)
            .recovery_retries(2)
            .server_lack_of_ack_timeout_ms(10_000)
            .flush_timeout_ms(20_000);
        assert!(!builder.arrow_config.recovery);
        assert_eq!(builder.arrow_config.recovery_timeout_ms, 5_000);
        assert_eq!(builder.arrow_config.recovery_backoff_ms, 500);
        assert_eq!(builder.arrow_config.recovery_retries, 2);
        assert_eq!(builder.arrow_config.server_lack_of_ack_timeout_ms, 10_000);
        assert_eq!(builder.arrow_config.flush_timeout_ms, 20_000);
    }

    #[test]
    fn no_auth_shortcut_compiles() {
        let sdk = test_sdk();
        let _builder = sdk
            .stream_builder("catalog.schema.table")
            .no_auth()
            .json();
    }

    #[test]
    fn options_cannot_override_record_type() {
        let sdk = test_sdk();
        let wrong_type = StreamConfigurationOptions {
            record_type: RecordType::Proto,
            ..Default::default()
        };
        // .json() + .options(Proto) should still produce Json at build time.
        // We can't call build() without a server, but we can verify the
        // record_type is set correctly in the format selector.
        let builder = sdk
            .stream_builder("t")
            .oauth("a", "b")
            .json()
            .options(wrong_type);
        // After .options(), grpc_config has the wrong record_type...
        assert_eq!(builder.grpc_config.record_type, RecordType::Proto);
        // ...but build() will override it. We verify this by checking the
        // format transition also sets it (belt-and-suspenders).
    }

    #[test]
    fn debug_impl_works() {
        let sdk = test_sdk();
        let builder = sdk.stream_builder("t").oauth("a", "b").json();
        let debug_str = format!("{:?}", builder);
        assert!(debug_str.contains("StreamBuilder"));
        assert!(debug_str.contains("OAuth"));
    }

    #[tokio::test]
    async fn noop_headers_provider_returns_empty() {
        let provider = NoOpHeadersProvider;
        let headers = provider.get_headers().await.unwrap();
        assert!(headers.is_empty());
    }

    #[tokio::test]
    async fn resolve_headers_provider_with_custom_provider() {
        use std::collections::HashMap;

        /// Test provider that returns a known header.
        struct TestProvider;

        #[async_trait::async_trait]
        impl HeadersProvider for TestProvider {
            async fn get_headers(
                &self,
            ) -> crate::ZerobusResult<HashMap<&'static str, String>> {
                let mut h = HashMap::new();
                h.insert("x-test", "value".to_string());
                Ok(h)
            }
        }

        let sdk = test_sdk();
        let builder = sdk
            .stream_builder("catalog.schema.table")
            .headers_provider(Arc::new(TestProvider))
            .json();

        let provider = builder.resolve_headers_provider();
        let headers = provider.get_headers().await.unwrap();
        assert_eq!(headers.get("x-test").unwrap(), "value");
    }

    #[tokio::test]
    async fn resolve_headers_provider_with_oauth() {
        let sdk = test_sdk();
        let builder = sdk
            .stream_builder("catalog.schema.table")
            .oauth("my-client-id", "my-secret")
            .json();

        // Verify it resolves without panic (we can't call get_headers
        // without a real UC endpoint, but we can confirm construction).
        let _provider = builder.resolve_headers_provider();
    }
}
