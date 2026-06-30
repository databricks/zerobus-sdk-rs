//! Fluent builder for creating Zerobus ingestion streams.
//!
//! All setters can be called in any order. The builder validates at
//! `build()` time that table name, authentication, and format have been
//! configured. You can also call `validate()` to check the builder state
//! without opening a stream.
//!
//! # Examples
//!
//! ```rust,ignore
//! let stream = sdk
//!     .stream_builder()
//!     .table("catalog.schema.table")
//!     .oauth("client-id", "client-secret")
//!     .json()
//!     .max_inflight_requests(500_000)
//!     .build()
//!     .await?;
//! ```

use std::fmt;
use std::sync::Arc;

use crate::callbacks::AckCallback;
use crate::databricks::zerobus::RecordType;
use crate::default_token_factory::DefaultTokenFactory;
#[cfg(feature = "testing")]
use crate::headers_provider::NoAuthHeadersProvider;
use crate::headers_provider::{HeadersProvider, OAuthHeadersProvider};
use crate::stream_configuration::StreamConfigurationOptions;
use crate::{TableProperties, ZerobusError, ZerobusResult, ZerobusSdk, ZerobusStream};

#[cfg(feature = "arrow-flight")]
use crate::arrow_configuration::ArrowStreamConfigurationOptions;
#[cfg(feature = "arrow-flight")]
use crate::arrow_stream::{ArrowSchema, ArrowTableProperties, ZerobusArrowStream};

/// Internal representation of the authentication configuration.
enum AuthConfig {
    OAuth {
        client_id: String,
        client_secret: String,
    },
    HeadersProvider(Arc<dyn HeadersProvider>),
    #[cfg(feature = "testing")]
    NoAuth,
}

/// Which record format was selected.
enum FormatConfig {
    Json,
    CompiledProto(Box<prost_types::DescriptorProto>),
    ProtoFromUc,
    #[cfg(feature = "arrow-flight")]
    Arrow(Arc<ArrowSchema>),
}

/// A fluent builder for creating Zerobus ingestion streams.
///
/// All setters can be called in any order. The builder validates at
/// `build()` time that table name, authentication, and format have been
/// configured. Use [`validate()`](Self::validate) to check the builder
/// state without opening a stream.
///
/// # Examples
///
/// ```rust,ignore
/// // JSON stream with OAuth
/// let stream = sdk
///     .stream_builder()
///     .table("catalog.schema.table")
///     .oauth("client-id", "client-secret")
///     .json()
///     .build()
///     .await?;
///
/// // Proto stream with custom headers
/// let stream = sdk
///     .stream_builder()
///     .table("catalog.schema.table")
///     .headers_provider(my_provider)
///     .compiled_proto(descriptor)
///     .max_inflight_requests(500_000)
///     .build()
///     .await?;
///
/// // Validate without opening a stream
/// let builder = sdk
///     .stream_builder()
///     .table("catalog.schema.table")
///     .oauth("client-id", "client-secret")
///     .json();
/// builder.validate()?;
/// let stream = builder.build().await?;
/// ```
#[must_use = "a StreamBuilder does nothing until `.build()` is called"]
pub struct StreamBuilder<'a> {
    sdk: &'a ZerobusSdk,
    table_name: String,
    auth: Option<AuthConfig>,
    format: Option<FormatConfig>,
    grpc_config: StreamConfigurationOptions,
    #[cfg(feature = "arrow-flight")]
    arrow_config: ArrowStreamConfigurationOptions,
}

impl fmt::Debug for StreamBuilder<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let auth_kind = match &self.auth {
            Some(AuthConfig::OAuth { .. }) => "OAuth",
            Some(AuthConfig::HeadersProvider(_)) => "HeadersProvider",
            #[cfg(feature = "testing")]
            Some(AuthConfig::NoAuth) => "NoAuth",
            None => "None",
        };
        let format_kind = match &self.format {
            Some(FormatConfig::Json) => "Json",
            Some(FormatConfig::CompiledProto(_)) => "CompiledProto",
            Some(FormatConfig::ProtoFromUc) => "ProtoFromUc",
            #[cfg(feature = "arrow-flight")]
            Some(FormatConfig::Arrow(_)) => "Arrow",
            None => "None",
        };
        f.debug_struct("StreamBuilder")
            .field("table_name", &self.table_name)
            .field("auth", &auth_kind)
            .field("format", &format_kind)
            .finish_non_exhaustive()
    }
}

const fn missing_auth_error() -> &'static str {
    #[cfg(feature = "testing")]
    {
        "authentication is required: call .oauth(), .headers_provider(), or .no_auth()"
    }
    #[cfg(not(feature = "testing"))]
    {
        "authentication is required: call .oauth() or .headers_provider()"
    }
}

#[allow(clippy::result_large_err)]
impl<'a> StreamBuilder<'a> {
    pub(crate) fn new(sdk: &'a ZerobusSdk) -> Self {
        Self {
            sdk,
            table_name: String::new(),
            auth: None,
            format: None,
            grpc_config: StreamConfigurationOptions::default(),
            #[cfg(feature = "arrow-flight")]
            arrow_config: ArrowStreamConfigurationOptions::default(),
        }
    }

    /// Set the fully-qualified Unity Catalog table name (e.g., `"catalog.schema.table"`).
    pub fn table(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = table_name.into();
        self
    }

    /// Authenticate with OAuth client credentials.
    pub fn oauth(mut self, client_id: impl Into<String>, client_secret: impl Into<String>) -> Self {
        self.auth = Some(AuthConfig::OAuth {
            client_id: client_id.into(),
            client_secret: client_secret.into(),
        });
        self
    }

    /// Authenticate with a custom headers provider.
    pub fn headers_provider(mut self, provider: Arc<dyn HeadersProvider>) -> Self {
        self.auth = Some(AuthConfig::HeadersProvider(provider));
        self
    }

    /// Use a no-op headers provider that sends no authentication credentials.
    ///
    /// Intended only for local testing against a Zerobus endpoint that does not
    /// enforce authentication. Available behind the `testing` feature flag.
    #[cfg(feature = "testing")]
    pub fn no_auth(mut self) -> Self {
        self.auth = Some(AuthConfig::NoAuth);
        self
    }

    /// Select JSON record format.
    pub fn json(mut self) -> Self {
        self.format = Some(FormatConfig::Json);
        self
    }

    /// Select compiled protobuf record format.
    ///
    /// Use this when you already hold a [`prost_types::DescriptorProto`] — from a
    /// compiled `.proto`, from
    /// [`schema::TableDescriptorBuilder`](crate::schema::TableDescriptorBuilder),
    /// or from [`schema::descriptor_from_uc`](crate::schema::descriptor_from_uc).
    /// To ingest without a compiled `.proto`, obtain a
    /// [`DynamicProtoEncoder`](crate::DynamicProtoEncoder) via
    /// [`ZerobusStream::encoder`](crate::ZerobusStream::encoder).
    pub fn compiled_proto(mut self, descriptor: prost_types::DescriptorProto) -> Self {
        self.format = Some(FormatConfig::CompiledProto(Box::new(descriptor)));
        self
    }

    /// Select dynamic protobuf, fetching the table's descriptor from Unity
    /// Catalog at `build()` time.
    ///
    /// No compiled `.proto` is required: the SDK fetches the table's schema from
    /// Unity Catalog, derives a protobuf descriptor, and opens a proto stream
    /// with it. Supply records as JSON and encode them with the stream's
    /// [`encoder`](crate::ZerobusStream::encoder), which is bound to the same
    /// descriptor.
    ///
    /// The fetch uses an `all-apis` token: with [`oauth`](Self::oauth) the SDK
    /// mints one from the client credentials; with
    /// [`headers_provider`](Self::headers_provider) it reuses the provider's
    /// `authorization` header, which must therefore be a token the Unity Catalog
    /// REST API accepts (a workspace `all-apis` token, not a Zerobus
    /// write-scoped one) or the schema fetch fails. The principal needs `SELECT`
    /// on the table.
    ///
    /// Each `build()` performs these UC calls fresh and uncached (a workspace-token
    /// mint plus the schema fetch), on top of the cached write token. For workloads
    /// that churn many short-lived streams to the same table, fetch the descriptor
    /// once with [`schema::descriptor_from_uc`](crate::schema::descriptor_from_uc),
    /// reuse it via [`compiled_proto`](Self::compiled_proto), and obtain the encoder
    /// from each resulting stream.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let stream = sdk
    ///     .stream_builder()
    ///     .table("catalog.schema.table")
    ///     .oauth("client-id", "client-secret")
    ///     .proto_from_uc()
    ///     .build()
    ///     .await?;
    /// let encoder = stream.encoder()?;
    /// let offset = stream
    ///     .ingest_record_offset(encoder.encode(r#"{"id": 1}"#)?)
    ///     .await?;
    /// ```
    pub fn proto_from_uc(mut self) -> Self {
        self.format = Some(FormatConfig::ProtoFromUc);
        self
    }

    /// Select Arrow Flight record format.
    #[cfg(feature = "arrow-flight")]
    pub fn arrow(mut self, schema: Arc<ArrowSchema>) -> Self {
        self.format = Some(FormatConfig::Arrow(schema));
        self
    }

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

    /// Set the maximum number of in-flight requests (gRPC streams only).
    pub fn max_inflight_requests(mut self, n: usize) -> Self {
        self.grpc_config.max_inflight_requests = n;
        self
    }

    /// Set the maximum total encoded record byte size allowed per ingest call
    /// (gRPC JSON/proto streams only).
    ///
    /// This is the sum of all record bytes passed to a single ingest call.
    /// Calls exceeding this limit fail fast with
    /// [`ZerobusError::InvalidArgument`] before any network I/O.
    ///
    /// Defaults to slightly below the 10 MiB server limit.
    ///
    /// Note: this setting only applies to streams built with [`build()`](Self::build).
    /// Arrow Flight streams (built with `build_arrow()`) do not read this value
    /// and have no client-side payload-size enforcement.
    pub fn max_ingest_payload_bytes(mut self, bytes: usize) -> Self {
        self.grpc_config.max_ingest_payload_bytes = bytes;
        self
    }

    /// Set the maximum wait time during graceful stream pause (JSON/proto and Arrow streams).
    pub fn stream_paused_max_wait_time_ms(mut self, ms: Option<u64>) -> Self {
        self.grpc_config.stream_paused_max_wait_time_ms = ms;
        #[cfg(feature = "arrow-flight")]
        {
            self.arrow_config.stream_paused_max_wait_time_ms = ms;
        }
        self
    }

    /// Set the acknowledgment callback (gRPC streams only).
    pub fn ack_callback(mut self, callback: Arc<dyn AckCallback>) -> Self {
        self.grpc_config.ack_callback = Some(callback);
        self
    }

    /// Set the maximum wait time for callbacks after stream close (gRPC streams only).
    pub fn callback_max_wait_time_ms(mut self, ms: Option<u64>) -> Self {
        self.grpc_config.callback_max_wait_time_ms = ms;
        self
    }

    /// Set the maximum number of in-flight Arrow batches (Arrow streams only).
    #[cfg(feature = "arrow-flight")]
    pub fn max_inflight_batches(mut self, n: usize) -> Self {
        self.arrow_config.max_inflight_batches = n;
        self
    }

    /// Set the connection timeout in milliseconds for Arrow Flight (Arrow streams only).
    #[cfg(feature = "arrow-flight")]
    pub fn connection_timeout_ms(mut self, ms: u64) -> Self {
        self.arrow_config.connection_timeout_ms = ms;
        self
    }

    /// Set the Arrow IPC compression type (Arrow streams only).
    #[cfg(feature = "arrow-flight")]
    pub fn ipc_compression(mut self, compression: Option<arrow_ipc::CompressionType>) -> Self {
        self.arrow_config.ipc_compression = compression;
        self
    }

    /// Validate that the builder has all required fields configured.
    ///
    /// Returns `Ok(())` if table name, authentication, and format are all set.
    /// This performs the same checks as `build()` without actually opening
    /// a stream — useful for fail-fast validation during startup or config
    /// parsing.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let builder = sdk
    ///     .stream_builder()
    ///     .table("catalog.schema.table")
    ///     .oauth("client-id", "client-secret")
    ///     .json();
    ///
    /// // Check configuration before opening the stream
    /// builder.validate()?;
    /// let stream = builder.build().await?;
    /// ```
    pub fn validate(&self) -> ZerobusResult<()> {
        if self.table_name.is_empty() {
            return Err(ZerobusError::InvalidArgument(
                "table name is required: call .table()".into(),
            ));
        }
        if self.auth.is_none() {
            return Err(ZerobusError::InvalidArgument(missing_auth_error().into()));
        }
        if self.format.is_none() {
            return Err(ZerobusError::InvalidArgument(
                "record format is required: call .json(), .compiled_proto(), .proto_from_uc(), \
                 or .arrow()"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Resolve an `all-apis` token for the Unity Catalog REST API used by
    /// [`proto_from_uc`](Self::proto_from_uc) to fetch the table schema.
    ///
    /// With OAuth credentials the SDK mints a workspace token (the Zerobus
    /// write token is not accepted by the REST API). With a custom headers
    /// provider it reuses the `authorization` header that provider supplies.
    async fn resolve_uc_api_token(&self) -> ZerobusResult<String> {
        match self.auth.as_ref() {
            Some(AuthConfig::OAuth {
                client_id,
                client_secret,
            }) => {
                DefaultTokenFactory::get_workspace_token(
                    &self.sdk.unity_catalog_url,
                    client_id,
                    client_secret,
                )
                .await
            }
            Some(AuthConfig::HeadersProvider(provider)) => {
                let headers = provider.get_headers().await?;
                let auth = headers.get("authorization").ok_or_else(|| {
                    ZerobusError::InvalidArgument(
                        "headers provider did not supply an 'authorization' header required to \
                         fetch the Unity Catalog schema for proto_from_uc()"
                            .to_string(),
                    )
                })?;
                Ok(auth.strip_prefix("Bearer ").unwrap_or(auth).to_string())
            }
            // No credentials in the no-auth testing path; the (mock) Unity
            // Catalog endpoint is expected not to require authorization.
            #[cfg(feature = "testing")]
            Some(AuthConfig::NoAuth) => Ok(String::new()),
            None => Err(ZerobusError::InvalidArgument(
                "authentication is required: call .oauth() or .headers_provider()".into(),
            )),
        }
    }

    /// Resolve the headers provider from the stored auth config.
    fn resolve_headers_provider(&self) -> ZerobusResult<Arc<dyn HeadersProvider>> {
        match self.auth.as_ref() {
            Some(AuthConfig::OAuth {
                client_id,
                client_secret,
            }) => Ok(Arc::new(OAuthHeadersProvider::with_cache(
                client_id.clone(),
                client_secret.clone(),
                self.table_name.clone(),
                self.sdk.workspace_id.clone(),
                self.sdk.unity_catalog_url.clone(),
                Arc::clone(&self.sdk.token_cache),
            ))),
            Some(AuthConfig::HeadersProvider(p)) => Ok(Arc::clone(p)),
            #[cfg(feature = "testing")]
            Some(AuthConfig::NoAuth) => Ok(Arc::new(NoAuthHeadersProvider)),
            None => Err(ZerobusError::InvalidArgument(missing_auth_error().into())),
        }
    }

    /// Build and open a gRPC ingestion stream (JSON or compiled protobuf).
    ///
    /// Returns an error if table name, authentication, or format has not been set,
    /// or if an Arrow format was selected (use `build_arrow()` instead).
    pub async fn build(mut self) -> ZerobusResult<ZerobusStream> {
        self.validate()?;
        let headers_provider = self.resolve_headers_provider()?;

        // Take the format out so the `proto_from_uc` arm can borrow `&self`
        // (for the UC fetch) without tripping a partial-move of `self.format`.
        let format = self.format.take();
        let (record_type, descriptor_proto) = match format {
            Some(FormatConfig::Json) => (RecordType::Json, None),
            Some(FormatConfig::CompiledProto(desc)) => (RecordType::Proto, Some(*desc)),
            Some(FormatConfig::ProtoFromUc) => {
                let token = self.resolve_uc_api_token().await?;
                let descriptor = crate::schema::descriptor_from_uc(
                    &self.sdk.unity_catalog_url,
                    &self.table_name,
                    &token,
                )
                .await?;
                (RecordType::Proto, Some(descriptor))
            }
            #[cfg(feature = "arrow-flight")]
            Some(FormatConfig::Arrow(_)) => {
                return Err(ZerobusError::InvalidArgument(
                    "Arrow format requires .build_arrow() instead of .build()".into(),
                ));
            }
            None => {
                return Err(ZerobusError::InvalidArgument(
                    "record format is required: call .json(), .compiled_proto(), or \
                     .proto_from_uc() before .build()"
                        .into(),
                ));
            }
        };

        self.grpc_config.record_type = record_type;
        let table_properties = TableProperties {
            table_name: self.table_name,
            descriptor_proto,
        };

        let channel = self.sdk.get_or_create_channel_zerobus_client().await?;
        let stream = ZerobusStream::new_stream(
            channel,
            table_properties,
            headers_provider,
            self.grpc_config,
        )
        .await?;
        crate::client_warnings::record_stream_creation(stream.table_properties.table_name.as_str());
        Ok(stream)
    }

    /// Build and open an Arrow Flight ingestion stream.
    ///
    /// Returns an error if table name, authentication, or format has not been set,
    /// or if a non-Arrow format was selected (use `build()` instead).
    #[cfg(feature = "arrow-flight")]
    pub async fn build_arrow(self) -> ZerobusResult<ZerobusArrowStream> {
        self.validate()?;

        // `max_ingest_payload_bytes` only applies to gRPC streams; warn if the
        // user changed it from the default before building an Arrow stream.
        if self.grpc_config.max_ingest_payload_bytes
            != StreamConfigurationOptions::default().max_ingest_payload_bytes
        {
            crate::client_warnings::warn_payload_limit_ignored_for_arrow();
        }

        let headers_provider = self.resolve_headers_provider()?;

        let schema = match self.format {
            Some(FormatConfig::Arrow(schema)) => schema,
            Some(_) => {
                return Err(ZerobusError::InvalidArgument(
                    "non-Arrow format requires .build() instead of .build_arrow()".into(),
                ));
            }
            None => {
                return Err(ZerobusError::InvalidArgument(
                    "record format is required: call .arrow() before .build_arrow()".into(),
                ));
            }
        };

        let table_properties = ArrowTableProperties {
            table_name: self.table_name,
            schema,
        };

        let table_name = table_properties.table_name.clone();
        let stream = ZerobusArrowStream::new(
            &self.sdk.zerobus_endpoint,
            Arc::clone(&self.sdk.tls_config),
            table_properties,
            headers_provider,
            self.arrow_config,
            Arc::clone(&self.sdk.sdk_identifier),
        )
        .await?;
        crate::client_warnings::record_stream_creation(&table_name);
        Ok(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn test_sdk() -> ZerobusSdk {
        ZerobusSdk::new_with_config(
            "http://localhost:1234".to_string(),
            "http://localhost:5678".to_string(),
            "test-workspace".to_string(),
            Arc::new(crate::tls_config::SecureTlsConfig::new()),
            None,
            Arc::from(crate::DEFAULT_SDK_IDENTIFIER),
            true,
            crate::token_cache::DEFAULT_REFRESH_BUFFER,
        )
    }

    #[test]
    fn json_oauth_builder() {
        let sdk = test_sdk();
        let _builder = sdk
            .stream_builder()
            .table("catalog.schema.table")
            .oauth("cid", "csec")
            .json()
            .max_inflight_requests(100);
    }

    #[test]
    fn compiled_proto_headers_provider() {
        struct StubProvider;
        #[async_trait::async_trait]
        impl HeadersProvider for StubProvider {
            async fn get_headers(&self) -> crate::ZerobusResult<HashMap<&'static str, String>> {
                Ok(HashMap::new())
            }
        }

        let sdk = test_sdk();
        let provider: Arc<dyn HeadersProvider> = Arc::new(StubProvider);
        let _builder = sdk
            .stream_builder()
            .table("catalog.schema.table")
            .headers_provider(provider)
            .compiled_proto(prost_types::DescriptorProto::default());
    }

    #[test]
    fn proto_from_uc_builder() {
        let sdk = test_sdk();
        let builder = sdk
            .stream_builder()
            .table("catalog.schema.table")
            .oauth("cid", "csec")
            .proto_from_uc()
            .max_inflight_requests(100);
        // Validation passes (table + auth + format all set) without opening a stream.
        builder.validate().unwrap();
        let debug_str = format!("{:?}", builder);
        assert!(debug_str.contains("ProtoFromUc"));
    }

    #[test]
    fn any_order_format_before_auth() {
        let sdk = test_sdk();
        let _builder = sdk
            .stream_builder()
            .table("catalog.schema.table")
            .json()
            .oauth("cid", "csec")
            .max_inflight_requests(100);
    }

    #[test]
    fn any_order_config_before_format() {
        let sdk = test_sdk();
        let _builder = sdk
            .stream_builder()
            .table("catalog.schema.table")
            .max_inflight_requests(100)
            .recovery(false)
            .oauth("cid", "csec")
            .json();
    }

    #[test]
    fn config_setters_chain() {
        let sdk = test_sdk();
        let _builder = sdk
            .stream_builder()
            .table("t")
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
    fn default_config_without_setters() {
        let sdk = test_sdk();
        let builder = sdk.stream_builder().table("t").oauth("a", "b").json();
        assert_eq!(builder.grpc_config.max_inflight_requests, 1_000_000);
        assert!(builder.grpc_config.recovery);
        assert_eq!(
            builder.grpc_config.max_ingest_payload_bytes,
            crate::stream_options::defaults::MAX_INGEST_PAYLOAD_BYTES
        );
        assert!(builder.grpc_config.max_ingest_payload_bytes < 10 * 1024 * 1024);
    }

    #[test]
    fn max_ingest_payload_bytes_override() {
        let sdk = test_sdk();
        let builder = sdk
            .stream_builder()
            .table("t")
            .oauth("a", "b")
            .json()
            .max_ingest_payload_bytes(5 * 1024 * 1024);
        assert_eq!(
            builder.grpc_config.max_ingest_payload_bytes,
            5 * 1024 * 1024
        );
    }

    #[tokio::test]
    async fn build_without_auth_returns_error() {
        let sdk = test_sdk();
        let result = sdk.stream_builder().table("t").json().build().await;
        match result {
            Err(ZerobusError::InvalidArgument(msg)) => {
                assert!(msg.contains("authentication is required"));
            }
            _ => panic!("expected InvalidArgument error"),
        }
    }

    #[tokio::test]
    async fn build_without_table_returns_error() {
        let sdk = test_sdk();
        let result = sdk.stream_builder().oauth("a", "b").json().build().await;
        match result {
            Err(ZerobusError::InvalidArgument(msg)) => {
                assert!(msg.contains("table name is required"));
            }
            _ => panic!("expected InvalidArgument error"),
        }
    }

    #[tokio::test]
    async fn build_without_format_returns_error() {
        let sdk = test_sdk();
        let result = sdk
            .stream_builder()
            .table("t")
            .oauth("a", "b")
            .build()
            .await;
        match result {
            Err(ZerobusError::InvalidArgument(msg)) => {
                assert!(msg.contains("record format is required"));
            }
            _ => panic!("expected InvalidArgument error"),
        }
    }

    #[test]
    fn debug_impl_works() {
        let sdk = test_sdk();
        let builder = sdk.stream_builder().table("t").oauth("a", "b").json();
        let debug_str = format!("{:?}", builder);
        assert!(debug_str.contains("StreamBuilder"));
        assert!(debug_str.contains("OAuth"));
        assert!(debug_str.contains("Json"));
    }

    #[tokio::test]
    async fn resolve_headers_provider_with_custom_provider() {
        struct TestProvider;

        #[async_trait::async_trait]
        impl HeadersProvider for TestProvider {
            async fn get_headers(&self) -> crate::ZerobusResult<HashMap<&'static str, String>> {
                let mut h = HashMap::new();
                h.insert("x-test", "value".to_string());
                Ok(h)
            }
        }

        let sdk = test_sdk();
        let builder = sdk
            .stream_builder()
            .table("catalog.schema.table")
            .headers_provider(Arc::new(TestProvider))
            .json();

        let provider = builder.resolve_headers_provider().unwrap();
        let headers = provider.get_headers().await.unwrap();
        assert_eq!(headers.get("x-test").unwrap(), "value");
    }

    #[tokio::test]
    async fn resolve_uc_api_token_from_headers_provider() {
        // A provider whose `authorization` header is configurable (or absent).
        struct AuthProvider(Option<&'static str>);
        #[async_trait::async_trait]
        impl HeadersProvider for AuthProvider {
            async fn get_headers(&self) -> crate::ZerobusResult<HashMap<&'static str, String>> {
                let mut h = HashMap::new();
                if let Some(v) = self.0 {
                    h.insert("authorization", v.to_string());
                }
                Ok(h)
            }
        }

        let sdk = test_sdk();

        // `Bearer ` prefix is stripped.
        let token = sdk
            .stream_builder()
            .table("c.s.t")
            .headers_provider(Arc::new(AuthProvider(Some("Bearer abc"))))
            .resolve_uc_api_token()
            .await
            .unwrap();
        assert_eq!(token, "abc");

        // A bare token is returned as-is.
        let token = sdk
            .stream_builder()
            .table("c.s.t")
            .headers_provider(Arc::new(AuthProvider(Some("rawtoken"))))
            .resolve_uc_api_token()
            .await
            .unwrap();
        assert_eq!(token, "rawtoken");

        // A provider with no `authorization` header is an error.
        let err = sdk
            .stream_builder()
            .table("c.s.t")
            .headers_provider(Arc::new(AuthProvider(None)))
            .resolve_uc_api_token()
            .await;
        assert!(matches!(err, Err(ZerobusError::InvalidArgument(_))));
    }

    #[cfg(feature = "testing")]
    #[tokio::test]
    async fn no_auth_resolves_to_no_auth_provider() {
        let sdk = test_sdk();
        let builder = sdk
            .stream_builder()
            .table("catalog.schema.table")
            .no_auth()
            .json();
        let provider = builder.resolve_headers_provider().unwrap();
        let headers = provider.get_headers().await.unwrap();
        assert!(headers.is_empty());
    }

    #[cfg(feature = "testing")]
    #[tokio::test]
    async fn no_auth_plus_no_tls_chain() {
        let sdk = crate::ZerobusSdkBuilder::new()
            .endpoint("http://localhost:1234")
            .no_tls()
            .build()
            .expect("sdk should build with no_tls");
        let builder = sdk
            .stream_builder()
            .table("catalog.schema.table")
            .no_auth()
            .json();
        builder.validate().expect("validation should succeed");
        let provider = builder.resolve_headers_provider().unwrap();
        let headers = provider.get_headers().await.unwrap();
        assert!(headers.is_empty());
    }

    #[tokio::test]
    async fn resolve_headers_provider_with_oauth() {
        let sdk = test_sdk();
        let builder = sdk
            .stream_builder()
            .table("catalog.schema.table")
            .oauth("my-client-id", "my-secret")
            .json();

        let _provider = builder.resolve_headers_provider().unwrap();
    }

    #[cfg(feature = "arrow-flight")]
    #[test]
    fn arrow_builder() {
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};

        let sdk = test_sdk();
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let _builder = sdk
            .stream_builder()
            .table("t")
            .oauth("a", "b")
            .arrow(schema)
            .max_inflight_batches(500)
            .connection_timeout_ms(10_000);
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
            .stream_builder()
            .table("t")
            .oauth("a", "b")
            .arrow(schema)
            .recovery(false)
            .recovery_timeout_ms(5_000)
            .recovery_backoff_ms(500)
            .recovery_retries(2)
            .server_lack_of_ack_timeout_ms(10_000)
            .flush_timeout_ms(20_000)
            .stream_paused_max_wait_time_ms(Some(5_000));
        assert!(!builder.arrow_config.recovery);
        assert_eq!(builder.arrow_config.recovery_timeout_ms, 5_000);
        assert_eq!(builder.arrow_config.recovery_backoff_ms, 500);
        assert_eq!(builder.arrow_config.recovery_retries, 2);
        assert_eq!(builder.arrow_config.server_lack_of_ack_timeout_ms, 10_000);
        assert_eq!(builder.arrow_config.flush_timeout_ms, 20_000);
        assert_eq!(
            builder.arrow_config.stream_paused_max_wait_time_ms,
            Some(5_000)
        );
    }
}
