//! Fluent builder for creating or resuming persistent (Eos) streams.
//!
//! Reached via [`ZerobusSdk::persistent_stream_builder`](crate::ZerobusSdk::persistent_stream_builder).
//! It reuses the ephemeral [`StreamBuilder`] for configuration — table, auth,
//! format, and the gRPC tuning knobs — and adds two terminals that open a
//! persistent stream:
//!
//! - [`build`](PersistentStreamBuilder::build) opens a brand-new persistent
//!   stream; the server mints its durable identity.
//! - [`resume`](PersistentStreamBuilder::resume) reconnects to an existing one
//!   by `stream_id`, continuing offset generation past the committed offset.
//!
//! # Examples
//!
//! ```rust,ignore
//! // Create a new persistent stream.
//! let stream = sdk
//!     .persistent_stream_builder()
//!     .table("catalog.schema.events")
//!     .oauth("client-id", "client-secret")
//!     .json()
//!     .build()
//!     .await?;
//! let id = stream.stream_id().unwrap().to_string();
//!
//! // Later, after a restart, resume it by id.
//! let stream = sdk
//!     .persistent_stream_builder()
//!     .table("catalog.schema.events")
//!     .oauth("client-id", "client-secret")
//!     .json()
//!     .resume(id)
//!     .await?;
//! ```

use std::sync::Arc;

use crate::builder::StreamBuilder;
use crate::callbacks::AckCallback;
use crate::headers_provider::HeadersProvider;
use crate::{PersistentStream, ZerobusResult, ZerobusStream};

/// A fluent builder for creating or resuming persistent (Eos) streams.
///
/// Configuration setters mirror [`StreamBuilder`] (they delegate to it); the
/// terminals [`build`](Self::build) and [`resume`](Self::resume) open the
/// stream. Arrow format is not supported for persistent streams.
#[must_use = "a PersistentStreamBuilder does nothing until `.build()` or `.resume()` is called"]
pub struct PersistentStreamBuilder<'a> {
    inner: StreamBuilder<'a>,
}

#[allow(clippy::result_large_err)]
impl<'a> PersistentStreamBuilder<'a> {
    pub(crate) fn new(inner: StreamBuilder<'a>) -> Self {
        Self { inner }
    }

    /// Set the fully-qualified Unity Catalog table name (e.g., `"catalog.schema.table"`).
    pub fn table(mut self, table_name: impl Into<String>) -> Self {
        self.inner = self.inner.table(table_name);
        self
    }

    /// Authenticate with OAuth client credentials.
    pub fn oauth(mut self, client_id: impl Into<String>, client_secret: impl Into<String>) -> Self {
        self.inner = self.inner.oauth(client_id, client_secret);
        self
    }

    /// Authenticate with a custom headers provider.
    pub fn headers_provider(mut self, provider: Arc<dyn HeadersProvider>) -> Self {
        self.inner = self.inner.headers_provider(provider);
        self
    }

    /// Use a no-op headers provider that sends no authentication credentials.
    ///
    /// Intended only for local testing. Available behind the `testing` feature.
    #[cfg(feature = "testing")]
    pub fn no_auth(mut self) -> Self {
        self.inner = self.inner.no_auth();
        self
    }

    /// Select JSON record format.
    pub fn json(mut self) -> Self {
        self.inner = self.inner.json();
        self
    }

    /// Select compiled protobuf record format.
    pub fn compiled_proto(mut self, descriptor: prost_types::DescriptorProto) -> Self {
        self.inner = self.inner.compiled_proto(descriptor);
        self
    }

    /// Enable or disable automatic stream recovery.
    pub fn recovery(mut self, enabled: bool) -> Self {
        self.inner = self.inner.recovery(enabled);
        self
    }

    /// Set the timeout in milliseconds for each recovery attempt.
    pub fn recovery_timeout_ms(mut self, ms: u64) -> Self {
        self.inner = self.inner.recovery_timeout_ms(ms);
        self
    }

    /// Set the backoff time in milliseconds between recovery retries.
    pub fn recovery_backoff_ms(mut self, ms: u64) -> Self {
        self.inner = self.inner.recovery_backoff_ms(ms);
        self
    }

    /// Set the maximum number of recovery retry attempts.
    pub fn recovery_retries(mut self, n: u32) -> Self {
        self.inner = self.inner.recovery_retries(n);
        self
    }

    /// Set the timeout in milliseconds for server acknowledgement.
    pub fn server_lack_of_ack_timeout_ms(mut self, ms: u64) -> Self {
        self.inner = self.inner.server_lack_of_ack_timeout_ms(ms);
        self
    }

    /// Set the timeout in milliseconds for flush operations.
    pub fn flush_timeout_ms(mut self, ms: u64) -> Self {
        self.inner = self.inner.flush_timeout_ms(ms);
        self
    }

    /// Set the maximum number of in-flight requests.
    pub fn max_inflight_requests(mut self, n: usize) -> Self {
        self.inner = self.inner.max_inflight_requests(n);
        self
    }

    /// Set the maximum total encoded record byte size allowed per ingest call.
    pub fn max_ingest_payload_bytes(mut self, bytes: usize) -> Self {
        self.inner = self.inner.max_ingest_payload_bytes(bytes);
        self
    }

    /// Set the maximum wait time during graceful stream pause.
    pub fn stream_paused_max_wait_time_ms(mut self, ms: Option<u64>) -> Self {
        self.inner = self.inner.stream_paused_max_wait_time_ms(ms);
        self
    }

    /// Set the acknowledgment callback.
    pub fn ack_callback(mut self, callback: Arc<dyn AckCallback>) -> Self {
        self.inner = self.inner.ack_callback(callback);
        self
    }

    /// Set the maximum wait time for callbacks after stream close.
    pub fn callback_max_wait_time_ms(mut self, ms: Option<u64>) -> Self {
        self.inner = self.inner.callback_max_wait_time_ms(ms);
        self
    }

    /// Validate that the builder has all required fields configured.
    ///
    /// Returns `Ok(())` if table name, authentication, and format are set.
    pub fn validate(&self) -> ZerobusResult<()> {
        self.inner.validate()
    }

    /// Build and open a new persistent (Eos) stream.
    ///
    /// The server mints a durable stream identity, available via
    /// [`PersistentStream::stream_id`]; persist it to reconnect later with
    /// [`resume`](Self::resume). Delivery into Delta is exactly-once.
    ///
    /// Returns an error if table name, authentication, or format has not been set.
    pub async fn build(self) -> ZerobusResult<PersistentStream> {
        self.open(None).await
    }

    /// Resume an existing persistent (Eos) stream by its `stream_id`.
    ///
    /// Reconnects to the stream created earlier by [`build`](Self::build),
    /// continuing offset generation past the server's committed offset. The
    /// table and record format must match those the stream was created with.
    ///
    /// Returns an error if table name, authentication, or format has not been set.
    pub async fn resume(self, stream_id: impl Into<String>) -> ZerobusResult<PersistentStream> {
        self.open(Some(stream_id.into())).await
    }

    async fn open(self, resume_stream_id: Option<String>) -> ZerobusResult<PersistentStream> {
        let (channel, table_properties, headers_provider, config) =
            self.inner.prepare_grpc().await?;
        let stream = ZerobusStream::new_persistent_stream(
            channel,
            table_properties,
            headers_provider,
            config,
            resume_stream_id,
        )
        .await?;
        crate::client_warnings::record_stream_creation(stream.table_properties.table_name.as_str());
        Ok(PersistentStream::new(stream))
    }
}
