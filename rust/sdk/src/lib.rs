//! # Databricks Zerobus Ingest SDK
//!
//! A high-performance Rust client for streaming data ingestion into Databricks Delta tables.
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use databricks_zerobus_ingest_sdk::{ZerobusSdk, JsonValue};
//!
//! let sdk = ZerobusSdk::builder()
//!     .endpoint(zerobus_endpoint)
//!     .unity_catalog_url(uc_endpoint)
//!     .build()?;
//!
//! let stream = sdk
//!     .stream_builder()
//!     .table("catalog.schema.table")
//!     .oauth(client_id, client_secret)
//!     .json()
//!     .build()
//!     .await?;
//!
//! // Idiomatic flow: ingest in a loop, then flush() once to confirm the batch.
//! for my_record in records {
//!     stream.ingest_record_offset(JsonValue(my_record)).await?;
//! }
//! stream.flush().await?; // Returns once every queued record is acknowledged.
//!
//! stream.close().await?;
//! ```
//!
//! See the `examples/` directory for complete working examples.

pub mod databricks {
    pub mod zerobus {
        include!(concat!(env!("OUT_DIR"), "/databricks.zerobus.rs"));
    }
}

mod builder;
mod callbacks;
mod client_warnings;
mod default_token_factory;
mod dynamic_proto;
mod errors;
mod headers_provider;
mod landing_zone;
#[cfg(feature = "testing")]
mod multiplexed_stream;
mod offset_generator;
mod proxy;
mod record_types;
pub mod schema;
mod sdk;
mod stream;
mod stream_configuration;
pub mod stream_options;
mod tls_config;
mod token_cache;

pub use builder::{StreamBuilder, ZerobusSdkBuilder};
pub use callbacks::AckCallback;
pub use default_token_factory::DefaultTokenFactory;
pub use dynamic_proto::{
    message_descriptor, missing_required_fields, DynamicMessage, DynamicRecord, IntoDynamicValue,
    MessageDescriptor, Value,
};
pub use errors::{SchemaValidationCause, ZerobusError};
#[cfg(feature = "testing")]
pub use headers_provider::NoAuthHeadersProvider;
pub use headers_provider::{HeadersProvider, OAuthHeadersProvider};
#[cfg(feature = "testing")]
pub use multiplexed_stream::{MessageId, MultiplexedStream};
pub use offset_generator::{OffsetId, OffsetIdGenerator};
pub use proxy::{ConnectorFactory, ProxyConnector};
pub use record_types::{
    EncodedBatch, EncodedBatchIter, EncodedRecord, JsonEncodedRecord, JsonString, JsonValue,
    ProtoBytes, ProtoEncodedRecord, ProtoMessage,
};
pub use sdk::{ZerobusSdk, DEFAULT_SDK_IDENTIFIER};
#[cfg(feature = "testing")]
pub use stream::CallbackHandlerHarness;
pub use stream::ZerobusStream;
#[cfg(feature = "arrow-flight")]
pub use stream::{
    ArrowSchema, ArrowStreamConfigurationOptions, DataType, Field, RecordBatch, TimeUnit,
    ZerobusArrowStream,
};
pub use stream_configuration::StreamConfigurationOptions;
#[cfg(feature = "testing")]
pub use tls_config::NoTlsConfig;
pub use tls_config::{SecureTlsConfig, TlsConfig};

#[cfg(feature = "zeroparser")]
pub mod zeroparser;

#[cfg(feature = "internal-arrow-c-data")]
#[doc(hidden)]
pub mod internal;

/// The type of the stream connection created with the server.
/// Currently we only support ephemeral streams on the server side, so we support only that in the SDK as well.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StreamType {
    /// Ephemeral streams exist only for the duration of the connection.
    /// They are not persisted and are not recoverable.
    Ephemeral,
    /// UNSUPPORTED: Persistent streams are durable and recoverable.
    Persistent,
}

/// The properties of the table to ingest to.
///
/// Configure the table via the builder API:
/// `sdk.stream_builder().table("catalog.schema.table").compiled_proto(descriptor)`.
///
/// # Common errors:
/// -`InvalidTableName`: table_name contains invalid characters or doesn't exist
/// -`PermissionDenied`: insufficient permissions to write to the specified table
/// -`InvalidArgument`: invalid or missing descriptor_proto or auth token
#[derive(Debug, Clone)]
pub(crate) struct TableProperties {
    pub(crate) table_name: String,
    pub(crate) descriptor_proto: Option<prost_types::DescriptorProto>,
    pub(crate) message_descriptor: Option<MessageDescriptor>,
}

pub type ZerobusResult<T> = Result<T, ZerobusError>;
