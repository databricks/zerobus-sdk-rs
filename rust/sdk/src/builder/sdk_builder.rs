//! Builder for creating [`ZerobusSdk`] instances.

use std::sync::Arc;

use crate::proxy::ConnectorFactory;
use crate::{SecureTlsConfig, TlsConfig, ZerobusError, ZerobusResult, ZerobusSdk};

/// Builder for creating a [`ZerobusSdk`] instance with fluent configuration.
///
/// # Examples
///
/// ```no_run
/// use databricks_zerobus_ingest_sdk::ZerobusSdkBuilder;
///
/// let sdk = ZerobusSdkBuilder::new()
///     .endpoint("https://workspace.zerobus.databricks.com")
///     .unity_catalog_url("https://workspace.cloud.databricks.com")
///     .build()?;
/// # Ok::<(), databricks_zerobus_ingest_sdk::ZerobusError>(())
/// ```
pub struct ZerobusSdkBuilder {
    zerobus_endpoint: Option<String>,
    unity_catalog_url: Option<String>,
    tls_config: Option<Arc<dyn TlsConfig>>,
    connector_factory: Option<ConnectorFactory>,
}

impl ZerobusSdkBuilder {
    /// Creates a new SDK builder with default settings.
    ///
    /// TLS is enabled by default using `SecureTlsConfig`.
    pub fn new() -> Self {
        Self {
            zerobus_endpoint: None,
            unity_catalog_url: None,
            tls_config: None,
            connector_factory: None,
        }
    }

    /// Sets the Zerobus API endpoint URL.
    ///
    /// This is required. The workspace ID is automatically extracted from this URL.
    ///
    /// # Arguments
    ///
    /// * `endpoint` - The Zerobus endpoint URL (e.g., "https://workspace-id.zerobus.region.cloud.databricks.com")
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.zerobus_endpoint = Some(endpoint.into());
        self
    }

    /// Sets the Unity Catalog endpoint URL.
    ///
    /// This is only required when using OAuth authentication via `create_stream()`.
    /// When using `create_stream_with_headers_provider()` with a custom headers
    /// provider, this can be omitted.
    ///
    /// # Arguments
    ///
    /// * `url` - The Unity Catalog URL (e.g., "https://workspace.cloud.databricks.com")
    pub fn unity_catalog_url(mut self, url: impl Into<String>) -> Self {
        self.unity_catalog_url = Some(url.into());
        self
    }

    /// Sets a custom TLS configuration.
    ///
    /// Use this to provide custom certificate handling or other TLS settings.
    /// If not set, the default `SecureTlsConfig` (system CA certificates) is used.
    ///
    /// # Arguments
    ///
    /// * `tls_config` - A TLS configuration implementing the `TlsConfig` trait
    pub fn tls_config(mut self, tls_config: Arc<dyn TlsConfig>) -> Self {
        self.tls_config = Some(tls_config);
        self
    }

    /// Override gRPC channel connector construction; see
    /// [`ConnectorFactory`] for semantics.
    pub fn connector_factory(mut self, factory: ConnectorFactory) -> Self {
        self.connector_factory = Some(factory);
        self
    }

    /// Builds the [`ZerobusSdk`] instance.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The endpoint is not set
    /// - The workspace ID cannot be extracted from the endpoint
    #[allow(clippy::result_large_err)]
    pub fn build(self) -> ZerobusResult<ZerobusSdk> {
        let zerobus_endpoint = self
            .zerobus_endpoint
            .ok_or_else(|| ZerobusError::InvalidArgument("endpoint is required".to_string()))?;

        let zerobus_endpoint = if !zerobus_endpoint.starts_with("https://")
            && !zerobus_endpoint.starts_with("http://")
        {
            format!("https://{}", zerobus_endpoint)
        } else {
            zerobus_endpoint
        };

        let unity_catalog_url = self.unity_catalog_url.unwrap_or_default();

        let workspace_id = zerobus_endpoint
            .strip_prefix("https://")
            .or_else(|| zerobus_endpoint.strip_prefix("http://"))
            .and_then(|s| s.split('.').next())
            .map(|s| s.to_string())
            .ok_or_else(|| {
                ZerobusError::InvalidArgument(
                    "Failed to extract workspace ID from zerobus_endpoint".to_string(),
                )
            })?;

        let tls_config = self
            .tls_config
            .unwrap_or_else(|| Arc::new(SecureTlsConfig::new()));

        Ok(ZerobusSdk::new_with_config(
            zerobus_endpoint,
            unity_catalog_url,
            workspace_id,
            tls_config,
            self.connector_factory,
        ))
    }
}

impl Default for ZerobusSdkBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_with_all_fields() {
        let sdk = ZerobusSdkBuilder::new()
            .endpoint("https://my-workspace.zerobus.us-east-1.cloud.databricks.com")
            .unity_catalog_url("https://my-workspace.cloud.databricks.com")
            .build()
            .expect("should build successfully");

        assert_eq!(
            sdk.zerobus_endpoint,
            "https://my-workspace.zerobus.us-east-1.cloud.databricks.com"
        );
        assert_eq!(
            sdk.unity_catalog_url,
            "https://my-workspace.cloud.databricks.com"
        );
    }

    #[test]
    fn test_builder_missing_endpoint() {
        let result = ZerobusSdkBuilder::new()
            .unity_catalog_url("https://workspace.cloud.databricks.com")
            .build();

        assert!(matches!(
            result,
            Err(ZerobusError::InvalidArgument(msg)) if msg.contains("endpoint is required")
        ));
    }

    #[test]
    fn test_builder_schemeless_endpoint() {
        // Endpoint without protocol prefix - https:// is prepended automatically
        let sdk = ZerobusSdkBuilder::new()
            .endpoint("my-workspace.zerobus.databricks.com")
            .build()
            .expect("should build successfully with schemeless endpoint");

        assert_eq!(sdk.workspace_id, "my-workspace");
        assert_eq!(
            sdk.zerobus_endpoint,
            "https://my-workspace.zerobus.databricks.com"
        );
    }

    #[test]
    fn test_builder_without_unity_catalog_url() {
        // Unity Catalog URL is optional for custom headers providers
        let sdk = ZerobusSdkBuilder::new()
            .endpoint("https://workspace.zerobus.databricks.com")
            .build()
            .expect("should build successfully without unity_catalog_url");

        assert_eq!(sdk.unity_catalog_url, "");
    }
}
