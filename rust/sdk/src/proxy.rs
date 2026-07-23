use std::sync::Arc;

use hyper_http_proxy::{Intercept, Proxy, ProxyConnector as HyperProxyConnector};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::connect::HttpConnector;
use tracing::info;

use crate::ZerobusError;

pub(crate) type ProxiedConnector = HyperProxyConnector<HttpsConnector<HttpConnector>>;

/// A proxy connector for Zerobus gRPC transport channels, including Arrow Flight.
///
/// Construct with [`ProxyConnector::new`] and install via
/// [`crate::ZerobusSdkBuilder::connector_factory`] to override the SDK's
/// default env-var proxy detection.
///
/// Supports both `http://` and `https://` proxy URIs — for HTTPS proxies, the
/// client→proxy hop does a TLS handshake using the system trust store, and
/// the CONNECT tunnel still carries raw TCP so tonic can layer its own TLS on
/// top of the target endpoint.
pub struct ProxyConnector(ProxiedConnector);

impl ProxyConnector {
    /// Build a proxy connector that routes all Zerobus gRPC traffic through
    /// `proxy_uri` (e.g. `"http://corp-proxy:3128"` or
    /// `"https://corp-proxy:3128"`).
    #[allow(clippy::result_large_err)]
    pub fn new(proxy_uri: &str) -> Result<Self, ZerobusError> {
        build_connector(proxy_uri).map(Self)
    }

    pub(crate) fn into_inner(self) -> ProxiedConnector {
        self.0
    }
}

#[allow(clippy::result_large_err)]
fn build_connector(proxy_uri: &str) -> Result<ProxiedConnector, ZerobusError> {
    let uri: tonic::transport::Uri = proxy_uri
        .parse()
        .map_err(|e| ZerobusError::InvalidArgument(format!("failed to parse proxy URL: {}", e)))?;
    info!(
        scheme = uri.scheme_str().unwrap_or_default(),
        host = uri.host().unwrap_or_default(),
        port = uri.port_u16(),
        "Using HTTP proxy"
    );
    let mut proxy = Proxy::new(Intercept::All, uri);
    // gRPC is HTTP/2 and cannot traverse a regular HTTP/1 forward proxy;
    // force CONNECT tunneling for all targets (matches gRPC core behavior).
    proxy.force_connect();
    // TLS here is exclusively for an HTTPS proxy. The proxy connector itself
    // must return the raw CONNECT tunnel so tonic can apply the endpoint's TLS
    // exactly once. Giving `HyperProxyConnector` its own target TLS config
    // would make HTTPS endpoints perform a second TLS handshake.
    let proxy_transport = HttpsConnectorBuilder::new()
        .with_native_roots()
        .map_err(|e| {
            ZerobusError::ChannelCreationError(format!(
                "failed to load native roots for proxy connector: {}",
                e
            ))
        })?
        .https_or_http()
        .enable_http1()
        .build();
    Ok(HyperProxyConnector::from_proxy_unsecured(
        proxy_transport,
        proxy,
    ))
}

/// Signature for caller-supplied proxy selection. Given the target host,
/// return a configured connector or `None` for a direct connection.
///
/// Set via [`crate::ZerobusSdkBuilder::connector_factory`]. When a factory is
/// installed it fully replaces the default env-var proxy detection — callers
/// own the complete proxy decision, including any no-proxy bypass rules. The
/// selected policy applies to both standard and Arrow Flight streams, including
/// replacement channels created during recovery.
pub type ConnectorFactory = Arc<dyn Fn(&str) -> Option<ProxyConnector> + Send + Sync>;

/// Resolves the connector policy for a target host.
///
/// A caller-supplied factory fully replaces environment-based proxy discovery.
/// Without a factory, the standard gRPC proxy and no-proxy environment variables
/// determine whether the connection is proxied.
pub(crate) fn resolve_connector(
    host: &str,
    connector_factory: Option<&ConnectorFactory>,
) -> Result<Option<ProxiedConnector>, ZerobusError> {
    match connector_factory {
        Some(factory) => Ok(factory(host).map(ProxyConnector::into_inner)),
        None if !is_no_proxy(host) => create_proxy_connector(),
        None => Ok(None),
    }
}

/// Env var names checked for proxy URL, in gRPC core precedence order.
const PROXY_ENV_VARS: &[&str] = &[
    "grpc_proxy",
    "GRPC_PROXY",
    "https_proxy",
    "HTTPS_PROXY",
    "http_proxy",
    "HTTP_PROXY",
];

/// Env var names checked for no-proxy list, in gRPC core precedence order.
const NO_PROXY_ENV_VARS: &[&str] = &["no_grpc_proxy", "NO_GRPC_PROXY", "no_proxy", "NO_PROXY"];

/// Reads the first non-empty value from the given env var names.
fn read_first_env(names: &[&str]) -> Option<String> {
    for name in names {
        if let Ok(val) = std::env::var(name) {
            if !val.is_empty() {
                return Some(val);
            }
        }
    }
    None
}

/// Reads proxy environment variables and returns a `ProxiedConnector`
/// if one is configured, or `None` for direct connections.
///
/// Follows gRPC core precedence: `grpc_proxy` → `https_proxy` → `http_proxy`.
/// For each name the lowercase variant is checked first, then uppercase
/// (matching standard convention and gRPC core behavior).
///
/// The underlying connector handles TLS for `https://` proxy URLs using the
/// system trust store. The CONNECT tunnel remains raw so tonic applies any
/// target TLS exactly once.
pub(crate) fn create_proxy_connector() -> Result<Option<ProxiedConnector>, ZerobusError> {
    let Some(proxy_url) = read_first_env(PROXY_ENV_VARS) else {
        return Ok(None);
    };
    build_connector(&proxy_url).map(Some)
}

/// Checks whether a given host should bypass the proxy.
///
/// Follows gRPC core precedence: `no_grpc_proxy` → `no_proxy`.
/// For each name the lowercase variant is checked first, then uppercase.
/// A wildcard `*` matches all hosts. Otherwise entries are matched as
/// suffix of the target host (e.g. `example.com` matches `foo.example.com`).
pub(crate) fn is_no_proxy(host: &str) -> bool {
    let no_proxy = read_first_env(NO_PROXY_ENV_VARS).unwrap_or_default();
    host_matches_no_proxy(host, &no_proxy)
}

/// Pure logic for no-proxy matching, separated for testability.
fn host_matches_no_proxy(host: &str, no_proxy: &str) -> bool {
    if no_proxy.is_empty() {
        return false;
    }

    if no_proxy.trim() == "*" {
        return true;
    }

    no_proxy.split(',').any(|entry| {
        let entry = entry.trim().trim_start_matches('.');
        host == entry || host.ends_with(&format!(".{}", entry))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_proxy_empty_returns_false() {
        assert!(!host_matches_no_proxy("example.com", ""));
    }

    #[test]
    fn no_proxy_wildcard_matches_everything() {
        assert!(host_matches_no_proxy("anything.com", "*"));
        assert!(host_matches_no_proxy("localhost", " * "));
    }

    #[test]
    fn no_proxy_exact_match() {
        assert!(host_matches_no_proxy("example.com", "example.com"));
        assert!(!host_matches_no_proxy("other.com", "example.com"));
    }

    #[test]
    fn no_proxy_suffix_match() {
        assert!(host_matches_no_proxy(
            "workspace.cloud.databricks.com",
            "databricks.com"
        ));
        assert!(host_matches_no_proxy("foo.example.com", "example.com"));
        // Must be a subdomain, not just a string suffix
        assert!(!host_matches_no_proxy("notexample.com", "example.com"));
    }

    #[test]
    fn no_proxy_leading_dot_stripped() {
        assert!(host_matches_no_proxy("foo.example.com", ".example.com"));
        assert!(host_matches_no_proxy("example.com", ".example.com"));
    }

    #[test]
    fn no_proxy_comma_separated() {
        let no_proxy = "localhost, 127.0.0.1, .internal.corp";
        assert!(host_matches_no_proxy("localhost", no_proxy));
        assert!(host_matches_no_proxy("127.0.0.1", no_proxy));
        assert!(host_matches_no_proxy("service.internal.corp", no_proxy));
        assert!(!host_matches_no_proxy("external.com", no_proxy));
    }

    #[test]
    fn no_proxy_whitespace_handling() {
        assert!(host_matches_no_proxy("example.com", "  example.com  "));
        assert!(host_matches_no_proxy(
            "example.com",
            "other.com , example.com , more.com"
        ));
    }

    #[test]
    fn invalid_proxy_error_does_not_expose_credentials() {
        let result = build_connector("http://proxy-user:super-secret@/proxy");
        let error = match result {
            Ok(_) => panic!("expected invalid proxy URL to fail"),
            Err(error) => error.to_string(),
        };

        assert!(!error.contains("proxy-user"));
        assert!(!error.contains("super-secret"));
    }
}
