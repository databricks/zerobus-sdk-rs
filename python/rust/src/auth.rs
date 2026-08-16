use async_trait::async_trait;
use pyo3::exceptions::{PyNotImplementedError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3_async_runtimes::TaskLocals;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use databricks_zerobus_ingest_sdk::{
    HeadersProvider as RustHeadersProvider, IdpTokenSupplier, ZerobusError as RustError,
    ZerobusResult as RustResult,
};

use crate::common::intern_header_name;

/// Maps a Python error raised while bridging into the Rust SDK's error type,
/// prefixed with a short context so failures are attributable.
fn py_err_to_rust(context: &str, err: PyErr) -> RustError {
    let msg = format!("{}: {}", context, err);
    RustError::CreateStreamError(tonic::Status::new(tonic::Code::InvalidArgument, msg))
}

/// Base class for custom authentication headers (subclassable from Python)
///
/// The Rust SDK handles OAuth authentication internally by default.
/// Only implement a custom HeadersProvider if you need non-standard authentication.
///
/// Example:
///     class CustomHeadersProvider(HeadersProvider):
///         def get_headers(self):
///             return [
///                 ("authorization", "Bearer my-custom-token"),
///                 ("x-custom-header", "value"),
///             ]
#[pyclass(subclass, skip_from_py_object)]
#[derive(Clone)]
pub struct HeadersProvider {}

#[pymethods]
impl HeadersProvider {
    #[new]
    #[pyo3(signature = (*_args, **_kwargs))]
    fn new(
        _args: &Bound<'_, pyo3::types::PyTuple>,
        _kwargs: Option<&Bound<'_, pyo3::types::PyDict>>,
    ) -> Self {
        // Accept and ignore any arguments to allow Python subclasses to pass their own arguments
        Self {}
    }

    /// Returns headers for gRPC metadata
    ///
    /// Subclasses must implement this method.
    ///
    /// Returns:
    ///     List of (header_name, header_value) tuples
    fn get_headers(&self, _py: Python) -> PyResult<Py<PyAny>> {
        Err(PyNotImplementedError::new_err(
            "Subclasses must implement get_headers()",
        ))
    }
}

// =============================================================================
// HEADERS PROVIDER WRAPPER
// =============================================================================

/// Wrapper that bridges Python HeadersProvider to Rust SDK's HeadersProvider trait
pub struct HeadersProviderWrapper {
    py_obj: Py<PyAny>,
}

impl HeadersProviderWrapper {
    pub fn new(py_obj: Py<PyAny>) -> Self {
        Self { py_obj }
    }
}

#[async_trait]
impl RustHeadersProvider for HeadersProviderWrapper {
    async fn get_headers(&self) -> RustResult<HashMap<&'static str, String>> {
        // Call into Python to get headers
        let headers_vec: Vec<(String, String)> = Python::attach(|py| {
            let method = self.py_obj.getattr(py, "get_headers")?;
            let result = method.call0(py)?;
            let headers: Vec<(String, String)> = result.extract(py)?;
            Ok::<_, PyErr>(headers)
        })
        .map_err(|e: PyErr| {
            let msg = format!("Python HeadersProvider error: {}", e);
            RustError::CreateStreamError(tonic::Status::new(tonic::Code::InvalidArgument, msg))
        })?;

        // Convert Vec<(String, String)> to HashMap<&'static str, String>.
        // The Rust SDK's HeadersProvider trait requires &'static str keys.
        // We intern each distinct header name in a process-wide table so the
        // leak is bounded to the set of names ever used (typically <10), not
        // proportional to the number of get_headers() invocations.
        let mut map = HashMap::with_capacity(headers_vec.len());
        for (key, value) in headers_vec {
            map.insert(intern_header_name(key), value);
        }
        Ok(map)
    }

    async fn invalidate(&self) {
        // Forward to the Python provider's optional `invalidate()` hook so a
        // custom provider can drop cached auth state when the server rejects a
        // token. Providers that do not define one (the common case) are a
        // no-op. Best-effort: any error is swallowed because `invalidate` is a
        // cache-drop hint with no return channel.
        let _ = Python::attach(|py| -> PyResult<()> {
            let obj = self.py_obj.bind(py);
            if let Ok(method) = obj.getattr("invalidate") {
                method.call0()?;
            }
            Ok(())
        });
    }
}

// =============================================================================
// FEDERATED IDP TOKEN SUPPLIER BRIDGE
// =============================================================================

/// The outcome of invoking the Python IdP-token callback: either a token was
/// returned directly (sync callback) or an awaitable was returned that must be
/// driven to completion (async callback).
enum TokenOutcome {
    Ready(String),
    Awaitable(Pin<Box<dyn Future<Output = PyResult<Py<PyAny>>> + Send>>),
}

/// Bridges a Python IdP-token callback to the Rust SDK's [`IdpTokenSupplier`].
///
/// The callback is invoked only when a fresh Databricks token must be minted
/// (a cache miss or refresh), and must return the current external IdP token as
/// a string. Both sync callbacks (return the string directly) and async
/// callbacks (return an awaitable) are supported. Async callbacks are driven
/// via the running event loop, so they require the async SDK.
pub fn make_idp_token_supplier(py_callable: Py<PyAny>) -> IdpTokenSupplier {
    // Capture the running asyncio event loop's task-locals up front, on the
    // Python thread constructing this supplier. The async SDK calls
    // create_stream from inside a running loop, so this succeeds there; the
    // sync SDK has no running loop, so it stays None and only sync callbacks
    // are supported. These locals are required to drive an async (awaitable)
    // callback later, because the supplier runs on a Rust worker thread where
    // no event loop is running (calling into_future there fails with
    // "no running event loop").
    let task_locals: Option<TaskLocals> =
        Python::attach(|py| pyo3_async_runtimes::tokio::get_current_locals(py).ok());

    Arc::new(move || {
        // Invoke the callback under the GIL. If it returned an awaitable,
        // convert it to a Rust future here (GIL held) using the captured
        // event-loop locals, then await it below without holding the GIL.
        let outcome = Python::attach(|py| -> PyResult<TokenOutcome> {
            let result = py_callable.bind(py).call0()?;
            if result.hasattr("__await__")? {
                match task_locals.as_ref() {
                    Some(locals) => {
                        let fut = pyo3_async_runtimes::into_future_with_locals(locals, result)?;
                        Ok(TokenOutcome::Awaitable(Box::pin(fut)))
                    }
                    None => Err(PyRuntimeError::new_err(
                        "async idp_token_supplier requires the async SDK (a running event loop); \
                         use a synchronous callback with the sync SDK",
                    )),
                }
            } else {
                Ok(TokenOutcome::Ready(result.extract::<String>()?))
            }
        });

        Box::pin(async move {
            match outcome {
                Ok(TokenOutcome::Ready(token)) => Ok(token),
                Ok(TokenOutcome::Awaitable(fut)) => {
                    let awaited = fut
                        .await
                        .map_err(|e| py_err_to_rust("federated IdP token callback failed", e))?;
                    Python::attach(|py| awaited.bind(py).extract::<String>()).map_err(|e| {
                        py_err_to_rust("federated IdP token callback returned a non-string", e)
                    })
                }
                Err(e) => Err(py_err_to_rust("federated IdP token callback failed", e)),
            }
        })
    })
}
