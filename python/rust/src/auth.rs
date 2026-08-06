use async_trait::async_trait;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;
use std::collections::HashMap;

use databricks_zerobus_ingest_sdk::{
    HeadersProvider as RustHeadersProvider, ZerobusError as RustError, ZerobusResult as RustResult,
};

use crate::common::intern_header_name;

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
    #[pyo3(signature = (**_kwargs))]
    fn new(_kwargs: Option<&Bound<'_, pyo3::types::PyDict>>) -> Self {
        // Accept and ignore kwargs to allow Python subclasses to pass their own arguments
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
}
