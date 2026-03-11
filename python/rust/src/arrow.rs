//! PyO3 bindings for Arrow Flight stream support.
//!
//! These types are always compiled into the wheel, but the Python-side API
//! gates usage on `pyarrow` being installed at runtime.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tokio::sync::RwLock;

use databricks_zerobus_ingest_sdk::{
    ArrowStreamConfigurationOptions as RustArrowStreamOptions,
    ArrowTableProperties as RustArrowTableProperties,
    ZerobusArrowStream as RustZerobusArrowStream, ZerobusError as RustError,
    ZerobusSdk as RustSdk,
};

use crate::auth::HeadersProviderWrapper;
use crate::common::map_error;

fn map_rust_error_to_pyerr(err: RustError) -> PyErr {
    map_error(err)
}

/// Deserialize Arrow IPC bytes into a RecordBatch.
fn ipc_bytes_to_record_batch(
    ipc_bytes: &[u8],
) -> Result<arrow_array::RecordBatch, RustError> {
    let reader =
        arrow_ipc::reader::StreamReader::try_new(ipc_bytes, None).map_err(|e| {
            RustError::InvalidArgument(format!("Failed to parse Arrow IPC data: {}", e))
        })?;

    let mut batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|e| {
            RustError::InvalidArgument(format!("Failed to read Arrow batch: {}", e))
        })?;
        batches.push(batch);
    }

    if batches.is_empty() {
        return Err(RustError::InvalidArgument(
            "No batches found in Arrow IPC data".to_string(),
        ));
    }

    Ok(batches.into_iter().next().unwrap())
}

/// Serialize a RecordBatch to Arrow IPC bytes.
fn record_batch_to_ipc_bytes(
    batch: &arrow_array::RecordBatch,
) -> Result<Vec<u8>, RustError> {
    let mut buffer = Vec::new();
    {
        let mut writer =
            arrow_ipc::writer::StreamWriter::try_new(&mut buffer, &batch.schema())
                .map_err(|e| {
                    RustError::InvalidArgument(format!(
                        "Failed to create Arrow IPC writer: {}",
                        e
                    ))
                })?;
        writer.write(batch).map_err(|e| {
            RustError::InvalidArgument(format!("Failed to write Arrow batch: {}", e))
        })?;
        writer.finish().map_err(|e| {
            RustError::InvalidArgument(format!(
                "Failed to finish Arrow IPC stream: {}",
                e
            ))
        })?;
    }
    Ok(buffer)
}

/// Build an ArrowSchema from IPC-serialized schema bytes.
///
/// Python side calls `schema.serialize().to_pybytes()` on a `pyarrow.Schema`
/// to produce the IPC stream bytes. The schema is the first message in the stream.
fn ipc_schema_bytes_to_arrow_schema(
    schema_bytes: &[u8],
) -> Result<arrow_schema::Schema, RustError> {
    let reader = arrow_ipc::reader::StreamReader::try_new(schema_bytes, None)
        .map_err(|e| {
            RustError::InvalidArgument(format!(
                "Failed to parse Arrow schema bytes: {}. \
                 Pass schema bytes obtained from pyarrow.Schema via \
                 schema.serialize().to_pybytes()",
                e
            ))
        })?;
    Ok(reader.schema().as_ref().clone())
}

// =============================================================================
// ARROW STREAM CONFIGURATION OPTIONS
// =============================================================================

/// Configuration options for Arrow Flight streams.
#[pyclass]
#[derive(Clone)]
pub struct ArrowStreamConfigurationOptions {
    #[pyo3(get, set)]
    pub max_inflight_batches: i32,

    #[pyo3(get, set)]
    pub recovery: bool,

    #[pyo3(get, set)]
    pub recovery_timeout_ms: i64,

    #[pyo3(get, set)]
    pub recovery_backoff_ms: i64,

    #[pyo3(get, set)]
    pub recovery_retries: i32,

    #[pyo3(get, set)]
    pub server_lack_of_ack_timeout_ms: i64,

    #[pyo3(get, set)]
    pub flush_timeout_ms: i64,

    #[pyo3(get, set)]
    pub connection_timeout_ms: i64,
}

impl Default for ArrowStreamConfigurationOptions {
    fn default() -> Self {
        let rust_default = RustArrowStreamOptions::default();
        Self {
            max_inflight_batches: rust_default.max_inflight_batches as i32,
            recovery: rust_default.recovery,
            recovery_timeout_ms: rust_default.recovery_timeout_ms as i64,
            recovery_backoff_ms: rust_default.recovery_backoff_ms as i64,
            recovery_retries: rust_default.recovery_retries as i32,
            server_lack_of_ack_timeout_ms: rust_default.server_lack_of_ack_timeout_ms
                as i64,
            flush_timeout_ms: rust_default.flush_timeout_ms as i64,
            connection_timeout_ms: rust_default.connection_timeout_ms as i64,
        }
    }
}

#[pymethods]
impl ArrowStreamConfigurationOptions {
    #[new]
    #[pyo3(signature = (**kwargs))]
    fn new(kwargs: Option<&pyo3::types::PyDict>) -> PyResult<Self> {
        let mut options = Self::default();

        if let Some(kwargs) = kwargs {
            for (key, value) in kwargs {
                let key_str: &str = key.extract()?;
                match key_str {
                    "max_inflight_batches" => {
                        options.max_inflight_batches = value.extract()?
                    }
                    "recovery" => options.recovery = value.extract()?,
                    "recovery_timeout_ms" => {
                        options.recovery_timeout_ms = value.extract()?
                    }
                    "recovery_backoff_ms" => {
                        options.recovery_backoff_ms = value.extract()?
                    }
                    "recovery_retries" => options.recovery_retries = value.extract()?,
                    "server_lack_of_ack_timeout_ms" => {
                        options.server_lack_of_ack_timeout_ms = value.extract()?
                    }
                    "flush_timeout_ms" => options.flush_timeout_ms = value.extract()?,
                    "connection_timeout_ms" => {
                        options.connection_timeout_ms = value.extract()?
                    }
                    _ => {
                        return Err(pyo3::exceptions::PyValueError::new_err(format!(
                            "Unknown configuration option: {}",
                            key_str
                        )));
                    }
                }
            }
        }

        Ok(options)
    }

    fn __repr__(&self) -> String {
        format!(
            "ArrowStreamConfigurationOptions(max_inflight_batches={}, recovery={}, \
             recovery_timeout_ms={}, recovery_backoff_ms={}, recovery_retries={}, \
             server_lack_of_ack_timeout_ms={}, flush_timeout_ms={}, connection_timeout_ms={})",
            self.max_inflight_batches,
            self.recovery,
            self.recovery_timeout_ms,
            self.recovery_backoff_ms,
            self.recovery_retries,
            self.server_lack_of_ack_timeout_ms,
            self.flush_timeout_ms,
            self.connection_timeout_ms,
        )
    }
}

impl ArrowStreamConfigurationOptions {
    pub fn to_rust(&self) -> RustArrowStreamOptions {
        RustArrowStreamOptions {
            max_inflight_batches: self.max_inflight_batches as usize,
            recovery: self.recovery,
            recovery_timeout_ms: self.recovery_timeout_ms as u64,
            recovery_backoff_ms: self.recovery_backoff_ms as u64,
            recovery_retries: self.recovery_retries as u32,
            server_lack_of_ack_timeout_ms: self.server_lack_of_ack_timeout_ms as u64,
            flush_timeout_ms: self.flush_timeout_ms as u64,
            connection_timeout_ms: self.connection_timeout_ms as u64,
            ipc_compression: None,
        }
    }
}

// =============================================================================
// SYNC ARROW STREAM
// =============================================================================

/// Synchronous Arrow Flight stream for ingesting pyarrow RecordBatches.
#[pyclass]
pub struct ZerobusArrowStream {
    inner: Arc<RwLock<RustZerobusArrowStream>>,
    runtime: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl ZerobusArrowStream {
    /// Ingest a single Arrow RecordBatch (as IPC bytes) and return the offset.
    ///
    /// Args:
    ///     ipc_bytes: Arrow IPC serialized bytes from pyarrow.RecordBatch.serialize()
    fn ingest_batch(&self, py: Python, ipc_bytes: &PyBytes) -> PyResult<i64> {
        let batch = ipc_bytes_to_record_batch(ipc_bytes.as_bytes())
            .map_err(|e| map_rust_error_to_pyerr(e))?;

        let stream_clone = self.inner.clone();
        let runtime = self.runtime.clone();

        py.allow_threads(|| {
            runtime.block_on(async move {
                let stream_guard = stream_clone.read().await;
                stream_guard
                    .ingest_batch(batch)
                    .await
                    .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))
            })
        })
    }

    /// Wait for a specific offset to be acknowledged.
    fn wait_for_offset(&self, py: Python, offset: i64) -> PyResult<()> {
        let stream_clone = self.inner.clone();
        let runtime = self.runtime.clone();

        py.allow_threads(|| {
            runtime.block_on(async move {
                let stream_guard = stream_clone.read().await;
                stream_guard
                    .wait_for_offset(offset)
                    .await
                    .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))
            })
        })
    }

    /// Flush all pending batches, waiting for acknowledgment.
    fn flush(&self, py: Python) -> PyResult<()> {
        let stream_clone = self.inner.clone();
        let runtime = self.runtime.clone();

        py.allow_threads(|| {
            runtime.block_on(async move {
                let stream_guard = stream_clone.read().await;
                stream_guard
                    .flush()
                    .await
                    .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))
            })
        })
    }

    /// Close the stream gracefully.
    fn close(&self, py: Python) -> PyResult<()> {
        let stream_clone = self.inner.clone();
        let runtime = self.runtime.clone();

        py.allow_threads(|| {
            runtime.block_on(async move {
                let mut stream_guard = stream_clone.write().await;
                stream_guard
                    .close()
                    .await
                    .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))
            })
        })
    }

    /// Check if the stream has been closed.
    #[getter]
    fn is_closed(&self) -> bool {
        let stream_clone = self.inner.clone();
        self.runtime.block_on(async move {
            let stream_guard = stream_clone.read().await;
            stream_guard.is_closed()
        })
    }

    /// Get the table name.
    #[getter]
    fn table_name(&self) -> String {
        let stream_clone = self.inner.clone();
        self.runtime.block_on(async move {
            let stream_guard = stream_clone.read().await;
            stream_guard.table_name().to_string()
        })
    }

    /// Get unacknowledged batches as a list of Arrow IPC byte buffers.
    fn get_unacked_batches(&self, py: Python) -> PyResult<Vec<PyObject>> {
        let stream_clone = self.inner.clone();
        let runtime = self.runtime.clone();

        py.allow_threads(|| {
            runtime.block_on(async move {
                let stream_guard = stream_clone.read().await;
                let batches = stream_guard
                    .get_unacked_batches()
                    .await
                    .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))?;

                Python::with_gil(|py| {
                    let mut py_batches: Vec<PyObject> = Vec::with_capacity(batches.len());
                    for batch in &batches {
                        let ipc_bytes = record_batch_to_ipc_bytes(batch)
                            .map_err(|e| map_rust_error_to_pyerr(e))?;
                        py_batches
                            .push(PyBytes::new(py, &ipc_bytes).into());
                    }
                    Ok(py_batches)
                })
            })
        })
    }
}

// =============================================================================
// ASYNC ARROW STREAM
// =============================================================================

/// Asynchronous Arrow Flight stream for ingesting pyarrow RecordBatches.
#[pyclass(name = "AsyncZerobusArrowStream")]
pub struct AsyncZerobusArrowStream {
    inner: Arc<RwLock<RustZerobusArrowStream>>,
}

#[pymethods]
impl AsyncZerobusArrowStream {
    /// Ingest a single Arrow RecordBatch (as IPC bytes) and return the offset.
    fn ingest_batch<'py>(
        &self,
        py: Python<'py>,
        ipc_bytes: &PyBytes,
    ) -> PyResult<&'py PyAny> {
        let batch = ipc_bytes_to_record_batch(ipc_bytes.as_bytes())
            .map_err(|e| map_rust_error_to_pyerr(e))?;

        let stream_clone = self.inner.clone();

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let stream_guard = stream_clone.read().await;
            stream_guard
                .ingest_batch(batch)
                .await
                .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))
        })
    }

    /// Wait for a specific offset to be acknowledged.
    fn wait_for_offset<'py>(
        &self,
        py: Python<'py>,
        offset: i64,
    ) -> PyResult<&'py PyAny> {
        let stream_clone = self.inner.clone();

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let stream_guard = stream_clone.read().await;
            stream_guard
                .wait_for_offset(offset)
                .await
                .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))?;
            Ok(())
        })
    }

    /// Flush all pending batches.
    fn flush<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        let stream_clone = self.inner.clone();

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let stream_guard = stream_clone.read().await;
            stream_guard
                .flush()
                .await
                .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))?;
            Ok(())
        })
    }

    /// Close the stream gracefully.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        let stream_clone = self.inner.clone();

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let mut stream_guard = stream_clone.write().await;
            stream_guard
                .close()
                .await
                .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))?;
            Ok(())
        })
    }

    /// Check if the stream has been closed.
    #[getter]
    fn is_closed(&self) -> bool {
        // This needs a runtime to lock, but is_closed is atomic so
        // we can use try_read or spawn.
        let stream_clone = self.inner.clone();
        pyo3_asyncio::tokio::get_runtime().block_on(async move {
            let stream_guard = stream_clone.read().await;
            stream_guard.is_closed()
        })
    }

    /// Get the table name.
    #[getter]
    fn table_name(&self) -> String {
        let stream_clone = self.inner.clone();
        pyo3_asyncio::tokio::get_runtime().block_on(async move {
            let stream_guard = stream_clone.read().await;
            stream_guard.table_name().to_string()
        })
    }

    /// Get unacknowledged batches as a list of Arrow IPC byte buffers.
    fn get_unacked_batches<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        let stream_clone = self.inner.clone();

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let stream_guard = stream_clone.read().await;
            let batches = stream_guard
                .get_unacked_batches()
                .await
                .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))?;

            Python::with_gil(|py| {
                let mut py_batches: Vec<PyObject> = Vec::with_capacity(batches.len());
                for batch in &batches {
                    let ipc_bytes = record_batch_to_ipc_bytes(batch)
                        .map_err(|e| map_rust_error_to_pyerr(e))?;
                    py_batches.push(PyBytes::new(py, &ipc_bytes).into());
                }
                Ok(py_batches)
            })
        })
    }
}

// =============================================================================
// SDK METHODS — called from sync_wrapper and async_wrapper
// =============================================================================

/// Create an Arrow stream (sync helper).
pub fn create_arrow_stream_sync(
    sdk: &Arc<RwLock<RustSdk>>,
    runtime: &Arc<tokio::runtime::Runtime>,
    py: Python,
    table_name: String,
    schema_ipc_bytes: &[u8],
    client_id: String,
    client_secret: String,
    options: Option<&ArrowStreamConfigurationOptions>,
) -> PyResult<ZerobusArrowStream> {
    let schema = ipc_schema_bytes_to_arrow_schema(schema_ipc_bytes)
        .map_err(|e| map_rust_error_to_pyerr(e))?;

    let table_props = RustArrowTableProperties {
        table_name,
        schema: Arc::new(schema),
    };

    let rust_options = options.map(|o| o.to_rust());

    let sdk_clone = sdk.clone();
    let runtime_clone = runtime.clone();

    let stream = py.allow_threads(|| {
        runtime_clone.block_on(async move {
            let sdk_guard = sdk_clone.read().await;
            sdk_guard
                .create_arrow_stream(table_props, client_id, client_secret, rust_options)
                .await
                .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))
        })
    })?;

    Ok(ZerobusArrowStream {
        inner: Arc::new(RwLock::new(stream)),
        runtime: runtime.clone(),
    })
}

/// Create an Arrow stream with headers provider (sync helper).
pub fn create_arrow_stream_with_headers_provider_sync(
    sdk: &Arc<RwLock<RustSdk>>,
    runtime: &Arc<tokio::runtime::Runtime>,
    py: Python,
    table_name: String,
    schema_ipc_bytes: &[u8],
    headers_provider: PyObject,
    options: Option<&ArrowStreamConfigurationOptions>,
) -> PyResult<ZerobusArrowStream> {
    let schema = ipc_schema_bytes_to_arrow_schema(schema_ipc_bytes)
        .map_err(|e| map_rust_error_to_pyerr(e))?;

    let table_props = RustArrowTableProperties {
        table_name,
        schema: Arc::new(schema),
    };

    let rust_options = options.map(|o| o.to_rust());
    let provider = Arc::new(HeadersProviderWrapper::new(headers_provider));

    let sdk_clone = sdk.clone();
    let runtime_clone = runtime.clone();

    let stream = py.allow_threads(|| {
        runtime_clone.block_on(async move {
            let sdk_guard = sdk_clone.read().await;
            sdk_guard
                .create_arrow_stream_with_headers_provider(
                    table_props,
                    provider,
                    rust_options,
                )
                .await
                .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))
        })
    })?;

    Ok(ZerobusArrowStream {
        inner: Arc::new(RwLock::new(stream)),
        runtime: runtime.clone(),
    })
}

/// Recreate an Arrow stream from a closed stream (sync helper).
pub fn recreate_arrow_stream_sync(
    sdk: &Arc<RwLock<RustSdk>>,
    runtime: &Arc<tokio::runtime::Runtime>,
    py: Python,
    old_stream: &ZerobusArrowStream,
) -> PyResult<ZerobusArrowStream> {
    let sdk_clone = sdk.clone();
    let old_inner = old_stream.inner.clone();
    let runtime_clone = runtime.clone();

    let stream = py.allow_threads(|| {
        runtime_clone.block_on(async move {
            let old_guard = old_inner.read().await;
            let sdk_guard = sdk_clone.read().await;
            sdk_guard
                .recreate_arrow_stream(&*old_guard)
                .await
                .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))
        })
    })?;

    Ok(ZerobusArrowStream {
        inner: Arc::new(RwLock::new(stream)),
        runtime: runtime.clone(),
    })
}

/// Create an Arrow stream (async helper).
pub fn create_arrow_stream_async<'py>(
    sdk: &Arc<RwLock<RustSdk>>,
    py: Python<'py>,
    table_name: String,
    schema_ipc_bytes: Vec<u8>,
    client_id: String,
    client_secret: String,
    options: Option<ArrowStreamConfigurationOptions>,
) -> PyResult<&'py PyAny> {
    let schema = ipc_schema_bytes_to_arrow_schema(&schema_ipc_bytes)
        .map_err(|e| map_rust_error_to_pyerr(e))?;

    let table_props = RustArrowTableProperties {
        table_name,
        schema: Arc::new(schema),
    };

    let rust_options = options.map(|o| o.to_rust());
    let sdk_clone = sdk.clone();

    pyo3_asyncio::tokio::future_into_py(py, async move {
        let sdk_guard = sdk_clone.read().await;
        let stream = sdk_guard
            .create_arrow_stream(table_props, client_id, client_secret, rust_options)
            .await
            .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))?;

        Ok(AsyncZerobusArrowStream {
            inner: Arc::new(RwLock::new(stream)),
        })
    })
}

/// Create an Arrow stream with headers provider (async helper).
pub fn create_arrow_stream_with_headers_provider_async<'py>(
    sdk: &Arc<RwLock<RustSdk>>,
    py: Python<'py>,
    table_name: String,
    schema_ipc_bytes: Vec<u8>,
    headers_provider: PyObject,
    options: Option<ArrowStreamConfigurationOptions>,
) -> PyResult<&'py PyAny> {
    let schema = ipc_schema_bytes_to_arrow_schema(&schema_ipc_bytes)
        .map_err(|e| map_rust_error_to_pyerr(e))?;

    let table_props = RustArrowTableProperties {
        table_name,
        schema: Arc::new(schema),
    };

    let rust_options = options.map(|o| o.to_rust());
    let provider = Arc::new(HeadersProviderWrapper::new(headers_provider));
    let sdk_clone = sdk.clone();

    pyo3_asyncio::tokio::future_into_py(py, async move {
        let sdk_guard = sdk_clone.read().await;
        let stream = sdk_guard
            .create_arrow_stream_with_headers_provider(
                table_props,
                provider,
                rust_options,
            )
            .await
            .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))?;

        Ok(AsyncZerobusArrowStream {
            inner: Arc::new(RwLock::new(stream)),
        })
    })
}

/// Recreate an Arrow stream from a closed stream (async helper).
pub fn recreate_arrow_stream_async<'py>(
    sdk: &Arc<RwLock<RustSdk>>,
    py: Python<'py>,
    old_stream: &AsyncZerobusArrowStream,
) -> PyResult<&'py PyAny> {
    let sdk_clone = sdk.clone();
    let old_inner = old_stream.inner.clone();

    pyo3_asyncio::tokio::future_into_py(py, async move {
        let old_guard = old_inner.read().await;
        let sdk_guard = sdk_clone.read().await;
        let stream = sdk_guard
            .recreate_arrow_stream(&*old_guard)
            .await
            .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))?;

        Ok(AsyncZerobusArrowStream {
            inner: Arc::new(RwLock::new(stream)),
        })
    })
}
