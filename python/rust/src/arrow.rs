//! PyO3 bindings for Arrow Flight stream support.
//!
//! **Beta**: Arrow Flight ingestion is in Beta. The API is stabilising but
//! may still change before reaching GA.
//!
//! These types are always compiled into the wheel, but the Python-side API
//! gates usage on `pyarrow` being installed at runtime.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tokio::sync::RwLock;

use databricks_zerobus_ingest_sdk::{
    StreamBuilder, ZerobusArrowStream as RustZerobusArrowStream, ZerobusError as RustError,
    ZerobusSdk as RustSdk,
};

use crate::auth::HeadersProviderWrapper;
use crate::common::map_error;

// =============================================================================
// IPC HELPERS
// =============================================================================

/// Deserialize Arrow IPC bytes into exactly one RecordBatch (used only on the
/// compressed ingest path).
fn ipc_bytes_to_record_batch(ipc_bytes: &[u8]) -> Result<arrow_array::RecordBatch, RustError> {
    let mut reader = arrow_ipc::reader::StreamReader::try_new(ipc_bytes, None).map_err(|e| {
        RustError::InvalidArgument(format!("Failed to parse Arrow IPC data: {}", e))
    })?;

    let batch = reader
        .next()
        .ok_or_else(|| {
            RustError::InvalidArgument("No batches found in Arrow IPC data".to_string())
        })?
        .map_err(|e| RustError::InvalidArgument(format!("Failed to read Arrow batch: {}", e)))?;

    if reader.next().is_some() {
        return Err(RustError::InvalidArgument(
            "Expected exactly one RecordBatch in Arrow IPC data, found multiple".to_string(),
        ));
    }

    Ok(batch)
}

/// Serialize a RecordBatch back to Arrow IPC bytes (used by `get_unacked_batches`).
fn record_batch_to_ipc_bytes(batch: &arrow_array::RecordBatch) -> Result<Vec<u8>, RustError> {
    let mut buffer = Vec::new();
    {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut buffer, &batch.schema())
            .map_err(|e| {
                RustError::InvalidArgument(format!("Failed to create Arrow IPC writer: {}", e))
            })?;
        writer.write(batch).map_err(|e| {
            RustError::InvalidArgument(format!("Failed to write Arrow batch: {}", e))
        })?;
        writer.finish().map_err(|e| {
            RustError::InvalidArgument(format!("Failed to finish Arrow IPC stream: {}", e))
        })?;
    }
    Ok(buffer)
}

/// Build an ArrowSchema from Arrow IPC stream bytes (schema-only, no batches).
fn ipc_schema_bytes_to_arrow_schema(
    schema_bytes: &[u8],
) -> Result<arrow_schema::Schema, RustError> {
    let reader = arrow_ipc::reader::StreamReader::try_new(schema_bytes, None).map_err(|e| {
        RustError::InvalidArgument(format!(
            "Failed to parse Arrow IPC schema bytes: {}. \
                 Pass bytes from pa.ipc.new_stream(sink, schema) with no batches written.",
            e
        ))
    })?;
    Ok(reader.schema().as_ref().clone())
}

// =============================================================================
// ARROW STREAM CONFIGURATION OPTIONS
// =============================================================================

/// Arrow IPC compression codec.
#[pyclass(from_py_object)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum IPCCompression {
    /// No compression (default).
    #[pyo3(name = "NONE")]
    Uncompressed = 0,
    /// LZ4 frame compression.
    #[pyo3(name = "LZ4_FRAME")]
    LZ4Frame = 1,
    /// Zstandard compression.
    #[pyo3(name = "ZSTD")]
    Zstd = 2,
}

#[pymethods]
impl IPCCompression {
    fn __repr__(&self) -> &'static str {
        match self {
            IPCCompression::Uncompressed => "IPCCompression.NONE",
            IPCCompression::LZ4Frame => "IPCCompression.LZ4_FRAME",
            IPCCompression::Zstd => "IPCCompression.ZSTD",
        }
    }
}

impl IPCCompression {
    fn to_rust(self) -> Option<arrow_ipc::CompressionType> {
        match self {
            IPCCompression::Uncompressed => None,
            IPCCompression::LZ4Frame => Some(arrow_ipc::CompressionType::LZ4_FRAME),
            IPCCompression::Zstd => Some(arrow_ipc::CompressionType::ZSTD),
        }
    }
}

/// Configuration options for Arrow Flight streams.
///
/// **Beta**: Arrow Flight ingestion is in Beta. The API is stabilising but
/// may still change before reaching GA.
#[pyclass(from_py_object)]
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

    /// IPC compression codec. Default: IPCCompression.NONE
    #[pyo3(get, set)]
    pub ipc_compression: IPCCompression,

    /// Maximum time in milliseconds to wait during graceful stream close.
    /// None = wait full server duration, 0 = immediate recovery, >0 = wait up to min(this, server_duration).
    #[pyo3(get, set)]
    pub stream_paused_max_wait_time_ms: Option<i64>,
}

impl Default for ArrowStreamConfigurationOptions {
    fn default() -> Self {
        // Mirror the Rust SDK 2.0.0 `ArrowStreamConfigurationOptions` defaults.
        // Hardcoded because the upstream struct is `#[non_exhaustive]` and
        // its `Default::default()` can no longer be destructured here.
        Self {
            max_inflight_batches: 1_000,
            recovery: true,
            recovery_timeout_ms: 15_000,
            recovery_backoff_ms: 2_000,
            recovery_retries: 4,
            server_lack_of_ack_timeout_ms: 60_000,
            flush_timeout_ms: 300_000,
            connection_timeout_ms: 30_000,
            ipc_compression: IPCCompression::Uncompressed,
            stream_paused_max_wait_time_ms: None,
        }
    }
}

#[pymethods]
impl ArrowStreamConfigurationOptions {
    #[new]
    #[pyo3(signature = (**kwargs))]
    fn new(kwargs: Option<&Bound<'_, pyo3::types::PyDict>>) -> PyResult<Self> {
        let mut options = Self::default();

        if let Some(kwargs) = kwargs {
            for (key, value) in kwargs.iter() {
                let key_str: String = key.extract()?;
                let key_str = key_str.as_str();
                match key_str {
                    "max_inflight_batches" => options.max_inflight_batches = value.extract()?,
                    "recovery" => options.recovery = value.extract()?,
                    "recovery_timeout_ms" => options.recovery_timeout_ms = value.extract()?,
                    "recovery_backoff_ms" => options.recovery_backoff_ms = value.extract()?,
                    "recovery_retries" => options.recovery_retries = value.extract()?,
                    "server_lack_of_ack_timeout_ms" => {
                        options.server_lack_of_ack_timeout_ms = value.extract()?
                    }
                    "flush_timeout_ms" => options.flush_timeout_ms = value.extract()?,
                    "connection_timeout_ms" => options.connection_timeout_ms = value.extract()?,
                    "ipc_compression" => options.ipc_compression = value.extract()?,
                    "stream_paused_max_wait_time_ms" => {
                        options.stream_paused_max_wait_time_ms = if value.is_none() {
                            None
                        } else {
                            Some(value.extract()?)
                        };
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
             server_lack_of_ack_timeout_ms={}, flush_timeout_ms={}, connection_timeout_ms={}, \
             ipc_compression={}, stream_paused_max_wait_time_ms={:?})",
            self.max_inflight_batches,
            self.recovery,
            self.recovery_timeout_ms,
            self.recovery_backoff_ms,
            self.recovery_retries,
            self.server_lack_of_ack_timeout_ms,
            self.flush_timeout_ms,
            self.connection_timeout_ms,
            self.ipc_compression.__repr__(),
            self.stream_paused_max_wait_time_ms,
        )
    }
}

impl ArrowStreamConfigurationOptions {
    fn validate(&self) -> PyResult<()> {
        for (name, val) in [
            ("max_inflight_batches", self.max_inflight_batches as i64),
            ("recovery_timeout_ms", self.recovery_timeout_ms),
            ("recovery_backoff_ms", self.recovery_backoff_ms),
            ("recovery_retries", self.recovery_retries as i64),
            (
                "server_lack_of_ack_timeout_ms",
                self.server_lack_of_ack_timeout_ms,
            ),
            ("flush_timeout_ms", self.flush_timeout_ms),
            ("connection_timeout_ms", self.connection_timeout_ms),
        ] {
            if val < 0 {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "{} must be non-negative",
                    name
                )));
            }
        }
        if let Some(v) = self.stream_paused_max_wait_time_ms {
            if v < 0 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "stream_paused_max_wait_time_ms must be non-negative",
                ));
            }
        }
        Ok(())
    }

    /// Apply Arrow options to a `StreamBuilder` via setters.
    pub(crate) fn apply<'a>(&self, builder: StreamBuilder<'a>) -> PyResult<StreamBuilder<'a>> {
        self.validate()?;
        let b = builder
            .max_inflight_batches(self.max_inflight_batches as usize)
            .recovery(self.recovery)
            .recovery_timeout_ms(self.recovery_timeout_ms as u64)
            .recovery_backoff_ms(self.recovery_backoff_ms as u64)
            .recovery_retries(self.recovery_retries as u32)
            .server_lack_of_ack_timeout_ms(self.server_lack_of_ack_timeout_ms as u64)
            .flush_timeout_ms(self.flush_timeout_ms as u64)
            .connection_timeout_ms(self.connection_timeout_ms as u64)
            .ipc_compression(self.ipc_compression.to_rust())
            .stream_paused_max_wait_time_ms(self.stream_paused_max_wait_time_ms.map(|v| v as u64));
        Ok(b)
    }
}

// =============================================================================
// SYNC ARROW STREAM
// =============================================================================

/// Synchronous Arrow Flight stream for ingesting pyarrow RecordBatches.
///
/// **Beta**: Arrow Flight ingestion is in Beta. The API is stabilising but
/// may still change before reaching GA.
#[pyclass]
pub struct ZerobusArrowStream {
    pub(crate) inner: Arc<RwLock<RustZerobusArrowStream>>,
    pub(crate) runtime: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl ZerobusArrowStream {
    /// Ingest a single Arrow RecordBatch (as IPC bytes) and return the offset.
    fn ingest_batch(&self, py: Python, ipc_bytes: &Bound<'_, PyBytes>) -> PyResult<i64> {
        let batch = ipc_bytes_to_record_batch(ipc_bytes.as_bytes()).map_err(map_error)?;
        let stream = self.inner.clone();
        let runtime = self.runtime.clone();
        py.detach(|| {
            runtime.block_on(async move {
                let guard = stream.read().await;
                guard.ingest_batch(batch).await.map_err(map_error)
            })
        })
    }

    /// Wait for a specific offset to be acknowledged.
    fn wait_for_offset(&self, py: Python, offset: i64) -> PyResult<()> {
        let stream = self.inner.clone();
        let runtime = self.runtime.clone();

        py.detach(|| {
            runtime.block_on(async move {
                let guard = stream.read().await;
                guard.wait_for_offset(offset).await.map_err(map_error)
            })
        })
    }

    /// Flush all pending batches, waiting for acknowledgment.
    fn flush(&self, py: Python) -> PyResult<()> {
        let stream = self.inner.clone();
        let runtime = self.runtime.clone();

        py.detach(|| {
            runtime.block_on(async move {
                let guard = stream.read().await;
                guard.flush().await.map_err(map_error)
            })
        })
    }

    /// Close the stream gracefully.
    fn close(&self, py: Python) -> PyResult<()> {
        let stream = self.inner.clone();
        let runtime = self.runtime.clone();

        py.detach(|| {
            runtime.block_on(async move {
                let mut guard = stream.write().await;
                guard.close().await.map_err(map_error)
            })
        })
    }

    /// Check if the stream has been closed.
    #[getter]
    fn is_closed(&self, py: Python) -> bool {
        let stream = self.inner.clone();
        let runtime = self.runtime.clone();
        py.detach(|| {
            runtime.block_on(async move {
                let guard = stream.read().await;
                guard.is_closed()
            })
        })
    }

    /// Get the table name.
    #[getter]
    fn table_name(&self, py: Python) -> String {
        let stream = self.inner.clone();
        let runtime = self.runtime.clone();
        py.detach(|| {
            runtime.block_on(async move {
                let guard = stream.read().await;
                guard.table_name().to_string()
            })
        })
    }

    /// Get unacknowledged batches as a list of Arrow IPC byte buffers.
    fn get_unacked_batches(&self, py: Python) -> PyResult<Vec<Py<PyAny>>> {
        let stream = self.inner.clone();
        let runtime = self.runtime.clone();

        py.detach(|| {
            runtime.block_on(async move {
                let guard = stream.read().await;
                let batches = guard.get_unacked_batches().await.map_err(map_error)?;

                Python::attach(|py| {
                    let mut out = Vec::with_capacity(batches.len());
                    for batch in &batches {
                        let ipc = record_batch_to_ipc_bytes(batch).map_err(map_error)?;
                        out.push(PyBytes::new(py, &ipc).into());
                    }
                    Ok(out)
                })
            })
        })
    }
}

// =============================================================================
// ASYNC ARROW STREAM
// =============================================================================

/// Asynchronous Arrow Flight stream for ingesting pyarrow RecordBatches.
///
/// **Beta**: Arrow Flight ingestion is in Beta. The API is stabilising but
/// may still change before reaching GA.
#[pyclass(name = "AsyncZerobusArrowStream")]
pub struct AsyncZerobusArrowStream {
    pub(crate) inner: Arc<RwLock<RustZerobusArrowStream>>,
}

#[pymethods]
impl AsyncZerobusArrowStream {
    /// Ingest a single Arrow RecordBatch (as IPC bytes) and return the offset.
    fn ingest_batch<'py>(
        &self,
        py: Python<'py>,
        ipc_bytes: &Bound<'_, PyBytes>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let batch = ipc_bytes_to_record_batch(ipc_bytes.as_bytes()).map_err(map_error)?;
        let stream = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = stream.read().await;
            guard.ingest_batch(batch).await.map_err(map_error)
        })
    }

    /// Wait for a specific offset to be acknowledged.
    fn wait_for_offset<'py>(&self, py: Python<'py>, offset: i64) -> PyResult<Bound<'py, PyAny>> {
        let stream = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = stream.read().await;
            guard.wait_for_offset(offset).await.map_err(map_error)?;
            Ok(())
        })
    }

    /// Flush all pending batches.
    fn flush<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let stream = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = stream.read().await;
            guard.flush().await.map_err(map_error)?;
            Ok(())
        })
    }

    /// Close the stream gracefully.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let stream = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = stream.write().await;
            guard.close().await.map_err(map_error)?;
            Ok(())
        })
    }

    /// Check if the stream has been closed.
    #[getter]
    fn is_closed(&self) -> PyResult<bool> {
        match self.inner.try_read() {
            Ok(guard) => Ok(guard.is_closed()),
            Err(_) => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Cannot read stream state: lock is held by another operation",
            )),
        }
    }

    /// Get the table name.
    #[getter]
    fn table_name(&self) -> PyResult<String> {
        match self.inner.try_read() {
            Ok(guard) => Ok(guard.table_name().to_string()),
            Err(_) => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Cannot read stream state: lock is held by another operation",
            )),
        }
    }

    /// Get unacknowledged batches as a list of Arrow IPC byte buffers.
    fn get_unacked_batches<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let stream = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = stream.read().await;
            let batches = guard.get_unacked_batches().await.map_err(map_error)?;

            Python::attach(|py| {
                let mut out: Vec<Py<PyAny>> = Vec::with_capacity(batches.len());
                for batch in &batches {
                    let ipc = record_batch_to_ipc_bytes(batch).map_err(map_error)?;
                    out.push(PyBytes::new(py, &ipc).into());
                }
                Ok(out)
            })
        })
    }
}

// =============================================================================
// SDK METHODS — called from sync_wrapper and async_wrapper
// =============================================================================

/// Build an Arrow stream via `StreamBuilder` (shared by sync + async helpers).
fn build_arrow_builder<'a>(
    sdk_guard: &'a RustSdk,
    table_name: String,
    schema_ipc_bytes: &[u8],
    client_id: Option<String>,
    client_secret: Option<String>,
    headers_provider: Option<Py<PyAny>>,
    options: &ArrowStreamConfigurationOptions,
) -> PyResult<StreamBuilder<'a>> {
    let schema = ipc_schema_bytes_to_arrow_schema(schema_ipc_bytes).map_err(map_error)?;
    let mut builder = sdk_guard.stream_builder().table(table_name);
    if let Some(provider) = headers_provider {
        builder = builder.headers_provider(Arc::new(HeadersProviderWrapper::new(provider)));
    } else if let (Some(id), Some(secret)) = (client_id, client_secret) {
        builder = builder.oauth(id, secret);
    } else {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "auth required: pass client_id+client_secret or a headers_provider",
        ));
    }
    builder = builder.arrow(Arc::new(schema));
    builder = options.apply(builder)?;
    Ok(builder)
}

/// Create an Arrow stream (sync helper).
#[allow(clippy::too_many_arguments)]
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
    let opts = options.cloned().unwrap_or_default();
    let sdk = sdk.clone();
    let runtime = runtime.clone();
    let runtime_for_return = runtime.clone();
    let schema_bytes = schema_ipc_bytes.to_vec();

    let stream = py.detach(|| {
        runtime.block_on(async move {
            let sdk_guard = sdk.read().await;
            let builder = build_arrow_builder(
                &*sdk_guard,
                table_name,
                &schema_bytes,
                Some(client_id),
                Some(client_secret),
                None,
                &opts,
            )?;
            builder.build_arrow().await.map_err(map_error)
        })
    })?;

    Ok(ZerobusArrowStream {
        inner: Arc::new(RwLock::new(stream)),
        runtime: runtime_for_return,
    })
}

/// Create an Arrow stream with headers provider (sync helper).
pub fn create_arrow_stream_with_headers_provider_sync(
    sdk: &Arc<RwLock<RustSdk>>,
    runtime: &Arc<tokio::runtime::Runtime>,
    py: Python,
    table_name: String,
    schema_ipc_bytes: &[u8],
    headers_provider: Py<PyAny>,
    options: Option<&ArrowStreamConfigurationOptions>,
) -> PyResult<ZerobusArrowStream> {
    let opts = options.cloned().unwrap_or_default();
    let sdk = sdk.clone();
    let runtime = runtime.clone();
    let runtime_for_return = runtime.clone();
    let schema_bytes = schema_ipc_bytes.to_vec();

    let stream = py.detach(|| {
        runtime.block_on(async move {
            let sdk_guard = sdk.read().await;
            let builder = build_arrow_builder(
                &*sdk_guard,
                table_name,
                &schema_bytes,
                None,
                None,
                Some(headers_provider),
                &opts,
            )?;
            builder.build_arrow().await.map_err(map_error)
        })
    })?;

    Ok(ZerobusArrowStream {
        inner: Arc::new(RwLock::new(stream)),
        runtime: runtime_for_return,
    })
}

/// Recreate an Arrow stream from a closed stream (sync helper).
pub fn recreate_arrow_stream_sync(
    sdk: &Arc<RwLock<RustSdk>>,
    runtime: &Arc<tokio::runtime::Runtime>,
    py: Python,
    old_stream: &ZerobusArrowStream,
) -> PyResult<ZerobusArrowStream> {
    let sdk = sdk.clone();
    let old = old_stream.inner.clone();
    let runtime = runtime.clone();
    let runtime_for_return = runtime.clone();

    let stream = py.detach(|| {
        runtime.block_on(async move {
            let old_guard = old.read().await;
            let sdk_guard = sdk.read().await;
            sdk_guard
                .recreate_arrow_stream(&*old_guard)
                .await
                .map_err(map_error)
        })
    })?;

    Ok(ZerobusArrowStream {
        inner: Arc::new(RwLock::new(stream)),
        runtime: runtime_for_return,
    })
}

/// Create an Arrow stream (async helper).
#[allow(clippy::too_many_arguments)]
pub fn create_arrow_stream_async<'py>(
    sdk: &Arc<RwLock<RustSdk>>,
    py: Python<'py>,
    table_name: String,
    schema_ipc_bytes: Vec<u8>,
    client_id: String,
    client_secret: String,
    options: Option<ArrowStreamConfigurationOptions>,
) -> PyResult<Bound<'py, PyAny>> {
    let opts = options.unwrap_or_default();
    let sdk = sdk.clone();

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let sdk_guard = sdk.read().await;
        let builder = build_arrow_builder(
            &*sdk_guard,
            table_name,
            &schema_ipc_bytes,
            Some(client_id),
            Some(client_secret),
            None,
            &opts,
        )?;
        let stream = builder.build_arrow().await.map_err(map_error)?;
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
    headers_provider: Py<PyAny>,
    options: Option<ArrowStreamConfigurationOptions>,
) -> PyResult<Bound<'py, PyAny>> {
    let opts = options.unwrap_or_default();
    let sdk = sdk.clone();

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let sdk_guard = sdk.read().await;
        let builder = build_arrow_builder(
            &*sdk_guard,
            table_name,
            &schema_ipc_bytes,
            None,
            None,
            Some(headers_provider),
            &opts,
        )?;
        let stream = builder.build_arrow().await.map_err(map_error)?;
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
) -> PyResult<Bound<'py, PyAny>> {
    let sdk = sdk.clone();
    let old = old_stream.inner.clone();

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let old_guard = old.read().await;
        let sdk_guard = sdk.read().await;
        let stream = sdk_guard
            .recreate_arrow_stream(&*old_guard)
            .await
            .map_err(map_error)?;
        Ok(AsyncZerobusArrowStream {
            inner: Arc::new(RwLock::new(stream)),
        })
    })
}
