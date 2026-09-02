use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;

use databricks_zerobus_ingest_sdk::{
    StreamBuilder, ZerobusSdk as RustSdk, ZerobusStream as RustStream,
};

use crate::arrow;
use crate::arrow::{ArrowStreamConfigurationOptions, ZerobusArrowStream};
use crate::auth::HeadersProviderWrapper;
use crate::common::{
    apply_grpc_options, encoded_record_to_pybytes, extract_record_payload, extract_record_payloads,
    map_error, StreamConfigurationOptions, TableProperties, SDK_IDENTIFIER_PREFIX,
};

// =============================================================================
// STREAM-BUILDER HELPERS
// =============================================================================

/// Apply table name + record-format selection (JSON or compiled-proto, based
/// on whether `TableProperties` carried a descriptor) to the builder.
fn apply_table_and_format<'a>(
    builder: StreamBuilder<'a>,
    table_properties: &TableProperties,
) -> StreamBuilder<'a> {
    let builder = builder.table(table_properties.table_name.clone());
    match table_properties.descriptor_proto.clone() {
        Some(descriptor) => builder.compiled_proto(descriptor),
        None => builder.json(),
    }
}

// =============================================================================
// RECORD ACKNOWLEDGMENT (legacy, returned by deprecated ingest_record)
// =============================================================================

#[pyclass]
pub struct RecordAcknowledgment {
    stream: Arc<RwLock<RustStream>>,
    runtime: Arc<Runtime>,
    offset: i64,
    done: bool,
}

#[pymethods]
impl RecordAcknowledgment {
    /// Wait for the acknowledgment and return the offset ID.
    /// This method can only be called once.
    #[pyo3(signature = (_timeout_sec = None))]
    pub fn wait_for_ack(&mut self, py: Python, _timeout_sec: Option<f64>) -> PyResult<i64> {
        if self.done {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "wait_for_ack has already been called.",
            ));
        }
        let stream = self.stream.clone();
        let runtime = self.runtime.clone();
        let offset = self.offset;

        py.detach(|| {
            runtime.block_on(async move {
                let guard = stream.read().await;
                guard.wait_for_offset(offset).await.map_err(map_error)
            })
        })?;

        self.done = true;
        Ok(offset)
    }

    /// Check if the acknowledgment is done
    pub fn is_done(&self) -> bool {
        self.done
    }
}

// =============================================================================
// ZEROBUS STREAM
// =============================================================================

#[pyclass]
pub struct ZerobusStream {
    inner: Arc<RwLock<RustStream>>,
    runtime: Arc<Runtime>,
}

#[pymethods]
#[allow(deprecated)]
impl ZerobusStream {
    /// Ingest a single record and return RecordAcknowledgment (legacy API)
    #[deprecated(
        since = "0.3.0",
        note = "Use ingest_record_offset() instead for better performance"
    )]
    fn ingest_record(
        &self,
        py: Python,
        payload: &Bound<'_, PyAny>,
    ) -> PyResult<RecordAcknowledgment> {
        let record_payload = extract_record_payload(payload)?;
        let stream = self.inner.clone();
        let runtime = self.runtime.clone();

        // Stage 1: enqueue and get the offset.
        let offset = py.detach(|| {
            runtime.block_on(async move {
                let guard = stream.read().await;
                guard
                    .ingest_record_offset(record_payload)
                    .await
                    .map_err(map_error)
            })
        })?;

        Ok(RecordAcknowledgment {
            stream: self.inner.clone(),
            runtime: self.runtime.clone(),
            offset,
            done: false,
        })
    }

    /// Ingest a single record and return the offset directly (optimized API)
    fn ingest_record_offset(&self, py: Python, payload: &Bound<'_, PyAny>) -> PyResult<i64> {
        let record_payload = extract_record_payload(payload)?;
        let stream = self.inner.clone();
        let runtime = self.runtime.clone();

        py.detach(|| {
            runtime.block_on(async move {
                let guard = stream.read().await;
                guard
                    .ingest_record_offset(record_payload)
                    .await
                    .map_err(map_error)
            })
        })
    }

    /// Ingest a single record without waiting for acknowledgment (fire-and-forget)
    fn ingest_record_nowait(&self, payload: &Bound<'_, PyAny>) -> PyResult<()> {
        let record_payload = extract_record_payload(payload)?;
        let stream = self.inner.clone();

        self.runtime.spawn(async move {
            let guard = stream.read().await;
            let _ = guard.ingest_record_offset(record_payload).await;
        });

        Ok(())
    }

    /// Ingest multiple records and return one offset for the whole batch (batch API)
    fn ingest_records_offset(
        &self,
        py: Python,
        payloads: &Bound<'_, PyAny>,
    ) -> PyResult<Option<i64>> {
        let record_payloads = extract_record_payloads(payloads)?;
        if record_payloads.is_empty() {
            return Ok(None);
        }

        let stream = self.inner.clone();
        let runtime = self.runtime.clone();

        py.detach(|| {
            runtime.block_on(async move {
                let guard = stream.read().await;
                guard
                    .ingest_records_offset(record_payloads)
                    .await
                    .map_err(map_error)
            })
        })
    }

    /// Ingest multiple records without waiting for acknowledgments (batch fire-and-forget)
    fn ingest_records_nowait(&self, payloads: &Bound<'_, PyAny>) -> PyResult<()> {
        let record_payloads = extract_record_payloads(payloads)?;
        let stream = self.inner.clone();

        self.runtime.spawn(async move {
            let guard = stream.read().await;
            let _ = guard.ingest_records_offset(record_payloads).await;
        });

        Ok(())
    }

    /// Wait for a specific offset to be acknowledged
    #[pyo3(signature = (offset, timeout_sec = None))]
    fn wait_for_offset(&self, py: Python, offset: i64, timeout_sec: Option<f64>) -> PyResult<()> {
        let _ = timeout_sec; // Timeout is handled internally by the Rust SDK
        let stream = self.inner.clone();
        let runtime = self.runtime.clone();

        py.detach(|| {
            runtime.block_on(async move {
                let guard = stream.read().await;
                guard.wait_for_offset(offset).await.map_err(map_error)
            })
        })
    }

    /// Flush the stream, waiting for all pending records to be acknowledged
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

    /// Close the stream gracefully
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

    /// Get unacknowledged records
    fn get_unacked_records(&self, py: Python) -> PyResult<Vec<Py<PyAny>>> {
        let stream = self.inner.clone();
        let runtime = self.runtime.clone();

        py.detach(|| {
            runtime.block_on(async move {
                let guard = stream.read().await;
                let records = guard.get_unacked_records().await.map_err(map_error)?;

                Python::attach(|py| {
                    let out: Vec<Py<PyAny>> = records
                        .map(|record| encoded_record_to_pybytes(py, record))
                        .collect();
                    Ok(out)
                })
            })
        })
    }

    /// Get unacknowledged batches
    fn get_unacked_batches(&self, py: Python) -> PyResult<Vec<Vec<Py<PyAny>>>> {
        let stream = self.inner.clone();
        let runtime = self.runtime.clone();

        py.detach(|| {
            runtime.block_on(async move {
                let guard = stream.read().await;
                let batches = guard.get_unacked_batches().await.map_err(map_error)?;

                Python::attach(|py| {
                    let out: Vec<Vec<Py<PyAny>>> = batches
                        .into_iter()
                        .map(|batch| {
                            batch
                                .into_iter()
                                .map(|r| encoded_record_to_pybytes(py, r))
                                .collect()
                        })
                        .collect();
                    Ok(out)
                })
            })
        })
    }
}

// =============================================================================
// ZEROBUS SDK
// =============================================================================

#[pyclass]
pub struct ZerobusSdk {
    pub(crate) inner: Arc<RwLock<RustSdk>>,
    pub(crate) runtime: Arc<Runtime>,
}

#[pymethods]
impl ZerobusSdk {
    #[new]
    #[pyo3(signature = (host, unity_catalog_url, application_name = None))]
    fn new(
        host: String,
        unity_catalog_url: String,
        application_name: Option<String>,
    ) -> PyResult<Self> {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Failed to create tokio runtime: {:?}",
                        e
                    ))
                })?,
        );

        let py_version = env!("CARGO_PKG_VERSION");
        let sdk_identifier = format!("{}/{}", SDK_IDENTIFIER_PREFIX, py_version);

        let builder = RustSdk::builder()
            .endpoint(host)
            .unity_catalog_url(unity_catalog_url)
            .sdk_identifier(sdk_identifier);
        let builder = match application_name {
            Some(application_name) => builder.application_name(application_name),
            None => builder,
        };
        let inner = builder.build().map_err(map_error)?;

        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
            runtime,
        })
    }

    /// Set whether to use TLS (default: true).
    ///
    /// Kept as a no-op for backwards compatibility. Rust SDK 2.0.0 removed the
    /// underlying field; TLS is always controlled via the SDK builder. This
    /// method is retained so existing code keeps loading.
    fn set_use_tls(&self, _py: Python, _use_tls: bool) -> PyResult<()> {
        Ok(())
    }

    /// Create a new stream with OAuth authentication
    #[pyo3(signature = (client_id, client_secret, table_properties, options = None))]
    fn create_stream(
        &self,
        py: Python,
        client_id: String,
        client_secret: String,
        table_properties: TableProperties,
        options: Option<StreamConfigurationOptions>,
    ) -> PyResult<ZerobusStream> {
        let opts = options.unwrap_or_default();
        opts.validate()?;
        let sdk = self.inner.clone();
        let runtime = self.runtime.clone();
        let runtime_for_stream = self.runtime.clone();

        let stream = py.detach(|| {
            runtime.block_on(async move {
                let sdk_guard = sdk.read().await;
                let builder = sdk_guard.stream_builder().oauth(client_id, client_secret);
                let builder = apply_table_and_format(builder, &table_properties);
                let builder = apply_grpc_options(builder, &opts)?;
                builder.build().await.map_err(map_error)
            })
        })?;

        Ok(ZerobusStream {
            inner: Arc::new(RwLock::new(stream)),
            runtime: runtime_for_stream,
        })
    }

    /// Create a new stream with custom headers provider
    #[pyo3(signature = (table_properties, headers_provider, options = None))]
    fn create_stream_with_headers_provider(
        &self,
        py: Python,
        table_properties: TableProperties,
        headers_provider: Py<PyAny>,
        options: Option<StreamConfigurationOptions>,
    ) -> PyResult<ZerobusStream> {
        let opts = options.unwrap_or_default();
        opts.validate()?;
        let provider = Arc::new(HeadersProviderWrapper::new(headers_provider));
        let sdk = self.inner.clone();
        let runtime = self.runtime.clone();
        let runtime_for_stream = self.runtime.clone();

        let stream = py.detach(|| {
            runtime.block_on(async move {
                let sdk_guard = sdk.read().await;
                let builder = sdk_guard.stream_builder().headers_provider(provider);
                let builder = apply_table_and_format(builder, &table_properties);
                let builder = apply_grpc_options(builder, &opts)?;
                builder.build().await.map_err(map_error)
            })
        })?;

        Ok(ZerobusStream {
            inner: Arc::new(RwLock::new(stream)),
            runtime: runtime_for_stream,
        })
    }

    /// Create a new Arrow Flight stream with OAuth authentication.
    ///
    #[pyo3(signature = (table_name, schema_ipc_bytes, client_id, client_secret, options = None))]
    fn create_arrow_stream(
        &self,
        py: Python,
        table_name: String,
        schema_ipc_bytes: &Bound<'_, PyBytes>,
        client_id: String,
        client_secret: String,
        options: Option<&ArrowStreamConfigurationOptions>,
    ) -> PyResult<ZerobusArrowStream> {
        arrow::create_arrow_stream_sync(
            &self.inner,
            &self.runtime,
            py,
            table_name,
            schema_ipc_bytes.as_bytes(),
            client_id,
            client_secret,
            options,
        )
    }

    /// Create a new Arrow Flight stream with custom headers provider.
    #[pyo3(signature = (table_name, schema_ipc_bytes, headers_provider, options = None))]
    fn create_arrow_stream_with_headers_provider(
        &self,
        py: Python,
        table_name: String,
        schema_ipc_bytes: &Bound<'_, PyBytes>,
        headers_provider: Py<PyAny>,
        options: Option<&ArrowStreamConfigurationOptions>,
    ) -> PyResult<ZerobusArrowStream> {
        arrow::create_arrow_stream_with_headers_provider_sync(
            &self.inner,
            &self.runtime,
            py,
            table_name,
            schema_ipc_bytes.as_bytes(),
            headers_provider,
            options,
        )
    }

    /// Recreate a closed Arrow stream with the same configuration.
    fn recreate_arrow_stream(
        &self,
        py: Python,
        old_stream: &ZerobusArrowStream,
    ) -> PyResult<ZerobusArrowStream> {
        arrow::recreate_arrow_stream_sync(&self.inner, &self.runtime, py, old_stream)
    }

    /// Recreate a closed stream with the same configuration
    fn recreate_stream(&self, py: Python, old_stream: &ZerobusStream) -> PyResult<ZerobusStream> {
        let sdk = self.inner.clone();
        let old_stream_inner = old_stream.inner.clone();
        let runtime = self.runtime.clone();
        let runtime_for_stream = self.runtime.clone();

        let new_stream = py.detach(|| {
            runtime.block_on(async move {
                let guard = old_stream_inner.read().await;
                let sdk_guard = sdk.read().await;
                sdk_guard.recreate_stream(&*guard).await.map_err(map_error)
            })
        })?;

        Ok(ZerobusStream {
            inner: Arc::new(RwLock::new(new_stream)),
            runtime: runtime_for_stream,
        })
    }
}
