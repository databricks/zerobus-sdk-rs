use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use pyo3::prelude::*;
use pyo3_asyncio::tokio::future_into_py;
use tokio::sync::RwLock;

use databricks_zerobus_ingest_sdk::{
    StreamBuilder, ZerobusSdk as RustSdk, ZerobusStream as RustStream,
};

use crate::arrow;
use crate::arrow::{ArrowStreamConfigurationOptions, AsyncZerobusArrowStream};
use crate::auth::HeadersProviderWrapper;
use crate::common::{
    apply_grpc_options, encoded_record_to_pybytes, extract_record_payload, extract_record_payloads,
    map_error, StreamConfigurationOptions, TableProperties, SDK_IDENTIFIER_PREFIX,
};

// =============================================================================
// STREAM-BUILDER HELPERS
// =============================================================================

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
// ACK FUTURE FOR LEGACY API
// =============================================================================

type AckFutureInner = Pin<Box<dyn Future<Output = PyResult<i64>> + Send + 'static>>;

/// A future that resolves with the acknowledgment ID of an ingested record.
/// This future can only be awaited once.
#[pyclass(name = "RecordIngestionFuture")]
pub struct PyAckFuture {
    inner: Arc<Mutex<Option<AckFutureInner>>>,
}

impl PyAckFuture {
    pub fn new(future: impl Future<Output = PyResult<i64>> + Send + 'static) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(Box::pin(future)))),
        }
    }
}

#[pymethods]
impl PyAckFuture {
    fn __await__<'py>(slf: PyRef<'_, Self>, py: Python<'py>) -> PyResult<&'py PyAny> {
        let inner_clone = slf.inner.clone();

        let rust_future = async move {
            let future_opt = {
                let mut guard = inner_clone.lock().unwrap();
                guard.take()
            };
            if let Some(future) = future_opt {
                future.await
            } else {
                Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "RecordIngestionFuture has already been awaited.",
                ))
            }
        };
        future_into_py(py, rust_future)
    }

    fn __repr__(&self) -> &'static str {
        "<RecordIngestionFuture (pending)>"
    }
}

// =============================================================================
// ZEROBUS STREAM (ASYNC)
// =============================================================================

#[pyclass]
pub struct ZerobusStream {
    pub(crate) inner: Arc<RwLock<RustStream>>,
}

#[pymethods]
#[allow(deprecated)]
impl ZerobusStream {
    /// Ingest a record and return a future that can be awaited for acknowledgment (legacy API)
    #[deprecated(
        since = "0.3.0",
        note = "Use ingest_record_offset() instead for better performance"
    )]
    fn ingest_record<'py>(&self, py: Python<'py>, payload: &PyAny) -> PyResult<&'py PyAny> {
        let record_payload = extract_record_payload(payload)?;
        let stream_clone = self.inner.clone();

        // Stage 1: enqueue, get offset. Stage 2: lazy wait_for_offset.
        let outer_future = async move {
            let offset = {
                let stream_guard = stream_clone.read().await;
                stream_guard
                    .ingest_record_offset(record_payload)
                    .await
                    .map_err(map_error)?
            };

            let stream_for_ack = stream_clone.clone();
            let wait_future = async move {
                let stream_guard = stream_for_ack.read().await;
                stream_guard
                    .wait_for_offset(offset)
                    .await
                    .map_err(map_error)?;
                Ok::<i64, PyErr>(offset)
            };
            Ok::<PyAckFuture, PyErr>(PyAckFuture::new(wait_future))
        };

        future_into_py(py, outer_future)
    }

    /// Ingest a single record and return the offset ID (async)
    fn ingest_record_offset<'py>(&self, py: Python<'py>, payload: &PyAny) -> PyResult<&'py PyAny> {
        let record_payload = extract_record_payload(payload)?;
        let stream = self.inner.clone();

        future_into_py(py, async move {
            let guard = stream.read().await;
            let offset = guard
                .ingest_record_offset(record_payload)
                .await
                .map_err(map_error)?;
            Ok(offset)
        })
    }

    /// Ingest a single record without waiting (fire-and-forget async)
    fn ingest_record_nowait(&self, payload: &PyAny) -> PyResult<()> {
        let record_payload = extract_record_payload(payload)?;
        let stream = self.inner.clone();

        pyo3_asyncio::tokio::get_runtime().spawn(async move {
            let guard = stream.read().await;
            let _ = guard.ingest_record_offset(record_payload).await;
        });

        Ok(())
    }

    /// Ingest a batch of records and return one offset for the whole batch (async)
    fn ingest_records_offset<'py>(
        &self,
        py: Python<'py>,
        payloads: &PyAny,
    ) -> PyResult<&'py PyAny> {
        let record_payloads = extract_record_payloads(payloads)?;
        let stream = self.inner.clone();

        future_into_py(py, async move {
            let guard = stream.read().await;
            let offset = guard
                .ingest_records_offset(record_payloads)
                .await
                .map_err(map_error)?;
            Ok(offset)
        })
    }

    /// Ingest a batch of records without waiting (async)
    fn ingest_records_nowait(&self, payloads: &PyAny) -> PyResult<()> {
        let record_payloads = extract_record_payloads(payloads)?;
        let stream = self.inner.clone();

        pyo3_asyncio::tokio::get_runtime().spawn(async move {
            let guard = stream.read().await;
            let _ = guard.ingest_records_offset(record_payloads).await;
        });

        Ok(())
    }

    /// Wait for a specific offset to be acknowledged (async)
    fn wait_for_offset<'py>(&self, py: Python<'py>, offset: i64) -> PyResult<&'py PyAny> {
        let stream = self.inner.clone();
        future_into_py(py, async move {
            let guard = stream.read().await;
            guard.wait_for_offset(offset).await.map_err(map_error)?;
            Ok(())
        })
    }

    /// Flush the stream (async)
    fn flush<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        let stream = self.inner.clone();
        future_into_py(py, async move {
            let guard = stream.read().await;
            guard.flush().await.map_err(map_error)?;
            Ok(())
        })
    }

    /// Close the stream (async)
    fn close<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        let stream = self.inner.clone();
        future_into_py(py, async move {
            let mut guard = stream.write().await;
            guard.close().await.map_err(map_error)?;
            Ok(())
        })
    }

    /// Get unacknowledged records
    fn get_unacked_records<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        let stream = self.inner.clone();

        future_into_py(py, async move {
            let guard = stream.read().await;
            let records = guard.get_unacked_records().await.map_err(map_error)?;

            Python::with_gil(|py| {
                let out: Vec<PyObject> =
                    records.map(|r| encoded_record_to_pybytes(py, r)).collect();
                Ok(out)
            })
        })
    }

    /// Get unacknowledged batches
    fn get_unacked_batches<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        let stream = self.inner.clone();

        future_into_py(py, async move {
            let guard = stream.read().await;
            let batches = guard.get_unacked_batches().await.map_err(map_error)?;

            Python::with_gil(|py| {
                let out: Vec<Vec<PyObject>> = batches
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
    }
}

// =============================================================================
// ZEROBUS SDK (ASYNC)
// =============================================================================

#[pyclass]
pub struct ZerobusSdk {
    pub(crate) inner: Arc<RwLock<RustSdk>>,
}

#[pymethods]
impl ZerobusSdk {
    #[new]
    fn new(host: String, unity_catalog_url: String) -> PyResult<Self> {
        let py_version = env!("CARGO_PKG_VERSION");
        let sdk_identifier = format!("{}/{}", SDK_IDENTIFIER_PREFIX, py_version);

        let sdk = RustSdk::builder()
            .endpoint(host)
            .unity_catalog_url(unity_catalog_url)
            .sdk_identifier(sdk_identifier)
            .build()
            .map_err(map_error)?;

        Ok(ZerobusSdk {
            inner: Arc::new(RwLock::new(sdk)),
        })
    }

    /// Set whether to use TLS (default: true).
    ///
    /// Kept as a no-op for backwards compatibility. Rust SDK 2.0.0 removed the
    /// underlying field; TLS is always controlled via the SDK builder.
    fn set_use_tls<'py>(&self, py: Python<'py>, _use_tls: bool) -> PyResult<&'py PyAny> {
        future_into_py(py, async move { Ok(()) })
    }

    /// Create stream with client credentials (async)
    #[pyo3(signature = (client_id, client_secret, table_properties, options = None))]
    fn create_stream<'py>(
        &self,
        py: Python<'py>,
        client_id: String,
        client_secret: String,
        table_properties: &TableProperties,
        options: Option<StreamConfigurationOptions>,
    ) -> PyResult<&'py PyAny> {
        let sdk = self.inner.clone();
        let table_properties = table_properties.clone();
        let opts = options.unwrap_or_default();
        opts.validate()?;

        future_into_py(py, async move {
            let sdk_guard = sdk.read().await;
            let builder = sdk_guard.stream_builder().oauth(client_id, client_secret);
            let builder = apply_table_and_format(builder, &table_properties);
            let builder = apply_grpc_options(builder, &opts)?;
            let stream = builder.build().await.map_err(map_error)?;
            Ok(ZerobusStream {
                inner: Arc::new(RwLock::new(stream)),
            })
        })
    }

    /// Create stream with custom headers provider (async)
    #[pyo3(signature = (table_properties, headers_provider, options = None))]
    fn create_stream_with_headers_provider<'py>(
        &self,
        py: Python<'py>,
        table_properties: &TableProperties,
        headers_provider: PyObject,
        options: Option<StreamConfigurationOptions>,
    ) -> PyResult<&'py PyAny> {
        let sdk = self.inner.clone();
        let table_properties = table_properties.clone();
        let opts = options.unwrap_or_default();
        opts.validate()?;
        let provider = Arc::new(HeadersProviderWrapper::new(headers_provider));

        future_into_py(py, async move {
            let sdk_guard = sdk.read().await;
            let builder = sdk_guard.stream_builder().headers_provider(provider);
            let builder = apply_table_and_format(builder, &table_properties);
            let builder = apply_grpc_options(builder, &opts)?;
            let stream = builder.build().await.map_err(map_error)?;
            Ok(ZerobusStream {
                inner: Arc::new(RwLock::new(stream)),
            })
        })
    }

    /// Create a new Arrow Flight stream with OAuth authentication (async).
    #[pyo3(signature = (table_name, schema_ipc_bytes, client_id, client_secret, options = None))]
    fn create_arrow_stream<'py>(
        &self,
        py: Python<'py>,
        table_name: String,
        schema_ipc_bytes: Vec<u8>,
        client_id: String,
        client_secret: String,
        options: Option<ArrowStreamConfigurationOptions>,
    ) -> PyResult<&'py PyAny> {
        arrow::create_arrow_stream_async(
            &self.inner,
            py,
            table_name,
            schema_ipc_bytes,
            client_id,
            client_secret,
            options,
        )
    }

    /// Create a new Arrow Flight stream with custom headers provider (async).
    #[pyo3(signature = (table_name, schema_ipc_bytes, headers_provider, options = None))]
    fn create_arrow_stream_with_headers_provider<'py>(
        &self,
        py: Python<'py>,
        table_name: String,
        schema_ipc_bytes: Vec<u8>,
        headers_provider: PyObject,
        options: Option<ArrowStreamConfigurationOptions>,
    ) -> PyResult<&'py PyAny> {
        arrow::create_arrow_stream_with_headers_provider_async(
            &self.inner,
            py,
            table_name,
            schema_ipc_bytes,
            headers_provider,
            options,
        )
    }

    /// Recreate a closed Arrow stream (async).
    fn recreate_arrow_stream<'py>(
        &self,
        py: Python<'py>,
        old_stream: &AsyncZerobusArrowStream,
    ) -> PyResult<&'py PyAny> {
        arrow::recreate_arrow_stream_async(&self.inner, py, old_stream)
    }

    /// Recreate a stream from an old stream (async)
    fn recreate_stream<'py>(
        &self,
        py: Python<'py>,
        old_stream: &ZerobusStream,
    ) -> PyResult<&'py PyAny> {
        let sdk = self.inner.clone();
        let old_stream_inner = old_stream.inner.clone();

        future_into_py(py, async move {
            let guard = old_stream_inner.read().await;
            let sdk_guard = sdk.read().await;
            let new_stream = sdk_guard
                .recreate_stream(&*guard)
                .await
                .map_err(map_error)?;

            Ok(ZerobusStream {
                inner: Arc::new(RwLock::new(new_stream)),
            })
        })
    }
}
