use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;

use databricks_zerobus_ingest_sdk::{ZerobusSdk as RustSdk, ZerobusStream as RustStream};

use crate::arrow::{
    create_arrow_stream_sync, recreate_arrow_stream_sync, ArrowStreamConfigurationOptions,
    ZerobusArrowStream,
};
use crate::auth::HeadersProviderWrapper;
use crate::common::{
    apply_grpc_options, descriptor_from_file_bytes, encoded_record_to_pybytes,
    extract_record_payload, extract_record_payloads, map_error, StreamConfigurationOptions,
    SDK_IDENTIFIER_PREFIX,
};

// =============================================================================
// ZEROBUS STREAM
// =============================================================================

#[pyclass]
pub struct ZerobusStream {
    inner: Arc<RwLock<RustStream>>,
    runtime: Arc<Runtime>,
}

impl ZerobusStream {
    pub(crate) fn new(stream: RustStream, runtime: Arc<Runtime>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(stream)),
            runtime,
        }
    }
}

#[pymethods]
impl ZerobusStream {
    /// Ingest a single record and return the offset assigned by the server.
    fn ingest_record_offset(&self, py: Python, payload: &PyAny) -> PyResult<i64> {
        let record_payload = extract_record_payload(payload)?;
        let stream = self.inner.clone();
        let runtime = self.runtime.clone();

        py.allow_threads(|| {
            runtime.block_on(async move {
                let guard = stream.read().await;
                guard
                    .ingest_record_offset(record_payload)
                    .await
                    .map_err(map_error)
            })
        })
    }

    /// Ingest a single record without waiting (fire-and-forget).
    fn ingest_record_nowait(&self, payload: &PyAny) -> PyResult<()> {
        let record_payload = extract_record_payload(payload)?;
        let stream = self.inner.clone();

        self.runtime.spawn(async move {
            let guard = stream.read().await;
            let _ = guard.ingest_record_offset(record_payload).await;
        });

        Ok(())
    }

    /// Ingest a batch of records and return one offset for the whole batch.
    fn ingest_records_offset(&self, py: Python, payloads: &PyAny) -> PyResult<Option<i64>> {
        let record_payloads = extract_record_payloads(payloads)?;
        if record_payloads.is_empty() {
            return Ok(None);
        }

        let stream = self.inner.clone();
        let runtime = self.runtime.clone();

        py.allow_threads(|| {
            runtime.block_on(async move {
                let guard = stream.read().await;
                guard
                    .ingest_records_offset(record_payloads)
                    .await
                    .map_err(map_error)
            })
        })
    }

    /// Ingest a batch of records without waiting (fire-and-forget).
    fn ingest_records_nowait(&self, payloads: &PyAny) -> PyResult<()> {
        let record_payloads = extract_record_payloads(payloads)?;
        let stream = self.inner.clone();

        self.runtime.spawn(async move {
            let guard = stream.read().await;
            let _ = guard.ingest_records_offset(record_payloads).await;
        });

        Ok(())
    }

    fn wait_for_offset(&self, py: Python, offset: i64) -> PyResult<()> {
        let stream = self.inner.clone();
        let runtime = self.runtime.clone();

        py.allow_threads(|| {
            runtime.block_on(async move {
                let guard = stream.read().await;
                guard.wait_for_offset(offset).await.map_err(map_error)
            })
        })
    }

    fn flush(&self, py: Python) -> PyResult<()> {
        let stream = self.inner.clone();
        let runtime = self.runtime.clone();

        py.allow_threads(|| {
            runtime.block_on(async move {
                let guard = stream.read().await;
                guard.flush().await.map_err(map_error)
            })
        })
    }

    fn close(&self, py: Python) -> PyResult<()> {
        let stream = self.inner.clone();
        let runtime = self.runtime.clone();

        py.allow_threads(|| {
            runtime.block_on(async move {
                let mut guard = stream.write().await;
                guard.close().await.map_err(map_error)
            })
        })
    }

    /// Get unacknowledged records as a `list[bytes]`.
    fn get_unacked_records(&self, py: Python) -> PyResult<Vec<PyObject>> {
        let stream = self.inner.clone();
        let runtime = self.runtime.clone();

        py.allow_threads(|| {
            runtime.block_on(async move {
                let guard = stream.read().await;
                let records = guard.get_unacked_records().await.map_err(map_error)?;

                Python::with_gil(|py| {
                    let out: Vec<PyObject> = records
                        .map(|record| encoded_record_to_pybytes(py, record))
                        .collect();
                    Ok(out)
                })
            })
        })
    }

    /// Get unacknowledged batches as a `list[list[bytes]]`.
    fn get_unacked_batches(&self, py: Python) -> PyResult<Vec<Vec<PyObject>>> {
        let stream = self.inner.clone();
        let runtime = self.runtime.clone();

        py.allow_threads(|| {
            runtime.block_on(async move {
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
        })
    }
}

// =============================================================================
// ZEROBUS SDK
// =============================================================================

#[pyclass]
pub struct ZerobusSdk {
    inner: Arc<RwLock<RustSdk>>,
    runtime: Arc<Runtime>,
}

#[pymethods]
impl ZerobusSdk {
    /// Construct a new Zerobus SDK instance.
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
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Failed to create tokio runtime: {:?}",
                        e
                    ))
                })?,
        );

        let py_version = env!("CARGO_PKG_VERSION");
        let sdk_identifier = format!("{}/{}", SDK_IDENTIFIER_PREFIX, py_version);

        let mut builder = RustSdk::builder()
            .endpoint(host)
            .unity_catalog_url(unity_catalog_url)
            .sdk_identifier(sdk_identifier);

        if let Some(name) = application_name {
            if !name.is_empty() {
                builder = builder.application_name(name);
            }
        }

        let inner = builder.build().map_err(map_error)?;

        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
            runtime,
        })
    }

    /// Create a JSON or protobuf ingestion stream.
    ///
    /// Exactly one of `client_id`+`client_secret` or `headers_provider` must be
    /// provided. Exactly one record format is selected: provide
    /// `descriptor_bytes` (serialized `FileDescriptorProto`) plus optional
    /// `descriptor_message_name` for protobuf, or pass `descriptor_bytes=None`
    /// with `record_type=RecordType.JSON` in `options` for JSON.
    #[pyo3(signature = (
        table,
        client_id = None,
        client_secret = None,
        headers_provider = None,
        descriptor_bytes = None,
        descriptor_message_name = None,
        options = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn create_stream(
        &self,
        py: Python,
        table: String,
        client_id: Option<String>,
        client_secret: Option<String>,
        headers_provider: Option<PyObject>,
        descriptor_bytes: Option<&PyBytes>,
        descriptor_message_name: Option<String>,
        options: Option<StreamConfigurationOptions>,
    ) -> PyResult<ZerobusStream> {
        let descriptor = match descriptor_bytes {
            Some(b) => Some(descriptor_from_file_bytes(
                b.as_bytes(),
                descriptor_message_name.as_deref(),
            )?),
            None => None,
        };

        let opts = options.unwrap_or_default();
        opts.validate()?;
        let record_type_is_proto = opts.record_type.value == 1;

        if descriptor.is_some() && !record_type_is_proto {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "descriptor_bytes provided but record_type is not PROTO",
            ));
        }
        if descriptor.is_none() && record_type_is_proto {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "PROTO record type requires descriptor_bytes",
            ));
        }

        let sdk = self.inner.clone();
        let runtime = self.runtime.clone();

        let stream = py.allow_threads(|| {
            runtime.block_on(async move {
                let sdk_guard = sdk.read().await;
                let mut builder = sdk_guard.stream_builder().table(table);

                if let Some(provider) = headers_provider {
                    builder =
                        builder.headers_provider(Arc::new(HeadersProviderWrapper::new(provider)));
                } else if let (Some(id), Some(secret)) = (client_id, client_secret) {
                    builder = builder.oauth(id, secret);
                } else {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "auth required: pass client_id+client_secret or a headers_provider",
                    ));
                }

                builder = match descriptor {
                    Some(d) => builder.compiled_proto(d),
                    None => builder.json(),
                };

                builder = apply_grpc_options(builder, &opts)?;

                builder.build().await.map_err(map_error)
            })
        })?;

        Ok(ZerobusStream::new(stream, self.runtime.clone()))
    }

    /// Create an Arrow Flight stream.
    ///
    /// **Beta**: Arrow Flight ingestion is in Beta — the API is stabilising
    /// but may still change before reaching GA.
    #[pyo3(signature = (
        table,
        schema_ipc_bytes,
        client_id = None,
        client_secret = None,
        headers_provider = None,
        options = None,
    ))]
    fn create_arrow_stream(
        &self,
        py: Python,
        table: String,
        schema_ipc_bytes: &PyBytes,
        client_id: Option<String>,
        client_secret: Option<String>,
        headers_provider: Option<PyObject>,
        options: Option<ArrowStreamConfigurationOptions>,
    ) -> PyResult<ZerobusArrowStream> {
        let opts = options.unwrap_or_default();
        create_arrow_stream_sync(
            &self.inner,
            &self.runtime,
            py,
            table,
            schema_ipc_bytes.as_bytes(),
            client_id,
            client_secret,
            headers_provider,
            opts,
        )
    }

    /// Recreate a closed Arrow stream with the same config.
    fn recreate_arrow_stream(
        &self,
        py: Python,
        old_stream: &ZerobusArrowStream,
    ) -> PyResult<ZerobusArrowStream> {
        recreate_arrow_stream_sync(&self.inner, &self.runtime, py, old_stream)
    }

    /// Recreate a closed gRPC stream with the same table and config.
    fn recreate_stream(&self, py: Python, old_stream: &ZerobusStream) -> PyResult<ZerobusStream> {
        let sdk = self.inner.clone();
        let old = old_stream.inner.clone();
        let runtime = self.runtime.clone();

        let new_stream = py.allow_threads(|| {
            runtime.block_on(async move {
                let old_guard = old.read().await;
                let sdk_guard = sdk.read().await;
                sdk_guard
                    .recreate_stream(&*old_guard)
                    .await
                    .map_err(map_error)
            })
        })?;

        Ok(ZerobusStream::new(new_stream, self.runtime.clone()))
    }
}
