use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3_asyncio::tokio::future_into_py;
use tokio::sync::RwLock;

use databricks_zerobus_ingest_sdk::{ZerobusSdk as RustSdk, ZerobusStream as RustStream};

use crate::arrow::{
    create_arrow_stream_async, recreate_arrow_stream_async, ArrowStreamConfigurationOptions,
    AsyncZerobusArrowStream,
};
use crate::auth::HeadersProviderWrapper;
use crate::common::{
    apply_grpc_options, descriptor_from_file_bytes, encoded_record_to_pybytes,
    extract_record_payload, extract_record_payloads, map_error, StreamConfigurationOptions,
    SDK_IDENTIFIER_PREFIX,
};

// =============================================================================
// ZEROBUS STREAM (ASYNC)
// =============================================================================

#[pyclass]
pub struct ZerobusStream {
    inner: Arc<RwLock<RustStream>>,
}

#[pymethods]
impl ZerobusStream {
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

    fn ingest_record_nowait(&self, payload: &PyAny) -> PyResult<()> {
        let record_payload = extract_record_payload(payload)?;
        let stream = self.inner.clone();

        pyo3_asyncio::tokio::get_runtime().spawn(async move {
            let guard = stream.read().await;
            let _ = guard.ingest_record_offset(record_payload).await;
        });

        Ok(())
    }

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

    fn ingest_records_nowait(&self, payloads: &PyAny) -> PyResult<()> {
        let record_payloads = extract_record_payloads(payloads)?;
        let stream = self.inner.clone();

        pyo3_asyncio::tokio::get_runtime().spawn(async move {
            let guard = stream.read().await;
            let _ = guard.ingest_records_offset(record_payloads).await;
        });

        Ok(())
    }

    fn wait_for_offset<'py>(&self, py: Python<'py>, offset: i64) -> PyResult<&'py PyAny> {
        let stream = self.inner.clone();
        future_into_py(py, async move {
            let guard = stream.read().await;
            guard.wait_for_offset(offset).await.map_err(map_error)?;
            Ok(())
        })
    }

    fn flush<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        let stream = self.inner.clone();
        future_into_py(py, async move {
            let guard = stream.read().await;
            guard.flush().await.map_err(map_error)?;
            Ok(())
        })
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        let stream = self.inner.clone();
        future_into_py(py, async move {
            let mut guard = stream.write().await;
            guard.close().await.map_err(map_error)?;
            Ok(())
        })
    }

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
    inner: Arc<RwLock<RustSdk>>,
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

        let sdk = builder.build().map_err(map_error)?;

        Ok(ZerobusSdk {
            inner: Arc::new(RwLock::new(sdk)),
        })
    }

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
    fn create_stream<'py>(
        &self,
        py: Python<'py>,
        table: String,
        client_id: Option<String>,
        client_secret: Option<String>,
        headers_provider: Option<PyObject>,
        descriptor_bytes: Option<&PyBytes>,
        descriptor_message_name: Option<String>,
        options: Option<StreamConfigurationOptions>,
    ) -> PyResult<&'py PyAny> {
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

        future_into_py(py, async move {
            let sdk_guard = sdk.read().await;
            let mut builder = sdk_guard.stream_builder().table(table);

            if let Some(provider) = headers_provider {
                builder = builder.headers_provider(Arc::new(HeadersProviderWrapper::new(provider)));
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

            let stream = builder.build().await.map_err(map_error)?;
            Ok(ZerobusStream {
                inner: Arc::new(RwLock::new(stream)),
            })
        })
    }

    #[pyo3(signature = (
        table,
        schema_ipc_bytes,
        client_id = None,
        client_secret = None,
        headers_provider = None,
        options = None,
    ))]
    fn create_arrow_stream<'py>(
        &self,
        py: Python<'py>,
        table: String,
        schema_ipc_bytes: Vec<u8>,
        client_id: Option<String>,
        client_secret: Option<String>,
        headers_provider: Option<PyObject>,
        options: Option<ArrowStreamConfigurationOptions>,
    ) -> PyResult<&'py PyAny> {
        let opts = options.unwrap_or_default();
        create_arrow_stream_async(
            &self.inner,
            py,
            table,
            schema_ipc_bytes,
            client_id,
            client_secret,
            headers_provider,
            opts,
        )
    }

    fn recreate_arrow_stream<'py>(
        &self,
        py: Python<'py>,
        old_stream: &AsyncZerobusArrowStream,
    ) -> PyResult<&'py PyAny> {
        recreate_arrow_stream_async(&self.inner, py, old_stream)
    }

    fn recreate_stream<'py>(
        &self,
        py: Python<'py>,
        old_stream: &ZerobusStream,
    ) -> PyResult<&'py PyAny> {
        let sdk = self.inner.clone();
        let old = old_stream.inner.clone();

        future_into_py(py, async move {
            let old_guard = old.read().await;
            let sdk_guard = sdk.read().await;
            let stream = sdk_guard
                .recreate_stream(&*old_guard)
                .await
                .map_err(map_error)?;
            Ok(ZerobusStream {
                inner: Arc::new(RwLock::new(stream)),
            })
        })
    }
}
