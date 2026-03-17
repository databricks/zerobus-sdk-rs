use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};
use pyo3_asyncio::tokio::future_into_py;
use tokio::sync::RwLock;

use databricks_zerobus_ingest_sdk::{
    databricks::zerobus::RecordType as RustRecordType, AckCallback as RustAckCallback,
    EncodedRecord, OffsetId, StreamConfigurationOptions as RustStreamOptions,
    TableProperties as RustTableProperties, ZerobusError as RustError, ZerobusSdk as RustSdk,
    ZerobusStream as RustStream,
};

use crate::arrow::{self, ArrowStreamConfigurationOptions, AsyncZerobusArrowStream};
use crate::auth::HeadersProviderWrapper;
use crate::common::{map_error, AckCallback, StreamConfigurationOptions, TableProperties};

// =============================================================================
// ACK CALLBACK WRAPPER
// =============================================================================

/// Wraps Python AckCallback to implement Rust AckCallback trait
struct AckCallbackWrapper {
    py_callback: Py<AckCallback>,
}

impl AckCallbackWrapper {
    pub fn new(py_callback: Py<AckCallback>) -> Self {
        Self { py_callback }
    }
}

impl RustAckCallback for AckCallbackWrapper {
    fn on_ack(&self, offset_id: OffsetId) {
        Python::with_gil(|py| {
            if let Err(e) = self.py_callback.call_method1(py, "on_ack", (offset_id,)) {
                eprintln!("Error invoking ack callback: {:?}", e);
            }
        });
    }

    fn on_error(&self, offset_id: OffsetId, error: &str) {
        Python::with_gil(|py| {
            if let Err(e) = self
                .py_callback
                .call_method1(py, "on_error", (offset_id, error))
            {
                eprintln!("Error invoking error callback: {:?}", e);
            }
        });
    }
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

fn extract_record_payload(payload: &PyAny) -> PyResult<EncodedRecord> {
    if let Ok(bytes) = payload.downcast::<PyBytes>() {
        Ok(EncodedRecord::Proto(bytes.as_bytes().to_vec()))
    } else if let Ok(json_str) = payload.extract::<String>() {
        Ok(EncodedRecord::Json(json_str))
    } else if let Ok(bytes) = payload.extract::<Vec<u8>>() {
        Ok(EncodedRecord::Proto(bytes))
    } else if payload.hasattr("SerializeToString")? {
        // It's a protobuf Message object - serialize it
        let serialize_method = payload.getattr("SerializeToString")?;
        let serialized_bytes: Vec<u8> = serialize_method.call0()?.extract()?;
        Ok(EncodedRecord::Proto(serialized_bytes))
    } else {
        // Try to serialize as JSON (dict, list, etc.)
        Python::with_gil(|py| {
            let json_module = py.import("json")?;
            let json_dumps = json_module.getattr("dumps")?;
            let json_str: String = json_dumps.call1((payload,))?.extract()?;
            Ok(EncodedRecord::Json(json_str))
        })
    }
}

fn extract_record_payloads(payloads: &PyAny) -> PyResult<Vec<EncodedRecord>> {
    let mut record_payloads = Vec::new();

    if let Ok(list) = payloads.downcast::<PyList>() {
        record_payloads.reserve(list.len());

        for item in list {
            record_payloads.push(extract_record_payload(item)?);
        }
    } else if let Ok(bytes_list) = payloads.extract::<Vec<Vec<u8>>>() {
        for bytes in bytes_list {
            record_payloads.push(EncodedRecord::Proto(bytes));
        }
    } else if let Ok(json_list) = payloads.extract::<Vec<String>>() {
        for json in json_list {
            record_payloads.push(EncodedRecord::Json(json));
        }
    } else {
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Payloads must be a list",
        ));
    }

    Ok(record_payloads)
}

fn map_rust_error_to_pyerr(err: RustError) -> PyErr {
    map_error(err)
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
    inner: Arc<RwLock<RustStream>>,
}

#[pymethods]
impl ZerobusStream {
    /// Ingest a record and return a future that can be awaited for acknowledgment (legacy API)
    ///
    /// Args:
    ///     payload: Bytes (serialized protobuf) or str (JSON)
    ///
    /// Returns:
    ///     RecordIngestionFuture that can be awaited for acknowledgment
    #[deprecated(
        since = "0.3.0",
        note = "Use ingest_record_offset() instead for better performance"
    )]
    fn ingest_record<'py>(&self, py: Python<'py>, payload: &PyAny) -> PyResult<&'py PyAny> {
        let record_payload = extract_record_payload(payload)?;
        let stream_clone = self.inner.clone();

        let outer_future = async move {
            let stream_guard = stream_clone.read().await;

            let ack_future_result = stream_guard.ingest_record(record_payload).await;
            drop(stream_guard);

            ack_future_result
                .map(|ack_future| {
                    let converted_future = async move {
                        ack_future
                            .await
                            .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))
                    };
                    PyAckFuture::new(converted_future)
                })
                .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))
        };

        future_into_py(py, outer_future)
    }

    /// Ingest a single record and return the offset ID (async)
    fn ingest_record_offset<'py>(&self, py: Python<'py>, payload: &PyAny) -> PyResult<&'py PyAny> {
        let record_payload = extract_record_payload(payload)?;
        let stream_clone = self.inner.clone();

        future_into_py(py, async move {
            let stream_guard = stream_clone.read().await;
            let offset_id = stream_guard
                .ingest_record_offset(record_payload)
                .await
                .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))?;
            Ok(offset_id)
        })
    }

    /// Ingest a single record without waiting (fire-and-forget async)
    fn ingest_record_nowait(&self, payload: &PyAny) -> PyResult<()> {
        let record_payload = extract_record_payload(payload)?;
        let stream_clone = self.inner.clone();

        // Spawn a background task
        pyo3_asyncio::tokio::get_runtime().spawn(async move {
            let stream_guard = stream_clone.read().await;
            let _ = stream_guard.ingest_record_offset(record_payload).await;
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
        let stream_clone = self.inner.clone();

        future_into_py(py, async move {
            let stream_guard = stream_clone.read().await;
            let offset_id = stream_guard
                .ingest_records_offset(record_payloads)
                .await
                .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))?;
            Ok(offset_id)
        })
    }

    /// Ingest a batch of records without waiting (async)
    fn ingest_records_nowait(&self, payloads: &PyAny) -> PyResult<()> {
        let record_payloads = extract_record_payloads(payloads)?;
        let stream_clone = self.inner.clone();

        pyo3_asyncio::tokio::get_runtime().spawn(async move {
            let stream_guard = stream_clone.read().await;
            let _ = stream_guard.ingest_records_offset(record_payloads).await;
        });

        Ok(())
    }

    /// Wait for a specific offset to be acknowledged (async)
    fn wait_for_offset<'py>(&self, py: Python<'py>, offset: i64) -> PyResult<&'py PyAny> {
        let stream = self.inner.clone();

        future_into_py(py, async move {
            let stream_guard = stream.read().await;
            stream_guard
                .wait_for_offset(offset)
                .await
                .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))?;
            Ok(())
        })
    }

    /// Flush the stream (async)
    fn flush<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        let stream = self.inner.clone();

        future_into_py(py, async move {
            let stream_guard = stream.read().await;
            stream_guard
                .flush()
                .await
                .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))?;
            Ok(())
        })
    }

    /// Close the stream (async)
    fn close<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        let stream = self.inner.clone();

        future_into_py(py, async move {
            let mut stream_guard = stream.write().await;
            stream_guard
                .close()
                .await
                .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))?;
            Ok(())
        })
    }

    /// Get unacknowledged records
    fn get_unacked_records<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        let stream_clone = self.inner.clone();

        let rust_future = async move {
            let stream_guard = stream_clone.read().await;
            let records = stream_guard
                .get_unacked_records()
                .await
                .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))?;

            // Convert records to Python bytes objects
            Python::with_gil(|py| {
                let py_records: Vec<PyObject> = records
                    .into_iter()
                    .map(|record| match record {
                        EncodedRecord::Proto(bytes) => pyo3::types::PyBytes::new(py, &bytes).into(),
                        EncodedRecord::Json(json_str) => {
                            pyo3::types::PyBytes::new(py, json_str.as_bytes()).into()
                        }
                    })
                    .collect();
                Ok(py_records)
            })
        };

        future_into_py(py, rust_future)
    }

    /// Get unacknowledged batches
    fn get_unacked_batches<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        let stream_clone = self.inner.clone();

        let rust_future = async move {
            let stream_guard = stream_clone.read().await;
            let batches = stream_guard
                .get_unacked_batches()
                .await
                .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))?;

            // Convert batches to Python list of lists of bytes
            Python::with_gil(|py| {
                let py_batches: Vec<Vec<PyObject>> = batches
                    .into_iter()
                    .map(|batch| {
                        batch
                            .into_iter()
                            .map(|record| match record {
                                EncodedRecord::Proto(bytes) => {
                                    pyo3::types::PyBytes::new(py, &bytes).into()
                                }
                                EncodedRecord::Json(json_str) => {
                                    pyo3::types::PyBytes::new(py, json_str.as_bytes()).into()
                                }
                            })
                            .collect()
                    })
                    .collect();
                Ok(py_batches)
            })
        };

        future_into_py(py, rust_future)
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
    fn new(host: String, unity_catalog_url: String) -> PyResult<Self> {
        let sdk = RustSdk::new(host, unity_catalog_url)
            .map_err(|err| Python::with_gil(|_py| map_rust_error_to_pyerr(err)))?;

        Ok(ZerobusSdk {
            inner: Arc::new(RwLock::new(sdk)),
        })
    }

    /// Set whether to use TLS (default: true)
    /// Set to false for testing with local mock servers
    fn set_use_tls<'py>(&self, py: Python<'py>, use_tls: bool) -> PyResult<&'py PyAny> {
        let sdk = self.inner.clone();

        future_into_py(py, async move {
            let mut sdk_guard = sdk.write().await;
            sdk_guard.use_tls = use_tls;
            Ok(())
        })
    }

    /// Create stream with client credentials (async)
    #[pyo3(signature = (client_id, client_secret, table_properties, options = None))]
    fn create_stream<'py>(
        &self,
        py: Python<'py>,
        client_id: String,
        client_secret: String,
        table_properties: &TableProperties,
        options: Option<&StreamConfigurationOptions>,
    ) -> PyResult<&'py PyAny> {
        let sdk = self.inner.clone();

        // Convert Python TableProperties to Rust TableProperties
        let props = RustTableProperties {
            table_name: table_properties.table_name.clone(),
            descriptor_proto: table_properties.descriptor_proto.clone(),
        };

        let opts = convert_stream_options(options)?;

        future_into_py(py, async move {
            let sdk_guard = sdk.read().await;
            sdk_guard
                .create_stream(props, client_id, client_secret, opts)
                .await
                .map(|stream| ZerobusStream {
                    inner: Arc::new(RwLock::new(stream)),
                })
                .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))
        })
    }

    /// Create stream with custom headers provider (async)
    #[pyo3(signature = (table_properties, headers_provider, options = None))]
    fn create_stream_with_headers_provider<'py>(
        &self,
        py: Python<'py>,
        table_properties: &TableProperties,
        headers_provider: PyObject,
        options: Option<&StreamConfigurationOptions>,
    ) -> PyResult<&'py PyAny> {
        let sdk = self.inner.clone();

        let props = RustTableProperties {
            table_name: table_properties.table_name.clone(),
            descriptor_proto: table_properties.descriptor_proto.clone(),
        };

        let opts = convert_stream_options(options)?;
        let wrapper = Arc::new(HeadersProviderWrapper::new(headers_provider));

        future_into_py(py, async move {
            let sdk_guard = sdk.read().await;
            sdk_guard
                .create_stream_with_headers_provider(props, wrapper, opts)
                .await
                .map(|stream| ZerobusStream {
                    inner: Arc::new(RwLock::new(stream)),
                })
                .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))
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
                .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))?;

            Ok(ZerobusStream {
                inner: Arc::new(RwLock::new(new_stream)),
            })
        })
    }
}

// Helper to convert Python StreamConfigurationOptions to Rust options
fn convert_stream_options(
    options: Option<&StreamConfigurationOptions>,
) -> PyResult<Option<RustStreamOptions>> {
    match options {
        Some(opts) => {
            opts.validate()?;
            let ack_callback = opts
                .ack_callback
                .clone()
                .map(|cb| Arc::new(AckCallbackWrapper::new(cb)) as Arc<dyn RustAckCallback>);

            Ok(Some(RustStreamOptions {
                max_inflight_requests: opts.max_inflight_records as usize,
                recovery: opts.recovery,
                recovery_timeout_ms: opts.recovery_timeout_ms as u64,
                recovery_backoff_ms: opts.recovery_backoff_ms as u64,
                recovery_retries: opts.recovery_retries as u32,
                server_lack_of_ack_timeout_ms: opts.server_lack_of_ack_timeout_ms as u64,
                flush_timeout_ms: opts.flush_timeout_ms as u64,
                record_type: match opts.record_type.value {
                    1 => RustRecordType::Proto,
                    2 => RustRecordType::Json,
                    _ => RustRecordType::Proto,
                },
                stream_paused_max_wait_time_ms: opts.stream_paused_max_wait_time_ms.map(|v| v as u64),
                callback_max_wait_time_ms: opts.callback_max_wait_time_ms.map(|v| v as u64),
                ack_callback,
                ..Default::default()
            }))
        }
        None => Ok(None),
    }
}
