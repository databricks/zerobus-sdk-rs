use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};
use tokio::runtime::Runtime;
use tokio::sync::RwLock;

use databricks_zerobus_ingest_sdk::{
    databricks::zerobus::RecordType as RustRecordType, AckCallback as RustAckCallback,
    EncodedRecord, OffsetId, StreamConfigurationOptions as RustStreamOptions,
    TableProperties as RustTableProperties, ZerobusError as RustError, ZerobusResult as RustResult,
    ZerobusSdk as RustSdk, ZerobusStream as RustStream,
};

use crate::arrow::{self, ArrowStreamConfigurationOptions, ZerobusArrowStream};
use crate::auth::HeadersProviderWrapper;
use crate::common::{map_error, AckCallback, StreamConfigurationOptions, TableProperties};

type AckFuture = Pin<Box<dyn Future<Output = RustResult<OffsetId>> + Send>>;

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
// CALLBACK WRAPPER
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
                eprintln!("Error invoking Python callback: {:?}", e);
            }
        });
    }

    fn on_error(&self, offset_id: OffsetId, error_message: &str) {
        Python::with_gil(|_py| {
            // For now, just log errors - could add an on_error method to AckCallback
            eprintln!("Ack error for offset {}: {}", offset_id, error_message);
        });
    }
}

// =============================================================================
// RECORD ACKNOWLEDGMENT
// =============================================================================

#[pyclass]
pub struct RecordAcknowledgment {
    ack_future: Option<AckFuture>,
    runtime: Arc<Runtime>,
}

#[pymethods]
impl RecordAcknowledgment {
    /// Wait for the acknowledgment and return the offset ID.
    /// This method can only be called once.
    pub fn wait_for_ack(&mut self, py: Python, _timeout_sec: Option<f64>) -> PyResult<i64> {
        if let Some(future) = self.ack_future.take() {
            let runtime = self.runtime.clone();

            // Release GIL before blocking to allow callbacks to run
            py.allow_threads(|| {
                // Spawn on the runtime to allow worker threads to make progress
                let handle = runtime.spawn(future);
                runtime.block_on(handle).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Task join error: {:?}",
                        e
                    ))
                })
            })?
            .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "wait_for_ack has already been called.",
            ))
        }
    }

    /// Check if the acknowledgment is done
    pub fn is_done(&self) -> bool {
        self.ack_future.is_none()
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
    fn ingest_record(&self, payload: &PyAny) -> PyResult<RecordAcknowledgment> {
        let record_payload = extract_record_payload(payload)?;
        let stream_clone = self.inner.clone();
        let runtime = self.runtime.clone();

        let handle = self.runtime.spawn(async move {
            let stream_guard = stream_clone.read().await;
            stream_guard.ingest_record(record_payload).await
        });

        let inner_ack_future = self
            .runtime
            .block_on(handle)
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Task join error: {:?}",
                    e
                ))
            })?
            .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))?;

        Ok(RecordAcknowledgment {
            ack_future: Some(Box::pin(inner_ack_future)),
            runtime,
        })
    }

    /// Ingest a single record and return the offset directly (optimized API)
    fn ingest_record_offset(&self, py: Python, payload: &PyAny) -> PyResult<i64> {
        let record_payload = extract_record_payload(payload)?;
        let stream_clone = self.inner.clone();
        let runtime = self.runtime.clone();

        py.allow_threads(|| {
            runtime.block_on(async move {
                let stream_guard = stream_clone.read().await;
                stream_guard
                    .ingest_record_offset(record_payload)
                    .await
                    .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))
            })
        })
    }

    /// Ingest a single record without waiting for acknowledgment (fire-and-forget)
    fn ingest_record_nowait(&self, payload: &PyAny) -> PyResult<()> {
        let record_payload = extract_record_payload(payload)?;
        let stream_clone = self.inner.clone();

        self.runtime.spawn(async move {
            let stream_guard = stream_clone.read().await;
            let _ = stream_guard.ingest_record_offset(record_payload).await;
        });

        Ok(())
    }

    /// Ingest multiple records and return one offset for the whole batch (batch API)
    fn ingest_records_offset(&self, py: Python, payloads: &PyAny) -> PyResult<Option<i64>> {
        let record_payloads = extract_record_payloads(payloads)?;
        if record_payloads.is_empty() {
            return Ok(None);
        }

        let stream_clone = self.inner.clone();
        let runtime = self.runtime.clone();

        py.allow_threads(|| {
            runtime.block_on(async move {
                let stream_guard = stream_clone.read().await;
                let offset = stream_guard
                    .ingest_records_offset(record_payloads)
                    .await
                    .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))?;
                Ok(offset)
            })
        })
    }

    /// Ingest multiple records without waiting for acknowledgments (batch fire-and-forget)
    fn ingest_records_nowait(&self, payloads: &PyAny) -> PyResult<()> {
        let record_payloads = extract_record_payloads(payloads)?;
        let stream_clone = self.inner.clone();

        self.runtime.spawn(async move {
            let stream_guard = stream_clone.read().await;
            let _ = stream_guard.ingest_records_offset(record_payloads).await;
        });

        Ok(())
    }

    /// Wait for a specific offset to be acknowledged
    fn wait_for_offset(&self, py: Python, offset: i64, timeout_sec: Option<f64>) -> PyResult<()> {
        let _ = timeout_sec; // Timeout is handled internally by the Rust SDK
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

    /// Flush the stream, waiting for all pending records to be acknowledged
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

    /// Close the stream gracefully
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

    /// Get unacknowledged records
    fn get_unacked_records(&self, py: Python) -> PyResult<Vec<PyObject>> {
        let stream_clone = self.inner.clone();
        let runtime = self.runtime.clone();

        py.allow_threads(|| {
            runtime.block_on(async move {
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
                            EncodedRecord::Proto(bytes) => {
                                pyo3::types::PyBytes::new(py, &bytes).into()
                            }
                            EncodedRecord::Json(json_str) => {
                                pyo3::types::PyBytes::new(py, json_str.as_bytes()).into()
                            }
                        })
                        .collect();
                    Ok(py_records)
                })
            })
        })
    }

    /// Get unacknowledged batches
    fn get_unacked_batches(&self, py: Python) -> PyResult<Vec<Vec<PyObject>>> {
        let stream_clone = self.inner.clone();
        let runtime = self.runtime.clone();

        py.allow_threads(|| {
            runtime.block_on(async move {
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
    #[new]
    fn new(host: String, unity_catalog_url: String) -> PyResult<Self> {
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

        let inner = runtime.block_on(async {
            RustSdk::new(host, unity_catalog_url)
                .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))
        })?;

        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
            runtime,
        })
    }

    /// Set whether to use TLS (default: true)
    /// Set to false for testing with local mock servers
    fn set_use_tls(&self, py: Python, use_tls: bool) -> PyResult<()> {
        let sdk_clone = self.inner.clone();
        let runtime = self.runtime.clone();

        py.allow_threads(|| {
            runtime.block_on(async move {
                let mut sdk_guard = sdk_clone.write().await;
                sdk_guard.use_tls = use_tls;
                Ok::<(), PyErr>(())
            })
        })
    }

    /// Create a new stream with OAuth authentication
    fn create_stream(
        &self,
        py: Python,
        client_id: String,
        client_secret: String,
        table_properties: TableProperties,
        options: Option<StreamConfigurationOptions>,
    ) -> PyResult<ZerobusStream> {
        // descriptor_proto is already a DescriptorProto from TableProperties constructor
        let rust_table_props = RustTableProperties {
            table_name: table_properties.table_name,
            descriptor_proto: table_properties.descriptor_proto,
        };

        let rust_options = if let Some(opts) = options.clone() {
            let ack_callback = opts
                .ack_callback
                .map(|cb| Arc::new(AckCallbackWrapper::new(cb)) as Arc<dyn RustAckCallback>);

            RustStreamOptions {
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
                    _ => RustRecordType::Unspecified,
                },
                stream_paused_max_wait_time_ms: opts
                    .stream_paused_max_wait_time_ms
                    .map(|v| v as u64),
                callback_max_wait_time_ms: opts.callback_max_wait_time_ms.map(|v| v as u64),
                ack_callback,
                ..Default::default()
            }
        } else {
            RustStreamOptions::default()
        };

        let sdk = self.inner.clone();
        let runtime = self.runtime.clone();

        let stream = py.allow_threads(|| {
            runtime.block_on(async move {
                let sdk_guard = sdk.read().await;
                sdk_guard
                    .create_stream(
                        rust_table_props,
                        client_id,
                        client_secret,
                        Some(rust_options),
                    )
                    .await
                    .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))
            })
        })?;

        Ok(ZerobusStream {
            inner: Arc::new(RwLock::new(stream)),
            runtime,
        })
    }

    /// Create a new stream with custom headers provider
    fn create_stream_with_headers_provider(
        &self,
        py: Python,
        table_properties: TableProperties,
        headers_provider: PyObject,
        options: Option<StreamConfigurationOptions>,
    ) -> PyResult<ZerobusStream> {
        // descriptor_proto is already a DescriptorProto from TableProperties constructor
        let rust_table_props = RustTableProperties {
            table_name: table_properties.table_name,
            descriptor_proto: table_properties.descriptor_proto,
        };

        let rust_options = if let Some(opts) = options.clone() {
            let ack_callback = opts
                .ack_callback
                .map(|cb| Arc::new(AckCallbackWrapper::new(cb)) as Arc<dyn RustAckCallback>);

            RustStreamOptions {
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
                    _ => RustRecordType::Unspecified,
                },
                stream_paused_max_wait_time_ms: opts
                    .stream_paused_max_wait_time_ms
                    .map(|v| v as u64),
                callback_max_wait_time_ms: opts.callback_max_wait_time_ms.map(|v| v as u64),
                ack_callback,
                ..Default::default()
            }
        } else {
            RustStreamOptions::default()
        };

        let provider = Arc::new(HeadersProviderWrapper::new(headers_provider));
        let sdk = self.inner.clone();
        let runtime = self.runtime.clone();

        let stream = py.allow_threads(|| {
            runtime.block_on(async move {
                let sdk_guard = sdk.read().await;
                sdk_guard
                    .create_stream_with_headers_provider(
                        rust_table_props,
                        provider,
                        Some(rust_options),
                    )
                    .await
                    .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))
            })
        })?;

        Ok(ZerobusStream {
            inner: Arc::new(RwLock::new(stream)),
            runtime,
        })
    }

    /// Create a new Arrow Flight stream with OAuth authentication.
    ///
    /// Args:
    ///     table_name: Fully qualified table name (catalog.schema.table)
    ///     schema_ipc_bytes: Arrow IPC serialized schema bytes
    ///     client_id: OAuth client ID
    ///     client_secret: OAuth client secret
    ///     options: Optional ArrowStreamConfigurationOptions
    #[pyo3(signature = (table_name, schema_ipc_bytes, client_id, client_secret, options = None))]
    fn create_arrow_stream(
        &self,
        py: Python,
        table_name: String,
        schema_ipc_bytes: &PyBytes,
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
        schema_ipc_bytes: &PyBytes,
        headers_provider: PyObject,
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

        let new_stream = py.allow_threads(|| {
            runtime.block_on(async move {
                let guard = old_stream_inner.read().await;
                let sdk_guard = sdk.read().await;
                sdk_guard
                    .recreate_stream(&*guard)
                    .await
                    .map_err(|e| Python::with_gil(|_py| map_rust_error_to_pyerr(e)))
            })
        })?;

        Ok(ZerobusStream {
            inner: Arc::new(RwLock::new(new_stream)),
            runtime,
        })
    }
}
