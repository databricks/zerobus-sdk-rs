use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use prost::Message;
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};

use databricks_zerobus_ingest_sdk::{
    AckCallback as RustAckCallback, EncodedRecord, OffsetId, StreamBuilder,
};

/// User-agent prefix emitted by this wrapper SDK. Combined with the wrapper
/// crate version via `env!("CARGO_PKG_VERSION")` at the call site.
pub(crate) const SDK_IDENTIFIER_PREFIX: &str = "zerobus-sdk-py";

/// Type of records to ingest into the stream
#[pyclass]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordType {
    #[pyo3(get)]
    pub value: i32,
}

#[pymethods]
impl RecordType {
    #[classattr]
    #[allow(non_snake_case)]
    fn PROTO() -> Self {
        RecordType { value: 1 }
    }

    #[classattr]
    #[allow(non_snake_case)]
    fn JSON() -> Self {
        RecordType { value: 2 }
    }

    fn __int__(&self) -> i32 {
        self.value
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.value == other.value
    }

    fn __repr__(&self) -> String {
        match self.value {
            1 => "RecordType.PROTO".to_string(),
            2 => "RecordType.JSON".to_string(),
            _ => format!("RecordType({})", self.value),
        }
    }
}

impl Default for RecordType {
    fn default() -> Self {
        Self::PROTO()
    }
}

/// Base class for record acknowledgment callbacks
#[pyclass(subclass)]
#[derive(Clone)]
pub struct AckCallback {
    _phantom: std::marker::PhantomData<()>,
}

#[pymethods]
impl AckCallback {
    #[new]
    #[pyo3(signature = (*_args, **_kwargs))]
    fn new(_args: &PyTuple, _kwargs: Option<&PyDict>) -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }

    /// Called when a record is acknowledged by the server.
    fn on_ack(&self, py: Python, offset: i64) -> PyResult<()> {
        let _ = (py, offset);
        Ok(())
    }

    /// Called when a record encounters an error during ingestion.
    fn on_error(&self, py: Python, offset: i64, error_message: &str) -> PyResult<()> {
        let _ = (py, offset, error_message);
        Ok(())
    }
}

/// Bridges Python `AckCallback` subclasses to Rust's `AckCallback` trait.
pub(crate) struct AckCallbackWrapper {
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
                eprintln!("Error invoking Python AckCallback.on_ack: {:?}", e);
            }
        });
    }

    fn on_error(&self, offset_id: OffsetId, error_message: &str) {
        Python::with_gil(|py| {
            if let Err(e) =
                self.py_callback
                    .call_method1(py, "on_error", (offset_id, error_message))
            {
                eprintln!("Error invoking Python AckCallback.on_error: {:?}", e);
            }
        });
    }
}

/// Configuration options for JSON / protobuf gRPC streams.
#[pyclass]
#[derive(Clone)]
pub struct StreamConfigurationOptions {
    #[pyo3(get, set)]
    pub max_inflight_records: i32,

    #[pyo3(get, set)]
    pub recovery: bool,

    #[pyo3(get, set)]
    pub recovery_timeout_ms: i32,

    #[pyo3(get, set)]
    pub recovery_backoff_ms: i32,

    #[pyo3(get, set)]
    pub recovery_retries: i32,

    #[pyo3(get, set)]
    pub server_lack_of_ack_timeout_ms: i32,

    #[pyo3(get, set)]
    pub flush_timeout_ms: i32,

    #[pyo3(get, set)]
    pub record_type: RecordType,

    #[pyo3(get, set)]
    pub stream_paused_max_wait_time_ms: Option<i32>,

    #[pyo3(get, set)]
    pub callback_max_wait_time_ms: Option<i32>,

    #[pyo3(get, set)]
    pub ack_callback: Option<Py<AckCallback>>,
}

impl StreamConfigurationOptions {
    pub fn validate(&self) -> PyResult<()> {
        if self.max_inflight_records < 0 {
            return Err(PyValueError::new_err(
                "max_inflight_records must be non-negative",
            ));
        }
        if self.recovery_timeout_ms < 0 {
            return Err(PyValueError::new_err(
                "recovery_timeout_ms must be non-negative",
            ));
        }
        if self.recovery_backoff_ms < 0 {
            return Err(PyValueError::new_err(
                "recovery_backoff_ms must be non-negative",
            ));
        }
        if self.recovery_retries < 0 {
            return Err(PyValueError::new_err(
                "recovery_retries must be non-negative",
            ));
        }
        if self.server_lack_of_ack_timeout_ms < 0 {
            return Err(PyValueError::new_err(
                "server_lack_of_ack_timeout_ms must be non-negative",
            ));
        }
        if self.flush_timeout_ms < 0 {
            return Err(PyValueError::new_err(
                "flush_timeout_ms must be non-negative",
            ));
        }
        match self.record_type.value {
            1 | 2 => {}
            other => {
                return Err(PyValueError::new_err(format!(
                    "record_type must be RecordType.PROTO or RecordType.JSON; got value {}",
                    other
                )));
            }
        }
        if let Some(v) = self.stream_paused_max_wait_time_ms {
            if v < 0 {
                return Err(PyValueError::new_err(
                    "stream_paused_max_wait_time_ms must be non-negative",
                ));
            }
        }
        if let Some(v) = self.callback_max_wait_time_ms {
            if v < 0 {
                return Err(PyValueError::new_err(
                    "callback_max_wait_time_ms must be non-negative",
                ));
            }
        }
        Ok(())
    }
}

impl Default for StreamConfigurationOptions {
    fn default() -> Self {
        Self {
            max_inflight_records: 50_000,
            recovery: true,
            recovery_timeout_ms: 15_000,
            recovery_backoff_ms: 2_000,
            recovery_retries: 3,
            server_lack_of_ack_timeout_ms: 60_000,
            flush_timeout_ms: 300_000,
            record_type: RecordType::default(),
            stream_paused_max_wait_time_ms: None,
            callback_max_wait_time_ms: Some(5_000),
            ack_callback: None,
        }
    }
}

#[pymethods]
impl StreamConfigurationOptions {
    #[new]
    #[pyo3(signature = (**kwargs))]
    fn new(kwargs: Option<&PyDict>) -> PyResult<Self> {
        let mut options = Self::default();

        if let Some(kwargs) = kwargs {
            for (key, value) in kwargs {
                let key_str: &str = key.extract()?;
                match key_str {
                    "max_inflight_records" => options.max_inflight_records = value.extract()?,
                    "recovery" => options.recovery = value.extract()?,
                    "recovery_timeout_ms" => options.recovery_timeout_ms = value.extract()?,
                    "recovery_backoff_ms" => options.recovery_backoff_ms = value.extract()?,
                    "recovery_retries" => options.recovery_retries = value.extract()?,
                    "server_lack_of_ack_timeout_ms" => {
                        options.server_lack_of_ack_timeout_ms = value.extract()?
                    }
                    "flush_timeout_ms" => options.flush_timeout_ms = value.extract()?,
                    "record_type" => options.record_type = value.extract()?,
                    "stream_paused_max_wait_time_ms" => {
                        options.stream_paused_max_wait_time_ms = if value.is_none() {
                            None
                        } else {
                            Some(value.extract()?)
                        };
                    }
                    "callback_max_wait_time_ms" => {
                        options.callback_max_wait_time_ms = if value.is_none() {
                            None
                        } else {
                            Some(value.extract()?)
                        };
                    }
                    "ack_callback" => {
                        options.ack_callback = if value.is_none() {
                            None
                        } else {
                            Some(value.extract()?)
                        };
                    }
                    _ => {
                        return Err(PyValueError::new_err(format!(
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
            "StreamConfigurationOptions(max_inflight_records={}, recovery={}, recovery_timeout_ms={}, \
             recovery_backoff_ms={}, recovery_retries={}, server_lack_of_ack_timeout_ms={}, \
             flush_timeout_ms={}, record_type={}, stream_paused_max_wait_time_ms={:?}, \
             callback_max_wait_time_ms={:?}, ack_callback={})",
            self.max_inflight_records,
            self.recovery,
            self.recovery_timeout_ms,
            self.recovery_backoff_ms,
            self.recovery_retries,
            self.server_lack_of_ack_timeout_ms,
            self.flush_timeout_ms,
            self.record_type.__repr__(),
            self.stream_paused_max_wait_time_ms,
            self.callback_max_wait_time_ms,
            if self.ack_callback.is_some() { "Some(...)" } else { "None" }
        )
    }
}

// =============================================================================
// SHARED HELPERS (used by sync_wrapper and async_wrapper)
// =============================================================================

/// Coerce a Python record payload into a Rust `EncodedRecord`.
///
/// Accepts `bytes` (treated as proto), `str` (treated as JSON), `list[int]`
/// fallback for `bytes`-like buffers, protobuf `Message` objects (via
/// `SerializeToString`), or anything `json.dumps` will accept.
pub(crate) fn extract_record_payload(payload: &PyAny) -> PyResult<EncodedRecord> {
    if let Ok(bytes) = payload.downcast::<PyBytes>() {
        Ok(EncodedRecord::Proto(bytes.as_bytes().to_vec()))
    } else if let Ok(json_str) = payload.extract::<String>() {
        Ok(EncodedRecord::Json(json_str))
    } else if let Ok(bytes) = payload.extract::<Vec<u8>>() {
        Ok(EncodedRecord::Proto(bytes))
    } else if payload.hasattr("SerializeToString")? {
        let serialize_method = payload.getattr("SerializeToString")?;
        let serialized_bytes: Vec<u8> = serialize_method.call0()?.extract()?;
        Ok(EncodedRecord::Proto(serialized_bytes))
    } else {
        Python::with_gil(|py| {
            let json_module = py.import("json")?;
            let json_dumps = json_module.getattr("dumps")?;
            let json_str: String = json_dumps.call1((payload,))?.extract()?;
            Ok(EncodedRecord::Json(json_str))
        })
    }
}

pub(crate) fn extract_record_payloads(payloads: &PyAny) -> PyResult<Vec<EncodedRecord>> {
    let mut out = Vec::new();

    if let Ok(list) = payloads.downcast::<PyList>() {
        out.reserve(list.len());
        for item in list {
            out.push(extract_record_payload(item)?);
        }
    } else if let Ok(bytes_list) = payloads.extract::<Vec<Vec<u8>>>() {
        for bytes in bytes_list {
            out.push(EncodedRecord::Proto(bytes));
        }
    } else if let Ok(json_list) = payloads.extract::<Vec<String>>() {
        for json in json_list {
            out.push(EncodedRecord::Json(json));
        }
    } else {
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Payloads must be a list",
        ));
    }

    Ok(out)
}

pub(crate) fn encoded_record_to_pybytes(py: Python, record: EncodedRecord) -> PyObject {
    match record {
        EncodedRecord::Proto(bytes) => PyBytes::new(py, &bytes).into(),
        EncodedRecord::Json(json_str) => PyBytes::new(py, json_str.as_bytes()).into(),
    }
}

/// Apply `StreamConfigurationOptions` fields to a `StreamBuilder` via setters.
///
/// The Rust `StreamConfigurationOptions` is `#[non_exhaustive]`, so the binding
/// cannot construct one directly — every option must flow through a setter on
/// the builder.
pub(crate) fn apply_grpc_options<'a>(
    builder: StreamBuilder<'a>,
    opts: &StreamConfigurationOptions,
) -> PyResult<StreamBuilder<'a>> {
    opts.validate()?;

    let mut b = builder
        .max_inflight_requests(opts.max_inflight_records as usize)
        .recovery(opts.recovery)
        .recovery_timeout_ms(opts.recovery_timeout_ms as u64)
        .recovery_backoff_ms(opts.recovery_backoff_ms as u64)
        .recovery_retries(opts.recovery_retries as u32)
        .server_lack_of_ack_timeout_ms(opts.server_lack_of_ack_timeout_ms as u64)
        .flush_timeout_ms(opts.flush_timeout_ms as u64)
        .stream_paused_max_wait_time_ms(opts.stream_paused_max_wait_time_ms.map(|v| v as u64))
        .callback_max_wait_time_ms(opts.callback_max_wait_time_ms.map(|v| v as u64));

    if let Some(cb) = opts.ack_callback.clone() {
        b = b.ack_callback(Arc::new(AckCallbackWrapper::new(cb)) as Arc<dyn RustAckCallback>);
    }

    Ok(b)
}

/// Parse a serialized `FileDescriptorProto` and return the named message
/// descriptor.
///
/// If `message_name` is `Some(name)`, only the message whose `name` matches
/// is returned; if no such message exists in the file the call fails with a
/// `ValueError` that lists the available names. If `message_name` is `None`,
/// the first message in the file is returned (backwards-compatible behaviour
/// for callers that hand in raw bytes without a name hint).
pub(crate) fn descriptor_from_file_bytes(
    bytes: &[u8],
    message_name: Option<&str>,
) -> PyResult<prost_types::DescriptorProto> {
    let file = prost_types::FileDescriptorProto::decode(bytes)
        .map_err(|e| PyValueError::new_err(format!("Invalid FileDescriptorProto bytes: {}", e)))?;

    if file.message_type.is_empty() {
        return Err(PyValueError::new_err(
            "No message types found in the provided file descriptor",
        ));
    }

    match message_name {
        Some(name) => file
            .message_type
            .into_iter()
            .find(|m| m.name.as_deref() == Some(name))
            .ok_or_else(|| {
                PyValueError::new_err(format!(
                    "Message '{}' not found in the provided file descriptor",
                    name
                ))
            }),
        None => Ok(file.message_type.into_iter().next().unwrap()),
    }
}

// =============================================================================
// HEADER KEY INTERNER (shared by HeadersProviderWrapper in auth.rs)
// =============================================================================

/// Intern a header name string into a `&'static str`.
///
/// The Rust SDK's `HeadersProvider` trait requires header keys to have a
/// `'static` lifetime. To avoid leaking memory on every `get_headers()` call,
/// we maintain a process-wide interner that leaks each distinct header name
/// exactly once. Header *values* are not interned — they change per request
/// (e.g. OAuth tokens) and are passed through as `String`.
pub(crate) fn intern_header_name(name: String) -> &'static str {
    static INTERNER: OnceLock<Mutex<HashMap<String, &'static str>>> = OnceLock::new();
    let interner = INTERNER.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = interner.lock().expect("header interner mutex poisoned");
    if let Some(&existing) = guard.get(&name) {
        return existing;
    }
    let leaked: &'static str = Box::leak(name.clone().into_boxed_str());
    guard.insert(name, leaked);
    leaked
}

// =============================================================================
// EXCEPTIONS
// =============================================================================

pyo3::create_exception!(
    _zerobus_core,
    ZerobusException,
    PyException,
    "Base class for all exceptions in the Zerobus SDK"
);
pyo3::create_exception!(
    _zerobus_core,
    NonRetriableException,
    ZerobusException,
    "Indicates a non-retriable error has occurred"
);

pub fn map_error(err: impl std::fmt::Display) -> PyErr {
    ZerobusException::new_err(err.to_string())
}
