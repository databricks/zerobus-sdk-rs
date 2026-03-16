use prost::Message;
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

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
        Self { value: 1 } // PROTO
    }
}

/// Table properties for the stream
#[pyclass]
#[derive(Debug, Clone)]
pub struct TableProperties {
    #[pyo3(get)]
    pub table_name: String,

    // Internal field - stores the parsed DescriptorProto
    pub(crate) descriptor_proto: Option<prost_types::DescriptorProto>,
}

#[pymethods]
impl TableProperties {
    #[new]
    #[pyo3(signature = (table_name, descriptor_proto=None))]
    fn new(table_name: String, descriptor_proto: Option<&PyAny>) -> PyResult<Self> {
        let rust_descriptor = if let Some(obj) = descriptor_proto {
            if obj.is_none() {
                None
            } else {
                // Try to extract as bytes first
                let descriptor_bytes = if let Ok(bytes) = obj.extract::<Vec<u8>>() {
                    bytes
                } else if obj.hasattr("file")? {
                    // If it's a descriptor object, get the file.serialized_pb attribute
                    let file = obj.getattr("file")?;
                    if file.hasattr("serialized_pb")? {
                        let serialized_pb = file.getattr("serialized_pb")?;
                        serialized_pb.extract::<Vec<u8>>().map_err(|_| {
                            PyErr::new::<pyo3::exceptions::PyValueError, _>(
                                "Descriptor.file.serialized_pb must be bytes",
                            )
                        })?
                    } else {
                        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            "Descriptor.file does not have serialized_pb attribute",
                        ));
                    }
                } else {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "Descriptor must be bytes or a Descriptor object with file.serialized_pb",
                    ));
                };

                let file_descriptor_proto = prost_types::FileDescriptorProto::decode(
                    &descriptor_bytes[..],
                )
                .map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "Invalid FileDescriptorProto bytes: {}",
                        e
                    ))
                })?;

                let descriptor = file_descriptor_proto
                    .message_type
                    .into_iter()
                    .next()
                    .ok_or_else(|| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            "No message types found in the provided file descriptor",
                        )
                    })?;

                Some(descriptor)
            }
        } else {
            None
        };

        Ok(Self {
            table_name,
            descriptor_proto: rust_descriptor,
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "TableProperties(table_name='{}', descriptor_proto={})",
            self.table_name,
            if self.descriptor_proto.is_some() {
                "Some(...)"
            } else {
                "None"
            }
        )
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
        // Accept and ignore any arguments to allow subclasses to use __init__
        Self {
            _phantom: std::marker::PhantomData,
        }
    }

    fn on_ack(&self, py: Python, offset: i64) -> PyResult<()> {
        // Default implementation does nothing - subclasses override this
        let _ = (py, offset);
        Ok(())
    }
}

impl AckCallback {
    /// Invoke the callback with proper GIL handling
    pub fn invoke(&self, py: Python, offset: i64) -> PyResult<()> {
        self.on_ack(py, offset)
    }
}

/// Configuration options for the stream
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
    /// Validate that all numeric fields are non-negative before casting to unsigned types.
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
            record_type: RecordType { value: 1 }, // PROTO
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
    fn new(kwargs: Option<&pyo3::types::PyDict>) -> PyResult<Self> {
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
             flush_timeout_ms={}, record_type={:?}, ack_callback={})",
            self.max_inflight_records,
            self.recovery,
            self.recovery_timeout_ms,
            self.recovery_backoff_ms,
            self.recovery_retries,
            self.server_lack_of_ack_timeout_ms,
            self.flush_timeout_ms,
            self.record_type,
            if self.ack_callback.is_some() { "Some(...)" } else { "None" }
        )
    }
}

// Custom exception types
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

/// Map Rust SDK errors to Python exceptions
pub fn map_error(err: impl std::fmt::Display) -> PyErr {
    let msg = err.to_string();
    // For now, treat all errors as retriable ZerobusException
    // We can make this more sophisticated later by examining error messages/types
    ZerobusException::new_err(msg)
}
