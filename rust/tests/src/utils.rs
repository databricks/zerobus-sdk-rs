#![allow(dead_code)]

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::{Arc, Mutex, Once};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use async_trait::async_trait;
use databricks_zerobus_ingest_sdk::{AckCallback, HeadersProvider, ZerobusResult};
use prost_reflect::prost_types;
use tracing_subscriber::EnvFilter;

static SETUP: Once = Once::new();

#[derive(Default)]
pub struct TestHeadersProvider {}

#[async_trait]
impl HeadersProvider for TestHeadersProvider {
    async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
        let mut headers = HashMap::new();
        headers.insert("authorization", "Bearer test_token".to_string());
        headers.insert("x-databricks-zerobus-table-name", "test_table".to_string());
        Ok(headers)
    }
}

/// Helper function to create a simple descriptor proto for testing.
pub fn create_test_descriptor_proto() -> Option<prost_types::DescriptorProto> {
    Some(prost_types::DescriptorProto {
        name: Some("TestMessage".to_string()),
        field: vec![
            prost_types::FieldDescriptorProto {
                name: Some("id".to_string()),
                number: Some(1),
                r#type: Some(prost_types::field_descriptor_proto::Type::Int64 as i32),
                ..Default::default()
            },
            prost_types::FieldDescriptorProto {
                name: Some("message".to_string()),
                number: Some(2),
                r#type: Some(prost_types::field_descriptor_proto::Type::String as i32),
                ..Default::default()
            },
        ],
        ..Default::default()
    })
}

/// Create a test Arrow schema matching the protobuf descriptor.
pub fn create_test_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("message", DataType::Utf8, true),
    ]))
}

/// Create a test RecordBatch with the given data.
pub fn create_test_record_batch(
    schema: Arc<ArrowSchema>,
    ids: Vec<i64>,
    messages: Vec<Option<&str>>,
) -> RecordBatch {
    let id_array = Int64Array::from(ids);
    let message_array = StringArray::from(messages);

    RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(message_array)])
        .expect("Failed to create RecordBatch")
}

/// Serialise a [`RecordBatch`] into Arrow IPC stream bytes, as an FFI caller would produce.
pub fn record_batch_to_ipc_bytes(batch: &RecordBatch) -> bytes::Bytes {
    let mut buf = Vec::new();
    let mut writer =
        arrow_ipc::writer::StreamWriter::try_new(&mut buf, batch.schema_ref()).unwrap();
    writer.write(batch).unwrap();
    writer.finish().unwrap();
    bytes::Bytes::from(buf)
}

/// Create a test Arrow schema with a dictionary-encoded column.
pub fn create_test_dict_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "category",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
    ]))
}

/// Create a test RecordBatch with a dictionary-encoded column.
pub fn create_test_dict_record_batch(
    schema: Arc<ArrowSchema>,
    ids: Vec<i64>,
    categories: Vec<Option<&str>>,
) -> RecordBatch {
    use arrow_array::types::Int32Type;
    use arrow_array::DictionaryArray;

    let id_array = Int64Array::from(ids);
    let category_array: DictionaryArray<Int32Type> = categories
        .into_iter()
        .collect::<DictionaryArray<Int32Type>>();

    RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(category_array)])
        .expect("Failed to create dictionary RecordBatch")
}

/// Deserialise Arrow IPC stream bytes back into a [`RecordBatch`].
pub fn ipc_bytes_to_record_batch(bytes: &bytes::Bytes) -> RecordBatch {
    let mut reader = arrow_ipc::reader::StreamReader::try_new(Cursor::new(bytes.as_ref()), None)
        .expect("valid IPC stream");
    reader.next().expect("one batch").expect("no error")
}

/// Setup tracing for tests.
pub fn setup_tracing() {
    SETUP.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_writer(std::io::stdout)
            .with_env_filter(
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
            )
            .try_init()
            .ok();
    });
}

/// Test callback implementation for tracking acknowledgments and errors.
#[derive(Debug)]
pub struct TestCallback {
    acks: Arc<Mutex<Vec<i64>>>,
    errors: Arc<Mutex<Vec<(i64, String)>>>,
}

impl TestCallback {
    pub fn new() -> Self {
        Self {
            acks: Arc::new(Mutex::new(Vec::new())),
            errors: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn get_acks(&self) -> Vec<i64> {
        self.acks.lock().unwrap().clone()
    }

    pub fn get_errors(&self) -> Vec<(i64, String)> {
        self.errors.lock().unwrap().clone()
    }
}

impl AckCallback for TestCallback {
    fn on_ack(&self, offset_id: i64) {
        self.acks.lock().unwrap().push(offset_id);
    }

    fn on_error(&self, offset_id: i64, error_message: &str) {
        self.errors
            .lock()
            .unwrap()
            .push((offset_id, error_message.to_string()));
    }
}
