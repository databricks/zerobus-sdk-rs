//! Dynamic protobuf encoding: turn JSON records into protobuf wire bytes against
//! a descriptor obtained at runtime, with no compiled `.proto` types.
//!
//! This is the keystone of the SDK's *dynamic protobuf* path. A user who has a
//! [`prost_types::DescriptorProto`] — fetched from Unity Catalog (see
//! [`schema::descriptor_from_uc`](crate::schema::descriptor_from_uc)) or built in
//! code (see [`schema::TableDescriptorBuilder`](crate::schema::TableDescriptorBuilder))
//! — can encode records supplied as JSON and ingest them on the efficient proto
//! path, without generating Rust types from a `.proto` file.
//!
//! # Canonical record contract
//!
//! Across all Zerobus SDKs the canonical way to supply a record dynamically is a
//! **JSON value encoded against the descriptor**. The C/FFI path established this
//! contract; this module is its native-Rust counterpart. The SDK turns each JSON
//! record into protobuf wire bytes client-side and ships those bytes on the proto
//! stream — the validated, compact path the typed SDK already optimizes for.
//!
//! # Value-encoding rules
//!
//! Records follow protobuf's JSON mapping. A few Databricks column types need
//! shaping in the JSON you provide:
//!
//! - `DATE` / `TIMESTAMP` / `TIMESTAMP_NTZ`: integers (days / microseconds since
//!   the Unix epoch), **not** ISO-8601 strings.
//! - `BINARY`: a base64-encoded string, not a JSON array of bytes.
//! - `DECIMAL`: a string (e.g. `"123.45"`) to preserve precision/scale.
//! - `VARIANT`: a JSON-encoded string (a string whose contents are the variant's
//!   JSON).
//! - `ARRAY` / `MAP` / `STRUCT`: JSON array / object / object respectively.
//! - `LONG` / `BIGINT` above 2^53: pass as a JSON string, otherwise the value
//!   loses precision as a JSON number.
//!
//! Unknown keys are ignored, so records that carry extra non-column metadata are
//! accepted. Presence is enforced for top-level non-nullable scalar and struct
//! columns (proto2 `required`): a record omitting one is rejected. `ARRAY`/`MAP`
//! columns map to `repeated`, which has no presence, so an omitted one encodes as
//! empty rather than failing.
//!
//! # Example
//!
//! ```no_run
//! use databricks_zerobus_ingest_sdk::dynamic::DynamicProtoEncoder;
//! use databricks_zerobus_ingest_sdk::schema::TableDescriptorBuilder;
//!
//! # fn main() -> Result<(), databricks_zerobus_ingest_sdk::ZerobusError> {
//! let descriptor = TableDescriptorBuilder::new("orders")
//!     .column("id", "BIGINT", false)
//!     .column("name", "STRING", true)
//!     .build()?;
//!
//! let encoder = DynamicProtoEncoder::new(&descriptor)?;
//! let bytes = encoder.encode(r#"{"id": 1, "name": "Alice"}"#)?;
//! // `bytes` is protobuf wire format; ingest it on a proto stream.
//! # Ok(())
//! # }
//! ```

use prost::Message as _;
use prost_reflect::{
    Cardinality, DescriptorPool, DeserializeOptions, DynamicMessage, MessageDescriptor,
};
use prost_types::DescriptorProto;
use serde::Serialize;

use crate::{ZerobusError, ZerobusResult, ZerobusStream};

/// Synthetic file name for the single-message descriptor pool. Has no package,
/// so the message's fully-qualified name is its bare name.
const SYNTHETIC_FILE_NAME: &str = "zerobus_dynamic.proto";

/// Encodes JSON records into protobuf wire bytes against a runtime descriptor.
///
/// Build one from a [`DescriptorProto`] with [`DynamicProtoEncoder::new`], then
/// reuse it to encode many records — constructing it parses the descriptor into a
/// reflection pool, so build it once per stream and keep it around rather than
/// per record.
///
/// See the [module docs](crate::dynamic) for the JSON value-encoding rules.
///
/// Cloning is cheap: the reflection handle is reference-counted internally.
#[derive(Debug, Clone)]
pub struct DynamicProtoEncoder {
    /// The descriptor this encoder validates against. Kept so callers can recover
    /// the exact bytes the stream was (or should be) created with.
    descriptor: DescriptorProto,
    /// Reflection handle used to deserialize JSON into a dynamic message.
    message: MessageDescriptor,
}

impl DynamicProtoEncoder {
    /// Build an encoder for the given message descriptor.
    ///
    /// The descriptor is typically obtained from
    /// [`schema::descriptor_from_uc`](crate::schema::descriptor_from_uc) (Unity
    /// Catalog) or [`schema::TableDescriptorBuilder`](crate::schema::TableDescriptorBuilder)
    /// (in code). It must be the same descriptor the stream is created with, so
    /// the wire bytes this encoder produces match what the server validates.
    ///
    /// # Errors
    ///
    /// [`ZerobusError::InvalidArgument`] if the descriptor cannot be assembled
    /// into a reflection pool (e.g. it references a nested type it does not
    /// define).
    pub fn new(descriptor: &DescriptorProto) -> ZerobusResult<Self> {
        let message_name = descriptor.name().to_string();
        if message_name.is_empty() {
            return Err(ZerobusError::InvalidArgument(
                "descriptor has no message name".to_string(),
            ));
        }

        let file = prost_types::FileDescriptorProto {
            name: Some(SYNTHETIC_FILE_NAME.to_string()),
            message_type: vec![descriptor.clone()],
            ..Default::default()
        };
        let mut pool = DescriptorPool::new();
        pool.add_file_descriptor_proto(file).map_err(|e| {
            ZerobusError::InvalidArgument(format!("failed to build descriptor pool: {e}"))
        })?;
        // No package on the synthetic file, so the fully-qualified name is the
        // bare message name.
        let message = pool.get_message_by_name(&message_name).ok_or_else(|| {
            ZerobusError::InvalidArgument(format!(
                "message '{message_name}' not found in descriptor pool"
            ))
        })?;

        Ok(Self {
            descriptor: descriptor.clone(),
            message,
        })
    }

    /// The descriptor this encoder was built from.
    ///
    /// Use [`descriptor_bytes`](Self::descriptor_bytes) when you need the exact
    /// serialized form to create a stream with.
    pub fn descriptor(&self) -> &DescriptorProto {
        &self.descriptor
    }

    /// The serialized `DescriptorProto` bytes, byte-identical to what the encoder
    /// validates against. Pass these when creating the stream so client-side
    /// encoding and server-side validation agree.
    pub fn descriptor_bytes(&self) -> Vec<u8> {
        self.descriptor.encode_to_vec()
    }

    /// Encode a single JSON record (as a string) into protobuf wire bytes.
    ///
    /// # Errors
    ///
    /// [`ZerobusError::InvalidArgument`] if the JSON is malformed, has trailing
    /// content, does not match the descriptor's field types, or omits a required
    /// (non-nullable top-level scalar/struct) column.
    pub fn encode(&self, record_json: &str) -> ZerobusResult<Vec<u8>> {
        let mut deserializer = serde_json::Deserializer::from_str(record_json);
        let message = self.deserialize(&mut deserializer)?;
        // Reject extra bytes after the first JSON value rather than silently
        // ignoring them.
        deserializer.end().map_err(|e| {
            ZerobusError::InvalidArgument(format!(
                "unexpected trailing content in record JSON: {e}"
            ))
        })?;
        self.finish(message)
    }

    /// Encode a [`serde_json::Value`] into protobuf wire bytes.
    ///
    /// Useful when a record is already assembled as a JSON value (e.g. built with
    /// the `serde_json::json!` macro) so it need not be re-serialized to a string.
    ///
    /// # Errors
    ///
    /// See [`encode`](Self::encode).
    pub fn encode_value(&self, record: &serde_json::Value) -> ZerobusResult<Vec<u8>> {
        let message = self.deserialize(record)?;
        self.finish(message)
    }

    /// Encode any [`serde::Serialize`] type into protobuf wire bytes by first
    /// converting it to JSON.
    ///
    /// # Errors
    ///
    /// [`ZerobusError::InvalidArgument`] if serialization to JSON fails or the
    /// resulting record does not match the descriptor (see [`encode`](Self::encode)).
    pub fn encode_record<T: Serialize>(&self, record: &T) -> ZerobusResult<Vec<u8>> {
        let value = serde_json::to_value(record).map_err(|e| {
            ZerobusError::InvalidArgument(format!("failed to serialize record to JSON: {e}"))
        })?;
        self.encode_value(&value)
    }

    /// Deserialize JSON from any deserializer into a dynamic message, ignoring
    /// unknown keys (records often carry extra non-column metadata).
    fn deserialize<'de, D>(&self, deserializer: D) -> ZerobusResult<DynamicMessage>
    where
        D: serde::Deserializer<'de>,
    {
        let options = DeserializeOptions::new().deny_unknown_fields(false);
        DynamicMessage::deserialize_with_options(self.message.clone(), deserializer, &options)
            .map_err(|e| ZerobusError::InvalidArgument(format!("failed to encode record: {e}")))
    }

    /// Enforce proto2 `required` presence then serialize to wire bytes.
    ///
    /// `prost-reflect` does not enforce presence on encode, so a missing
    /// top-level required field is rejected here rather than emitting wire bytes
    /// the server would reject. (`ARRAY`/`MAP` columns are `repeated`, which has
    /// no presence; nested struct fields are not walked.)
    fn finish(&self, message: DynamicMessage) -> ZerobusResult<Vec<u8>> {
        let missing: Vec<String> = self
            .message
            .fields()
            .filter(|f| matches!(f.cardinality(), Cardinality::Required) && !message.has_field(f))
            .map(|f| f.name().to_string())
            .collect();
        if !missing.is_empty() {
            return Err(ZerobusError::InvalidArgument(format!(
                "record missing required field(s): {}",
                missing.join(", ")
            )));
        }
        Ok(message.encode_to_vec())
    }
}

impl ZerobusStream {
    /// Build a [`DynamicProtoEncoder`] for this stream's protobuf descriptor.
    ///
    /// Returns an encoder bound to the same descriptor the stream was created
    /// with — whether supplied via
    /// [`compiled_proto`](crate::StreamBuilder::compiled_proto) or fetched via
    /// [`proto_from_uc`](crate::StreamBuilder::proto_from_uc) — so JSON records
    /// encoded by it are guaranteed to match what the server validates.
    ///
    /// Constructing an encoder parses the descriptor, so call this once and reuse
    /// the returned encoder for the lifetime of the stream.
    ///
    /// # Errors
    ///
    /// [`ZerobusError::InvalidArgument`] if the stream has no descriptor (a JSON
    /// stream created with [`json`](crate::StreamBuilder::json)) or the descriptor
    /// cannot be assembled.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::ZerobusStream;
    /// # async fn example(stream: ZerobusStream) -> Result<(), databricks_zerobus_ingest_sdk::ZerobusError> {
    /// let encoder = stream.encoder()?;
    /// let bytes = encoder.encode(r#"{"id": 1, "name": "Alice"}"#)?;
    /// let offset = stream.ingest_record_offset(bytes).await?;
    /// stream.wait_for_offset(offset).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn encoder(&self) -> ZerobusResult<DynamicProtoEncoder> {
        let descriptor = self
            .table_properties
            .descriptor_proto
            .as_ref()
            .ok_or_else(|| {
                ZerobusError::InvalidArgument(
                    "stream has no protobuf descriptor; encoder() requires a proto stream created \
                     via compiled_proto() or proto_from_uc()"
                        .to_string(),
                )
            })?;
        DynamicProtoEncoder::new(descriptor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::TableDescriptorBuilder;
    use prost_reflect::Value;

    /// Build the air-quality descriptor used across these tests.
    fn air_quality_descriptor() -> DescriptorProto {
        TableDescriptorBuilder::new("air_quality")
            .column("device_name", "STRING", true)
            .column("temp", "INT", true)
            .column("humidity", "INT", true)
            .build()
            .expect("descriptor builds")
    }

    /// Decode encoded bytes back through the same descriptor for assertions.
    fn decode(encoder: &DynamicProtoEncoder, bytes: &[u8]) -> DynamicMessage {
        DynamicMessage::decode(encoder.message.clone(), bytes).expect("decodes")
    }

    #[test]
    fn encode_round_trips_scalar_fields() {
        let descriptor = air_quality_descriptor();
        let encoder = DynamicProtoEncoder::new(&descriptor).unwrap();

        let bytes = encoder
            .encode(r#"{"device_name": "sensor-1", "temp": 22, "humidity": 65}"#)
            .unwrap();

        let decoded = decode(&encoder, &bytes);
        assert_eq!(
            decoded
                .get_field_by_name("device_name")
                .unwrap()
                .as_str()
                .unwrap(),
            "sensor-1"
        );
        assert_eq!(
            decoded.get_field_by_name("temp").unwrap().as_i32().unwrap(),
            22
        );
        assert_eq!(
            decoded
                .get_field_by_name("humidity")
                .unwrap()
                .as_i32()
                .unwrap(),
            65
        );
    }

    #[test]
    fn encode_value_matches_encode_string() {
        let descriptor = air_quality_descriptor();
        let encoder = DynamicProtoEncoder::new(&descriptor).unwrap();

        let from_str = encoder
            .encode(r#"{"device_name": "s", "temp": 1, "humidity": 2}"#)
            .unwrap();
        let from_value = encoder
            .encode_value(&serde_json::json!({
                "device_name": "s",
                "temp": 1,
                "humidity": 2
            }))
            .unwrap();
        assert_eq!(from_str, from_value);
    }

    #[test]
    fn encode_record_accepts_serialize_types() {
        #[derive(serde::Serialize)]
        struct Reading {
            device_name: String,
            temp: i32,
            humidity: i32,
        }
        let descriptor = air_quality_descriptor();
        let encoder = DynamicProtoEncoder::new(&descriptor).unwrap();

        let bytes = encoder
            .encode_record(&Reading {
                device_name: "s".into(),
                temp: 7,
                humidity: 8,
            })
            .unwrap();
        let decoded = decode(&encoder, &bytes);
        assert_eq!(
            decoded.get_field_by_name("temp").unwrap().as_i32().unwrap(),
            7
        );
    }

    #[test]
    fn unknown_keys_are_ignored() {
        let descriptor = air_quality_descriptor();
        let encoder = DynamicProtoEncoder::new(&descriptor).unwrap();
        // `source` is not a column; it must be ignored rather than rejected.
        let bytes = encoder
            .encode(r#"{"device_name": "s", "temp": 1, "humidity": 2, "source": "kafka"}"#)
            .unwrap();
        let decoded = decode(&encoder, &bytes);
        assert_eq!(
            decoded
                .get_field_by_name("device_name")
                .unwrap()
                .as_str()
                .unwrap(),
            "s"
        );
    }

    #[test]
    fn omitted_optional_field_encodes() {
        let descriptor = air_quality_descriptor();
        let encoder = DynamicProtoEncoder::new(&descriptor).unwrap();
        // All columns are nullable, so omitting one is fine.
        let bytes = encoder.encode(r#"{"device_name": "s"}"#).unwrap();
        let decoded = decode(&encoder, &bytes);
        assert!(!decoded.has_field_by_name("temp"));
    }

    #[test]
    fn missing_required_field_is_rejected() {
        // A non-nullable scalar column is proto2 `required`; omitting it fails.
        let descriptor = TableDescriptorBuilder::new("t")
            .column("id", "BIGINT", false)
            .column("name", "STRING", true)
            .build()
            .unwrap();
        let encoder = DynamicProtoEncoder::new(&descriptor).unwrap();

        let err = encoder.encode(r#"{"name": "x"}"#).unwrap_err();
        match err {
            ZerobusError::InvalidArgument(msg) => {
                assert!(msg.contains("missing required field"), "got: {msg}");
                assert!(msg.contains("id"), "got: {msg}");
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn malformed_json_is_rejected() {
        let descriptor = air_quality_descriptor();
        let encoder = DynamicProtoEncoder::new(&descriptor).unwrap();
        assert!(encoder.encode(r#"{"device_name": "#).is_err());
    }

    #[test]
    fn trailing_content_is_rejected() {
        let descriptor = air_quality_descriptor();
        let encoder = DynamicProtoEncoder::new(&descriptor).unwrap();
        let err = encoder
            .encode(r#"{"device_name": "s"} {"device_name": "t"}"#)
            .unwrap_err();
        match err {
            ZerobusError::InvalidArgument(msg) => {
                assert!(msg.contains("trailing content"), "got: {msg}")
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn wrong_field_type_is_rejected() {
        let descriptor = air_quality_descriptor();
        let encoder = DynamicProtoEncoder::new(&descriptor).unwrap();
        // `temp` is an int; a non-numeric string is not coercible.
        assert!(encoder
            .encode(r#"{"device_name": "s", "temp": "hot"}"#)
            .is_err());
    }

    #[test]
    fn descriptor_bytes_round_trip_to_same_descriptor() {
        let descriptor = air_quality_descriptor();
        let encoder = DynamicProtoEncoder::new(&descriptor).unwrap();
        let bytes = encoder.descriptor_bytes();
        let decoded = DescriptorProto::decode(bytes.as_slice()).unwrap();
        assert_eq!(&decoded, encoder.descriptor());
    }

    #[test]
    fn rejects_descriptor_without_name() {
        let err = DynamicProtoEncoder::new(&DescriptorProto::default()).unwrap_err();
        assert!(matches!(err, ZerobusError::InvalidArgument(_)));
    }

    #[test]
    fn encodes_repeated_and_struct_via_json() {
        // ARRAY<long> -> repeated; STRUCT -> nested message. Both arrive as JSON.
        let descriptor = TableDescriptorBuilder::new("evt")
            .column("id", "BIGINT", true)
            .complex_column(
                "tags",
                "ARRAY",
                r#"{"type":"array","elementType":"long","containsNull":true}"#,
                true,
            )
            .complex_column(
                "addr",
                "STRUCT",
                r#"{"type":"struct","fields":[
                    {"name":"zip","type":"integer","nullable":true,"metadata":{}}
                ]}"#,
                true,
            )
            .build()
            .unwrap();
        let encoder = DynamicProtoEncoder::new(&descriptor).unwrap();

        let bytes = encoder
            .encode(r#"{"id": 1, "tags": [10, 20], "addr": {"zip": 94105}}"#)
            .unwrap();
        let decoded = decode(&encoder, &bytes);
        match decoded.get_field_by_name("tags").unwrap().as_ref() {
            Value::List(items) => assert_eq!(items.len(), 2),
            other => panic!("expected list, got {other:?}"),
        }
    }
}
