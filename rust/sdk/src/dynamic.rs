//! Dynamic protobuf encoding: turn JSON records into protobuf wire bytes against
//! a descriptor obtained at runtime, with no compiled `.proto` types.
//!
//! Build a [`DescriptorProto`] from Unity Catalog (see
//! [`schema::descriptor_from_uc`](crate::schema::descriptor_from_uc)) or in code
//! (see [`schema::TableDescriptorBuilder`](crate::schema::TableDescriptorBuilder)),
//! then encode records supplied as JSON and ingest them on the proto path. This
//! is the canonical dynamic-record contract across the Zerobus SDKs: a JSON value
//! encoded against the descriptor, turned into protobuf bytes client-side.
//!
//! Records follow protobuf's JSON mapping; a few Databricks column types need
//! shaping in the JSON you supply: `DATE`/`TIMESTAMP`/`TIMESTAMP_NTZ` as integers
//! (days / micros since epoch), `BINARY` as base64, `DECIMAL` as a string,
//! `VARIANT` as a JSON-encoded string, and `LONG` above 2^53 as a string. Unknown
//! keys are ignored; a missing non-nullable top-level column (proto2 `required`)
//! is rejected.
//!
//! ```no_run
//! use databricks_zerobus_ingest_sdk::dynamic::DynamicProtoEncoder;
//! use databricks_zerobus_ingest_sdk::schema::TableDescriptorBuilder;
//! # fn main() -> Result<(), databricks_zerobus_ingest_sdk::ZerobusError> {
//! let descriptor = TableDescriptorBuilder::new("orders")
//!     .column("id", "BIGINT", false)
//!     .column("name", "STRING", true)
//!     .build()?;
//! let encoder = DynamicProtoEncoder::new(&descriptor)?;
//! let bytes = encoder.encode(r#"{"id": 1, "name": "Alice"}"#)?;
//! # Ok(())
//! # }
//! ```

use prost::Message as _;
use prost_reflect::{
    Cardinality, DescriptorPool, DeserializeOptions, DynamicMessage, MessageDescriptor,
};
use prost_types::DescriptorProto;

use crate::{ZerobusError, ZerobusResult, ZerobusStream};

/// Encodes JSON records into protobuf wire bytes against a runtime descriptor.
///
/// Build once with [`new`](Self::new) and reuse — construction parses the
/// descriptor into a reflection pool. Cloning is cheap (the handle is
/// reference-counted). See the [module docs](crate::dynamic) for the JSON
/// value-encoding rules.
#[derive(Debug, Clone)]
pub struct DynamicProtoEncoder {
    message: MessageDescriptor,
}

impl DynamicProtoEncoder {
    /// Build an encoder for the given message descriptor. It must be the same
    /// descriptor the stream is created with, so the bytes it produces match what
    /// the server validates.
    ///
    /// # Errors
    ///
    /// [`ZerobusError::InvalidArgument`] if the descriptor has no name or cannot
    /// be assembled into a reflection pool.
    pub fn new(descriptor: &DescriptorProto) -> ZerobusResult<Self> {
        let message_name = descriptor.name().to_string();
        if message_name.is_empty() {
            return Err(ZerobusError::InvalidArgument(
                "descriptor has no message name".to_string(),
            ));
        }
        // Wrap the message in a synthetic, package-less file so its
        // fully-qualified name is just the bare message name.
        let file = prost_types::FileDescriptorProto {
            name: Some("zerobus_dynamic.proto".to_string()),
            message_type: vec![descriptor.clone()],
            ..Default::default()
        };
        let mut pool = DescriptorPool::new();
        pool.add_file_descriptor_proto(file).map_err(|e| {
            ZerobusError::InvalidArgument(format!("failed to build descriptor pool: {e}"))
        })?;
        let message = pool.get_message_by_name(&message_name).ok_or_else(|| {
            ZerobusError::InvalidArgument(format!("message '{message_name}' not found"))
        })?;
        Ok(Self { message })
    }

    /// Encode a JSON record (a string) into protobuf wire bytes.
    ///
    /// # Errors
    ///
    /// [`ZerobusError::InvalidArgument`] if the JSON is malformed, has trailing
    /// content, mismatches a field type, or omits a required column.
    pub fn encode(&self, record_json: &str) -> ZerobusResult<Vec<u8>> {
        let mut de = serde_json::Deserializer::from_str(record_json);
        let message = self.deserialize(&mut de)?;
        de.end().map_err(|e| {
            ZerobusError::InvalidArgument(format!(
                "unexpected trailing content in record JSON: {e}"
            ))
        })?;
        self.finish(message)
    }

    /// Encode a [`serde_json::Value`] into protobuf wire bytes, avoiding a
    /// re-serialization when the record is already a JSON value. See
    /// [`encode`](Self::encode) for the error conditions.
    pub fn encode_value(&self, record: &serde_json::Value) -> ZerobusResult<Vec<u8>> {
        let message = self.deserialize(record)?;
        self.finish(message)
    }

    /// Deserialize JSON into a dynamic message, ignoring unknown keys (records
    /// often carry extra non-column metadata).
    fn deserialize<'de, D: serde::Deserializer<'de>>(
        &self,
        deserializer: D,
    ) -> ZerobusResult<DynamicMessage> {
        let options = DeserializeOptions::new().deny_unknown_fields(false);
        DynamicMessage::deserialize_with_options(self.message.clone(), deserializer, &options)
            .map_err(|e| ZerobusError::InvalidArgument(format!("failed to encode record: {e}")))
    }

    /// Enforce proto2 `required` presence (prost-reflect does not on encode) then
    /// serialize. `repeated` columns (`ARRAY`/`MAP`) have no presence and nested
    /// struct fields are not walked.
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
    /// Build a [`DynamicProtoEncoder`] bound to this stream's protobuf descriptor
    /// — whether supplied via [`compiled_proto`](crate::StreamBuilder::compiled_proto)
    /// or fetched via [`proto_from_uc`](crate::StreamBuilder::proto_from_uc) — so
    /// encoded records are guaranteed to match what the server validates. Build it
    /// once and reuse.
    ///
    /// # Errors
    ///
    /// [`ZerobusError::InvalidArgument`] if the stream has no descriptor (a JSON
    /// stream) or the descriptor cannot be assembled.
    ///
    /// ```no_run
    /// # use databricks_zerobus_ingest_sdk::ZerobusStream;
    /// # async fn example(stream: ZerobusStream) -> Result<(), databricks_zerobus_ingest_sdk::ZerobusError> {
    /// let encoder = stream.encoder()?;
    /// let offset = stream.ingest_record_offset(encoder.encode(r#"{"id": 1}"#)?).await?;
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

    /// Build the air-quality descriptor used across these tests (all nullable).
    fn air_quality_descriptor() -> DescriptorProto {
        TableDescriptorBuilder::new("air_quality")
            .column("device_name", "STRING", true)
            .column("temp", "INT", true)
            .column("humidity", "INT", true)
            .build()
            .expect("descriptor builds")
    }

    fn decode(encoder: &DynamicProtoEncoder, bytes: &[u8]) -> DynamicMessage {
        DynamicMessage::decode(encoder.message.clone(), bytes).expect("decodes")
    }

    #[test]
    fn encode_round_trips_scalar_fields() {
        let encoder = DynamicProtoEncoder::new(&air_quality_descriptor()).unwrap();
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
    }

    #[test]
    fn encode_value_matches_encode_string() {
        let encoder = DynamicProtoEncoder::new(&air_quality_descriptor()).unwrap();
        let from_str = encoder
            .encode(r#"{"device_name": "s", "temp": 1}"#)
            .unwrap();
        let from_value = encoder
            .encode_value(&serde_json::json!({"device_name": "s", "temp": 1}))
            .unwrap();
        assert_eq!(from_str, from_value);
    }

    #[test]
    fn unknown_keys_are_ignored() {
        let encoder = DynamicProtoEncoder::new(&air_quality_descriptor()).unwrap();
        // `source` is not a column; it must be ignored rather than rejected.
        let bytes = encoder
            .encode(r#"{"device_name": "s", "temp": 1, "source": "kafka"}"#)
            .unwrap();
        assert_eq!(
            decode(&encoder, &bytes)
                .get_field_by_name("device_name")
                .unwrap()
                .as_str()
                .unwrap(),
            "s"
        );
    }

    #[test]
    fn omitted_optional_field_encodes() {
        let encoder = DynamicProtoEncoder::new(&air_quality_descriptor()).unwrap();
        let bytes = encoder.encode(r#"{"device_name": "s"}"#).unwrap();
        assert!(!decode(&encoder, &bytes).has_field_by_name("temp"));
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
        match encoder.encode(r#"{"name": "x"}"#).unwrap_err() {
            ZerobusError::InvalidArgument(msg) => {
                assert!(
                    msg.contains("missing required field") && msg.contains("id"),
                    "got: {msg}"
                );
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn rejects_invalid_input() {
        let encoder = DynamicProtoEncoder::new(&air_quality_descriptor()).unwrap();
        // Malformed JSON.
        assert!(encoder.encode(r#"{"device_name": "#).is_err());
        // Trailing content after the first value.
        assert!(encoder.encode(r#"{"device_name": "s"} {}"#).is_err());
        // Wrong field type (`temp` is an int; a non-numeric string is not coercible).
        assert!(encoder.encode(r#"{"temp": "hot"}"#).is_err());
    }

    #[test]
    fn rejects_descriptor_without_name() {
        let err = DynamicProtoEncoder::new(&DescriptorProto::default()).unwrap_err();
        assert!(matches!(err, ZerobusError::InvalidArgument(_)));
    }

    #[test]
    fn encoder_output_matches_compiled_prost_message() {
        // The wire output must byte-match a compiled prost message with the same
        // schema — the core "bytes match what the server validates" guarantee,
        // checked against an independent encoder (prost codegen, not our pool).
        #[derive(Clone, PartialEq, prost::Message)]
        struct AirQuality {
            #[prost(string, optional, tag = "1")]
            device_name: Option<String>,
            #[prost(int32, optional, tag = "2")]
            temp: Option<i32>,
            #[prost(int32, optional, tag = "3")]
            humidity: Option<i32>,
        }

        let encoder = DynamicProtoEncoder::new(&air_quality_descriptor()).unwrap();
        let dynamic = encoder
            .encode(r#"{"device_name": "s", "temp": 22, "humidity": 65}"#)
            .unwrap();
        let compiled = AirQuality {
            device_name: Some("s".into()),
            temp: Some(22),
            humidity: Some(65),
        }
        .encode_to_vec();
        assert_eq!(dynamic, compiled);
    }

    #[test]
    fn nested_required_field_is_not_validated_client_side() {
        // finish() enforces presence only for top-level required columns; a
        // required field nested in a STRUCT is delegated to the server. This pins
        // that documented boundary: `addr` is present but its required child
        // `zip` is omitted, and the record still encodes here.
        let descriptor = TableDescriptorBuilder::new("m")
            .complex_column(
                "addr",
                "STRUCT",
                r#"{"type":"struct","fields":[
                    {"name":"zip","type":"integer","nullable":false,"metadata":{}}
                ]}"#,
                true,
            )
            .build()
            .unwrap();
        let encoder = DynamicProtoEncoder::new(&descriptor).unwrap();
        assert!(encoder.encode(r#"{"addr": {}}"#).is_ok());
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
        match decode(&encoder, &bytes)
            .get_field_by_name("tags")
            .unwrap()
            .as_ref()
        {
            Value::List(items) => assert_eq!(items.len(), 2),
            other => panic!("expected list, got {other:?}"),
        }
    }
}
