//! Native dynamic-protobuf record support.
//!
//! For tables whose schema is known only at runtime, so there is no compiled
//! `prost::Message`. Pass a resolved [`MessageDescriptor`] to
//! [`StreamBuilder::dynamic_proto`](crate::StreamBuilder::dynamic_proto), then
//! build records field-by-field with [`DynamicRecord`] (obtain the same
//! descriptor back from
//! [`ZerobusStream::message_descriptor`](crate::ZerobusStream::message_descriptor)).
//!
//! Obtain the [`MessageDescriptor`] from [`message_descriptor`], which resolves a
//! [`prost_types::DescriptorProto`] (built with
//! [`crate::schema::descriptor_from_uc_columns`] or fetched from Unity Catalog),
//! or from your own [`prost_reflect::DescriptorPool`].
//!
//! Ingest in a loop, then `flush()` once — never wait per record.
//!
//! ```no_run
//! # use databricks_zerobus_ingest_sdk::{ProtoBytes, DynamicRecord, ZerobusStream};
//! # async fn example(stream: &ZerobusStream) -> Result<(), Box<dyn std::error::Error>> {
//! let descriptor = stream.message_descriptor()?;
//! for i in 0..1_000i64 {
//!     let mut record = DynamicRecord::new(descriptor.clone());
//!     record.set("id", i)?.set("name", "widget")?;
//!     stream.ingest_record_offset(ProtoBytes(record.encode()?)).await?; // queue only — do NOT wait here
//! }
//! stream.flush().await?; // wait once for all pending acks
//! # Ok(())
//! # }
//! ```

use std::fmt::Write as _;

use prost::Message as _;
use prost_reflect::{Cardinality, DescriptorPool, Kind, MapKey, ReflectMessage as _};

use crate::{ZerobusError, ZerobusResult};

pub use prost_reflect::{DynamicMessage, MessageDescriptor, Value};

/// Resolve a [`MessageDescriptor`] from a [`prost_types::DescriptorProto`], to
/// pass to [`StreamBuilder::dynamic_proto`](crate::StreamBuilder::dynamic_proto).
///
/// The descriptor is registered in a fresh single-file pool, so it must be
/// self-contained — it must not reference types defined in other files.
///
/// Resolving builds a descriptor pool; do it once per schema and reuse the result
/// (it is a cheap, Arc-backed clone) for both the builder and every record.
///
/// # Errors
///
/// [`ZerobusError::InvalidArgument`] if the descriptor cannot be registered in a
/// descriptor pool.
pub fn message_descriptor(
    descriptor: &prost_types::DescriptorProto,
) -> ZerobusResult<MessageDescriptor> {
    // An unnamed message can't be looked up by name; give it a fallback.
    let mut descriptor = descriptor.clone();
    let message_name = match descriptor.name.as_deref() {
        Some(name) if !name.is_empty() => name.to_string(),
        _ => {
            let name = "ZerobusDynamicMessage".to_string();
            descriptor.name = Some(name.clone());
            name
        }
    };

    let file = prost_types::FileDescriptorProto {
        name: Some("zerobus_dynamic.proto".to_string()),
        message_type: vec![descriptor],
        ..Default::default()
    };

    let mut pool = DescriptorPool::new();
    pool.add_file_descriptor_proto(file)
        .map_err(|e| ZerobusError::InvalidArgument(format!("invalid protobuf descriptor: {e}")))?;
    // No package on the synthetic file, so the FQ name is the bare message name.
    pool.get_message_by_name(&message_name).ok_or_else(|| {
        ZerobusError::InvalidArgument(format!(
            "message '{message_name}' not found in descriptor pool"
        ))
    })
}

/// Full paths of any proto2 `required` fields absent from `message`, descending
/// into nested messages, list elements, and map values. Empty if none are
/// missing. `prost-reflect` follows proto3 semantics and does not enforce this
/// on encode, so callers check it before sending records the server would reject.
pub fn missing_required_fields(message: &DynamicMessage) -> Vec<String> {
    let mut missing = Vec::new();
    let mut path = String::new();
    collect_missing_required_fields(message, &mut path, &mut missing);
    missing
}

/// Recurse over `message`'s fields, appending each field's segment to `path`
/// (truncated on return) and recording any missing required field.
fn collect_missing_required_fields(
    message: &DynamicMessage,
    path: &mut String,
    missing: &mut Vec<String>,
) {
    for field in message.descriptor().fields() {
        let base_len = path.len();
        if !path.is_empty() {
            path.push('.');
        }
        path.push_str(field.name());

        if matches!(field.cardinality(), Cardinality::Required) && !message.has_field(&field) {
            missing.push(path.clone());
        } else if matches!(field.kind(), Kind::Message(_)) && message.has_field(&field) {
            let value = message.get_field(&field);
            descend_into_messages(&value, path, missing);
        }

        path.truncate(base_len);
    }
}

/// Recurse into every [`DynamicMessage`] reachable from `value` — itself, list
/// elements, or map values — appending `[i]` / `[key]` path segments.
fn descend_into_messages(value: &Value, path: &mut String, missing: &mut Vec<String>) {
    match value {
        Value::Message(m) => collect_missing_required_fields(m, path, missing),
        Value::List(items) => {
            for (i, item) in items.iter().enumerate() {
                if let Value::Message(m) = item {
                    let base_len = path.len();
                    let _ = write!(path, "[{i}]");
                    collect_missing_required_fields(m, path, missing);
                    path.truncate(base_len);
                }
            }
        }
        Value::Map(entries) => {
            for (key, val) in entries {
                if let Value::Message(m) = val {
                    let base_len = path.len();
                    path.push('[');
                    append_map_key(path, key);
                    path.push(']');
                    collect_missing_required_fields(m, path, missing);
                    path.truncate(base_len);
                }
            }
        }
        _ => {}
    }
}

/// Append a protobuf map key to a missing-field path in place.
fn append_map_key(path: &mut String, key: &MapKey) {
    match key {
        MapKey::String(s) => path.push_str(s),
        MapKey::Bool(b) => {
            let _ = write!(path, "{b}");
        }
        MapKey::I32(i) => {
            let _ = write!(path, "{i}");
        }
        MapKey::I64(i) => {
            let _ = write!(path, "{i}");
        }
        MapKey::U32(i) => {
            let _ = write!(path, "{i}");
        }
        MapKey::U64(i) => {
            let _ = write!(path, "{i}");
        }
    }
}

/// Converts a Rust type into a protobuf [`Value`] for [`DynamicRecord::set`].
///
/// The type must match the proto field: `int64` needs an [`i64`], `int32` an
/// [`i32`], and so on. Covers the scalars, `String`/`&str`, and `Vec<u8>`; for
/// enums, messages, lists, or maps, build the [`Value`] and pass it directly.
pub trait IntoDynamicValue {
    fn into_value(self) -> Value;
}

impl IntoDynamicValue for Value {
    fn into_value(self) -> Value {
        self
    }
}

macro_rules! impl_into_dynamic_value {
    ($($ty:ty => $variant:ident),* $(,)?) => {
        $(
            impl IntoDynamicValue for $ty {
                fn into_value(self) -> Value {
                    Value::$variant(self)
                }
            }
        )*
    };
}

impl_into_dynamic_value! {
    bool => Bool,
    i32 => I32,
    i64 => I64,
    u32 => U32,
    u64 => U64,
    f32 => F32,
    f64 => F64,
    String => String,
}

impl IntoDynamicValue for &str {
    fn into_value(self) -> Value {
        Value::String(self.to_string())
    }
}

impl IntoDynamicValue for Vec<u8> {
    fn into_value(self) -> Value {
        Value::Bytes(self.into())
    }
}

/// A protobuf record built field-by-field against a runtime [`MessageDescriptor`],
/// then [`encode`](Self::encode)d to wire bytes and ingested.
///
/// ```no_run
/// # use databricks_zerobus_ingest_sdk::{ProtoBytes, DynamicRecord, ZerobusStream};
/// # async fn example(stream: &ZerobusStream) -> Result<(), Box<dyn std::error::Error>> {
/// let descriptor = stream.message_descriptor()?;
/// let mut record = DynamicRecord::new(descriptor);
/// record.set("id", 42i64)?.set("name", "widget")?;
/// let _offset = stream.ingest_record_offset(ProtoBytes(record.encode()?)).await?; // queue only
/// stream.flush().await?; // wait once for all pending acks
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct DynamicRecord {
    message: DynamicMessage,
}

impl DynamicRecord {
    /// Create an empty record bound to `descriptor`.
    ///
    /// Use the descriptor for the same schema passed to
    /// [`dynamic_proto`](crate::StreamBuilder::dynamic_proto), so field numbers
    /// match what the server expects.
    pub fn new(descriptor: MessageDescriptor) -> Self {
        Self {
            message: DynamicMessage::new(descriptor),
        }
    }

    /// Set a field by name, returning `&mut self` for chaining.
    ///
    /// # Errors
    ///
    /// [`ZerobusError::InvalidArgument`] if the field name is unknown or the
    /// value's type doesn't match the field's.
    pub fn set(&mut self, field: &str, value: impl IntoDynamicValue) -> ZerobusResult<&mut Self> {
        self.message
            .try_set_field_by_name(field, value.into_value())
            .map_err(|e| {
                ZerobusError::InvalidArgument(format!("cannot set field '{field}': {e}"))
            })?;
        Ok(self)
    }

    /// Encode the record to protobuf wire bytes, ready to ingest as
    /// [`ProtoBytes`](crate::ProtoBytes).
    ///
    /// # Errors
    ///
    /// [`ZerobusError::InvalidArgument`] if any proto2 `required` field is unset
    /// (`prost-reflect` does not enforce this on encode). The message is
    /// otherwise valid by construction, since [`set`](Self::set) type-checks each
    /// field.
    pub fn encode(&self) -> ZerobusResult<Vec<u8>> {
        let missing = missing_required_fields(&self.message);
        if !missing.is_empty() {
            return Err(ZerobusError::InvalidArgument(format!(
                "record missing required field(s): {}",
                missing.join(", ")
            )));
        }
        Ok(self.message.encode_to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost_types::{field_descriptor_proto::Type, DescriptorProto, FieldDescriptorProto};

    /// Build a descriptor with `id` (int64, tag 1) and `name` (string, tag 2).
    fn test_descriptor(name: Option<&str>) -> DescriptorProto {
        DescriptorProto {
            name: name.map(str::to_string),
            field: vec![
                FieldDescriptorProto {
                    name: Some("id".to_string()),
                    number: Some(1),
                    r#type: Some(Type::Int64 as i32),
                    ..Default::default()
                },
                FieldDescriptorProto {
                    name: Some("name".to_string()),
                    number: Some(2),
                    r#type: Some(Type::String as i32),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }
    }

    #[test]
    fn set_rejects_unknown_field_and_wrong_type() {
        let md = message_descriptor(&test_descriptor(Some("Order"))).unwrap();
        let mut record = DynamicRecord::new(md);
        // Unknown name, and a string into an int64 field: both InvalidArgument.
        assert!(matches!(
            record.set("nope", 1i64),
            Err(ZerobusError::InvalidArgument(_))
        ));
        assert!(matches!(
            record.set("id", "not-an-int"),
            Err(ZerobusError::InvalidArgument(_))
        ));
    }

    #[test]
    fn record_round_trips_to_proto_bytes() {
        // Also covers the unnamed-message fallback in `message_descriptor`.
        let md = message_descriptor(&test_descriptor(None)).unwrap();

        let mut record = DynamicRecord::new(md.clone());
        record
            .set("id", 7i64)
            .unwrap()
            .set("name", "widget")
            .unwrap();

        let bytes = record.encode().unwrap();
        let decoded = DynamicMessage::decode(md, bytes.as_slice()).unwrap();
        assert_eq!(decoded.get_field_by_name("id").unwrap().as_i64(), Some(7));
        assert_eq!(
            decoded.get_field_by_name("name").unwrap().as_str(),
            Some("widget")
        );
    }

    #[test]
    fn encode_rejects_missing_required_field() {
        use prost_types::field_descriptor_proto::Label;

        // `id` is proto2 `required`; leaving it unset must fail encode.
        let descriptor = DescriptorProto {
            name: Some("Order".to_string()),
            field: vec![FieldDescriptorProto {
                name: Some("id".to_string()),
                number: Some(1),
                label: Some(Label::Required as i32),
                r#type: Some(Type::Int64 as i32),
                ..Default::default()
            }],
            ..Default::default()
        };
        let md = message_descriptor(&descriptor).unwrap();

        let err = DynamicRecord::new(md.clone()).encode().unwrap_err();
        match err {
            ZerobusError::InvalidArgument(msg) => assert!(msg.contains("id")),
            other => panic!("expected InvalidArgument, got {other:?}"),
        }

        // Setting it makes encode succeed.
        let mut record = DynamicRecord::new(md);
        record.set("id", 1i64).unwrap();
        assert!(record.encode().is_ok());
    }

    /// A UC descriptor with a nested `address` struct whose `zip` field is
    /// non-nullable (proto2 `required`), built via `descriptor_from_uc_columns`.
    fn uc_descriptor_with_required_nested_field() -> MessageDescriptor {
        use crate::schema::{descriptor_from_uc_columns, UcColumn};

        let col = |name: &str, type_name: &str, type_json: &str, position: i32| UcColumn {
            name: name.to_string(),
            type_name: type_name.to_string(),
            type_text: String::new(),
            type_json: type_json.to_string(),
            nullable: true,
            position,
        };
        let address_json = r#"{
            "type":"struct",
            "fields":[
                {"name":"street","type":"string","nullable":true,"metadata":{}},
                {"name":"zip","type":"integer","nullable":false,"metadata":{}}
            ]
        }"#;
        let columns = vec![
            col("id", "BIGINT", "", 0),
            col("address", "STRUCT", address_json, 1),
        ];
        let proto = descriptor_from_uc_columns(&columns, "table_Orders").unwrap();
        message_descriptor(&proto).unwrap()
    }

    #[test]
    fn encode_reports_missing_required_field_in_nested_struct() {
        let md = uc_descriptor_with_required_nested_field();

        // Present `address` but omit its required `zip`: encode must report the
        // nested field by its dotted path.
        let address_desc = match md.get_field_by_name("address").unwrap().kind() {
            Kind::Message(m) => m,
            other => panic!("expected message field, got {other:?}"),
        };
        let mut record = DynamicRecord::new(md);
        record
            .set("id", 1i64)
            .unwrap()
            .set("address", Value::Message(DynamicMessage::new(address_desc)))
            .unwrap();

        let err = record.encode().unwrap_err();
        match err {
            ZerobusError::InvalidArgument(msg) => assert!(
                msg.contains("address.zip"),
                "expected nested path in error, got: {msg}"
            ),
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn valid_nested_record_round_trips() {
        let md = uc_descriptor_with_required_nested_field();
        let address_desc = match md.get_field_by_name("address").unwrap().kind() {
            Kind::Message(m) => m,
            other => panic!("expected message field, got {other:?}"),
        };

        let mut address = DynamicMessage::new(address_desc);
        address.set_field_by_name("street", Value::String("1 Main St".to_string()));
        address.set_field_by_name("zip", Value::I32(94105));

        let mut record = DynamicRecord::new(md.clone());
        record
            .set("id", 7i64)
            .unwrap()
            .set("address", Value::Message(address))
            .unwrap();

        let bytes = record.encode().unwrap();
        let decoded = DynamicMessage::decode(md, bytes.as_slice()).unwrap();
        assert_eq!(decoded.get_field_by_name("id").unwrap().as_i64(), Some(7));
        let decoded_addr = decoded.get_field_by_name("address").unwrap();
        let decoded_addr = decoded_addr.as_message().unwrap();
        assert_eq!(
            decoded_addr.get_field_by_name("zip").unwrap().as_i32(),
            Some(94105)
        );
        assert_eq!(
            decoded_addr.get_field_by_name("street").unwrap().as_str(),
            Some("1 Main St")
        );
    }
}
