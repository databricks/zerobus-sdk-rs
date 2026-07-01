//! Native dynamic-protobuf record support.
//!
//! For tables whose schema is known only at runtime (a
//! [`prost_types::DescriptorProto`] from Unity Catalog or built with
//! [`crate::schema::descriptor_from_uc_columns`]), so there is no compiled
//! `prost::Message`. Select the format with
//! [`StreamBuilder::dynamic_proto`](crate::StreamBuilder::dynamic_proto), then
//! build records field-by-field with [`DynamicRecord`] against the
//! [`MessageDescriptor`] from
//! [`ZerobusStream::message_descriptor`](crate::ZerobusStream::message_descriptor).
//!
//! Ingest in a loop, then `flush()` once — never wait per record.
//!
//! ```no_run
//! # use databricks_zerobus_ingest_sdk::{ZerobusStream, dynamic::DynamicRecord};
//! # async fn example(stream: &ZerobusStream) -> Result<(), Box<dyn std::error::Error>> {
//! let descriptor = stream.message_descriptor()?;
//! for i in 0..1_000i64 {
//!     let mut record = DynamicRecord::new(descriptor.clone());
//!     record.set("id", i)?.set("name", "widget")?;
//!     stream.ingest_record_offset(record).await?; // queue only — do NOT wait here
//! }
//! stream.flush().await?; // wait once for all pending acks
//! # Ok(())
//! # }
//! ```

use prost::Message as _;
use prost_reflect::DescriptorPool;

use crate::record_types::EncodedRecord;
use crate::{ZerobusError, ZerobusResult};

pub use prost_reflect::{DynamicMessage, MessageDescriptor, Value};

/// Resolve a [`MessageDescriptor`] from a bare [`prost_types::DescriptorProto`],
/// for building [`DynamicRecord`]s against a runtime schema.
///
/// Building the descriptor pool has a cost; resolve once and clone per record (a
/// cheap Arc-backed clone). [`ZerobusStream::message_descriptor`](crate::ZerobusStream::message_descriptor)
/// caches it for you.
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
/// then ingested (encoded to wire bytes automatically).
///
/// ```no_run
/// # use databricks_zerobus_ingest_sdk::{ZerobusStream, dynamic::DynamicRecord};
/// # async fn example(stream: &ZerobusStream) -> Result<(), Box<dyn std::error::Error>> {
/// let descriptor = stream.message_descriptor()?;
/// let mut record = DynamicRecord::new(descriptor);
/// record.set("id", 42i64)?.set("name", "widget")?;
/// let _offset = stream.ingest_record_offset(record).await?; // queue only
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
}

impl From<DynamicRecord> for EncodedRecord {
    fn from(record: DynamicRecord) -> Self {
        EncodedRecord::Proto(record.message.encode_to_vec())
    }
}

impl From<DynamicMessage> for EncodedRecord {
    fn from(message: DynamicMessage) -> Self {
        EncodedRecord::Proto(message.encode_to_vec())
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

        let EncodedRecord::Proto(bytes) = record.into() else {
            panic!("expected Proto variant");
        };
        let decoded = DynamicMessage::decode(md, bytes.as_slice()).unwrap();
        assert_eq!(decoded.get_field_by_name("id").unwrap().as_i64(), Some(7));
        assert_eq!(
            decoded.get_field_by_name("name").unwrap().as_str(),
            Some("widget")
        );
    }
}
