//! Convert a Unity Catalog table schema into a protobuf [`DescriptorProto`].
//!
//! The Zerobus service accepts records described by a protobuf message descriptor.
//! Callers that already have the Unity Catalog metadata for a table can use
//! [`descriptor_from_uc_columns`] or [`descriptor_from_uc_schema`] to build that
//! descriptor on the fly instead of pre-generating a `.proto` file offline.
//!
//! # Example
//!
//! ```no_run
//! use databricks_zerobus_ingest_sdk::schema::{UcColumn, descriptor_from_uc_columns};
//!
//! let columns = vec![
//!     UcColumn {
//!         name: "id".into(),
//!         type_name: "BIGINT".into(),
//!         type_text: "BIGINT".into(),
//!         type_json: String::new(),
//!         nullable: false,
//!         position: 0,
//!     },
//!     UcColumn {
//!         name: "payload".into(),
//!         type_name: "STRING".into(),
//!         type_text: "STRING".into(),
//!         type_json: String::new(),
//!         nullable: true,
//!         position: 1,
//!     },
//! ];
//! let descriptor = descriptor_from_uc_columns(&columns, "my_table").unwrap();
//! assert_eq!(descriptor.name(), "my_table");
//! ```
//!
//! For `STRUCT`, `ARRAY`, and `MAP` columns the `type_json` field must be populated
//! with the JSON representation returned by the Unity Catalog REST API (the
//! `/api/2.1/unity-catalog/tables/{name}` response includes it per column).
//!
//! # Type mapping
//!
//! | Unity Catalog type            | Proto type | Encoding contract                           |
//! |-------------------------------|------------|---------------------------------------------|
//! | `STRING`, `VARIANT`, `DECIMAL`| `string`   | UTF-8 text                                  |
//! | `INT`, `INTEGER`              | `int32`    |                                             |
//! | `LONG`, `BIGINT`              | `int64`    |                                             |
//! | `SHORT`, `SMALLINT`, `BYTE`, `TINYINT` | `int32` | zero-extended; range-checked by the server |
//! | `FLOAT`                       | `float`    |                                             |
//! | `DOUBLE`                      | `double`   |                                             |
//! | `BOOLEAN`                     | `bool`     |                                             |
//! | `BINARY`                      | `bytes`    |                                             |
//! | `DATE`                        | `int32`    | **days since 1970-01-01** (Unix epoch)      |
//! | `TIMESTAMP`                   | `int64`    | **microseconds since 1970-01-01 00:00:00 UTC** |
//! | `TIMESTAMP_NTZ`               | `int64`    | **microseconds since 1970-01-01 00:00:00**, no timezone |
//! | `STRUCT<...>`                 | nested message | fields sanitized to valid proto identifiers |
//! | `ARRAY<T>`                    | `repeated T` | elements are always present (protobuf repeated has no null elements) |
//! | `MAP<K, V>`                   | synthetic map-entry message + `repeated` | K must be integral, bool, or string |
//!
//! ## Timestamp and date encoding
//!
//! `DATE` and `TIMESTAMP*` columns are encoded as integers, **not** as
//! `google.protobuf.Timestamp` or ISO-8601 strings. Clients must convert their
//! source values into the expected unit before writing them into the generated
//! proto message; otherwise every row will be silently off by a factor of 10³
//! (milliseconds mistaken for microseconds) or 10⁶ (seconds mistaken for
//! microseconds), or will land on the wrong day (milliseconds-since-epoch written
//! into a `DATE` field).
//!
//! Quick reference:
//!
//! ```text
//! DATE          → (chrono::NaiveDate - 1970-01-01).num_days() as i32
//! TIMESTAMP     → instant.timestamp_micros() as i64  // UTC micros
//! TIMESTAMP_NTZ → naive.and_utc().timestamp_micros() as i64  // local-wall-clock micros
//! ```
//!
//! `TIMESTAMP` and `TIMESTAMP_NTZ` collapse to the same proto type (`int64`); the
//! descriptor alone does not preserve the timezone distinction. The server
//! recovers it from the Unity Catalog table schema on the write path, so the
//! caller only needs to ensure the integer value matches the column's declared
//! semantics.

use std::collections::HashSet;

use prost_types::field_descriptor_proto::{Label, Type as ProtoType};
use prost_types::{DescriptorProto, FieldDescriptorProto, MessageOptions};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use thiserror::Error;

/// A single column from a Unity Catalog table.
///
/// Field names mirror the Unity Catalog REST API response so the struct can be
/// deserialized directly from it (see the `Deserialize` impl).
#[derive(Debug, Clone, Deserialize)]
pub struct UcColumn {
    pub name: String,
    /// Top-level type name, e.g. `"STRING"`, `"INT"`, `"STRUCT"`, `"ARRAY"`, `"MAP"`.
    pub type_name: String,
    /// Human-readable type (e.g. `"struct<a:int,b:string>"`). Not used by the
    /// conversion — kept so the struct round-trips cleanly against the UC API.
    #[serde(default)]
    pub type_text: String,
    /// JSON representation of the type. Required for `STRUCT`, `ARRAY`, `MAP`.
    #[serde(default)]
    pub type_json: String,
    /// Defaults to `true` when absent, matching Spark/Delta `StructField`
    /// semantics (a missing `nullable` key means "unspecified, assume
    /// nullable"). This also matches the default used for nested struct
    /// fields in `type_json`.
    #[serde(default = "default_true")]
    pub nullable: bool,
    #[serde(default)]
    pub position: i32,
}

fn default_true() -> bool {
    true
}

/// A Unity Catalog table schema, as returned by the REST API.
#[derive(Debug, Clone, Deserialize)]
pub struct UcTableSchema {
    pub name: String,
    pub catalog_name: String,
    pub schema_name: String,
    pub columns: Vec<UcColumn>,
}

/// Errors produced while converting a UC schema to a protobuf descriptor.
#[derive(Debug, Error)]
pub enum SchemaError {
    #[error("invalid field name '{name}': {reason}")]
    InvalidFieldName { name: String, reason: String },
    #[error("unsupported Databricks type '{0}'")]
    UnsupportedType(String),
    #[error("missing type_json for complex column '{0}'")]
    MissingTypeJson(String),
    #[error("failed to parse type_json for column '{column}': {reason}")]
    InvalidTypeJson { column: String, reason: String },
    #[error("{0}")]
    Invalid(String),
}

/// Build a [`DescriptorProto`] from a Unity Catalog table's columns.
///
/// `message_name` becomes the top-level message name on the returned descriptor.
/// Columns with `type_name` of `STRUCT`, `ARRAY`, or `MAP` require `type_json`
/// to be populated; simple columns only need `type_name`.
pub fn descriptor_from_uc_columns(
    columns: &[UcColumn],
    message_name: &str,
) -> Result<DescriptorProto, SchemaError> {
    let mut collector = MessageCollector::new();
    let mut fields = Vec::with_capacity(columns.len());

    let mut sorted: Vec<&UcColumn> = columns.iter().filter(|c| c.position >= 0).collect();
    sorted.sort_by_key(|c| c.position);

    // Protobuf field number = UC position + 1. Unity Catalog's `position` is
    // 0-indexed; adding 1 produces a valid proto field number and preserves
    // any gaps UC reports (e.g. after DROP COLUMN in column-mapping mode),
    // keeping a one-to-one correspondence between field number and UC column.
    for column in sorted.iter() {
        validate_field_name(&column.name)?;

        let (field_type, type_name, is_repeated) = if is_complex(&column.type_name) {
            if column.type_json.is_empty() {
                return Err(SchemaError::MissingTypeJson(column.name.clone()));
            }
            let complex = parse_type_json(&column.type_json).map_err(|reason| {
                SchemaError::InvalidTypeJson {
                    column: column.name.clone(),
                    reason,
                }
            })?;
            let is_repeated = matches!(complex, ComplexType::Array(_) | ComplexType::Map { .. });
            let (ty, type_name) =
                map_complex_type_to_protobuf(&complex, &column.name, &mut collector)?;
            (ty, type_name, is_repeated)
        } else {
            (map_simple_databricks_type(&column.type_name)?, None, false)
        };

        fields.push(field_descriptor(
            &column.name,
            column.position + 1,
            field_type,
            type_name,
            column.nullable,
            is_repeated,
        ));
    }

    Ok(DescriptorProto {
        name: Some(message_name.to_string()),
        field: fields,
        nested_type: collector.nested,
        ..Default::default()
    })
}

/// Build a [`DescriptorProto`] from a full [`UcTableSchema`].
///
/// The generated message name is `<schema_name>_<table_name>` (sanitized to a
/// valid protobuf identifier).
pub fn descriptor_from_uc_schema(schema: &UcTableSchema) -> Result<DescriptorProto, SchemaError> {
    let message_name = sanitize_message_name(&format!("{}_{}", schema.schema_name, schema.name));
    descriptor_from_uc_columns(&schema.columns, &message_name)
}

fn is_complex(type_name: &str) -> bool {
    matches!(type_name, "STRUCT" | "ARRAY" | "MAP")
}

fn field_descriptor(
    name: &str,
    number: i32,
    field_type: ProtoType,
    type_name: Option<String>,
    nullable: bool,
    is_repeated: bool,
) -> FieldDescriptorProto {
    let label = if is_repeated {
        Label::Repeated
    } else if nullable {
        Label::Optional
    } else {
        Label::Required
    };
    FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(number),
        label: Some(label as i32),
        r#type: Some(field_type as i32),
        type_name,
        json_name: Some(name.to_string()),
        proto3_optional: Some(nullable && !is_repeated),
        ..Default::default()
    }
}

fn map_simple_databricks_type(type_name: &str) -> Result<ProtoType, SchemaError> {
    Ok(match type_name {
        "STRING" => ProtoType::String,
        "INT" | "INTEGER" => ProtoType::Int32,
        "LONG" | "BIGINT" => ProtoType::Int64,
        "SHORT" | "SMALLINT" | "BYTE" | "TINYINT" => ProtoType::Int32,
        "BOOLEAN" | "BOOL" => ProtoType::Bool,
        "DOUBLE" => ProtoType::Double,
        "FLOAT" => ProtoType::Float,
        "TIMESTAMP" | "TIMESTAMP_NTZ" => ProtoType::Int64,
        "DATE" => ProtoType::Int32,
        "BINARY" => ProtoType::Bytes,
        "DECIMAL" => ProtoType::String,
        "VARIANT" => ProtoType::String,
        other => return Err(SchemaError::UnsupportedType(other.to_string())),
    })
}

#[derive(Debug, Clone)]
enum ComplexType {
    Primitive(PrimitiveType),
    Struct(StructType),
    Array(Box<ComplexType>),
    Map {
        key: Box<ComplexType>,
        value: Box<ComplexType>,
    },
}

#[derive(Debug, Clone, Copy)]
enum PrimitiveType {
    String,
    Long,
    Integer,
    Short,
    Byte,
    Double,
    Float,
    Boolean,
    Binary,
    Timestamp,
    Date,
    Decimal,
}

#[derive(Debug, Clone)]
struct StructField {
    name: String,
    field_type: ComplexType,
    nullable: bool,
}

#[derive(Debug, Clone)]
struct StructType {
    fields: Vec<StructField>,
}

/// Maximum nesting depth accepted in `type_json`. Bounds recursion into
/// user-controlled JSON so a pathological input cannot blow the stack.
const MAX_NESTING_DEPTH: usize = 100;

/// Either a primitive name (`"integer"`) or a nested complex type object.
#[derive(Deserialize)]
#[serde(untagged)]
enum TypeRef {
    Complex(ComplexTypeJson),
    Primitive(String),
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum ComplexTypeJson {
    Struct {
        fields: Vec<StructFieldJson>,
    },
    Array {
        #[serde(rename = "elementType")]
        element_type: Box<TypeRef>,
    },
    // `valueContainsNull` is intentionally not captured: proto scalar maps
    // cannot carry null values on the wire, so there is nothing for the
    // encoder to coerce — only non-null values can be transmitted.
    Map {
        #[serde(rename = "keyType")]
        key_type: Box<TypeRef>,
        #[serde(rename = "valueType")]
        value_type: Box<TypeRef>,
    },
}

#[derive(Deserialize)]
struct StructFieldJson {
    name: String,
    #[serde(rename = "type")]
    ty: TypeRef,
    #[serde(default = "default_true")]
    nullable: bool,
}

fn parse_type_json(type_json: &str) -> Result<ComplexType, String> {
    if type_json.is_empty() || type_json == "{}" {
        return Err("empty type_json".into());
    }
    let raw: JsonValue = serde_json::from_str(type_json).map_err(|e| e.to_string())?;
    // Unity Catalog sometimes wraps the type in {"name": ..., "type": ...}.
    let inner = match raw.as_object() {
        Some(obj) if obj.contains_key("name") && obj.contains_key("type") => {
            obj.get("type").unwrap().clone()
        }
        _ => raw,
    };
    let tref: TypeRef = serde_json::from_value(inner).map_err(|e| e.to_string())?;
    type_ref_to_complex(&tref, 0)
}

fn type_ref_to_complex(tref: &TypeRef, level: usize) -> Result<ComplexType, String> {
    if level > MAX_NESTING_DEPTH {
        return Err(format!(
            "nesting level exceeds maximum depth of {}",
            MAX_NESTING_DEPTH
        ));
    }
    match tref {
        TypeRef::Primitive(s) => parse_primitive_type(s).map(ComplexType::Primitive),
        TypeRef::Complex(ComplexTypeJson::Struct { fields }) => {
            let mut out = Vec::with_capacity(fields.len());
            for f in fields {
                out.push(StructField {
                    name: f.name.clone(),
                    field_type: type_ref_to_complex(&f.ty, level + 1)?,
                    nullable: f.nullable,
                });
            }
            Ok(ComplexType::Struct(StructType { fields: out }))
        }
        TypeRef::Complex(ComplexTypeJson::Array { element_type }) => Ok(ComplexType::Array(
            Box::new(type_ref_to_complex(element_type, level + 1)?),
        )),
        TypeRef::Complex(ComplexTypeJson::Map { key_type, value_type }) => Ok(ComplexType::Map {
            key: Box::new(type_ref_to_complex(key_type, level + 1)?),
            value: Box::new(type_ref_to_complex(value_type, level + 1)?),
        }),
    }
}

fn parse_primitive_type(s: &str) -> Result<PrimitiveType, String> {
    Ok(match s {
        "string" => PrimitiveType::String,
        "long" => PrimitiveType::Long,
        "integer" => PrimitiveType::Integer,
        "short" => PrimitiveType::Short,
        "byte" => PrimitiveType::Byte,
        "double" => PrimitiveType::Double,
        "float" => PrimitiveType::Float,
        "boolean" => PrimitiveType::Boolean,
        "binary" => PrimitiveType::Binary,
        "timestamp" | "timestamp_ntz" => PrimitiveType::Timestamp,
        "date" => PrimitiveType::Date,
        s if s.starts_with("decimal") => PrimitiveType::Decimal,
        other => return Err(format!("unknown primitive type '{}'", other)),
    })
}

const fn map_primitive_to_protobuf(p: PrimitiveType) -> ProtoType {
    match p {
        PrimitiveType::String => ProtoType::String,
        PrimitiveType::Long => ProtoType::Int64,
        PrimitiveType::Integer => ProtoType::Int32,
        PrimitiveType::Short | PrimitiveType::Byte => ProtoType::Int32,
        PrimitiveType::Double => ProtoType::Double,
        PrimitiveType::Float => ProtoType::Float,
        PrimitiveType::Boolean => ProtoType::Bool,
        PrimitiveType::Binary => ProtoType::Bytes,
        PrimitiveType::Timestamp => ProtoType::Int64,
        PrimitiveType::Date => ProtoType::Int32,
        PrimitiveType::Decimal => ProtoType::String,
    }
}

const fn is_valid_map_key(p: PrimitiveType) -> bool {
    !matches!(
        p,
        PrimitiveType::Double | PrimitiveType::Float | PrimitiveType::Binary
    )
}

/// Accumulates nested message definitions during conversion and dedupes their names.
struct MessageCollector {
    nested: Vec<DescriptorProto>,
    used: HashSet<String>,
}

impl MessageCollector {
    fn new() -> Self {
        Self {
            nested: Vec::new(),
            used: HashSet::new(),
        }
    }

    fn unique_name(&mut self, base: String) -> String {
        if self.used.insert(base.clone()) {
            return base;
        }
        let mut n = 2u32;
        loop {
            let candidate = format!("{}{}", base, n);
            if self.used.insert(candidate.clone()) {
                return candidate;
            }
            n += 1;
        }
    }

    fn push(&mut self, message: DescriptorProto) {
        self.nested.push(message);
    }
}

fn map_complex_type_to_protobuf(
    ct: &ComplexType,
    path: &str,
    collector: &mut MessageCollector,
) -> Result<(ProtoType, Option<String>), SchemaError> {
    match ct {
        ComplexType::Primitive(p) => Ok((map_primitive_to_protobuf(*p), None)),
        ComplexType::Struct(st) => {
            let name = collector.unique_name(sanitize_message_name(path));
            // Each struct owns its own nested scope, so any messages it
            // generates (inner structs, map entries on its own fields) land in
            // that struct's `nested_type` rather than leaking up to the root.
            let msg = generate_struct_message(&name, st)?;
            collector.push(msg);
            Ok((ProtoType::Message, Some(name)))
        }
        ComplexType::Array(element) => match element.as_ref() {
            ComplexType::Primitive(p) => Ok((map_primitive_to_protobuf(*p), None)),
            ComplexType::Struct(_) => {
                let element_path = format!("{}_element", sanitize_message_name(path));
                map_complex_type_to_protobuf(element, &element_path, collector)
            }
            ComplexType::Array(_) => Err(SchemaError::Invalid(format!(
                "nested arrays not supported for field '{}'",
                path
            ))),
            ComplexType::Map { .. } => Err(SchemaError::Invalid(format!(
                "arrays of maps not supported for field '{}'",
                path
            ))),
        },
        ComplexType::Map { key, value } => {
            let key_primitive = match key.as_ref() {
                ComplexType::Primitive(p) if is_valid_map_key(*p) => *p,
                ComplexType::Primitive(p) => {
                    return Err(SchemaError::Invalid(format!(
                        "unsupported map key type {:?} for field '{}' \
                         (protobuf map keys must be integral, bool, or string)",
                        p, path
                    )));
                }
                _ => {
                    return Err(SchemaError::Invalid(format!(
                        "map keys must be primitive types (field '{}')",
                        path
                    )));
                }
            };
            match value.as_ref() {
                ComplexType::Primitive(v) => {
                    let entry_name =
                        collector.unique_name(format!("{}_entry", sanitize_message_name(path)));
                    let entry = generate_map_entry(&entry_name, key_primitive, MapValue::Primitive(*v));
                    collector.push(entry.clone());
                    Ok((ProtoType::Message, Some(entry.name().to_string())))
                }
                ComplexType::Struct(st) => {
                    let value_name =
                        collector.unique_name(format!("{}Value", sanitize_message_name(path)));
                    let value_msg = generate_struct_message(&value_name, st)?;
                    collector.push(value_msg);
                    let entry_name =
                        collector.unique_name(format!("{}Entry", sanitize_message_name(path)));
                    let entry = generate_map_entry(
                        &entry_name,
                        key_primitive,
                        MapValue::Message(&value_name),
                    );
                    collector.push(entry.clone());
                    Ok((ProtoType::Message, Some(entry.name().to_string())))
                }
                ComplexType::Array(_) | ComplexType::Map { .. } => Err(SchemaError::Invalid(
                    format!("maps with complex value types not supported for field '{}'", path),
                )),
            }
        }
    }
}

fn generate_struct_message(
    message_name: &str,
    st: &StructType,
) -> Result<DescriptorProto, SchemaError> {
    let mut local = MessageCollector::new();
    let mut fields = Vec::with_capacity(st.fields.len());
    for (index, f) in st.fields.iter().enumerate() {
        validate_field_name(&f.name)?;
        let path = format!("{}_{}", message_name, f.name);
        let (field_type, type_name) =
            map_complex_type_to_protobuf(&f.field_type, &path, &mut local)?;
        let is_repeated = matches!(f.field_type, ComplexType::Array(_) | ComplexType::Map { .. });
        fields.push(field_descriptor(
            &f.name,
            (index + 1) as i32,
            field_type,
            type_name,
            f.nullable,
            is_repeated,
        ));
    }
    Ok(DescriptorProto {
        name: Some(message_name.to_string()),
        field: fields,
        nested_type: local.nested,
        ..Default::default()
    })
}

enum MapValue<'a> {
    Primitive(PrimitiveType),
    Message(&'a str),
}

fn generate_map_entry(name: &str, key: PrimitiveType, value: MapValue<'_>) -> DescriptorProto {
    let key_field = FieldDescriptorProto {
        name: Some("key".into()),
        number: Some(1),
        label: Some(Label::Optional as i32),
        r#type: Some(map_primitive_to_protobuf(key) as i32),
        json_name: Some("key".into()),
        proto3_optional: Some(false),
        ..Default::default()
    };
    let (value_type, value_type_name) = match value {
        MapValue::Primitive(p) => (map_primitive_to_protobuf(p), None),
        MapValue::Message(n) => (ProtoType::Message, Some(n.to_string())),
    };
    let value_field = FieldDescriptorProto {
        name: Some("value".into()),
        number: Some(2),
        label: Some(Label::Optional as i32),
        r#type: Some(value_type as i32),
        type_name: value_type_name,
        json_name: Some("value".into()),
        proto3_optional: Some(true),
        ..Default::default()
    };
    DescriptorProto {
        name: Some(name.to_string()),
        field: vec![key_field, value_field],
        options: Some(MessageOptions {
            map_entry: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// proto2 keywords and primitive type names reserved as field names. A column
/// named `message` or `int32` produces a technically-valid `DescriptorProto`
/// for direct wire encoding, but would collide with the proto grammar when
/// the descriptor is printed back to `.proto` text or fed to protoc for
/// language-code generation. Rejecting them here keeps the descriptor safe
/// for both uses.
const RESERVED_FIELD_NAMES: &[&str] = &[
    "syntax", "import", "option", "package", "message", "enum", "service", "rpc", "returns",
    "reserved", "to", "max", "double", "float", "int32", "int64", "uint32", "uint64", "sint32",
    "sint64", "fixed32", "fixed64", "sfixed32", "sfixed64", "bool", "string", "bytes",
];

fn validate_field_name(name: &str) -> Result<(), SchemaError> {
    if name.is_empty() {
        return Err(SchemaError::InvalidFieldName {
            name: name.to_string(),
            reason: "empty".into(),
        });
    }
    if name.starts_with(|c: char| c.is_ascii_digit()) {
        return Err(SchemaError::InvalidFieldName {
            name: name.to_string(),
            reason: "cannot start with a digit".into(),
        });
    }
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(SchemaError::InvalidFieldName {
            name: name.to_string(),
            reason: "only alphanumeric and '_' characters allowed".into(),
        });
    }
    if RESERVED_FIELD_NAMES.contains(&name) {
        return Err(SchemaError::InvalidFieldName {
            name: name.to_string(),
            reason: "reserved proto keyword".into(),
        });
    }
    Ok(())
}

/// Convert a Unity Catalog identifier to a valid PascalCase protobuf message name.
///
/// Protobuf identifiers must be ASCII (`[A-Za-z_][A-Za-z0-9_]*`), so any
/// non-ASCII characters (e.g. `é`, `中`) are dropped even though they are
/// Unicode-alphanumeric — otherwise the generated descriptor would fail
/// to compile.
fn sanitize_message_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut capitalize = true;
    for c in name.chars() {
        if c.is_ascii_alphanumeric() {
            if capitalize {
                out.push(c.to_ascii_uppercase());
                capitalize = false;
            } else {
                out.push(c);
            }
        } else {
            capitalize = true;
        }
    }
    if out.is_empty() || !out.chars().next().unwrap().is_ascii_alphabetic() {
        out.insert(0, 'M');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, type_name: &str, nullable: bool, position: i32) -> UcColumn {
        UcColumn {
            name: name.into(),
            type_name: type_name.into(),
            type_text: type_name.to_lowercase(),
            type_json: String::new(),
            nullable,
            position,
        }
    }

    fn complex_col(name: &str, type_name: &str, type_json: &str, position: i32) -> UcColumn {
        UcColumn {
            name: name.into(),
            type_name: type_name.into(),
            type_text: String::new(),
            type_json: type_json.into(),
            nullable: true,
            position,
        }
    }

    fn field<'a>(desc: &'a DescriptorProto, name: &str) -> &'a FieldDescriptorProto {
        desc.field
            .iter()
            .find(|f| f.name() == name)
            .unwrap_or_else(|| panic!("field '{}' not found in {:?}", name, desc.name()))
    }

    #[test]
    fn scalars_round_trip() {
        let cols = vec![
            col("id", "BIGINT", false, 0),
            col("name", "STRING", true, 1),
            col("score", "DOUBLE", true, 2),
            col("created_at", "TIMESTAMP", true, 3),
            col("d", "DATE", false, 4),
            col("data", "BINARY", false, 5),
        ];
        let d = descriptor_from_uc_columns(&cols, "m").unwrap();
        assert_eq!(d.name(), "m");
        assert_eq!(field(&d, "id").r#type(), ProtoType::Int64);
        assert_eq!(field(&d, "id").label(), Label::Required);
        assert_eq!(field(&d, "name").label(), Label::Optional);
        assert_eq!(field(&d, "score").r#type(), ProtoType::Double);
        assert_eq!(field(&d, "created_at").r#type(), ProtoType::Int64);
        assert_eq!(field(&d, "d").r#type(), ProtoType::Int32);
        assert_eq!(field(&d, "data").r#type(), ProtoType::Bytes);
        // Field numbers are position + 1 and preserve UC ordering.
        assert_eq!(field(&d, "id").number(), 1);
        assert_eq!(field(&d, "data").number(), 6);
    }

    #[test]
    fn field_numbers_mirror_uc_position() {
        // Unity Catalog `position` is 0-indexed; proto field number = position + 1.
        // Gaps (e.g. from DROP COLUMN under Delta column-mapping) are preserved so
        // that field number uniquely identifies a UC column even across schema edits.
        let cols = vec![
            col("a", "STRING", true, 0),
            col("b", "STRING", true, 4),
            col("c", "STRING", true, 8),
        ];
        let d = descriptor_from_uc_columns(&cols, "m").unwrap();
        assert_eq!(field(&d, "a").number(), 1);
        assert_eq!(field(&d, "b").number(), 5);
        assert_eq!(field(&d, "c").number(), 9);
    }

    #[test]
    fn struct_becomes_nested_message() {
        let type_json = r#"{
            "type":"struct",
            "fields":[
                {"name":"street","type":"string","nullable":true,"metadata":{}},
                {"name":"zip","type":"integer","nullable":false,"metadata":{}}
            ]
        }"#;
        let cols = vec![complex_col("address", "STRUCT", type_json, 0)];
        let d = descriptor_from_uc_columns(&cols, "m").unwrap();

        let f = field(&d, "address");
        assert_eq!(f.r#type(), ProtoType::Message);
        assert_eq!(f.label(), Label::Optional);
        let type_name = f.type_name.as_deref().unwrap();
        let nested = d
            .nested_type
            .iter()
            .find(|n| n.name() == type_name)
            .expect("nested struct message not emitted");
        assert_eq!(field(nested, "street").r#type(), ProtoType::String);
        assert_eq!(field(nested, "zip").r#type(), ProtoType::Int32);
        assert_eq!(field(nested, "zip").label(), Label::Required);
    }

    #[test]
    fn array_of_primitive_is_repeated_scalar() {
        let type_json = r#"{"type":"array","elementType":"long","containsNull":true}"#;
        let cols = vec![complex_col("tags", "ARRAY", type_json, 0)];
        let d = descriptor_from_uc_columns(&cols, "m").unwrap();
        let f = field(&d, "tags");
        assert_eq!(f.label(), Label::Repeated);
        assert_eq!(f.r#type(), ProtoType::Int64);
        assert!(f.type_name.is_none());
    }

    #[test]
    fn array_of_struct_emits_nested_message() {
        let type_json = r#"{
            "type":"array",
            "elementType":{
                "type":"struct",
                "fields":[{"name":"k","type":"string","nullable":true,"metadata":{}}]
            },
            "containsNull":true
        }"#;
        let cols = vec![complex_col("items", "ARRAY", type_json, 0)];
        let d = descriptor_from_uc_columns(&cols, "m").unwrap();
        let f = field(&d, "items");
        assert_eq!(f.label(), Label::Repeated);
        assert_eq!(f.r#type(), ProtoType::Message);
        let name = f.type_name.as_deref().unwrap();
        assert!(d.nested_type.iter().any(|n| n.name() == name));
    }

    #[test]
    fn map_of_primitive_generates_entry_message() {
        let type_json = r#"{"type":"map","keyType":"string","valueType":"integer","valueContainsNull":true}"#;
        let cols = vec![complex_col("props", "MAP", type_json, 0)];
        let d = descriptor_from_uc_columns(&cols, "m").unwrap();
        let f = field(&d, "props");
        assert_eq!(f.label(), Label::Repeated);
        assert_eq!(f.r#type(), ProtoType::Message);
        let entry_name = f.type_name.as_deref().unwrap();
        let entry = d
            .nested_type
            .iter()
            .find(|n| n.name() == entry_name)
            .expect("map entry message missing");
        assert_eq!(entry.options.as_ref().and_then(|o| o.map_entry), Some(true));
        assert_eq!(field(entry, "key").r#type(), ProtoType::String);
        assert_eq!(field(entry, "value").r#type(), ProtoType::Int32);
    }

    #[test]
    fn map_with_struct_value_emits_value_and_entry() {
        let type_json = r#"{
            "type":"map",
            "keyType":"string",
            "valueType":{
                "type":"struct",
                "fields":[{"name":"v","type":"long","nullable":true,"metadata":{}}]
            },
            "valueContainsNull":true
        }"#;
        let cols = vec![complex_col("lookup", "MAP", type_json, 0)];
        let d = descriptor_from_uc_columns(&cols, "m").unwrap();
        let f = field(&d, "lookup");
        let entry_name = f.type_name.as_deref().unwrap();
        let entry = d
            .nested_type
            .iter()
            .find(|n| n.name() == entry_name)
            .unwrap();
        assert_eq!(entry.options.as_ref().and_then(|o| o.map_entry), Some(true));
        let value_type_name = field(entry, "value").type_name.as_deref().unwrap();
        // The referenced value message also exists as a nested type.
        assert!(d.nested_type.iter().any(|n| n.name() == value_type_name));
    }

    #[test]
    fn rejects_unsupported_map_key() {
        let type_json = r#"{"type":"map","keyType":"double","valueType":"integer","valueContainsNull":true}"#;
        let cols = vec![complex_col("bad", "MAP", type_json, 0)];
        let err = descriptor_from_uc_columns(&cols, "m").unwrap_err();
        assert!(matches!(err, SchemaError::Invalid(_)), "got {:?}", err);
    }

    #[test]
    fn rejects_excessively_deep_nesting() {
        // Build a chain of `MAX_NESTING_DEPTH + 2` nested arrays. The parser
        // should bail out with an InvalidTypeJson rather than overflowing the stack.
        let mut type_json = String::from("\"integer\"");
        for _ in 0..MAX_NESTING_DEPTH + 2 {
            type_json = format!(
                r#"{{"type":"array","elementType":{},"containsNull":true}}"#,
                type_json
            );
        }
        let cols = vec![complex_col("deep", "ARRAY", &type_json, 0)];
        let err = descriptor_from_uc_columns(&cols, "m").unwrap_err();
        match err {
            SchemaError::InvalidTypeJson { reason, .. } => {
                assert!(
                    reason.contains("maximum depth"),
                    "unexpected reason: {}",
                    reason
                );
            }
            other => panic!("expected InvalidTypeJson, got {:?}", other),
        }
    }

    #[test]
    fn rejects_nested_arrays() {
        let type_json =
            r#"{"type":"array","elementType":{"type":"array","elementType":"integer","containsNull":true},"containsNull":true}"#;
        let cols = vec![complex_col("nested", "ARRAY", type_json, 0)];
        let err = descriptor_from_uc_columns(&cols, "m").unwrap_err();
        assert!(matches!(err, SchemaError::Invalid(_)), "got {:?}", err);
    }

    #[test]
    fn rejects_invalid_field_name() {
        let cols = vec![col("1bad", "STRING", true, 0)];
        let err = descriptor_from_uc_columns(&cols, "m").unwrap_err();
        assert!(matches!(err, SchemaError::InvalidFieldName { .. }));
    }

    #[test]
    fn rejects_reserved_proto_keyword() {
        let cols = vec![col("message", "STRING", true, 0)];
        let err = descriptor_from_uc_columns(&cols, "m").unwrap_err();
        match err {
            SchemaError::InvalidFieldName { name, reason } => {
                assert_eq!(name, "message");
                assert!(reason.contains("reserved"), "got reason: {}", reason);
            }
            other => panic!("expected InvalidFieldName, got {:?}", other),
        }
    }

    #[test]
    fn complex_column_requires_type_json() {
        let cols = vec![col("x", "STRUCT", true, 0)];
        let err = descriptor_from_uc_columns(&cols, "m").unwrap_err();
        assert!(matches!(err, SchemaError::MissingTypeJson(_)));
    }

    #[test]
    fn descriptor_from_uc_schema_derives_name() {
        let schema = UcTableSchema {
            name: "events".into(),
            catalog_name: "main".into(),
            schema_name: "analytics".into(),
            columns: vec![col("id", "BIGINT", false, 0)],
        };
        let d = descriptor_from_uc_schema(&schema).unwrap();
        assert_eq!(d.name(), "AnalyticsEvents");
    }

    #[test]
    fn sanitize_message_name_handles_invalid_chars() {
        assert_eq!(sanitize_message_name("foo-bar"), "FooBar");
        assert_eq!(sanitize_message_name("1abc"), "M1abc");
        assert_eq!(sanitize_message_name("analytics.events"), "AnalyticsEvents");
    }

    #[test]
    fn sanitize_message_name_drops_non_ascii() {
        // Non-ASCII letters are Unicode-alphanumeric but invalid in protobuf
        // identifiers; they must be stripped rather than passed through.
        // Stripping a non-ASCII char triggers the same "capitalize next" behavior
        // as any other non-alphanumeric separator, so "événements" → "VNements".
        assert_eq!(sanitize_message_name("café"), "Caf");
        assert_eq!(sanitize_message_name("événements"), "VNements");
        assert_eq!(sanitize_message_name("中文_table"), "Table");
        // All-non-ASCII input must still produce a valid identifier start.
        assert_eq!(sanitize_message_name("中文"), "M");
        // Leading non-ASCII yields an ASCII-only result that still starts with a letter.
        let result = sanitize_message_name("éfoo");
        assert!(result.chars().next().unwrap().is_ascii_alphabetic());
        assert!(result.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'));
    }

    #[test]
    fn uc_column_deserializes_from_uc_api_shape() {
        let json = r#"{
            "name":"id",
            "type_name":"INT",
            "type_text":"int",
            "type_json":"{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}}",
            "nullable":false,
            "position":0
        }"#;
        let col: UcColumn = serde_json::from_str(json).unwrap();
        assert_eq!(col.name, "id");
        assert_eq!(col.type_name, "INT");
        assert!(!col.nullable);
    }
}
