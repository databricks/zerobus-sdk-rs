use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

use anyhow::{Context, Result, anyhow};
use databricks_zerobus_ingest_sdk::schema::{UcColumn, descriptor_from_uc_columns};
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{DescriptorProto, FieldDescriptorProto};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use serde::Deserialize;
use urlencoding::encode;

pub fn clean_filename(name: &str) -> String {
    let mut cleaned = name
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect::<String>()
        .to_lowercase();

    if cleaned.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        cleaned = format!("table_{}", cleaned);
    }

    if cleaned.is_empty() {
        "table".to_string()
    } else {
        cleaned
    }
}

/// UC `GET /api/2.1/unity-catalog/tables/{full_name}` response shape. Only
/// the `columns` array is needed here; the SDK's `UcColumn` covers every
/// per-column field we care about (`type_json`, `position`, …).
#[derive(Debug, Deserialize)]
pub struct TableInfo {
    pub columns: Vec<UcColumn>,
}

pub async fn fetch_table_info(endpoint: &str, token: &str, table: &str) -> Result<TableInfo> {
    let encoded_table = encode(table);
    let base = endpoint.trim_end_matches('/');
    let url = format!("{base}/api/2.1/unity-catalog/tables/{encoded_table}");

    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", token))?,
    );
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

    let client = reqwest::Client::builder()
        .user_agent("generate-proto-rs/1.0")
        .default_headers(headers)
        .build()?;

    let resp = client.get(&url).send().await?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!("UC request failed: {status} {body}"));
    }

    Ok(resp.json().await?)
}

pub fn generate_proto_file(
    message_name: &str,
    columns: &[UcColumn],
    output_path: &PathBuf,
    output_dir: &PathBuf,
) -> Result<()> {
    std::fs::create_dir_all(output_dir)?;

    let package = output_path
        .file_stem()
        .and_then(|s| s.to_str())
        .map(|s| s.to_string());

    // Delegate schema → descriptor conversion to the SDK, then render the
    // resulting `DescriptorProto` back to proto2 source. The `table_` prefix
    // preserves the existing file-naming convention.
    let top_name = format!("table_{}", message_name);
    let descriptor = descriptor_from_uc_columns(columns, &top_name)?;

    let text = render_proto2(&descriptor, package.as_deref());

    let mut file = File::create(output_path)?;
    file.write_all(text.as_bytes())?;
    Ok(())
}

fn render_proto2(desc: &DescriptorProto, package: Option<&str>) -> String {
    let mut out = String::new();
    out.push_str("syntax = \"proto2\";\n\n");
    if let Some(p) = package {
        out.push_str(&format!("package {};\n\n", p));
    }
    write_message(&mut out, desc, 0);
    out
}

fn write_message(out: &mut String, desc: &DescriptorProto, indent: usize) {
    let tab = "\t".repeat(indent);
    let inner_tab = "\t".repeat(indent + 1);
    let map_entries = collect_map_entries(desc);

    out.push_str(&format!("{}message {} {{\n", tab, desc.name()));
    for f in &desc.field {
        write_field(out, f, &map_entries, &inner_tab);
    }
    for nested in &desc.nested_type {
        if is_map_entry(nested) {
            continue; // rendered inline as `map<k, v>` at the field site
        }
        write_message(out, nested, indent + 1);
    }
    out.push_str(&format!("{}}}\n", tab));
}

fn write_field(
    out: &mut String,
    f: &FieldDescriptorProto,
    map_entries: &HashMap<String, MapEntry>,
    indent: &str,
) {
    if f.label() == Label::Repeated && f.r#type() == Type::Message {
        if let Some(type_name) = f.type_name.as_deref() {
            if let Some(entry) = map_entries.get(short_name(type_name)) {
                out.push_str(&format!(
                    "{}map<{}, {}> {} = {};\n",
                    indent,
                    entry.key,
                    entry.value,
                    f.name(),
                    f.number()
                ));
                return;
            }
        }
    }

    let label = match f.label() {
        Label::Optional => "optional",
        Label::Required => "required",
        Label::Repeated => "repeated",
    };
    let type_str = type_to_str(f.r#type(), f.type_name.as_deref());
    out.push_str(&format!(
        "{}{} {} {} = {};\n",
        indent,
        label,
        type_str,
        f.name(),
        f.number()
    ));
}

struct MapEntry {
    key: Cow<'static, str>,
    value: Cow<'static, str>,
}

fn collect_map_entries(desc: &DescriptorProto) -> HashMap<String, MapEntry> {
    let mut out = HashMap::new();
    for n in &desc.nested_type {
        if !is_map_entry(n) {
            continue;
        }
        let key = n
            .field
            .iter()
            .find(|f| f.name() == "key")
            .map(|f| type_to_str(f.r#type(), f.type_name.as_deref()));
        let value = n
            .field
            .iter()
            .find(|f| f.name() == "value")
            .map(|f| type_to_str(f.r#type(), f.type_name.as_deref()));
        if let (Some(key), Some(value)) = (key, value) {
            out.insert(n.name().to_string(), MapEntry { key, value });
        }
    }
    out
}

fn is_map_entry(desc: &DescriptorProto) -> bool {
    desc.options
        .as_ref()
        .and_then(|o| o.map_entry)
        .unwrap_or(false)
}

fn short_name(type_name: &str) -> &str {
    type_name
        .rsplit('.')
        .next()
        .unwrap_or(type_name)
        .trim_start_matches('.')
}

fn type_to_str(t: Type, type_name: Option<&str>) -> Cow<'static, str> {
    match t {
        Type::Double => Cow::Borrowed("double"),
        Type::Float => Cow::Borrowed("float"),
        Type::Int64 => Cow::Borrowed("int64"),
        Type::Uint64 => Cow::Borrowed("uint64"),
        Type::Int32 => Cow::Borrowed("int32"),
        Type::Fixed64 => Cow::Borrowed("fixed64"),
        Type::Fixed32 => Cow::Borrowed("fixed32"),
        Type::Bool => Cow::Borrowed("bool"),
        Type::String => Cow::Borrowed("string"),
        Type::Bytes => Cow::Borrowed("bytes"),
        Type::Uint32 => Cow::Borrowed("uint32"),
        Type::Sfixed32 => Cow::Borrowed("sfixed32"),
        Type::Sfixed64 => Cow::Borrowed("sfixed64"),
        Type::Sint32 => Cow::Borrowed("sint32"),
        Type::Sint64 => Cow::Borrowed("sint64"),
        Type::Message | Type::Enum | Type::Group => {
            Cow::Owned(short_name(type_name.unwrap_or("")).to_string())
        }
    }
}

pub fn generate_rust_and_descriptor(
    proto_path: &str,
    _proto_name: &str,
    output_dir: &PathBuf,
) -> Result<()> {
    use std::path::Path;

    let proto_file = Path::new(proto_path);
    let proto_dir = proto_file.parent().context("no parent dir")?;

    let file_name = proto_file
        .file_stem()
        .and_then(|s| s.to_str())
        .context("bad filename")?;
    let desc_file = output_dir.join(format!("{}.descriptor", file_name));

    tonic_build::configure()
        .out_dir(output_dir)
        .file_descriptor_set_path(&desc_file)
        .compile_protos(
            &[proto_file.to_str().unwrap()],
            &[proto_dir.to_str().unwrap()],
        )
        .context("protoc compilation failed")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    fn simple(name: &str, type_name: &str, nullable: bool, position: i32) -> UcColumn {
        UcColumn {
            name: name.into(),
            type_name: type_name.into(),
            type_text: type_name.to_lowercase(),
            type_json: String::new(),
            nullable,
            position,
        }
    }

    fn complex(
        name: &str,
        type_name: &str,
        type_json: &str,
        nullable: bool,
        position: i32,
    ) -> UcColumn {
        UcColumn {
            name: name.into(),
            type_name: type_name.into(),
            type_text: String::new(),
            type_json: type_json.into(),
            nullable,
            position,
        }
    }

    #[test]
    fn test_generate_proto_file_happy_path() {
        let columns = vec![
            simple("id", "INT", false, 0),
            simple("name", "STRING", true, 1),
            simple("data", "BINARY", false, 2),
            complex(
                "props",
                "MAP",
                r#"{"type":"map","keyType":"string","valueType":"string","valueContainsNull":true}"#,
                false,
                3,
            ),
            complex(
                "scores",
                "ARRAY",
                r#"{"type":"array","elementType":"double","containsNull":true}"#,
                false,
                4,
            ),
            complex(
                "address",
                "STRUCT",
                r#"{"type":"struct","fields":[
                    {"name":"street","type":"string","nullable":true,"metadata":{}},
                    {"name":"city","type":"string","nullable":true,"metadata":{}}
                ]}"#,
                true,
                5,
            ),
        ];

        let dir = tempdir().unwrap();
        let proto_path = dir.path().join("test.proto");
        let output_dir = dir.path().to_path_buf();

        generate_proto_file("TestMessage", &columns, &proto_path, &output_dir).unwrap();

        let content = fs::read_to_string(proto_path.clone()).unwrap();
        let expected = "syntax = \"proto2\";\n\npackage test;\n\nmessage table_TestMessage {\n\trequired int32 id = 1;\n\toptional string name = 2;\n\trequired bytes data = 3;\n\tmap<string, string> props = 4;\n\trepeated double scores = 5;\n\toptional Address address = 6;\n\tmessage Address {\n\t\toptional string street = 1;\n\t\toptional string city = 2;\n\t}\n}\n";
        assert_eq!(content, expected);

        generate_rust_and_descriptor(proto_path.to_str().unwrap(), "TestMessage", &output_dir)
            .unwrap();
    }

    #[test]
    fn test_nested_structs() {
        let type_json = r#"{
            "type":"struct",
            "fields":[
                {"name":"id","type":"integer","nullable":true,"metadata":{}},
                {"name":"inner","type":{
                    "type":"struct",
                    "fields":[
                        {"name":"value","type":"string","nullable":true,"metadata":{}}
                    ]
                },"nullable":true,"metadata":{}}
            ]
        }"#;
        let columns = vec![complex("outer", "STRUCT", type_json, false, 0)];

        let dir = tempdir().unwrap();
        let proto_path = dir.path().join("nested.proto");
        let output_dir = dir.path().to_path_buf();

        generate_proto_file("NestedMessage", &columns, &proto_path, &output_dir).unwrap();

        let content = fs::read_to_string(proto_path.clone()).unwrap();
        let expected = "syntax = \"proto2\";\n\npackage nested;\n\nmessage table_NestedMessage {\n\trequired Outer outer = 1;\n\tmessage Outer {\n\t\toptional int32 id = 1;\n\t\toptional OuterInner inner = 2;\n\t\tmessage OuterInner {\n\t\t\toptional string value = 1;\n\t\t}\n\t}\n}\n";
        assert_eq!(content, expected);

        generate_rust_and_descriptor(proto_path.to_str().unwrap(), "NestedMessage", &output_dir)
            .unwrap();
    }

    #[test]
    fn test_unsupported_map_key() {
        let type_json = r#"{
            "type":"map",
            "keyType":{"type":"struct","fields":[{"name":"a","type":"integer","nullable":false,"metadata":{}}]},
            "valueType":"string",
            "valueContainsNull":true
        }"#;
        let columns = vec![complex("invalid_map", "MAP", type_json, false, 0)];

        let dir = tempdir().unwrap();
        let proto_path = dir.path().join("test.proto");
        let output_dir = dir.path().to_path_buf();

        let result = generate_proto_file("TestMessage", &columns, &proto_path, &output_dir);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("map keys must be primitive types")
        );
    }

    #[test]
    fn test_nested_map() {
        let type_json = r#"{
            "type":"map",
            "keyType":"string",
            "valueType":{"type":"map","keyType":"string","valueType":"integer","valueContainsNull":true},
            "valueContainsNull":true
        }"#;
        let columns = vec![complex("nested_map", "MAP", type_json, false, 0)];

        let dir = tempdir().unwrap();
        let proto_path = dir.path().join("test.proto");
        let output_dir = dir.path().to_path_buf();

        let result = generate_proto_file("TestMessage", &columns, &proto_path, &output_dir);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("maps with complex value types not supported")
        );
    }

    #[test]
    fn test_nested_array() {
        let type_json = r#"{
            "type":"array",
            "elementType":{"type":"array","elementType":"integer","containsNull":true},
            "containsNull":true
        }"#;
        let columns = vec![complex("nested_array", "ARRAY", type_json, false, 0)];

        let dir = tempdir().unwrap();
        let proto_path = dir.path().join("test.proto");
        let output_dir = dir.path().to_path_buf();

        let result = generate_proto_file("TestMessage", &columns, &proto_path, &output_dir);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("nested arrays not supported")
        );
    }

    #[test]
    fn test_map_field_proto2() {
        let type_json =
            r#"{"type":"map","keyType":"string","valueType":"integer","valueContainsNull":true}"#;
        let columns = vec![complex("attributes", "MAP", type_json, false, 0)];

        let dir = tempdir().unwrap();
        let proto_path = dir.path().join("map_test.proto");
        let output_dir = dir.path().to_path_buf();

        generate_proto_file("MapMessage", &columns, &proto_path, &output_dir).unwrap();

        let content = fs::read_to_string(proto_path.clone()).unwrap();
        let expected = "syntax = \"proto2\";\n\npackage map_test;\n\nmessage table_MapMessage {\n\tmap<string, int32> attributes = 1;\n}\n";
        assert_eq!(content, expected);

        generate_rust_and_descriptor(proto_path.to_str().unwrap(), "MapMessage", &output_dir)
            .unwrap();
    }

    #[test]
    fn test_invalid_field_name() {
        let columns = vec![simple("invalid-name", "STRING", false, 0)];

        let dir = tempdir().unwrap();
        let proto_path = dir.path().join("test.proto");
        let output_dir = dir.path().to_path_buf();

        let result = generate_proto_file("TestMessage", &columns, &proto_path, &output_dir);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid field name 'invalid-name'")
        );
    }

    #[test]
    fn test_digit_start_field_name() {
        let columns = vec![simple("1field", "STRING", false, 0)];

        let dir = tempdir().unwrap();
        let proto_path = dir.path().join("test.proto");
        let output_dir = dir.path().to_path_buf();

        let result = generate_proto_file("TestMessage", &columns, &proto_path, &output_dir);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("cannot start with a digit")
        );
    }

    #[test]
    fn test_all_scalar_types() {
        let columns = vec![
            simple("tiny_value", "TINYINT", false, 0),
            simple("byte_value", "BYTE", true, 1),
            simple("small_value", "SMALLINT", false, 2),
            simple("short_value", "SHORT", true, 3),
            simple("timestamp_ntz", "TIMESTAMP_NTZ", false, 4),
            simple("timestamp_tz", "TIMESTAMP", true, 5),
            simple("variant_data", "VARIANT", true, 6),
            simple("date_value", "DATE", false, 7),
        ];

        let dir = tempdir().unwrap();
        let proto_path = dir.path().join("types_test.proto");
        let output_dir = dir.path().to_path_buf();

        generate_proto_file("TypesMessage", &columns, &proto_path, &output_dir).unwrap();

        let content = fs::read_to_string(proto_path.clone()).unwrap();
        let expected = "syntax = \"proto2\";\n\npackage types_test;\n\nmessage table_TypesMessage {\n\trequired int32 tiny_value = 1;\n\toptional int32 byte_value = 2;\n\trequired int32 small_value = 3;\n\toptional int32 short_value = 4;\n\trequired int64 timestamp_ntz = 5;\n\toptional int64 timestamp_tz = 6;\n\toptional string variant_data = 7;\n\trequired int32 date_value = 8;\n}\n";
        assert_eq!(content, expected);

        generate_rust_and_descriptor(proto_path.to_str().unwrap(), "TypesMessage", &output_dir)
            .unwrap();
    }
}
