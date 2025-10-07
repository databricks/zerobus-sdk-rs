use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

use anyhow::{Context, Result, anyhow};
use regex::Regex;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use serde::Deserialize;
use urlencoding::encode;

fn to_pascal_case(s: &str) -> String {
    let mut result = String::new();

    for word in s.split('_') {
        if word.is_empty() {
            continue;
        }

        let mut chars = word.chars();
        if let Some(first) = chars.next() {
            result.push_str(&first.to_uppercase().to_string());
            result.push_str(chars.as_str());
        }
    }

    result
}

fn get_proto_field_info(
    field_name: &str,
    column_type: &str,
    nullable: bool,
    struct_counter: &mut usize,
    level: usize,
) -> Result<(&'static str, String, Option<String>)> {
    if level > 100 {
        return Err(anyhow!("Nesting level exceeds maximum depth of 100"));
    }
    let col_type = column_type.trim().to_uppercase();

    // Base scalar types
    let proto_type = match col_type.as_str() {
        "SMALLINT" | "SHORT" => Some("int32"),
        "INT" => Some("int32"),
        "BIGINT" | "LONG" => Some("int64"),
        "FLOAT" => Some("float"),
        "DOUBLE" => Some("double"),
        "STRING" => Some("string"),
        "BOOLEAN" => Some("bool"),
        "BINARY" => Some("bytes"),
        "DATE" => Some("int32"),
        "TIMESTAMP" => Some("int64"),
        _ => None,
    };

    if let Some(p) = proto_type {
        return Ok((
            if nullable { "optional" } else { "required" },
            p.to_string(),
            None,
        ));
    }

    // Handle arrays
    if let Some(elem) = parse_array_type(column_type) {
        if parse_array_type(&elem).is_some() {
            return Err(anyhow!("Direct nested arrays are not supported: {}", elem));
        }

        let (_, elem_proto_type, nested_def) =
            get_proto_field_info(field_name, &elem, false, struct_counter, level + 1)?;
        return Ok(("repeated", elem_proto_type, nested_def));
    }

    // Handle maps
    if let Some((key_type, value_type)) = parse_map_type(column_type) {
        // Protobuf map keys must be integral or string types.
        let (_, key_proto_type, key_nested_def) =
            get_proto_field_info(field_name, &key_type, false, struct_counter, level + 1)?;
        if key_nested_def.is_some()
            || !matches!(
                key_proto_type.as_str(),
                "int32"
                    | "int64"
                    | "uint32"
                    | "uint64"
                    | "sint32"
                    | "sint64"
                    | "fixed32"
                    | "fixed64"
                    | "sfixed32"
                    | "sfixed64"
                    | "bool"
                    | "string"
            )
        {
            return Err(anyhow!(
                "Unsupported map key type for Protobuf: {}",
                key_type
            ));
        }

        // Protobuf map values cannot be other maps.
        if parse_map_type(&value_type).is_some() {
            return Err(anyhow!(
                "Protobuf does not support nested maps. Found in: {}",
                column_type
            ));
        }

        let (_, value_proto_type, value_nested_def) =
            get_proto_field_info(field_name, &value_type, false, struct_counter, level + 1)?;

        let map_type = format!("map<{}, {}>", key_proto_type, value_proto_type);

        // Map fields cannot be repeated, and are not marked optional/required.
        return Ok(("", map_type, value_nested_def));
    }

    // Handle structs
    if let Some(fields) = parse_struct_type(column_type) {
        *struct_counter += 1;
        let base_name = to_pascal_case(field_name);
        let struct_name = if base_name.is_empty() {
            format!("Struct{}", *struct_counter)
        } else {
            base_name
        };

        let indent = "\t".repeat(level);
        let inner_indent = "\t".repeat(level + 1);

        let mut struct_def = format!("{}message {} {{\n", indent, struct_name);
        for (i, (fname, ftype)) in fields.into_iter().enumerate() {
            // Struct fields are always optional to avoid issues with required fields.
            let (modifier, field_type, nested_def) =
                get_proto_field_info(&fname, &ftype, true, struct_counter, level + 1)?;

            if let Some(def) = nested_def {
                struct_def.push_str(&def);
                struct_def.push('\n');
            }

            let cleaned_name = validate_field_name(&fname)?;
            struct_def.push_str(&format!(
                "{}{} {} {} = {};\n",
                inner_indent,
                modifier,
                field_type,
                cleaned_name,
                i + 1
            ));
        }
        struct_def.push_str(&format!("{}}}", indent));

        return Ok((
            if nullable { "optional" } else { "required" },
            struct_name,
            Some(struct_def),
        ));
    }

    Err(anyhow!("Unknown column type: {}", column_type))
}

/// Validates field names for Protobuf compatibility.
fn validate_field_name(name: &str) -> Result<&str> {
    const RESERVED: &[&str] = &[
        "syntax", "import", "option", "package", "message", "enum", "service", "rpc", "returns",
        "reserved", "to", "max", "double", "float", "int32", "int64", "uint32", "uint64", "sint32",
        "sint64", "fixed32", "fixed64", "sfixed32", "sfixed64", "bool", "string", "bytes",
    ];

    if name.chars().any(|c| !c.is_alphanumeric() && c != '_') {
        return Err(anyhow!(
            "Invalid Protobuf field name '{}'. Contains non-alphanumeric characters (besides underscore).",
            name
        ));
    }

    if name.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        return Err(anyhow!(
            "Invalid Protobuf field name '{}'. Cannot start with a digit.",
            name
        ));
    }

    if RESERVED.contains(&name) {
        return Err(anyhow!(
            "Invalid Protobuf field name '{}'. It is a reserved keyword.",
            name
        ));
    }

    Ok(name)
}

/// Parses the key and value types from a "MAP<...>" string.
fn parse_map_type(type_str: &str) -> Option<(String, String)> {
    let upper_type = type_str.trim().to_uppercase();
    if !upper_type.starts_with("MAP<") || !upper_type.ends_with('>') {
        return None;
    }

    let inner = &type_str[4..type_str.len() - 1];

    let mut depth = 0;
    let mut split_index = 0;

    for (i, c) in inner.char_indices() {
        match c {
            '<' => depth += 1,
            '>' => depth -= 1,
            ',' if depth == 0 => {
                split_index = i;
                break;
            }
            _ => {}
        }
    }

    if split_index == 0 {
        return None;
    }

    let key_type = inner[..split_index].trim().to_string();
    let value_type = inner[split_index + 1..].trim().to_string();

    if key_type.is_empty() || value_type.is_empty() {
        None
    } else {
        Some((key_type, value_type))
    }
}

/// Parses the element type from an "ARRAY<...>" string, handling nested complex types.
fn parse_array_type(type_str: &str) -> Option<String> {
    let upper_type = type_str.trim().to_uppercase();
    if !upper_type.starts_with("ARRAY<") || !upper_type.ends_with('>') {
        return None;
    }

    let start_index = type_str.find('<')? + 1;
    let end_index = type_str.rfind('>')?;

    if start_index >= end_index {
        return None;
    }

    Some(type_str[start_index..end_index].trim().to_string())
}

/// Parses a struct type string into a vector of field name and type pairs.
fn parse_struct_type(type_str: &str) -> Option<Vec<(String, String)>> {
    static STRUCT_REGEX: once_cell::sync::Lazy<Regex> =
        once_cell::sync::Lazy::new(|| Regex::new(r"(?i)^STRUCT<\s*(.+)\s*>$").unwrap());

    let inner = STRUCT_REGEX.captures(type_str)?.get(1)?.as_str();

    let mut fields = Vec::new();
    let mut depth = 0;
    let mut current = String::new();
    for c in inner.chars() {
        match c {
            '<' => {
                depth += 1;
                current.push(c);
            }
            '>' => {
                depth -= 1;
                current.push(c);
            }
            ',' if depth == 0 => {
                fields.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(c),
        }
    }
    if !current.trim().is_empty() {
        fields.push(current.trim().to_string());
    }

    fields
        .into_iter()
        .map(|f| {
            let mut parts = f.splitn(2, ':');
            match (parts.next(), parts.next()) {
                (Some(name), Some(type_str)) => {
                    Some((name.trim().to_string(), type_str.trim().to_string()))
                }
                _ => None,
            }
        })
        .collect::<Option<Vec<_>>>()
}

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

#[derive(Debug, Deserialize)]
pub struct TableInfo {
    pub columns: Vec<Column>,
}

#[derive(Debug, Deserialize)]
pub struct Column {
    pub name: String,
    pub type_text: String,
    pub nullable: bool,
}

pub fn fetch_table_info(endpoint: &str, token: &str, table: &str) -> Result<TableInfo> {
    let encoded_table = encode(table);
    let base = endpoint.trim_end_matches('/');
    let url = format!("{base}/api/2.1/unity-catalog/tables/{encoded_table}");

    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", token))?,
    );
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

    let client = reqwest::blocking::Client::builder()
        .user_agent("generate-proto-rs/1.0")
        .default_headers(headers)
        .build()?;

    let resp = client.get(&url).send()?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().unwrap_or_default();
        return Err(anyhow!("UC request failed: {status} {body}"));
    }

    Ok(resp.json()?)
}

pub fn generate_proto_file(
    message_name: &str,
    columns: &[Column],
    output_path: &PathBuf,
    output_dir: &PathBuf,
) -> Result<()> {
    std::fs::create_dir_all(output_dir)?;
    let mut out = String::new();
    out.push_str("syntax = \"proto2\";\n\n");

    if let Some(name) = output_path.file_stem().and_then(|s| s.to_str()) {
        out.push_str(&format!("package {};\n\n", name));
    }

    let mut struct_counter = 0;
    let mut fields_and_definitions = String::new();

    for (i, col) in columns.iter().enumerate() {
        let (modifier, proto_type, nested_def) = get_proto_field_info(
            &col.name,
            &col.type_text,
            col.nullable,
            &mut struct_counter,
            1,
        )?;

        if let Some(def) = nested_def {
            fields_and_definitions.push_str(&def);
            fields_and_definitions.push_str("\n\n");
        }

        let field_name = validate_field_name(&col.name)?;
        if modifier.is_empty() {
            fields_and_definitions.push_str(&format!(
                "\t{} {} = {};\n",
                proto_type,
                field_name,
                i + 1
            ));
        } else {
            fields_and_definitions.push_str(&format!(
                "\t{} {} {} = {};\n",
                modifier,
                proto_type,
                field_name,
                i + 1
            ));
        }
    }

    // Constructing the main message.
    out.push_str(&format!("message table_{} {{\n", message_name));
    out.push_str(&fields_and_definitions);
    out.push_str("}\n");

    let mut file = File::create(output_path)?;
    file.write_all(out.as_bytes())?;
    Ok(())
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

    #[test]
    fn test_generate_proto_file_happy_path() {
        let table_info = TableInfo {
            columns: vec![
                Column {
                    name: "id".to_string(),
                    type_text: "INT".to_string(),
                    nullable: false,
                },
                Column {
                    name: "name".to_string(),
                    type_text: "STRING".to_string(),
                    nullable: true,
                },
                Column {
                    name: "data".to_string(),
                    type_text: "BINARY".to_string(),
                    nullable: false,
                },
                Column {
                    name: "props".to_string(),
                    type_text: "MAP<STRING, STRING>".to_string(),
                    nullable: false,
                },
                Column {
                    name: "scores".to_string(),
                    type_text: "ARRAY<DOUBLE>".to_string(),
                    nullable: false,
                },
                Column {
                    name: "address".to_string(),
                    type_text: "STRUCT<street:STRING, city:STRING>".to_string(),
                    nullable: true,
                },
            ],
        };

        let dir = tempdir().unwrap();
        let proto_path = dir.path().join("test.proto");
        let output_dir = dir.path().to_path_buf();

        generate_proto_file("TestMessage", &table_info.columns, &proto_path, &output_dir).unwrap();

        let content = fs::read_to_string(proto_path.clone()).unwrap();
        let expected = "syntax = \"proto2\";\n\npackage test;\n\nmessage table_TestMessage {\n\trequired int32 id = 1;\n\toptional string name = 2;\n\trequired bytes data = 3;\n\tmap<string, string> props = 4;\n\trepeated double scores = 5;\n\tmessage Address {\n\t\toptional string street = 1;\n\t\toptional string city = 2;\n\t}\n\n\toptional Address address = 6;\n}\n";
        assert_eq!(content, expected);

        generate_rust_and_descriptor(proto_path.to_str().unwrap(), "TestMessage", &output_dir)
            .unwrap();
    }

    #[test]
    fn test_nested_structs() {
        let table_info = TableInfo {
            columns: vec![Column {
                name: "outer".to_string(),
                type_text: "STRUCT<id:INT, inner:STRUCT<value:STRING>>".to_string(),
                nullable: false,
            }],
        };

        let dir = tempdir().unwrap();
        let proto_path = dir.path().join("nested.proto");
        let output_dir = dir.path().to_path_buf();

        generate_proto_file(
            "NestedMessage",
            &table_info.columns,
            &proto_path,
            &output_dir,
        )
        .unwrap();

        let content = fs::read_to_string(proto_path.clone()).unwrap();
        let expected = "syntax = \"proto2\";\n\npackage nested;\n\nmessage table_NestedMessage {\n\tmessage Outer {\n\t\toptional int32 id = 1;\n\t\tmessage Inner {\n\t\t\toptional string value = 1;\n\t\t}\n\t\toptional Inner inner = 2;\n\t}\n\n\trequired Outer outer = 1;\n}\n";
        assert_eq!(content, expected);

        generate_rust_and_descriptor(proto_path.to_str().unwrap(), "NestedMessage", &output_dir)
            .unwrap();
    }

    #[test]
    fn test_unsupported_map_key() {
        let table_info = TableInfo {
            columns: vec![Column {
                name: "invalid_map".to_string(),
                type_text: "MAP<STRUCT<a:INT>, STRING>".to_string(),
                nullable: false,
            }],
        };

        let dir = tempdir().unwrap();
        let proto_path = dir.path().join("test.proto");
        let output_dir = dir.path().to_path_buf();

        let result =
            generate_proto_file("TestMessage", &table_info.columns, &proto_path, &output_dir);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Unsupported map key type for Protobuf: STRUCT<a:INT>"
        );
    }

    #[test]
    fn test_nested_map() {
        let table_info = TableInfo {
            columns: vec![Column {
                name: "nested_map".to_string(),
                type_text: "MAP<STRING, MAP<STRING, INT>>".to_string(),
                nullable: false,
            }],
        };

        let dir = tempdir().unwrap();
        let proto_path = dir.path().join("test.proto");
        let output_dir = dir.path().to_path_buf();

        let result =
            generate_proto_file("TestMessage", &table_info.columns, &proto_path, &output_dir);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Protobuf does not support nested maps. Found in: MAP<STRING, MAP<STRING, INT>>"
        );
    }

    #[test]
    fn test_nested_array() {
        let table_info = TableInfo {
            columns: vec![Column {
                name: "nested_array".to_string(),
                type_text: "ARRAY<ARRAY<INT>>".to_string(),
                nullable: false,
            }],
        };

        let dir = tempdir().unwrap();
        let proto_path = dir.path().join("test.proto");
        let output_dir = dir.path().to_path_buf();

        let result =
            generate_proto_file("TestMessage", &table_info.columns, &proto_path, &output_dir);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Direct nested arrays are not supported: ARRAY<INT>"
        );
    }

    #[test]
    fn test_map_field_proto2() {
        let table_info = TableInfo {
            columns: vec![Column {
                name: "attributes".to_string(),
                type_text: "MAP<STRING, INT>".to_string(),
                nullable: false,
            }],
        };

        let dir = tempdir().unwrap();
        let proto_path = dir.path().join("map_test.proto");
        let output_dir = dir.path().to_path_buf();

        generate_proto_file("MapMessage", &table_info.columns, &proto_path, &output_dir).unwrap();

        let content = fs::read_to_string(proto_path.clone()).unwrap();
        let expected = "syntax = \"proto2\";\n\npackage map_test;\n\nmessage table_MapMessage {\n\tmap<string, int32> attributes = 1;\n}\n";
        assert_eq!(content, expected);

        // Also verify that the generated proto is valid and can be compiled.
        generate_rust_and_descriptor(proto_path.to_str().unwrap(), "MapMessage", &output_dir)
            .unwrap();
    }

    #[test]
    fn test_invalid_field_name() {
        let table_info = TableInfo {
            columns: vec![Column {
                name: "invalid-name".to_string(),
                type_text: "STRING".to_string(),
                nullable: false,
            }],
        };

        let dir = tempdir().unwrap();
        let proto_path = dir.path().join("test.proto");
        let output_dir = dir.path().to_path_buf();

        let result =
            generate_proto_file("TestMessage", &table_info.columns, &proto_path, &output_dir);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid Protobuf field name 'invalid-name'")
        );
    }

    #[test]
    fn test_reserved_field_name() {
        let table_info = TableInfo {
            columns: vec![Column {
                name: "message".to_string(),
                type_text: "STRING".to_string(),
                nullable: false,
            }],
        };

        let dir = tempdir().unwrap();
        let proto_path = dir.path().join("test.proto");
        let output_dir = dir.path().to_path_buf();

        let result =
            generate_proto_file("TestMessage", &table_info.columns, &proto_path, &output_dir);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid Protobuf field name 'message'. It is a reserved keyword.")
        );
    }

    #[test]
    fn test_digit_start_field_name() {
        let table_info = TableInfo {
            columns: vec![Column {
                name: "1field".to_string(),
                type_text: "STRING".to_string(),
                nullable: false,
            }],
        };

        let dir = tempdir().unwrap();
        let proto_path = dir.path().join("test.proto");
        let output_dir = dir.path().to_path_buf();

        let result =
            generate_proto_file("TestMessage", &table_info.columns, &proto_path, &output_dir);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid Protobuf field name '1field'. Cannot start with a digit.")
        );
    }
}
