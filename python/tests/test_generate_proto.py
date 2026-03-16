"""Tests for generate_proto.py pure functions (no mocking required)."""

import os
import tempfile

import pytest

from zerobus.tools.generate_proto import (
    extract_columns,
    generate_proto_file,
    get_proto_field_info,
    parse_array_type,
    parse_map_type,
    parse_struct_type,
    to_pascal_case,
    validate_field_name,
)


class TestToPascalCase:
    def test_single_word(self):
        assert to_pascal_case("hello") == "Hello"

    def test_snake_case(self):
        assert to_pascal_case("hello_world") == "HelloWorld"

    def test_multiple_underscores(self):
        assert to_pascal_case("hello_world_test") == "HelloWorldTest"

    def test_already_capitalized(self):
        assert to_pascal_case("Hello_World") == "HelloWorld"

    def test_empty_string(self):
        assert to_pascal_case("") == ""

    def test_single_char(self):
        assert to_pascal_case("a") == "A"

    def test_consecutive_underscores(self):
        assert to_pascal_case("hello__world") == "HelloWorld"


class TestParseArrayType:
    def test_simple_array(self):
        assert parse_array_type("ARRAY<STRING>") == "STRING"

    def test_array_lowercase(self):
        assert parse_array_type("array<string>") == "string"

    def test_array_with_spaces(self):
        # Leading/trailing spaces are stripped from element type
        assert parse_array_type("ARRAY< STRING >") == "STRING"

    def test_nested_array(self):
        assert parse_array_type("ARRAY<ARRAY<INT>>") == "ARRAY<INT>"

    def test_array_of_struct(self):
        assert parse_array_type("ARRAY<STRUCT<a:INT>>") == "STRUCT<a:INT>"

    def test_not_array(self):
        assert parse_array_type("STRING") is None

    def test_map_type(self):
        assert parse_array_type("MAP<STRING,INT>") is None


class TestParseMapType:
    def test_simple_map(self):
        assert parse_map_type("MAP<STRING,INT>") == ("STRING", "INT")

    def test_map_lowercase(self):
        assert parse_map_type("map<string,int>") == ("string", "int")

    def test_map_with_spaces(self):
        assert parse_map_type("MAP<STRING, INT>") == ("STRING", "INT")

    def test_map_with_nested_value(self):
        result = parse_map_type("MAP<STRING,STRUCT<a:INT>>")
        assert result == ("STRING", "STRUCT<a:INT>")

    def test_map_with_array_value(self):
        result = parse_map_type("MAP<STRING,ARRAY<INT>>")
        assert result == ("STRING", "ARRAY<INT>")

    def test_not_map(self):
        assert parse_map_type("STRING") is None

    def test_array_type(self):
        assert parse_map_type("ARRAY<STRING>") is None

    def test_invalid_map_no_comma(self):
        assert parse_map_type("MAP<STRING>") is None


class TestParseStructType:
    def test_simple_struct(self):
        result = parse_struct_type("STRUCT<name:STRING>")
        assert result == [("name", "STRING")]

    def test_multiple_fields(self):
        result = parse_struct_type("STRUCT<name:STRING,age:INT>")
        assert result == [("name", "STRING"), ("age", "INT")]

    def test_struct_with_spaces(self):
        result = parse_struct_type("STRUCT<name: STRING, age: INT>")
        assert result == [("name", "STRING"), ("age", "INT")]

    def test_nested_struct(self):
        result = parse_struct_type("STRUCT<outer:STRUCT<inner:INT>>")
        assert result == [("outer", "STRUCT<inner:INT>")]

    def test_struct_with_array_field(self):
        result = parse_struct_type("STRUCT<items:ARRAY<STRING>>")
        assert result == [("items", "ARRAY<STRING>")]

    def test_struct_with_map_field(self):
        result = parse_struct_type("STRUCT<data:MAP<STRING,INT>>")
        assert result == [("data", "MAP<STRING,INT>")]

    def test_not_struct(self):
        assert parse_struct_type("STRING") is None

    def test_array_type(self):
        assert parse_struct_type("ARRAY<STRING>") is None


class TestValidateFieldName:
    def test_valid_name(self):
        assert validate_field_name("field_name") == "field_name"

    def test_valid_name_with_numbers(self):
        assert validate_field_name("field123") == "field123"

    def test_reserved_keyword(self):
        with pytest.raises(ValueError, match="reserved keyword"):
            validate_field_name("message")

    def test_reserved_type(self):
        with pytest.raises(ValueError, match="reserved keyword"):
            validate_field_name("string")

    def test_starts_with_digit(self):
        with pytest.raises(ValueError, match="Cannot start with a digit"):
            validate_field_name("123field")

    def test_special_characters(self):
        with pytest.raises(ValueError, match="non-alphanumeric"):
            validate_field_name("field-name")

    def test_spaces(self):
        with pytest.raises(ValueError, match="non-alphanumeric"):
            validate_field_name("field name")


class TestGetProtoFieldInfo:
    """Tests for type mapping from Delta types to Proto2 types."""

    # Scalar types
    def test_tinyint(self):
        modifier, proto_type, nested = get_proto_field_info(
            "f", "TINYINT", True, {"count": 0}
        )
        assert proto_type == "int32"
        assert modifier == "optional"
        assert nested is None

    def test_byte(self):
        modifier, proto_type, nested = get_proto_field_info(
            "f", "BYTE", True, {"count": 0}
        )
        assert proto_type == "int32"
        assert modifier == "optional"
        assert nested is None

    def test_smallint(self):
        modifier, proto_type, nested = get_proto_field_info(
            "f", "SMALLINT", True, {"count": 0}
        )
        assert proto_type == "int32"
        assert modifier == "optional"
        assert nested is None

    def test_short(self):
        modifier, proto_type, nested = get_proto_field_info(
            "f", "SHORT", True, {"count": 0}
        )
        assert proto_type == "int32"
        assert modifier == "optional"
        assert nested is None

    def test_int(self):
        modifier, proto_type, nested = get_proto_field_info(
            "f", "INT", True, {"count": 0}
        )
        assert proto_type == "int32"
        assert modifier == "optional"
        assert nested is None

    def test_bigint(self):
        modifier, proto_type, nested = get_proto_field_info(
            "f", "BIGINT", True, {"count": 0}
        )
        assert proto_type == "int64"
        assert modifier == "optional"
        assert nested is None

    def test_long(self):
        modifier, proto_type, nested = get_proto_field_info(
            "f", "LONG", True, {"count": 0}
        )
        assert proto_type == "int64"
        assert modifier == "optional"
        assert nested is None

    def test_float(self):
        modifier, proto_type, nested = get_proto_field_info(
            "f", "FLOAT", True, {"count": 0}
        )
        assert proto_type == "float"
        assert modifier == "optional"
        assert nested is None

    def test_double(self):
        modifier, proto_type, nested = get_proto_field_info(
            "f", "DOUBLE", True, {"count": 0}
        )
        assert proto_type == "double"
        assert modifier == "optional"
        assert nested is None

    def test_string(self):
        modifier, proto_type, nested = get_proto_field_info(
            "f", "STRING", True, {"count": 0}
        )
        assert proto_type == "string"
        assert modifier == "optional"
        assert nested is None

    def test_varchar(self):
        modifier, proto_type, nested = get_proto_field_info(
            "f", "VARCHAR(255)", True, {"count": 0}
        )
        assert proto_type == "string"
        assert modifier == "optional"
        assert nested is None

    def test_boolean(self):
        modifier, proto_type, nested = get_proto_field_info(
            "f", "BOOLEAN", True, {"count": 0}
        )
        assert proto_type == "bool"
        assert modifier == "optional"
        assert nested is None

    def test_binary(self):
        modifier, proto_type, nested = get_proto_field_info(
            "f", "BINARY", True, {"count": 0}
        )
        assert proto_type == "bytes"
        assert modifier == "optional"
        assert nested is None

    def test_date(self):
        modifier, proto_type, nested = get_proto_field_info(
            "f", "DATE", True, {"count": 0}
        )
        assert proto_type == "int32"
        assert modifier == "optional"
        assert nested is None

    def test_timestamp(self):
        modifier, proto_type, nested = get_proto_field_info(
            "f", "TIMESTAMP", True, {"count": 0}
        )
        assert proto_type == "int64"
        assert modifier == "optional"
        assert nested is None

    def test_timestamp_ntz(self):
        modifier, proto_type, nested = get_proto_field_info(
            "f", "TIMESTAMP_NTZ", True, {"count": 0}
        )
        assert proto_type == "int64"
        assert modifier == "optional"
        assert nested is None

    def test_variant(self):
        modifier, proto_type, nested = get_proto_field_info(
            "f", "VARIANT", True, {"count": 0}
        )
        assert proto_type == "string"
        assert modifier == "optional"
        assert nested is None

    # Nullable vs required
    def test_nullable_true(self):
        modifier, proto_type, nested = get_proto_field_info(
            "f", "INT", True, {"count": 0}
        )
        assert modifier == "optional"

    def test_nullable_false(self):
        modifier, proto_type, nested = get_proto_field_info(
            "f", "INT", False, {"count": 0}
        )
        assert modifier == "required"

    # Case insensitivity
    def test_lowercase_type(self):
        modifier, proto_type, nested = get_proto_field_info(
            "f", "timestamp_ntz", True, {"count": 0}
        )
        assert proto_type == "int64"

    def test_mixed_case_type(self):
        modifier, proto_type, nested = get_proto_field_info(
            "f", "Variant", True, {"count": 0}
        )
        assert proto_type == "string"

    # Array types
    def test_array_of_string(self):
        modifier, proto_type, nested = get_proto_field_info(
            "f", "ARRAY<STRING>", True, {"count": 0}
        )
        assert proto_type == "string"
        assert modifier == "repeated"
        assert nested is None

    def test_array_of_int(self):
        modifier, proto_type, nested = get_proto_field_info(
            "f", "ARRAY<INT>", True, {"count": 0}
        )
        assert proto_type == "int32"
        assert modifier == "repeated"
        assert nested is None

    def test_array_of_struct(self):
        modifier, proto_type, nested = get_proto_field_info(
            "items", "ARRAY<STRUCT<name:STRING>>", True, {"count": 0}
        )
        assert modifier == "repeated"
        assert proto_type == "Items"
        assert nested is not None
        assert "message Items" in nested

    def test_nested_array_raises(self):
        with pytest.raises(ValueError, match="Nested arrays are not supported"):
            get_proto_field_info("f", "ARRAY<ARRAY<INT>>", True, {"count": 0})

    def test_array_of_map_raises(self):
        with pytest.raises(ValueError, match="Arrays of maps are not supported"):
            get_proto_field_info("f", "ARRAY<MAP<STRING,INT>>", True, {"count": 0})

    # Map types
    def test_map_string_to_int(self):
        modifier, proto_type, nested = get_proto_field_info(
            "f", "MAP<STRING,INT>", True, {"count": 0}
        )
        assert proto_type == "map<string, int32>"
        assert modifier == ""
        assert nested is None

    def test_map_int_to_string(self):
        modifier, proto_type, nested = get_proto_field_info(
            "f", "MAP<INT,STRING>", True, {"count": 0}
        )
        assert proto_type == "map<int32, string>"
        assert modifier == ""
        assert nested is None

    def test_map_with_struct_value(self):
        modifier, proto_type, nested = get_proto_field_info(
            "data", "MAP<STRING,STRUCT<a:INT>>", True, {"count": 0}
        )
        assert modifier == ""
        assert "map<string, Data>" in proto_type
        assert nested is not None

    def test_map_with_map_key_raises(self):
        with pytest.raises(ValueError, match="Maps with map keys are not supported"):
            get_proto_field_info("f", "MAP<MAP<STRING,INT>,STRING>", True, {"count": 0})

    def test_map_with_array_key_raises(self):
        with pytest.raises(ValueError, match="Maps with array keys are not supported"):
            get_proto_field_info("f", "MAP<ARRAY<INT>,STRING>", True, {"count": 0})

    def test_map_with_map_value_raises(self):
        with pytest.raises(ValueError, match="Maps with map values are not supported"):
            get_proto_field_info("f", "MAP<STRING,MAP<INT,STRING>>", True, {"count": 0})

    def test_map_with_array_value_raises(self):
        with pytest.raises(
            ValueError, match="Maps with array values are not supported"
        ):
            get_proto_field_info("f", "MAP<STRING,ARRAY<INT>>", True, {"count": 0})

    # Struct types
    def test_simple_struct(self):
        modifier, proto_type, nested = get_proto_field_info(
            "address", "STRUCT<city:STRING,zip:INT>", True, {"count": 0}
        )
        assert modifier == "optional"
        assert proto_type == "Address"
        assert nested is not None
        assert "message Address" in nested
        assert "optional string city" in nested
        assert "optional int32 zip" in nested

    def test_nested_struct(self):
        modifier, proto_type, nested = get_proto_field_info(
            "outer", "STRUCT<inner:STRUCT<value:INT>>", True, {"count": 0}
        )
        assert modifier == "optional"
        assert proto_type == "Outer"
        assert nested is not None
        assert "message Outer" in nested
        assert "message Inner" in nested

    # Unknown type
    def test_unknown_type_raises(self):
        with pytest.raises(ValueError, match="Unknown column type"):
            get_proto_field_info("f", "UNKNOWN_TYPE", True, {"count": 0})

    # Nesting depth
    def test_max_nesting_depth_raises(self):
        with pytest.raises(ValueError, match="Nesting level exceeds maximum depth"):
            get_proto_field_info("f", "INT", True, {"count": 0}, level=101)


class TestExtractColumns:
    def test_extract_simple_columns(self):
        table_info = {
            "columns": [
                {"name": "id", "type_text": "INT", "nullable": False},
                {"name": "name", "type_text": "STRING", "nullable": True},
            ]
        }
        columns = extract_columns(table_info)
        assert len(columns) == 2
        assert columns[0] == {"name": "id", "type_text": "INT", "nullable": False}
        assert columns[1] == {"name": "name", "type_text": "STRING", "nullable": True}

    def test_missing_columns_key(self):
        with pytest.raises(KeyError):
            extract_columns({})

    def test_missing_column_field(self):
        table_info = {
            "columns": [{"name": "id", "type_text": "INT"}]
        }  # missing nullable
        with pytest.raises(KeyError):
            extract_columns(table_info)


class TestGenerateProtoFile:
    def test_generate_simple_proto(self):
        columns = [
            {"name": "id", "type_text": "INT", "nullable": False},
            {"name": "name", "type_text": "STRING", "nullable": True},
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".proto", delete=False) as f:
            output_path = f.name

        try:
            generate_proto_file("TestMessage", columns, output_path)

            with open(output_path) as f:
                content = f.read()

            assert 'syntax = "proto2";' in content
            assert "message TestMessage {" in content
            assert "required int32 id = 1;" in content
            assert "optional string name = 2;" in content
        finally:
            os.unlink(output_path)

    def test_generate_proto_with_timestamp_ntz(self):
        columns = [
            {"name": "event_time", "type_text": "TIMESTAMP_NTZ", "nullable": True}
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".proto", delete=False) as f:
            output_path = f.name

        try:
            generate_proto_file("Event", columns, output_path)

            with open(output_path) as f:
                content = f.read()

            assert "optional int64 event_time = 1;" in content
        finally:
            os.unlink(output_path)

    def test_generate_proto_with_variant(self):
        columns = [{"name": "payload", "type_text": "VARIANT", "nullable": True}]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".proto", delete=False) as f:
            output_path = f.name

        try:
            generate_proto_file("Record", columns, output_path)

            with open(output_path) as f:
                content = f.read()

            assert "optional string payload = 1;" in content
        finally:
            os.unlink(output_path)

    def test_generate_proto_with_all_new_types(self):
        columns = [
            {"name": "byte_field", "type_text": "BYTE", "nullable": True},
            {"name": "ts_ntz_field", "type_text": "TIMESTAMP_NTZ", "nullable": False},
            {"name": "variant_field", "type_text": "VARIANT", "nullable": True},
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".proto", delete=False) as f:
            output_path = f.name

        try:
            generate_proto_file("NewTypes", columns, output_path)

            with open(output_path) as f:
                content = f.read()

            assert "optional int32 byte_field = 1;" in content
            assert "required int64 ts_ntz_field = 2;" in content
            assert "optional string variant_field = 3;" in content
        finally:
            os.unlink(output_path)

    def test_generate_proto_with_array(self):
        columns = [{"name": "tags", "type_text": "ARRAY<STRING>", "nullable": True}]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".proto", delete=False) as f:
            output_path = f.name

        try:
            generate_proto_file("Tagged", columns, output_path)

            with open(output_path) as f:
                content = f.read()

            assert "repeated string tags = 1;" in content
        finally:
            os.unlink(output_path)

    def test_generate_proto_with_map(self):
        columns = [
            {"name": "metadata", "type_text": "MAP<STRING,INT>", "nullable": True}
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".proto", delete=False) as f:
            output_path = f.name

        try:
            generate_proto_file("WithMetadata", columns, output_path)

            with open(output_path) as f:
                content = f.read()

            assert "map<string, int32> metadata = 1;" in content
        finally:
            os.unlink(output_path)

    def test_generate_proto_with_struct(self):
        columns = [
            {
                "name": "address",
                "type_text": "STRUCT<city:STRING,zip:INT>",
                "nullable": True,
            }
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".proto", delete=False) as f:
            output_path = f.name

        try:
            generate_proto_file("Person", columns, output_path)

            with open(output_path) as f:
                content = f.read()

            assert "message Person {" in content
            assert "message Address {" in content
            assert "optional string city = 1;" in content
            assert "optional int32 zip = 2;" in content
            assert "optional Address address = 1;" in content
        finally:
            os.unlink(output_path)
