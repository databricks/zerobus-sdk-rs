#!/usr/bin/env python3

import argparse
import base64
import re
import sys
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote, urljoin

import requests


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    description = """
Generate proto2 file from Unity Catalog table schema.
This script fetches table schema from Unity Catalog and generates a corresponding proto2 definition file.
    """

    epilog = """
Examples:
    # Generate proto file for a Unity Catalog table
    # Note: For AWS, use https://your-workspace.cloud.databricks.com
    #       For Azure, use https://your-workspace.azuredatabricks.net
    python generate_proto.py \\
        --uc-endpoint "https://your-workspace.cloud.databricks.com" \\
        --client-id "your-client-id" \\
        --client-secret "your-client-secret" \\
        --table "catalog.schema.table_name" \\
        --proto-msg "TableMessage" \\
        --output "output.proto"

Type mappings:
    Delta            -> Proto2
    TINYINT/BYTE     -> int32
    SMALLINT/SHORT   -> int32
    INT              -> int32
    BIGINT/LONG      -> int64
    STRING           -> string
    FLOAT            -> float
    DOUBLE           -> double
    BOOLEAN          -> bool
    BINARY           -> bytes
    DATE             -> int32
    TIMESTAMP        -> int64
    TIMESTAMP_NTZ    -> int64
    VARIANT          -> string (unshredded, JSON string)
    ARRAY<type>      -> repeated type
    MAP<key_type, value_type> -> map<key_type, value_type>
    STRUCT<field1:type1, field2:type2> -> nested message
    """

    parser = argparse.ArgumentParser(
        description=description,
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--uc-endpoint",
        type=str,
        required=True,
        help="Unity Catalog endpoint URL (e.g., https://your-workspace.cloud.databricks.com for AWS"
        " or https://your-workspace.azuredatabricks.net for Azure)",
    )

    parser.add_argument(
        "--client-id",
        type=str,
        required=True,
        help="OAuth client ID (service principal application ID)",
    )

    parser.add_argument(
        "--client-secret",
        type=str,
        required=True,
        help="OAuth client secret (service principal secret)",
    )

    parser.add_argument(
        "--table",
        type=str,
        required=True,
        help="Full table name in format: catalog.schema.table_name",
    )

    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Output path for the generated proto file (e.g., output.proto)",
    )

    parser.add_argument(
        "--proto-msg",
        type=str,
        required=False,
        help="Name of the protobuf message (defaults to table_name)",
    )

    return parser.parse_args()


def get_oauth_token(uc_endpoint: str, client_id: str, client_secret: str) -> str:
    """
    Obtains an OAuth token using client credentials flow.

    This method uses basic OAuth 2.0 client credentials flow without resource or authorization details.

    Args:
        uc_endpoint: The Unity Catalog endpoint URL
        client_id: The OAuth client ID
        client_secret: The OAuth client secret

    Returns:
        The OAuth access token (JWT)

    Raises:
        requests.exceptions.RequestException: If the token request fails
    """
    url = urljoin(uc_endpoint, "/oidc/v1/token")

    # Build OAuth 2.0 client credentials request with minimal scope
    data = {"grant_type": "client_credentials", "scope": "all-apis"}

    # Encode credentials for HTTP Basic authentication
    credentials = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {credentials}",
    }

    response = requests.post(url, data=data, headers=headers)

    if response.status_code != 200:
        raise requests.exceptions.RequestException(
            f"OAuth request failed with status {response.status_code}: {response.text}"
        )

    response_json = response.json()
    access_token = response_json.get("access_token")

    if not access_token:
        raise requests.exceptions.RequestException("No access token received from OAuth response")

    return access_token


def fetch_table_info(endpoint: str, token: str, table: str) -> Dict[str, str]:
    """
    Fetch table information from Unity Catalog.

    Args:
        endpoint: Base URL of the Unity Catalog endpoint
        token: Authentication token
        table: Table identifier

    Returns:
        Dictionary containing the table information

    Raises:
        requests.exceptions.RequestException: If the HTTP request fails
    """
    encoded_table = quote(table)
    url = urljoin(endpoint, f"/api/2.1/unity-catalog/tables/{encoded_table}")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    return response.json()


def extract_columns(table_info: dict) -> List[Dict[str, str]]:
    """
    Extract column information from the table schema.

    Args:
        table_info: Raw table information from Unity Catalog

    Returns:
        List of dictionaries containing column name and type information

    Raises:
        KeyError: If the expected schema structure is not found
    """
    try:
        columns = table_info["columns"]
        return [
            {
                "name": col["name"],
                "type_text": col["type_text"],
                "nullable": col["nullable"],
            }
            for col in columns
        ]
    except KeyError as e:
        raise KeyError(f"Failed to extract column information: missing key {e}")


def to_pascal_case(s: str) -> str:
    """
    Convert a snake_case string to PascalCase.

    Args:
        s: The string to convert (e.g., "field_name")

    Returns:
        The string in PascalCase (e.g., "FieldName")
    """
    result = ""
    for word in s.split("_"):
        if word:
            result += word[0].upper() + word[1:] if len(word) > 1 else word.upper()
    return result


def parse_array_type(column_type: str) -> Optional[str]:
    """
    Parse array type and extract the element type.

    Args:
        column_type: The Unity Catalog column type (e.g., "array<string>")

    Returns:
        Element type if it's an array, None otherwise
    """
    match = re.match(r"^ARRAY<(.+)>$", column_type.upper())
    if match:
        return column_type[6:-1].strip()
    return None


def parse_map_type(column_type: str) -> Optional[Tuple[str, str]]:
    """
    Parse map type and extract key and value types.

    Args:
        column_type: The Unity Catalog column type (e.g., "map<string,int>")

    Returns:
        Tuple of (key_type, value_type) if it's a map, None otherwise
    """
    upper_type = column_type.strip().upper()
    if not upper_type.startswith("MAP<") or not upper_type.endswith(">"):
        return None

    inner = column_type[4 : len(column_type) - 1]

    depth = 0
    split_index = 0

    for i, c in enumerate(inner):
        if c == "<":
            depth += 1
        elif c == ">":
            depth -= 1
        elif c == "," and depth == 0:
            split_index = i
            break

    if split_index == 0:
        return None

    key_type = inner[:split_index].strip()
    value_type = inner[split_index + 1 :].strip()

    if not key_type or not value_type:
        return None

    return (key_type, value_type)


def parse_struct_type(column_type: str) -> Optional[List[Tuple[str, str]]]:
    """
    Parse struct type and extract field names and types.

    Args:
        column_type: The Unity Catalog column type (e.g., "STRUCT<field1:STRING, field2:INT>")

    Returns:
        List of (field_name, field_type) tuples if it's a struct, None otherwise
    """
    match = re.match(r"^STRUCT<\s*(.+)\s*>$", column_type.upper(), re.IGNORECASE)
    if not match:
        return None

    inner = column_type[7 : len(column_type) - 1]

    fields = []
    depth = 0
    current = ""

    for c in inner:
        if c == "<":
            depth += 1
            current += c
        elif c == ">":
            depth -= 1
            current += c
        elif c == "," and depth == 0:
            fields.append(current.strip())
            current = ""
        else:
            current += c

    if current.strip():
        fields.append(current.strip())

    result = []
    for field in fields:
        parts = field.split(":", 1)
        if len(parts) == 2:
            field_name = parts[0].strip()
            field_type = parts[1].strip()
            result.append((field_name, field_type))
        else:
            return None

    return result if result else None


def validate_field_name(name: str) -> str:
    """
    Validate field names for Protobuf compatibility.

    Args:
        name: The field name to validate

    Returns:
        The validated field name

    Raises:
        ValueError: If the field name is invalid
    """
    reserved = [
        "syntax",
        "import",
        "option",
        "package",
        "message",
        "enum",
        "service",
        "rpc",
        "returns",
        "reserved",
        "to",
        "max",
        "double",
        "float",
        "int32",
        "int64",
        "uint32",
        "uint64",
        "sint32",
        "sint64",
        "fixed32",
        "fixed64",
        "sfixed32",
        "sfixed64",
        "bool",
        "string",
        "bytes",
    ]

    # Check for non-alphanumeric characters (besides underscore)
    if not all(c.isalnum() or c == "_" for c in name):
        raise ValueError(
            f"Invalid Protobuf field name '{name}'. Contains non-alphanumeric characters (besides underscore)."
        )

    # Check if name starts with a digit
    if name and name[0].isdigit():
        raise ValueError(f"Invalid Protobuf field name '{name}'. Cannot start with a digit.")

    # Check if name is a reserved keyword
    if name in reserved:
        raise ValueError(f"Invalid Protobuf field name '{name}'. It is a reserved keyword.")

    return name


def get_proto_field_info(
    field_name: str,
    column_type: str,
    nullable: bool,
    struct_counter: Dict[str, int],
    level: int = 0,
) -> Tuple[str, str, Optional[str]]:
    """
    Map Unity Catalog column types to proto2 field information.

    Args:
        field_name: The field name (used for naming struct messages)
        column_type: The Unity Catalog column type
        nullable: Whether the column is nullable
        struct_counter: Dictionary to track struct counter (mutable)
        level: Current nesting level (to prevent infinite recursion)

    Returns:
        Tuple of (field_modifier, proto_type, nested_definition) where:
        - field_modifier is 'optional', 'repeated' or empty string in case of a map
        - proto_type is the protobuf type
        - nested_definition is the nested message definition if applicable (None otherwise)

    Raises:
        ValueError: If the column type is not supported or nesting is too deep
    """
    if level > 100:
        raise ValueError("Nesting level exceeds maximum depth of 100")

    col_type = column_type.strip().upper()

    # Base scalar types
    type_mapping = {
        "TINYINT": "int32",
        "BYTE": "int32",
        "SMALLINT": "int32",
        "SHORT": "int32",
        "INT": "int32",
        "BIGINT": "int64",
        "LONG": "int64",
        "FLOAT": "float",
        "DOUBLE": "double",
        "STRING": "string",
        "BOOLEAN": "bool",
        "BINARY": "bytes",
        "DATE": "int32",
        "TIMESTAMP": "int64",
        "TIMESTAMP_NTZ": "int64",
        "VARIANT": "string",
    }

    proto_type = type_mapping.get(col_type)
    if proto_type is not None:
        return ("optional" if nullable else "required", proto_type, None)

    if col_type.startswith("VARCHAR"):
        return ("optional" if nullable else "required", "string", None)

    # Handle arrays
    element_type = parse_array_type(column_type)
    if element_type is not None:
        # Check for nested arrays (not supported)
        if parse_array_type(element_type) is not None:
            raise ValueError("Nested arrays are not supported: array<array<...>>")

        # Check for array of maps (not supported)
        if parse_map_type(element_type) is not None:
            raise ValueError("Arrays of maps are not supported: array<map<...>>")

        modifier, elem_proto_type, nested_def = get_proto_field_info(
            field_name, element_type, False, struct_counter, level + 1
        )
        return ("repeated", elem_proto_type, nested_def)

    # Handle maps
    map_types = parse_map_type(column_type)
    if map_types is not None:
        key_type, value_type = map_types

        # Protobuf map keys cannot be maps
        if parse_map_type(key_type) is not None:
            raise ValueError("Maps with map keys are not supported: map<map<...>, ...>")

        # Protobuf map keys cannot be arrays
        if parse_array_type(key_type) is not None:
            raise ValueError("Maps with array keys are not supported: map<array<...>, ...>")

        # Protobuf map keys must be integral or string types
        _, key_proto_type, key_nested_def = get_proto_field_info(field_name, key_type, False, struct_counter, level + 1)

        valid_key_types = [
            "int32",
            "int64",
            "uint32",
            "uint64",
            "sint32",
            "sint64",
            "fixed32",
            "fixed64",
            "sfixed32",
            "sfixed64",
            "bool",
            "string",
        ]

        if key_nested_def is not None or key_proto_type not in valid_key_types:
            raise ValueError(f"Unsupported map key type for Protobuf: {key_type}")

        # Protobuf map values cannot be other maps
        if parse_map_type(value_type) is not None:
            raise ValueError("Maps with map values are not supported: map<..., map<...>>")

        # Protobuf map values cannot be arrays
        if parse_array_type(value_type) is not None:
            raise ValueError("Maps with array values are not supported: map<..., array<...>>")

        _, value_proto_type, value_nested_def = get_proto_field_info(
            field_name, value_type, False, struct_counter, level + 1
        )

        map_type = f"map<{key_proto_type}, {value_proto_type}>"

        # Map fields cannot be repeated, and are not marked optional/required
        return ("", map_type, value_nested_def)

    # Handle structs
    struct_fields = parse_struct_type(column_type)
    if struct_fields is not None:
        struct_counter["count"] += 1
        base_name = to_pascal_case(field_name)
        struct_name = base_name if base_name else f"Struct{struct_counter['count']}"

        indent = "\t" * level
        inner_indent = "\t" * (level + 1)

        struct_def = f"{indent}message {struct_name} {{\n"

        for i, (fname, ftype) in enumerate(struct_fields, start=1):
            # Struct fields are always optional to avoid issues with required fields
            modifier, field_type, nested_def = get_proto_field_info(fname, ftype, True, struct_counter, level + 1)

            if nested_def is not None:
                struct_def += nested_def + "\n\n"

            cleaned_name = validate_field_name(fname)
            if modifier == "":
                struct_def += f"{inner_indent}{field_type} {cleaned_name} = {i};\n"
            else:
                struct_def += f"{inner_indent}{modifier} {field_type} {cleaned_name} = {i};\n"

        struct_def += f"{indent}}}"

        return ("optional" if nullable else "required", struct_name, struct_def)

    raise ValueError(f"Unknown column type: {column_type}")


def generate_proto_file(message_name: str, columns: List[Dict[str, str]], output_path: str) -> None:
    """
    Generate a proto2 file from the column information.

    Args:
        message_name: Name of the protobuf message
        columns: List of column information dictionaries
        output_path: Path where to write the proto file
    """
    struct_counter = {"count": 0}
    proto_content = ['syntax = "proto2";', ""]

    # Collect all field definitions and nested message definitions
    fields_and_definitions = ""

    field_number = 1
    for col in columns:
        # Skip protobuf reserved field numbers 19000-19999
        if field_number == 19000:
            field_number = 20000

        field_modifier, proto_type, nested_def = get_proto_field_info(
            col["name"], col["type_text"], col["nullable"], struct_counter, 1
        )

        # Add nested message definition if present
        if nested_def is not None:
            fields_and_definitions += nested_def + "\n\n"

        # Validate field name
        field_name = validate_field_name(col["name"])

        # Add field definition
        if field_modifier == "":
            fields_and_definitions += f"\t{proto_type} {field_name} = {field_number};\n"
        else:
            fields_and_definitions += f"\t{field_modifier} {proto_type} {field_name} = {field_number};\n"

        field_number += 1

    # Construct the main message
    proto_content.append(f"message {message_name} {{")
    proto_content.append(fields_and_definitions.rstrip("\n"))
    proto_content.append("}")
    proto_content.append("")

    with open(output_path, "w") as f:
        f.write("\n".join(proto_content))


def main() -> Optional[int]:
    """Main function to process the arguments and execute the script logic."""
    args = parse_args()

    try:
        # Get OAuth token using client credentials
        token = get_oauth_token(args.uc_endpoint, args.client_id, args.client_secret)

        # Fetch table information from Unity Catalog
        table_info = fetch_table_info(args.uc_endpoint, token, args.table)

        # Extract column information
        columns = extract_columns(table_info)

        # If proto_msg is not provided, use the table name
        message_name = args.proto_msg if args.proto_msg else args.table.split(".")[-1]

        # Generate proto file
        generate_proto_file(message_name, columns, args.output)

        print(f"Successfully generated proto file at: {args.output}")
        return 0

    except requests.exceptions.RequestException as e:
        print(f"Error making request to Unity Catalog: {e}", file=sys.stderr)
        return 1
    except KeyError as e:
        print(f"Error processing table schema: {e}", file=sys.stderr)
        return 1
    except ValueError as e:
        print(f"Error mapping column type: {e}", file=sys.stderr)
        return 1
    except IOError as e:
        print(f"Error writing proto file: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
