# NEXT CHANGELOG

## Release v1.3.0

### Major Changes

### New Features and Improvements

- **C-builder API for SDK construction**: `zerobus_sdk_builder_new`, per-option setters (`_endpoint`, `_unity_catalog_url`, `_sdk_identifier`, `_application_name`, `_disable_tls`), and `_build` / `_free`. Mirrors the Rust `ZerobusSdkBuilder`; new options are added as setters without ABI breaks. Legacy `zerobus_sdk_new` is retained and delegates to the builder.
- **Dynamic protobuf from a Unity Catalog schema**: a pure-C consumer can now follow the same UC-schema → protobuf-descriptor → record-encoding path the Vector sink uses, without a companion Rust crate. New opaque type `CZerobusProtoSchema` and functions:
  - `zerobus_proto_schema_from_uc_json` — build a schema handle from UC table-metadata JSON (the body of `GET /api/2.1/unity-catalog/tables/{name}`).
  - `zerobus_proto_schema_descriptor_bytes` — borrow the serialized `DescriptorProto` to pass straight to `zerobus_sdk_create_stream` (byte-identical to the descriptor the encoder uses).
  - `zerobus_proto_schema_encode_json` — encode one JSON record into protobuf bytes; unknown keys are ignored. `DATE`/`TIMESTAMP`/`TIMESTAMP_NTZ` columns are integers (days / micros since epoch), `BINARY` is a base64 string, `DECIMAL` is a string, and 64-bit integers above 2^53 should be passed as strings to avoid JSON precision loss. Non-nullable columns are proto2 `required`; a record missing one is rejected.
  - `zerobus_free_proto_bytes` / `zerobus_proto_schema_free` — free an encoded buffer / a schema handle.

### Bug Fixes

### Documentation

### Internal Changes

### Behavior Changes

### Breaking Changes

### Deprecations

### API Changes
