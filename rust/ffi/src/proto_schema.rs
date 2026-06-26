//! Dynamic protobuf schema FFI surface.

use crate::common::*;
use databricks_zerobus_ingest_sdk::schema::{descriptor_from_uc_schema, UcTableSchema};
use prost::Message;
use prost_reflect::{
    Cardinality, DescriptorPool, DeserializeOptions, DynamicMessage, Kind, MapKey,
    MessageDescriptor, ReflectMessage, Value,
};
use std::fmt::Write;
use std::os::raw::c_char;
use std::ptr;

// ============================================================================
// Dynamic Protobuf FFI
// ============================================================================
//
// Pure-C consumers can build a protobuf descriptor from Unity Catalog metadata
// and encode JSON records to protobuf bytes without a companion Rust crate.
// Lifecycle: `_from_uc_json` → `_descriptor_bytes` / `_encode_json` → `_free`.

/// Opaque handle to a table's protobuf schema: its serialized descriptor plus a
/// prepared encoder. C code only ever holds a pointer to it; the backing
/// allocation is owned by the SDK and released by zerobus_proto_schema_free.
#[repr(C)]
pub struct CZerobusProtoSchema {
    _private: [u8; 0],
}

/// Concrete type behind `*mut CZerobusProtoSchema`.
struct ProtoSchema {
    /// Serialized `DescriptorProto` bytes (passed to `zerobus_sdk_create_stream`).
    descriptor_bytes: Vec<u8>,
    /// Message descriptor for encoding JSON records to protobuf.
    message: MessageDescriptor,
}

/// Null-check a schema handle and borrow the [`ProtoSchema`] behind it.
///
/// # Safety
///
/// `schema` must be null or a live handle from
/// [`zerobus_proto_schema_from_uc_json`]. The caller must not free the handle
/// (via [`zerobus_proto_schema_free`]) for the lifetime of the returned borrow.
unsafe fn proto_schema_ref<'a>(
    schema: *const CZerobusProtoSchema,
) -> Result<&'a ProtoSchema, &'static str> {
    if schema.is_null() {
        return Err("Proto schema pointer is null");
    }
    Ok(&*(schema as *const ProtoSchema))
}

/// Builds a [`ProtoSchema`] from Unity Catalog table-metadata JSON.
fn build_proto_schema(uc_table_json: &str) -> Result<ProtoSchema, String> {
    let schema: UcTableSchema = serde_json::from_str(uc_table_json)
        .map_err(|e| format!("failed to parse Unity Catalog table JSON: {e}"))?;
    let descriptor = descriptor_from_uc_schema(&schema).map_err(|e| e.to_string())?;
    let descriptor_bytes = descriptor.encode_to_vec();
    let message_name = descriptor.name().to_string();

    let file = prost_types::FileDescriptorProto {
        name: Some("zerobus_dynamic.proto".to_string()),
        message_type: vec![descriptor],
        ..Default::default()
    };
    let mut pool = DescriptorPool::new();
    pool.add_file_descriptor_proto(file)
        .map_err(|e| format!("failed to build descriptor pool: {e}"))?;
    // No package on the synthetic file, so the fully-qualified name is the
    // bare message name.
    let message = pool
        .get_message_by_name(&message_name)
        .ok_or_else(|| format!("message '{message_name}' not found in descriptor pool"))?;

    Ok(ProtoSchema {
        descriptor_bytes,
        message,
    })
}

/// Build a protobuf schema from Unity Catalog table metadata JSON.
/// Returns NULL on error; free with `zerobus_proto_schema_free`.
#[no_mangle]
pub extern "C" fn zerobus_proto_schema_from_uc_json(
    uc_table_json: *const c_char,
    result: *mut CResult,
) -> *mut CZerobusProtoSchema {
    let json = match unsafe { c_str_to_string(uc_table_json) } {
        Ok(s) => s,
        Err(e) => {
            write_error_result(result, e, false);
            return ptr::null_mut();
        }
    };

    match build_proto_schema(&json) {
        Ok(schema) => {
            write_success_result(result);
            // Hand ownership of the allocation to C as a raw pointer; it is
            // reclaimed by zerobus_proto_schema_free.
            Box::into_raw(Box::new(schema)) as *mut CZerobusProtoSchema
        }
        Err(err) => {
            write_error_result(result, &err, false);
            ptr::null_mut()
        }
    }
}

/// Borrow the serialized descriptor bytes. Valid until `zerobus_proto_schema_free`.
/// Pass directly to `zerobus_sdk_create_stream`.
///
/// `out_len` is required: the bytes are not null-terminated, so the caller needs
/// the length to read them. Returns NULL without touching `out_len` if it is
/// NULL, and NULL with `*out_len` set to 0 on a null handle.
#[no_mangle]
pub extern "C" fn zerobus_proto_schema_descriptor_bytes(
    schema: *const CZerobusProtoSchema,
    out_len: *mut usize,
) -> *const u8 {
    // The bytes are not null-terminated, so a pointer without a length is
    // unusable. Refuse rather than hand back something the caller can't size.
    if out_len.is_null() {
        return ptr::null();
    }
    // SAFETY: caller upholds the handle contract (valid, unfreed handle).
    let schema_ref = match unsafe { proto_schema_ref(schema) } {
        Ok(s) => s,
        Err(_) => {
            unsafe {
                *out_len = 0;
            }
            return ptr::null();
        }
    };
    unsafe {
        *out_len = schema_ref.descriptor_bytes.len();
    }
    // Valid until the caller's `_free`, which owns the backing allocation.
    schema_ref.descriptor_bytes.as_ptr()
}

/// Recursively collect the paths of `Required` (proto2) fields absent from
/// `message`, descending into present message-typed values.
///
/// prost-reflect does not enforce `required` presence on encode, and walking
/// only top-level fields misses required fields nested inside a `STRUCT`, inside
/// each element of an `ARRAY<STRUCT>`, or inside a `MAP` value. Paths use dotted
/// field names with `[i]` / `[key]` segments for list elements and map values
/// (e.g. `addr.zip`, `items[2].id`, `props[home].zip`).
///
/// `path` is a scratch buffer reused across the whole walk: each frame appends
/// its segment, recurses, then truncates back. This keeps the hot path (a valid
/// record) allocation-free — a heap `String` is produced only when a missing
/// field is actually recorded (`path.clone()`), not once per field visited.
///
/// Recursion depth is bounded by the descriptor depth, itself capped at
/// schema-build time (`MAX_NESTING_DEPTH` in `type_json` parsing), so a
/// pathological record cannot blow the stack here.
fn collect_missing_required_fields(
    message: &DynamicMessage,
    path: &mut String,
    missing: &mut Vec<String>,
) {
    for field in message.descriptor().fields() {
        // Append `.<field>` (or just `<field>` at the root), remembering where to
        // cut back to so the buffer is restored for the next sibling.
        let base = path.len();
        if !path.is_empty() {
            path.push('.');
        }
        path.push_str(field.name());

        if matches!(field.cardinality(), Cardinality::Required) && !message.has_field(&field) {
            // Absent required field: record it; there is nothing to descend into.
            missing.push(path.clone());
        } else if matches!(field.kind(), Kind::Message(_)) && message.has_field(&field) {
            // Descend into present message-typed values — singular structs, array
            // elements, and map values — to validate their required fields too.
            // Scalar fields (including arrays/maps of scalars) have no nested
            // requirements, and an absent message field imposes none.
            let value = message.get_field(&field);
            descend_into_messages(&value, path, missing);
        }

        path.truncate(base);
    }
}

/// Recurse into every [`DynamicMessage`] reachable from `value` — itself, list
/// elements, or map values — accumulating missing required-field paths. Index
/// (`[i]`) and map-key (`[key]`) segments are appended to and truncated from the
/// shared `path` buffer, mirroring [`collect_missing_required_fields`].
fn descend_into_messages(value: &Value, path: &mut String, missing: &mut Vec<String>) {
    match value {
        Value::Message(m) => collect_missing_required_fields(m, path, missing),
        Value::List(items) => {
            for (i, item) in items.iter().enumerate() {
                if let Value::Message(m) = item {
                    let base = path.len();
                    // Write straight into the buffer; avoids a throwaway String.
                    let _ = write!(path, "[{i}]");
                    collect_missing_required_fields(m, path, missing);
                    path.truncate(base);
                }
            }
        }
        Value::Map(entries) => {
            for (key, val) in entries {
                if let Value::Message(m) = val {
                    let base = path.len();
                    path.push('[');
                    append_map_key(path, key);
                    path.push(']');
                    collect_missing_required_fields(m, path, missing);
                    path.truncate(base);
                }
            }
        }
        _ => {}
    }
}

/// Append a protobuf map key to a missing-field path, in place (no allocation
/// for the common string-key case).
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

/// Encode JSON record to protobuf bytes. Unknown keys are ignored.
///
/// Values follow protobuf's JSON mapping; a few column types need shaping:
/// - DATE/TIMESTAMP/TIMESTAMP_NTZ: integers (days / micros since epoch), not strings.
/// - BINARY: base64-encoded string, not a JSON array of bytes.
/// - DECIMAL: string (e.g. "123.45"), to preserve precision/scale.
/// - VARIANT: a JSON-encoded string (a string whose contents are the variant's JSON).
/// - ARRAY/MAP/STRUCT: JSON array / object / object respectively.
/// - LONG/BIGINT above 2^53: pass as a JSON string, else the value loses
///   precision as a JSON number.
///
/// Presence is enforced for every non-nullable scalar and struct field (proto2
/// `required`), at any depth: a record that omits one fails. The check descends
/// into nested STRUCTs, each element of an ARRAY<STRUCT>, and MAP values, so a
/// required field missing inside a struct is rejected locally rather than by the
/// server after a round-trip. Non-nullable ARRAY/MAP columns map to `repeated`,
/// which has no presence, so an omitted one encodes as empty rather than failing.
/// Returns true on success; caller must free buffer with `zerobus_free_proto_bytes`.
/// On failure `*out_data` is set to NULL and `*out_len` to 0.
#[no_mangle]
pub extern "C" fn zerobus_proto_schema_encode_json(
    schema: *const CZerobusProtoSchema,
    record_json: *const c_char,
    out_data: *mut *mut u8,
    out_len: *mut usize,
    result: *mut CResult,
) -> bool {
    if out_data.is_null() || out_len.is_null() {
        write_error_result(result, "Output pointers are null", false);
        return false;
    }
    // Initialize outputs up front so every failure path leaves them null/0 — a
    // caller that frees on failure then hits a no-op rather than a stale or
    // uninitialized pointer.
    unsafe {
        *out_data = ptr::null_mut();
        *out_len = 0;
    }

    // SAFETY: caller upholds the handle contract — a valid handle not freed for
    // the duration of this call.
    let schema_ref = match unsafe { proto_schema_ref(schema) } {
        Ok(s) => s,
        Err(msg) => {
            write_error_result(result, msg, false);
            return false;
        }
    };
    let json = match unsafe { c_str_to_string(record_json) } {
        Ok(s) => s,
        Err(e) => {
            write_error_result(result, e, false);
            return false;
        }
    };

    let mut deserializer = serde_json::Deserializer::from_str(&json);
    // Records carry extra non-column fields; ignore them rather than erroring.
    let options = DeserializeOptions::new().deny_unknown_fields(false);
    let message = match DynamicMessage::deserialize_with_options(
        schema_ref.message.clone(),
        &mut deserializer,
        &options,
    ) {
        Ok(m) => m,
        Err(e) => {
            write_error_result(result, &format!("failed to encode record: {e}"), false);
            return false;
        }
    };
    if let Err(e) = deserializer.end() {
        write_error_result(
            result,
            &format!("unexpected trailing content in record JSON: {e}"),
            false,
        );
        return false;
    }

    // Non-nullable scalar/struct columns are proto2 `required`, but prost-reflect
    // doesn't enforce presence on encode — reject a missing one here rather than
    // emit wire bytes the server rejects after a round-trip. The walk descends
    // into nested structs, array elements, and map values so a required field
    // missing at any depth is caught locally. (ARRAY/MAP themselves are
    // `repeated`, which has no presence.)
    let mut missing: Vec<String> = Vec::new();
    let mut path = String::new();
    collect_missing_required_fields(&message, &mut path, &mut missing);
    if !missing.is_empty() {
        write_error_result(
            result,
            &format!("record missing required field(s): {}", missing.join(", ")),
            false,
        );
        return false;
    }

    let bytes = message.encode_to_vec();
    let len = bytes.len();
    // into_boxed_slice() shrinks capacity to len so the matching
    // zerobus_free_proto_bytes reconstruction is sound.
    let data_ptr = Box::into_raw(bytes.into_boxed_slice()) as *mut u8;
    unsafe {
        *out_data = data_ptr;
        *out_len = len;
    }
    write_success_result(result);
    true
}

/// Free a buffer returned by `zerobus_proto_schema_encode_json`.
#[no_mangle]
pub extern "C" fn zerobus_free_proto_bytes(data: *mut u8, len: usize) {
    // An all-default record encodes to zero bytes: a non-null, zero-length
    // boxed slice. Reconstruct on `!data.is_null()` alone; gating on `len > 0`
    // would leak it.
    if !data.is_null() {
        unsafe {
            // data came from Box::into_raw(bytes.into_boxed_slice()), so
            // capacity == len and this reconstruction is sound (len 0 included).
            let _ = Box::from_raw(std::ptr::slice_from_raw_parts_mut(data, len));
        }
    }
}

/// Free a handle from `zerobus_proto_schema_from_uc_json`. Call exactly once,
/// after every other call using this handle has returned. The handle may be
/// shared by concurrent readers (`descriptor_bytes`, `encode_json`), but `free`
/// must not race any of them.
#[no_mangle]
pub extern "C" fn zerobus_proto_schema_free(schema: *mut CZerobusProtoSchema) {
    if !schema.is_null() {
        unsafe {
            // Reclaim the Box handed to C by from_uc_json and drop it.
            let _ = Box::from_raw(schema as *mut ProtoSchema);
        }
    }
}
