# Example binaries for the Zerobus C++ SDK.
#
# Built when ZEROBUS_BUILD_EXAMPLES is on (the default for a top-level build).
# Each example links the zerobus::zerobus target, which transitively pulls in
# the Rust C FFI archive and the platform system libraries it needs.
#
# Every example reads its connection settings from the environment
# (ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME,
# DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET); the proto examples additionally
# read the Unity Catalog table metadata JSON (ZEROBUS_UC_TABLE_JSON). See each
# file's header for the variables it expects and a sample invocation.
#
# The JSON and proto examples need no protobuf toolchain: the proto examples use
# dynamic ProtoSchema::from_uc_json(), which builds the descriptor at runtime.
# Only the Arrow example has an external dependency (Apache Arrow C++), and it is
# skipped gracefully when Arrow is not installed.

# ---------------------------------------------------------------------------
# JSON examples
# ---------------------------------------------------------------------------
add_executable(json_single json/single.cpp)
target_link_libraries(json_single PRIVATE zerobus::zerobus)

add_executable(json_batch json/batch.cpp)
target_link_libraries(json_batch PRIVATE zerobus::zerobus)

# ---------------------------------------------------------------------------
# Protocol Buffers examples (dynamic schema — no protoc required)
# ---------------------------------------------------------------------------
add_executable(proto_single proto/single.cpp)
target_link_libraries(proto_single PRIVATE zerobus::zerobus)

add_executable(proto_batch proto/batch.cpp)
target_link_libraries(proto_batch PRIVATE zerobus::zerobus)

if(DEFINED ZEROBUS_FFI_BUILD_TARGET)
  add_dependencies(json_single ${ZEROBUS_FFI_BUILD_TARGET})
  add_dependencies(json_batch ${ZEROBUS_FFI_BUILD_TARGET})
  add_dependencies(proto_single ${ZEROBUS_FFI_BUILD_TARGET})
  add_dependencies(proto_batch ${ZEROBUS_FFI_BUILD_TARGET})
endif()

# ---------------------------------------------------------------------------
# Arrow Flight example (arrow_ingest) — Beta
# ---------------------------------------------------------------------------
# Uses the Apache Arrow C++ library to build RecordBatches and serialize them to
# Arrow IPC bytes for ArrowStream::ingest_batch. Skipped if Arrow is not
# installed — a machine without libarrow still gets every other example.
find_package(Arrow QUIET)
if(Arrow_FOUND)
  add_executable(arrow_ingest arrow/arrow_ingest.cpp)
  target_link_libraries(arrow_ingest PRIVATE zerobus::zerobus Arrow::arrow_shared)
  if(DEFINED ZEROBUS_FFI_BUILD_TARGET)
    add_dependencies(arrow_ingest ${ZEROBUS_FFI_BUILD_TARGET})
  endif()
else()
  message(STATUS
      "Zerobus examples: Apache Arrow C++ not found; skipping arrow_ingest "
      "example (install libarrow-dev to enable). The JSON and proto examples "
      "still build.")
endif()
