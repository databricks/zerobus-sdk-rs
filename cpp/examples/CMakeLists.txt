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

# ---------------------------------------------------------------------------
# Advanced examples (ack callback, recovery, custom headers provider)
# ---------------------------------------------------------------------------
add_executable(advanced_ack_callback advanced/ack_callback.cpp)
target_link_libraries(advanced_ack_callback PRIVATE zerobus::zerobus)

add_executable(advanced_recovery advanced/recovery.cpp)
target_link_libraries(advanced_recovery PRIVATE zerobus::zerobus)

add_executable(advanced_headers_provider advanced/headers_provider.cpp)
target_link_libraries(advanced_headers_provider PRIVATE zerobus::zerobus)

if(DEFINED ZEROBUS_FFI_BUILD_TARGET)
  add_dependencies(json_single ${ZEROBUS_FFI_BUILD_TARGET})
  add_dependencies(json_batch ${ZEROBUS_FFI_BUILD_TARGET})
  add_dependencies(proto_single ${ZEROBUS_FFI_BUILD_TARGET})
  add_dependencies(proto_batch ${ZEROBUS_FFI_BUILD_TARGET})
  add_dependencies(advanced_ack_callback ${ZEROBUS_FFI_BUILD_TARGET})
  add_dependencies(advanced_recovery ${ZEROBUS_FFI_BUILD_TARGET})
  add_dependencies(advanced_headers_provider ${ZEROBUS_FFI_BUILD_TARGET})
endif()

# ---------------------------------------------------------------------------
# Arrow Flight examples (arrow_ingest, arrow_compression) — Beta
# ---------------------------------------------------------------------------
# Use the Apache Arrow C++ library to build RecordBatches and serialize them to
# Arrow IPC bytes for ArrowStream::ingest_batch. Skipped if Arrow is not
# installed — a machine without libarrow still gets every other example.
find_package(Arrow QUIET)
if(Arrow_FOUND)
  add_executable(arrow_ingest arrow/arrow_ingest.cpp)
  target_link_libraries(arrow_ingest PRIVATE zerobus::zerobus Arrow::arrow_shared)

  add_executable(arrow_compression arrow/compression.cpp)
  target_link_libraries(arrow_compression PRIVATE zerobus::zerobus Arrow::arrow_shared)

  if(DEFINED ZEROBUS_FFI_BUILD_TARGET)
    add_dependencies(arrow_ingest ${ZEROBUS_FFI_BUILD_TARGET})
    add_dependencies(arrow_compression ${ZEROBUS_FFI_BUILD_TARGET})
  endif()
else()
  message(STATUS
      "Zerobus examples: Apache Arrow C++ not found; skipping arrow_ingest and "
      "arrow_compression examples (install libarrow-dev to enable). The JSON, "
      "proto, and advanced examples still build.")
endif()
