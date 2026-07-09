# Example binaries for the Zerobus C++ SDK.
#
# Built when ZEROBUS_BUILD_EXAMPLES is on (the default for a top-level build).
# Each example links the zerobus::zerobus target, which transitively pulls in
# the Rust C FFI archive and the platform system libraries it needs.
#
# The examples read their secrets (OAuth client id/secret, and for proto_ingest
# the Unity Catalog table metadata) from environment variables; all other
# connection info lives in demo_config.hpp. See each source file's header for
# the variables it expects and a sample invocation.

add_executable(json_quickstart json_quickstart.cpp)
target_link_libraries(json_quickstart PRIVATE zerobus::zerobus)

add_executable(proto_ingest proto_ingest.cpp)
target_link_libraries(proto_ingest PRIVATE zerobus::zerobus)

# ClickBench (wide-schema) throughput benchmark. Uses dynamic proto
# (ProtoSchema::from_uc_json), so it needs no protobuf toolchain — just the SDK.
# Its own target so it builds via `cmake --build build --target
# clickbench_benchmark` independent of the ack-using examples.
add_executable(clickbench_benchmark clickbench_benchmark.cpp)
target_link_libraries(clickbench_benchmark PRIVATE zerobus::zerobus)

if(DEFINED ZEROBUS_FFI_BUILD_TARGET)
  add_dependencies(json_quickstart ${ZEROBUS_FFI_BUILD_TARGET})
  add_dependencies(proto_ingest ${ZEROBUS_FFI_BUILD_TARGET})
  add_dependencies(clickbench_benchmark ${ZEROBUS_FFI_BUILD_TARGET})
endif()

# ---------------------------------------------------------------------------
# Static-proto example (proto_static)
# ---------------------------------------------------------------------------
# Unlike proto_ingest (which fetches the descriptor from Unity Catalog at
# runtime), this example compiles a checked-in air_quality.proto and builds the
# descriptor from the generated C++ class. That needs the protobuf compiler and
# runtime, so the example is built only when both are found — a machine without
# them still gets json_quickstart and proto_ingest.
#
# Pin protoc to the one that sits next to the runtime headers if a newer protoc
# (e.g. a hand-installed /usr/local/bin/protoc) is also on PATH: generated code
# must match the libprotobuf version we link, or it will not compile. Callers
# can override with -DProtobuf_PROTOC_EXECUTABLE=<path>.
if(NOT DEFINED Protobuf_PROTOC_EXECUTABLE AND EXISTS "/usr/bin/protoc")
  set(Protobuf_PROTOC_EXECUTABLE "/usr/bin/protoc")
endif()
find_package(Protobuf QUIET)

if(Protobuf_FOUND)
  set(_proto_src "${CMAKE_CURRENT_SOURCE_DIR}/proto/air_quality.proto")
  set(_proto_gen_dir "${CMAKE_CURRENT_BINARY_DIR}/gen")
  set(_proto_cc "${_proto_gen_dir}/air_quality.pb.cc")
  set(_proto_h "${_proto_gen_dir}/air_quality.pb.h")

  file(MAKE_DIRECTORY "${_proto_gen_dir}")

  # Generate air_quality.pb.{cc,h} from the .proto at build time. Pin to the
  # protoc found by find_package so the generated code matches the runtime we
  # link (mismatched protoc/runtime versions do not compile).
  add_custom_command(
      OUTPUT "${_proto_cc}" "${_proto_h}"
      COMMAND ${Protobuf_PROTOC_EXECUTABLE}
              --proto_path "${CMAKE_CURRENT_SOURCE_DIR}/proto"
              --cpp_out "${_proto_gen_dir}"
              "${_proto_src}"
      DEPENDS "${_proto_src}"
      COMMENT "Generating air_quality.pb.{cc,h} with protoc"
      VERBATIM)

  add_executable(proto_static proto_static.cpp "${_proto_cc}")
  target_link_libraries(proto_static PRIVATE zerobus::zerobus protobuf::libprotobuf)
  # The generated header lands in the build tree; make it includable.
  target_include_directories(proto_static PRIVATE "${_proto_gen_dir}")

  # Throughput benchmark (its own target so it builds in isolation via
  # `cmake --build build --target zerobus_benchmark`, independent of the other
  # examples). Reuses the protoc-generated air_quality.pb.cc built above.
  add_executable(zerobus_benchmark benchmark.cpp "${_proto_cc}")
  target_link_libraries(zerobus_benchmark PRIVATE zerobus::zerobus protobuf::libprotobuf)
  target_include_directories(zerobus_benchmark PRIVATE "${_proto_gen_dir}")

  if(DEFINED ZEROBUS_FFI_BUILD_TARGET)
    add_dependencies(proto_static ${ZEROBUS_FFI_BUILD_TARGET})
    add_dependencies(zerobus_benchmark ${ZEROBUS_FFI_BUILD_TARGET})
  endif()
else()
  message(STATUS
      "Zerobus examples: protobuf not found; skipping proto_static example "
      "(json_quickstart and proto_ingest still build).")
endif()

# ---------------------------------------------------------------------------
# Arrow Flight example (arrow_ingest)
# ---------------------------------------------------------------------------
# Uses the Apache Arrow C++ library to build RecordBatches and serialize them
# to Arrow IPC bytes for ArrowStream::ingest_batch. Skipped if Arrow is not
# installed — a machine without libarrow still gets every other example.
find_package(Arrow QUIET)
if(Arrow_FOUND)
  add_executable(arrow_ingest arrow_ingest.cpp)
  target_link_libraries(arrow_ingest PRIVATE zerobus::zerobus Arrow::arrow_shared)
  if(DEFINED ZEROBUS_FFI_BUILD_TARGET)
    add_dependencies(arrow_ingest ${ZEROBUS_FFI_BUILD_TARGET})
  endif()
else()
  message(STATUS
      "Zerobus examples: Apache Arrow C++ not found; skipping arrow_ingest example "
      "(install libarrow-dev to enable).")
endif()
