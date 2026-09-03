# Example binaries. They link the SDK's public API only.
add_executable(zerobus_json_example json/main.c)
target_link_libraries(zerobus_json_example PRIVATE zerobus_c)
target_compile_options(zerobus_json_example PRIVATE ${ZEROBUS_WARN_FLAGS})
