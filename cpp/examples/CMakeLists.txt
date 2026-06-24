# Each example is a standalone executable linking the SDK. They read connection
# settings from environment variables at runtime (see each file's header).
set(ZEROBUS_EXAMPLES
    json_single
    json_batch
    proto_uc_schema
    headers_provider
    arrow_stream)

foreach(example ${ZEROBUS_EXAMPLES})
  add_executable(${example} ${example}.cpp)
  target_link_libraries(${example} PRIVATE zerobus::zerobus)
endforeach()
