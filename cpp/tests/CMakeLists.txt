# Tests for the Zerobus C++ SDK.
#
# Dependency-free by design: the SDK build pulls in no test framework, so each
# test is a plain executable that returns non-zero on failure and is registered
# with CTest. Run via `make test` (ctest) from the cpp/ directory.

# Tests reach into the private src/detail headers, so each links the library
# target (which also pulls in the FFI archive, e.g. zerobus_get_default_config)
# and adds src/ to its include path.
function(zerobus_add_test name)
  add_executable(${name} ${name}.cpp)
  target_link_libraries(${name} PRIVATE zerobus::zerobus)
  target_include_directories(${name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)
  if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(${name} PRIVATE -Wall -Wextra)
  endif()
  add_test(NAME ${name} COMMAND ${name})
endfunction()

zerobus_add_test(config_defaults_test)
zerobus_add_test(arrow_config_defaults_test)
zerobus_add_test(arrow_config_convert_test)
zerobus_add_test(embedded_nul_test)
zerobus_add_test(ack_callback_test)
zerobus_add_test(headers_provider_test)
zerobus_add_test(error_test)
zerobus_add_test(record_test)
zerobus_add_test(version_test)
# Inject the CMake project version so the test can assert version() against
# project(... VERSION ...), the release source of truth.
target_compile_definitions(version_test
    PRIVATE ZEROBUS_CMAKE_PROJECT_VERSION="${PROJECT_VERSION}")
zerobus_add_test(proto_schema_test)
zerobus_add_test(headers_callback_test)
zerobus_add_test(sdk_test)
zerobus_add_test(integration_test)

# Concurrency test spawns std::threads, so it needs the platform thread library.
zerobus_add_test(concurrency_test)
find_package(Threads REQUIRED)
target_link_libraries(concurrency_test PRIVATE Threads::Threads)
